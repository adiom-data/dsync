package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Config holds our limits
type Config struct {
	Bucket         string
	Prefix         string
	MaxFileSize    int64 // X MB in bytes
	MaxTotalMemory int64 // Y MB in bytes
	PrettyJSON     bool
}

type bufferInfo struct {
	buffer    *bytes.Buffer
	docCount  int
	namespace string
}

// BatchProcessor manages the ingestion and buffering
type BatchProcessor struct {
	s3Client *s3.Client
	config   Config

	// State
	mu           sync.Mutex
	cond         *sync.Cond
	buffers      map[string]*bufferInfo // Namespace -> buffer
	currentUsage int64                  // Total bytes in RAM (Buffered + In-Flight)

	// Lifecycle
	wg       sync.WaitGroup // To wait for pending uploads on shutdown
	shutdown bool

	// Metadata
	metadataMutex sync.Mutex
}

func NewBatchProcessor(client *s3.Client, cfg Config) *BatchProcessor {
	bp := &BatchProcessor{
		s3Client: client,
		config:   cfg,
		buffers:  make(map[string]*bufferInfo),
	}
	// Initialize the condition variable with the mutex
	bp.cond = sync.NewCond(&bp.mu)
	return bp
}

// Add safely handles multi-threaded ingestion
func (bp *BatchProcessor) Add(namespace string, data [][]byte) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	dataLen := int64(0)
	for _, d := range data {
		dataLen += int64(len(d))
	}

	// 1. BACKPRESSURE MECHANISM
	// If adding this data exceeds global memory, we must wait (block)
	// until space is freed by finished uploads.
	// We error in earnest if the data itself exceeds the max total memory.
	if dataLen > bp.config.MaxTotalMemory {
		return fmt.Errorf("data size %d exceeds max total memory %d", dataLen, bp.config.MaxTotalMemory)
	}

	for (bp.currentUsage + dataLen) > bp.config.MaxTotalMemory {
		if bp.shutdown {
			return fmt.Errorf("processor is shutting down")
		}

		// Attempt to free space by force-flushing the largest buffer
		if len(bp.buffers) > 0 {
			bp.evictLargestBuffer()
		} else {
			// If we have no buffers (all memory is taken by in-flight uploads),
			// we have no choice but to wait.
			bp.cond.Wait()
		}
	}

	// 2. Add to Memory State
	bp.currentUsage += dataLen

	// 3. Append to Buffer
	if _, exists := bp.buffers[namespace]; !exists {
		bp.buffers[namespace] = &bufferInfo{
			buffer:    new(bytes.Buffer),
			namespace: namespace,
		}
		bp.buffers[namespace].buffer.WriteByte('[')
	}

	for i, doc := range data {
		if bp.buffers[namespace].docCount > 0 || i > 0 {
			bp.buffers[namespace].buffer.WriteByte(',')
		}
		if bp.config.PrettyJSON {
			var prettyBuf bytes.Buffer
			prettyBuf.Grow(len(doc) + len(doc)/10)
			if err := json.Indent(&prettyBuf, doc, "", "  "); err == nil {
				bp.buffers[namespace].buffer.WriteByte('\n')
				doc = prettyBuf.Bytes()
			} else {
				slog.Warn("Failed JSON indentation. Falling back to no-indent")
			}
		}
		bp.buffers[namespace].buffer.Write(doc)
		bp.buffers[namespace].docCount++
	}

	// 4. Check Individual File Size Limit (X MB)
	if int64(bp.buffers[namespace].buffer.Len()) >= bp.config.MaxFileSize {
		bp.asyncFlushBuffer(namespace)
	}

	return nil
}

// evictLargestBuffer finds the namespace using the most RAM and flushes it
// Caller must hold the lock.
func (bp *BatchProcessor) evictLargestBuffer() {
	var largestNS string
	var largestSize int

	for ns, bufInfo := range bp.buffers {
		if bufInfo.buffer.Len() > largestSize {
			largestSize = bufInfo.buffer.Len()
			largestNS = ns
		}
	}

	if largestNS != "" {
		slog.Debug("Memory Pressure! Evicting buffer",
			"namespace", largestNS,
			"size_bytes", largestSize,
		)
		bp.asyncFlushBuffer(largestNS)
	}
}

// asyncFlushBuffer moves data from the map to the async uploader.
// Caller must hold the lock.
func (bp *BatchProcessor) asyncFlushBuffer(namespace string) {
	bufInfo, exists := bp.buffers[namespace]
	if !exists || bufInfo.buffer.Len() == 0 {
		return
	}

	if bp.config.PrettyJSON {
		bufInfo.buffer.WriteByte('\n')
	}
	bufInfo.buffer.WriteByte(']')

	// Snapshot data for upload
	payload := make([]byte, bufInfo.buffer.Len())
	copy(payload, bufInfo.buffer.Bytes())
	payloadSize := int64(len(payload))
	docCount := bufInfo.docCount

	// Clear the buffer entry from the map immediately so new writes can start a fresh buffer
	delete(bp.buffers, namespace)

	// NOTE: We do NOT decrease bp.currentUsage here.
	// The memory is still "alive" inside the payload slice, moving to the uploader.

	bp.wg.Add(1)

	// Launch Async Upload
	go func(ns string, data []byte, size int64, numDocs int) {
		defer bp.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), MAX_S3_WRITE_TIMEOUT_SEC*time.Second)
		defer cancel()

		// Generate Key
		key := bp.objectKey(ns)

		// Perform Upload
		_, err := bp.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bp.config.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})

		if err != nil {
			slog.Error("Upload failed",
				"namespace", ns,
				"key", key,
				"error", err,
			)
			// In a real system, you might implement a retry mechanism or Dead Letter Queue here.
		} else {
			// Update metadata on successful upload
			if err := bp.updateMetadataWithRetries(ctx, ns, key, uint64(numDocs)); err != nil {
				slog.Error("Failed to update metadata after retries",
					"namespace", ns,
					"key", key,
					"error", err,
				)
			}
		}

		// MEMORY RECLAMATION
		bp.mu.Lock()
		bp.currentUsage -= size
		// Signal waiting threads that memory is now available
		bp.cond.Broadcast()
		bp.mu.Unlock()

	}(namespace, payload, payloadSize, docCount)
}

// Close ensures all remaining buffers are flushed
func (bp *BatchProcessor) Close() {
	bp.mu.Lock()
	bp.shutdown = true

	// Flush everything remaining
	names := make([]string, 0, len(bp.buffers))
	for ns := range bp.buffers {
		names = append(names, ns)
	}
	for _, ns := range names {
		bp.asyncFlushBuffer(ns)
	}
	bp.mu.Unlock()

	// Wait for all uploads to finish
	bp.wg.Wait()
	slog.Debug("Finished final S3 flush")
}

func (bp *BatchProcessor) objectKey(namespace string) string {
	nsPath := strings.ReplaceAll(strings.Trim(namespace, "/"), ".", "/")
	if nsPath == "" {
		nsPath = "default"
	}
	fileName := fmt.Sprintf("%d.json", time.Now().UnixNano())

	prefix := strings.Trim(bp.config.Prefix, "/")
	if prefix == "" {
		return path.Join(nsPath, fileName)
	}
	return path.Join(prefix, nsPath, fileName)
}

func (bp *BatchProcessor) metadataKey(namespace string) string {
	return MetadataKey(bp.config.Prefix, namespace)
}

func (bp *BatchProcessor) readMetadata(ctx context.Context, namespace string) (map[string]uint64, *string, error) {
	key := bp.metadataKey(namespace)
	getOut, err := bp.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bp.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NoSuchKey" {
				return nil, nil, nil
			}
		}
		return nil, nil, fmt.Errorf("get metadata object %s: %w", key, err)
	}
	defer getOut.Body.Close()

	var metadata map[string]uint64
	if err := json.NewDecoder(getOut.Body).Decode(&metadata); err != nil {
		return nil, nil, fmt.Errorf("decode metadata json from %s: %w", key, err)
	}
	return metadata, getOut.ETag, nil
}

func (bp *BatchProcessor) writeMetadata(ctx context.Context, namespace string, metadata map[string]uint64, etag *string) error {
	key := bp.metadataKey(namespace)
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata json: %w", err)
	}

	_, err = bp.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bp.config.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(metadataJSON),
		ContentType: aws.String("application/json"),
		IfMatch:     etag,
	})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && IsS3OptimisticLockFailedError(ae) {
			return &ErrS3OptimisticLockFailed{err: err}
		}
		return fmt.Errorf("put metadata object %s: %w", key, err)
	}
	return nil
}

func (bp *BatchProcessor) updateMetadataWithRetries(ctx context.Context, namespace string, fileKey string, recordCount uint64) error {
	var lastErr error
	for i := 0; i < S3_METADATA_UPDATES_RETRY; i++ { // 1 initial attempt + 2 retries
		err := bp.updateMetadata(ctx, namespace, fileKey, recordCount)
		if err == nil {
			return nil
		}

		lastErr = err

		// Only retry on precondition failed errors
		var preconditionErr *ErrS3OptimisticLockFailed
		if !errors.As(err, &preconditionErr) {
			return err // Not a retryable error
		}

		slog.Warn("Optimistic lock failed on metadata update, retrying...",
			"attempt", i+1,
			"namespace", namespace,
			"fileKey", fileKey,
			"error", err,
		)

		// Exponential backoff with jitter
		delay := time.Duration(50*(1<<i)) * time.Millisecond
		//nolint:gosec
		jitter := time.Duration(rand.N(25)) * time.Millisecond
		time.Sleep(delay + jitter)
	}
	return fmt.Errorf("failed to update metadata after 3 attempts: %w", lastErr)
}

func (bp *BatchProcessor) updateMetadata(ctx context.Context, namespace string, fileKey string, recordCount uint64) error {
	bp.metadataMutex.Lock()
	defer bp.metadataMutex.Unlock()

	metadata, etag, err := bp.readMetadata(ctx, namespace)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	if metadata == nil {
		metadata = make(map[string]uint64)
	}

	fileName := path.Base(fileKey)
	metadata[fileName] = recordCount

	if err := bp.writeMetadata(ctx, namespace, metadata, etag); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	return nil
}
