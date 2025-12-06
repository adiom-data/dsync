package s3

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds our limits
type Config struct {
	Bucket         string
	MaxFileSize    int64 // X MB in bytes
	MaxTotalMemory int64 // Y MB in bytes
}

// BatchProcessor manages the ingestion and buffering
type BatchProcessor struct {
	s3Client *s3.Client
	config   Config

	// State
	mu           sync.Mutex
	cond         *sync.Cond
	buffers      map[string]*bytes.Buffer // Namespace -> Data
	currentUsage int64                    // Total bytes in RAM (Buffered + In-Flight)

	// Lifecycle
	wg       sync.WaitGroup // To wait for pending uploads on shutdown
	shutdown bool
}

func NewBatchProcessor(client *s3.Client, cfg Config) *BatchProcessor {
	bp := &BatchProcessor{
		s3Client: client,
		config:   cfg,
		buffers:  make(map[string]*bytes.Buffer),
	}
	// Initialize the condition variable with the mutex
	bp.cond = sync.NewCond(&bp.mu)
	return bp
}

// Add safely handles multi-threaded ingestion
func (bp *BatchProcessor) Add(namespace string, data []byte) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	dataLen := int64(len(data))

	// 1. BACKPRESSURE MECHANISM
	// If adding this data exceeds global memory, we must wait (block)
	// until space is freed by finished uploads.
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
		bp.buffers[namespace] = new(bytes.Buffer)
	}
	bp.buffers[namespace].Write(data)

	// 4. Check Individual File Size Limit (X MB)
	if int64(bp.buffers[namespace].Len()) >= bp.config.MaxFileSize {
		bp.flushBuffer(namespace)
	}

	return nil
}

// evictLargestBuffer finds the namespace using the most RAM and flushes it
// Caller must hold the lock.
func (bp *BatchProcessor) evictLargestBuffer() {
	var largestNS string
	var largestSize int

	for ns, buf := range bp.buffers {
		if buf.Len() > largestSize {
			largestSize = buf.Len()
			largestNS = ns
		}
	}

	if largestNS != "" {
		// log.Printf("Memory Pressure! Evicting %s (%d bytes)", largestNS, largestSize)
		bp.flushBuffer(largestNS)
	}
}

// flushBuffer moves data from the map to the async uploader.
// Caller must hold the lock.
func (bp *BatchProcessor) flushBuffer(namespace string) {
	buf, exists := bp.buffers[namespace]
	if !exists || buf.Len() == 0 {
		return
	}

	// Snapshot data for upload
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	payloadSize := int64(len(payload))

	// Clear the buffer entry from the map immediately so new writes can start a fresh buffer
	delete(bp.buffers, namespace)

	// NOTE: We do NOT decrease bp.currentUsage here.
	// The memory is still "alive" inside the payload slice, moving to the uploader.

	bp.wg.Add(1)

	// Launch Async Upload
	go func(ns string, data []byte, size int64) {
		defer bp.wg.Done()

		// Generate Key
		key := fmt.Sprintf("%s/%d.json", ns, time.Now().UnixNano())

		// Perform Upload
		_, err := bp.s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bp.config.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})

		if err != nil {
			log.Printf("Error uploading %s: %v", key, err)
			// In a real system, you might implement a retry mechanism or Dead Letter Queue here.
		}

		// MEMORY RECLAMATION
		bp.mu.Lock()
		bp.currentUsage -= size
		// Signal waiting threads that memory is now available
		bp.cond.Broadcast()
		bp.mu.Unlock()

	}(namespace, payload, payloadSize)
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
		bp.flushBuffer(ns)
	}
	bp.mu.Unlock()

	// Wait for all uploads to finish
	bp.wg.Wait()
}
