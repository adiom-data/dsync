package connectorCosmos

import (
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"
)

const (
	progressReportingIntervalSec = 10
)

type ReaderProgress struct {
	initialSyncDocs    atomic.Uint64
	changeStreamEvents uint64
	tasksTotal         uint64
	tasksCompleted     uint64
}

func (cc *CosmosConnector) printProgress(readerProgress *ReaderProgress) {
	ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
	defer ticker.Stop()
	startTime := time.Now()
	operations := uint64(0)
	for {
		select {
		case <-cc.flowCtx.Done():
			return
		case <-ticker.C:
			elapsedTime := time.Since(startTime).Seconds()
			operations_delta := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents - operations
			opsPerSec := math.Floor(float64(operations_delta) / elapsedTime)
			// Print reader progress
			slog.Info(fmt.Sprintf("Reader Progress: Initial Sync Docs - %d (%d/%d tasks completed), Change Stream Events - %d, Operations per Second - %.2f",
				readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted, readerProgress.tasksTotal, readerProgress.changeStreamEvents, opsPerSec))

			startTime = time.Now()
			operations = readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
		}
	}
}
