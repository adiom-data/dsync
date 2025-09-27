package dsync

import (
	"context"
	"sync"
	"time"

	"github.com/adiom-data/dsync/metrics"
	runnerLocal "github.com/adiom-data/dsync/runners/local"
)

func EmitProgressMetrics(ctx context.Context, r *runnerLocal.RunnerLocal, mut *sync.Mutex) error {
	ticker := time.NewTicker(time.Second)
	prefix, client := metrics.PrefixAndClient()
	state := r.GetRunnerProgress().SyncState

	onTick := func() {
		mut.Lock()
		r.UpdateRunnerProgress()
		p := r.GetRunnerProgress()
		if state != p.SyncState {
			client.SimpleEvent("state-change", p.SyncState)
			state = p.SyncState
		}
		client.Gauge(prefix+"state", 1, []string{"state:" + state}, 1)
		client.Gauge(prefix+"docs_synced_total", float64(p.NumDocsSynced), nil, 1)
		client.Gauge(prefix+"global_tasks_total", float64(p.TasksTotal), nil, 1)
		client.Gauge(prefix+"lag", float64(p.Lag), nil, 1)
		client.Gauge(prefix+"throughput", float64(p.Throughput), nil, 1)
		client.Gauge(prefix+"change_stream_events_count", float64(p.ChangeStreamEvents), nil, 1)
		client.Gauge(prefix+"deletes_caught", float64(p.DeletesCaught), nil, 1)
		client.Gauge(prefix+"total_progress", float64(percentCompleteTotal(p)), nil, 1)
		client.Gauge(prefix+"elapsed", float64(time.Since(p.StartTime).Round(time.Second)), nil, 1)
		if !p.ChangeStreamLastTime.IsZero() {
			client.Gauge(prefix+"change_stream_lag_seconds", float64(time.Since(p.ChangeStreamLastTime).Round(time.Second)), nil, 1)
		}
		for ns, s := range p.NsProgressMap {
			tags := []string{"namespace:" + ns.Db + "." + ns.Col}
			client.Gauge(prefix+"estimated_doc_count", float64(s.EstimatedDocCount), tags, 1)
			client.Gauge(prefix+"docs_copied", float64(s.DocsCopied), tags, 1)
			client.Gauge(prefix+"tasks_total", float64(len(s.Tasks)), tags, 1)
			client.Gauge(prefix+"tasks_started", float64(s.TasksStarted), tags, 1)
			client.Gauge(prefix+"tasks_completed", float64(s.TasksCompleted), tags, 1)
		}
		mut.Unlock()
	}

	for {
		select {
		case <-ctx.Done():
			onTick()
			return nil
		case <-ticker.C:
			onTick()
		}
	}
}
