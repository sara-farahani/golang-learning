package gopulse_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/developer/gopulse/internal/dlq"
	"github.com/developer/gopulse/internal/domain"
	"github.com/developer/gopulse/internal/metrics"
	"github.com/developer/gopulse/internal/pipeline"
	"github.com/developer/gopulse/internal/ratelimit"
)

type testE2EProcessor struct {
	dlq     *dlq.DeadLetterQueue
	metrics *metrics.TelemetryCollector
}

func (p *testE2EProcessor) Process(job *domain.Job) (*domain.Result, error) {
	if job.Payload == "POISON_PILL" {
		p.dlq.Push(job)
		p.metrics.IncFailed()
		p.metrics.IncDLQ()
		return nil, domain.ErrPoisonPill
	}
	p.metrics.IncProcessed(10 * time.Millisecond)
	return &domain.Result{
		JobID:  job.ID,
		Output: fmt.Sprintf("processed:%s", job.ID),
	}, nil
}

// TestEndToEndPipeline validates the full integration of rate limiter, worker pool,
// DLQ, and atomic telemetry without race conditions under parallel load.
// Run with: go test -v -race ./...
func TestEndToEndPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	telemetry := metrics.NewTelemetryCollector()
	queue := dlq.NewDLQ(100)
	proc := &testE2EProcessor{dlq: queue, metrics: telemetry}

	const workers = 4
	const queueCap = 50
	pool := pipeline.NewWorkerPool(ctx, workers, queueCap)
	pool.Start(proc)

	limiter := ratelimit.NewTokenBucket(ctx, 10*time.Millisecond, 20)
	defer limiter.Stop()

	// Drain results channel in background
	resultsReceived := 0
	doneCh := make(chan struct{})
	go func() {
		for range pool.Results() {
			resultsReceived++
		}
		close(doneCh)
	}()

	// Enqueue jobs
	totalJobs := 30
	for i := 1; i <= totalJobs; i++ {
		payload := fmt.Sprintf("event-%d", i)
		if i%6 == 0 {
			payload = "POISON_PILL"
		}
		job := &domain.Job{
			ID:       fmt.Sprintf("e2e-job-%d", i),
			Payload:  payload,
			Priority: domain.PriorityMedium,
		}

		for !limiter.Allow() {
			time.Sleep(2 * time.Millisecond)
		}
		for !pool.Enqueue(job) {
			time.Sleep(1 * time.Millisecond)
		}
	}

	// Trigger graceful drain
	pool.Stop()
	<-doneCh

	snap := telemetry.Snapshot()
	if snap.Processed == 0 && snap.Failed == 0 {
		t.Fatal("expected jobs to be processed by pipeline")
	}

	if queue.Count() == 0 {
		t.Fatal("expected poison pill jobs to be recorded in DLQ")
	}

	t.Logf("✅ End-to-End Pipeline Succeeded: Processed=%d Failed=%d DLQ=%d",
		snap.Processed, snap.Failed, snap.DLQ)
}
