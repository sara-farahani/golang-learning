package pipeline_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/developer/gopulse/internal/domain"
	"github.com/developer/gopulse/internal/pipeline"
)

type MockProcessor struct {
	processedCount uint64
}

func (m *MockProcessor) Process(job *domain.Job) (*domain.Result, error) {
	atomic.AddUint64(&m.processedCount, 1)
	return &domain.Result{
		JobID:  job.ID,
		Output: "mock-processed",
	}, nil
}

// TestWorkerPool_Concurrency verifies all jobs are handled without deadlocks or race conditions.
// Run with: go test -v -race ./internal/pipeline/...
func TestWorkerPool_Concurrency(t *testing.T) {
	ctx := context.Background()
	workers := 4
	queueCap := 50
	pool := pipeline.NewWorkerPool(ctx, workers, queueCap)

	mockProc := &MockProcessor{}
	pool.Start(mockProc)

	const totalJobs = 100
	done := make(chan struct{})

	// Consume results in background
	go func() {
		count := 0
		for range pool.Results() {
			count++
			if count == totalJobs {
				close(done)
			}
		}
	}()

	// Enqueue jobs
	for i := 0; i < totalJobs; i++ {
		job := &domain.Job{
			ID:       fmt.Sprintf("test-%d", i),
			Payload:  "payload",
			Priority: domain.PriorityHigh,
		}
		for !pool.Enqueue(job) {
			time.Sleep(1 * time.Millisecond) // Yield backpressure
		}
	}

	// Stop pool to trigger drain
	pool.Stop()

	select {
	case <-done:
		if atomic.LoadUint64(&mockProc.processedCount) != totalJobs {
			t.Fatalf("expected %d jobs processed, got %d", totalJobs, mockProc.processedCount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker pool to drain")
	}
}

// BenchmarkWorkerPool measures multi-core throughput using b.RunParallel.
// Run with: go test -bench=. -benchmem ./...
func BenchmarkWorkerPool(b *testing.B) {
	ctx := context.Background()
	pool := pipeline.NewWorkerPool(ctx, 8, 500)
	mockProc := &MockProcessor{}
	pool.Start(mockProc)

	// Consume results to prevent channel block
	go func() {
		for range pool.Results() {
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			counter++
			job := &domain.Job{
				ID:      fmt.Sprintf("bench-%d", counter),
				Payload: "data",
			}
			pool.Enqueue(job)
		}
	})

	b.StopTimer()
	pool.Stop()
}
