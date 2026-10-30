package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/developer/gopulse/internal/domain"
)

// WorkerPool coordinates a fixed set of goroutines reading from a shared job channel.
type WorkerPool struct {
	workerCount int
	jobQueueCap int
	jobs        chan *domain.Job
	results     chan *domain.Result
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewWorkerPool instantiates a worker pool with bounded capacity.
func NewWorkerPool(parentCtx context.Context, workers int, queueCap int) *WorkerPool {
	ctx, cancel := context.WithCancel(parentCtx)
	return &WorkerPool{
		workerCount: workers,
		jobQueueCap: queueCap,
		jobs:        make(chan *domain.Job, queueCap),
		results:     make(chan *domain.Result, queueCap),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start spawns worker goroutines. Concurrency is strictly bounded by workerCount.
func (p *WorkerPool) Start(processor domain.Processor) {
	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(i, processor)
	}
}

// worker is the main loop executed inside each background goroutine.
func (p *WorkerPool) worker(id int, processor domain.Processor) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			// Context canceled: abort promptly to prevent goroutine leak
			return

		case job, ok := <-p.jobs:
			if !ok {
				// Job channel was closed and drained: worker exits normally
				return
			}

			start := time.Now()
			result, err := processor.Process(job)
			duration := time.Since(start)

			if err != nil {
				result = &domain.Result{
					JobID:       job.ID,
					WorkerID:    id,
					Duration:    duration,
					ProcessedAt: time.Now(),
					Err:         err,
				}
			} else {
				result.WorkerID = id
				result.Duration = duration
				result.ProcessedAt = time.Now()
			}

			// Non-blocking result push or context exit
			select {
			case p.results <- result:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

// Enqueue submits a job into the queue.
// Returns false if queue is full or context expired (backpressure).
func (p *WorkerPool) Enqueue(job *domain.Job) bool {
	select {
	case p.jobs <- job:
		return true
	case <-p.ctx.Done():
		return false
	default:
		// Queue full: caller must handle backpressure instead of blocking indefinitely
		return false
	}
}

// Stop gracefully shuts down the worker pool.
// 1. Closes jobs channel so workers drain queued work.
// 2. Waits for all workers to finish (wg.Wait()).
// 3. Closes results channel.
func (p *WorkerPool) Stop() {
	close(p.jobs)       // Sender closes channel
	p.wg.Wait()         // Wait for all workers to exit
	close(p.results)    // Close results downstream
	p.cancel()          // Cancel context
}

// Results returns a receive-only channel for downstream consumers.
func (p *WorkerPool) Results() <-chan *domain.Result {
	return p.results
}
