package dlq

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/developer/gopulse/internal/domain"
)

// DeadLetterQueue stores failed jobs that could not be processed after retries.
type DeadLetterQueue struct {
	mu      sync.RWMutex
	records []*domain.Job
	maxCap  int
}

// NewDLQ instantiates a bounded dead-letter store.
func NewDLQ(capacity int) *DeadLetterQueue {
	return &DeadLetterQueue{
		records: make([]*domain.Job, 0, capacity),
		maxCap:  capacity,
	}
}

// Push appends a failed job to the DLQ in a thread-safe manner.
func (d *DeadLetterQueue) Push(job *domain.Job) {
	d.mu.Lock()
	defer d.mu.Unlock()

	job.Status = domain.StatusDLQ
	if len(d.records) >= d.maxCap {
		// Drop oldest (FIFO ring buffer behavior)
		d.records = d.records[1:]
	}
	d.records = append(d.records, job)
}

// List returns a defensive copy of dead-letter records.
func (d *DeadLetterQueue) List() []*domain.Job {
	d.mu.RLock()
	defer d.mu.RUnlock()

	copied := make([]*domain.Job, len(d.records))
	copy(copied, d.records)
	return copied
}

// Count returns current number of dead-letter jobs.
func (d *DeadLetterQueue) Count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.records)
}

// CalculateBackoff computes exponential backoff with full jitter to avoid stampedes.
func CalculateBackoff(attempt int, baseDelay time.Duration, maxDelay time.Duration) time.Duration {
	if attempt <= 0 {
		return baseDelay
	}

	// 2^attempt * baseDelay
	multiplier := math.Pow(2, float64(attempt))
	delay := time.Duration(float64(baseDelay) * multiplier)

	if delay > maxDelay {
		delay = maxDelay
	}

	// Add random jitter between 0 and 50% of delay
	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	return delay + jitter
}

// ShouldRetry checks if a job can be retried or should immediately route to DLQ.
func ShouldRetry(job *domain.Job, err error) bool {
	if errors.Is(err, domain.ErrPoisonPill) {
		// Poison pill is unrecoverable: do not waste retry attempts
		return false
	}
	return job.Retries < job.MaxRetries
}
