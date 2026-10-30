package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/developer/gopulse/internal/domain"
)

// HeavyTransformStage simulates a computationally intensive stage.
// For example: cryptographic hashing, JSON parsing, or enrichment.
func HeavyTransformStage(ctx context.Context, in <-chan *domain.Job) <-chan *domain.Job {
	out := make(chan *domain.Job)
	go func() {
		defer close(out)
		for job := range in {
			// Simulate compute cost
			enrichedPayload := fmt.Sprintf("[%s::ENRICHED_AT_%d]", strings.ToUpper(job.Payload), time.Now().UnixNano())
			job.Payload = enrichedPayload

			select {
			case out <- job:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// FanIn combines multiple input channels into a single multiplexed channel.
// Essential pattern for consolidating parallel stage outputs.
func FanIn(ctx context.Context, channels ...<-chan *domain.Result) <-chan *domain.Result {
	out := make(chan *domain.Result)
	var wg sync.WaitGroup

	// multiplex copies results from an input channel to out until exhausted
	multiplex := func(c <-chan *domain.Result) {
		defer wg.Done()
		for res := range c {
			select {
			case out <- res:
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	// Separate closer goroutine to avoid blocking the main thread
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
