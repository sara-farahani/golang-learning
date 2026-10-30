package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrRateLimitExceeded = errors.New("rate limit quota exceeded")

// TokenBucket regulates throughput to prevent downstream resource exhaustion.
type TokenBucket struct {
	rate       time.Duration     // Token refill frequency
	capacity   int               // Maximum burst capacity
	tokens     chan struct{}     // Channel acts as token holder
	ticker     *time.Ticker
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
}

// NewTokenBucket creates a token bucket with specified refill rate and burst capacity.
func NewTokenBucket(parentCtx context.Context, refillInterval time.Duration, burstCapacity int) *TokenBucket {
	ctx, cancel := context.WithCancel(parentCtx)
	tb := &TokenBucket{
		rate:     refillInterval,
		capacity: burstCapacity,
		tokens:   make(chan struct{}, burstCapacity),
		ticker:   time.NewTicker(refillInterval),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Pre-fill bucket with initial tokens
	for i := 0; i < burstCapacity; i++ {
		tb.tokens <- struct{}{}
	}

	// Background goroutine replenishes tokens periodically
	go tb.refillLoop()

	return tb
}

func (tb *TokenBucket) refillLoop() {
	defer tb.ticker.Stop()
	for {
		select {
		case <-tb.ctx.Done():
			return
		case <-tb.ticker.C:
			// Non-blocking send: if bucket is full, drop token
			select {
			case tb.tokens <- struct{}{}:
			default:
			}
		}
	}
}

// Allow checks if a token is immediately available without blocking.
func (tb *TokenBucket) Allow() bool {
	select {
	case <-tb.tokens:
		return true
	default:
		return false
	}
}

// Wait blocks until a token becomes available or context cancels.
func (tb *TokenBucket) Wait(ctx context.Context) error {
	select {
	case <-tb.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-tb.ctx.Done():
		return errors.New("rate limiter stopped")
	}
}

// Stop shuts down the refill ticker and cleans up resources.
func (tb *TokenBucket) Stop() {
	tb.cancel()
}
