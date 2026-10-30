package ratelimit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/developer/gopulse/internal/ratelimit"
)

func TestTokenBucket_BurstLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	burst := 5
	tb := ratelimit.NewTokenBucket(ctx, 100*time.Millisecond, burst)
	defer tb.Stop()

	// Initial burst must allow exactly 5 tokens
	allowed := 0
	for i := 0; i < 10; i++ {
		if tb.Allow() {
			allowed++
		}
	}

	if allowed != burst {
		t.Fatalf("expected initial burst of %d, got %d", burst, allowed)
	}

	// Immediate next call should be rejected (bucket empty)
	if tb.Allow() {
		t.Fatal("expected Allow() to return false when bucket is empty")
	}

	// Wait for 1 token to refill
	time.Sleep(120 * time.Millisecond)
	if !tb.Allow() {
		t.Fatal("expected token to be available after refill duration")
	}
}

func TestTokenBucket_ConcurrentAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tb := ratelimit.NewTokenBucket(ctx, 10*time.Millisecond, 20)
	defer tb.Stop()

	var wg sync.WaitGroup
	const goroutines = 25

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = tb.Allow()
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}
