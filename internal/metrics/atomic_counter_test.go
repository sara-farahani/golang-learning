package metrics_test

import (
	"sync"
	"testing"
	"time"

	"github.com/developer/gopulse/internal/metrics"
)

func TestTelemetryCollector_AtomicIncrements(t *testing.T) {
	tc := metrics.NewTelemetryCollector()

	var wg sync.WaitGroup
	const goroutines = 50
	const iterations = 100

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				tc.IncProcessed(5 * time.Millisecond)
				if j%10 == 0 {
					tc.IncFailed()
					tc.IncDLQ()
				}
			}
		}()
	}

	wg.Wait()

	snap := tc.Snapshot()
	expectedProcessed := uint64(goroutines * iterations)
	if snap.Processed != expectedProcessed {
		t.Fatalf("expected processed %d, got %d", expectedProcessed, snap.Processed)
	}

	expectedFailed := uint64(goroutines * (iterations / 10))
	if snap.Failed != expectedFailed {
		t.Fatalf("expected failed %d, got %d", expectedFailed, snap.Failed)
	}

	if snap.AvgLatency <= 0 {
		t.Fatal("expected positive average latency")
	}
}
