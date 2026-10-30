package dlq_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/developer/gopulse/internal/dlq"
	"github.com/developer/gopulse/internal/domain"
)

func TestDLQ_PushAndList(t *testing.T) {
	q := dlq.NewDLQ(10)

	if q.Count() != 0 {
		t.Fatalf("expected empty DLQ, got size %d", q.Count())
	}

	job := &domain.Job{
		ID:      "bad-job-1",
		Payload: "malformed",
	}

	q.Push(job)

	if q.Count() != 1 {
		t.Fatalf("expected size 1, got %d", q.Count())
	}

	list := q.List()
	if len(list) != 1 || list[0].ID != "bad-job-1" {
		t.Fatalf("expected job bad-job-1, got %v", list)
	}

	if list[0].Status != domain.StatusDLQ {
		t.Fatalf("expected StatusDLQ, got %s", list[0].Status)
	}
}

func TestDLQ_ConcurrentThreadSafety(t *testing.T) {
	const capacity = 200
	q := dlq.NewDLQ(capacity)

	var wg sync.WaitGroup
	const workers = 50

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 4; j++ {
				job := &domain.Job{
					ID:      fmt.Sprintf("dlq-w%d-j%d", workerID, j),
					Payload: "corrupt",
				}
				q.Push(job)
			}
		}(i)
	}

	wg.Wait()

	if q.Count() != workers*4 {
		t.Fatalf("expected DLQ count %d, got %d", workers*4, q.Count())
	}
}

func TestDLQ_CalculateBackoff(t *testing.T) {
	base := 10 * time.Millisecond
	max := 500 * time.Millisecond

	delay1 := dlq.CalculateBackoff(1, base, max)
	if delay1 < base {
		t.Fatalf("expected delay >= base delay, got %v", delay1)
	}

	delay5 := dlq.CalculateBackoff(5, base, max)
	if delay5 > max*2 { // Allows for jitter
		t.Fatalf("delay exceeded expected max threshold: %v", delay5)
	}
}
