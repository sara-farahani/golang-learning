package metrics

import (
	"sync/atomic"
	"time"
)

// TelemetryCollector records throughput and error rates without mutex contention.
type TelemetryCollector struct {
	processedJobs  uint64
	failedJobs     uint64
	dlqJobs        uint64
	totalLatencyNs uint64
	startTime      time.Time
}

// NewTelemetryCollector initializes the atomic metrics engine.
func NewTelemetryCollector() *TelemetryCollector {
	return &TelemetryCollector{
		startTime: time.Now(),
	}
}

// IncProcessed atomically increments processed count.
func (m *TelemetryCollector) IncProcessed(latency time.Duration) {
	atomic.AddUint64(&m.processedJobs, 1)
	atomic.AddUint64(&m.totalLatencyNs, uint64(latency.Nanoseconds()))
}

// IncFailed atomically increments failed count.
func (m *TelemetryCollector) IncFailed() {
	atomic.AddUint64(&m.failedJobs, 1)
}

// IncDLQ atomically increments dead-letter queue count.
func (m *TelemetryCollector) IncDLQ() {
	atomic.AddUint64(&m.dlqJobs, 1)
}

// Snapshot provides an instantaneous, thread-safe read of performance metrics.
type MetricsSnapshot struct {
	Processed       uint64        `json:"processed"`
	Failed          uint64        `json:"failed"`
	DLQ             uint64        `json:"dlq"`
	AvgLatency      time.Duration `json:"avg_latency"`
	ThroughputPerSec float64       `json:"throughput_per_sec"`
}

func (m *TelemetryCollector) Snapshot() MetricsSnapshot {
	processed := atomic.LoadUint64(&m.processedJobs)
	failed := atomic.LoadUint64(&m.failedJobs)
	dlq := atomic.LoadUint64(&m.dlqJobs)
	totalNs := atomic.LoadUint64(&m.totalLatencyNs)

	var avgLatency time.Duration
	if processed > 0 {
		avgLatency = time.Duration(totalNs / processed)
	}

	elapsed := time.Since(m.startTime).Seconds()
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(processed) / elapsed
	}

	return MetricsSnapshot{
		Processed:        processed,
		Failed:           failed,
		DLQ:              dlq,
		AvgLatency:       avgLatency,
		ThroughputPerSec: throughput,
	}
}
