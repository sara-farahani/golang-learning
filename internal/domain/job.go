package domain

import (
	"errors"
	"fmt"
	"time"
)

// JobPriority defines the execution priority level of a task.
type JobPriority int

const (
	PriorityLow JobPriority = iota
	PriorityMedium
	PriorityHigh
	PriorityCritical
)

func (p JobPriority) String() string {
	switch p {
	case PriorityCritical:
		return "CRITICAL"
	case PriorityHigh:
		return "HIGH"
	case PriorityMedium:
		return "MEDIUM"
	default:
		return "LOW"
	}
}

// JobStatus tracks lifecycle state of an in-flight job.
type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusDLQ       JobStatus = "DEAD_LETTER"
)

// Sentinel domain errors for idiomatic checking via errors.Is()
var (
	ErrEmptyPayload   = errors.New("job payload cannot be empty")
	ErrJobTimeout     = errors.New("job execution exceeded allotted deadline")
	ErrPoisonPill     = errors.New("unrecoverable corrupted data encountered")
	ErrMaxRetriesDone = errors.New("maximum retry attempts exceeded")
)

// Job represents a single concurrent work unit in GoPulse.
type Job struct {
	ID         string        `json:"id"`
	Payload    string        `json:"payload"`
	Priority   JobPriority   `json:"priority"`
	Status     JobStatus     `json:"status"`
	Retries    int           `json:"retries"`
	MaxRetries int           `json:"max_retries"`
	Duration   time.Duration `json:"duration"`
	CreatedAt  time.Time     `json:"created_at"`
	Error      error         `json:"error,omitempty"`
}

// Result encapsulates the processed output of a Job.
type Result struct {
	JobID       string        `json:"job_id"`
	Output      string        `json:"output"`
	WorkerID    int           `json:"worker_id"`
	Duration    time.Duration `json:"duration"`
	ProcessedAt time.Time     `json:"processed_at"`
	Err         error         `json:"err,omitempty"`
}

// Processor defines an interface for executing work.
// Following Go convention: 'Accept interfaces, return structs'.
type Processor interface {
	Process(job *Job) (*Result, error)
}

// Validate checks business invariants before enqueuing to channels.
func (j *Job) Validate() error {
	if j.ID == "" {
		return fmt.Errorf("job validation error: %w", errors.New("missing job id"))
	}
	if len(j.Payload) == 0 {
		return ErrEmptyPayload
	}
	return nil
}
