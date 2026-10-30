package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/developer/gopulse/internal/domain"
)

func TestJobValidation(t *testing.T) {
	tests := []struct {
		name    string
		job     domain.Job
		wantErr error
	}{
		{
			name: "valid job",
			job: domain.Job{
				ID:        "job-101",
				Payload:   "telemetry-packet",
				Priority:  domain.PriorityHigh,
				CreatedAt: time.Now(),
			},
			wantErr: nil,
		},
		{
			name: "missing job id",
			job: domain.Job{
				ID:      "",
				Payload: "packet",
			},
			wantErr: errors.New("missing job id"),
		},
		{
			name: "empty payload",
			job: domain.Job{
				ID:      "job-102",
				Payload: "",
			},
			wantErr: domain.ErrEmptyPayload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Validate()
			if tt.wantErr == nil && err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("expected error %v, got nil", tt.wantErr)
				}
				if errors.Is(tt.wantErr, domain.ErrEmptyPayload) && !errors.Is(err, domain.ErrEmptyPayload) {
					t.Fatalf("expected ErrEmptyPayload, got %v", err)
				}
			}
		})
	}
}

func TestJobPriorityString(t *testing.T) {
	if domain.PriorityCritical.String() != "CRITICAL" {
		t.Errorf("expected CRITICAL, got %s", domain.PriorityCritical.String())
	}
	if domain.PriorityHigh.String() != "HIGH" {
		t.Errorf("expected HIGH, got %s", domain.PriorityHigh.String())
	}
	if domain.PriorityMedium.String() != "MEDIUM" {
		t.Errorf("expected MEDIUM, got %s", domain.PriorityMedium.String())
	}
	if domain.PriorityLow.String() != "LOW" {
		t.Errorf("expected LOW, got %s", domain.PriorityLow.String())
	}
}
