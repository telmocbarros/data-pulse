package models

import "time"

// JobStatus is the lifecycle state of a background job.
type JobStatus string

// Job lifecycle states. Stored in the jobs.status column.
const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
)

// Job is the response shape for GET /api/jobs/{id}.
type Job struct {
	ID        string    `json:"id"`
	FileName  string    `json:"file_name"`
	FileType  string    `json:"file_type"`
	Status    JobStatus `json:"status"`
	Progress  int       `json:"progress"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
