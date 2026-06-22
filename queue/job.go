package queue

import (
	"time"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
)

type Job struct {
	ID          string    `json:"id"`
	HandlerName string    `json:"handler_name"`
	Priority    int       `json:"priority"`
	Payload     any       `json:"payload"`
	Status      JobStatus `json:"status"`

	Result   string `json:"result,omitempty"`
	Error    string `json:"error,omitempty"`
	WorkerID string `json:"worker_id,omitempty"`

	CreatedAt time.Time  `json:"created_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	DoneAt    *time.Time `json:"done_at,omitempty"`

	MaxRunTime time.Duration `json:"max_run_time,omitempty"`
}
