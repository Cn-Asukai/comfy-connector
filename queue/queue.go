package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrJobNotFound   = errors.New("job not found")
	ErrJobDuplicate  = errors.New("job ID already exists")
	ErrJobNotPending = errors.New("job is not in pending status")
	ErrJobNotRunning = errors.New("job is not in running status")
)

type Queue interface {
	Enqueue(ctx context.Context, job *Job) error

	Dequeue(ctx context.Context, timeout time.Duration) (*Job, error)

	Ack(ctx context.Context, jobID string, result json.RawMessage) error

	Nack(ctx context.Context, jobID string, reason string) error

	Get(ctx context.Context, jobID string) (*Job, error)

	Cancel(ctx context.Context, jobID string) error

	Size(ctx context.Context) (int, error)
}
