package scheduler

import (
	"context"

	"github.com/cn-asukai/comfy-connector/queue"
)

type SchedulerHandler interface {
	GetHandlerName() string
	Execute(ctx context.Context, job *queue.Job) (string, error)
}
