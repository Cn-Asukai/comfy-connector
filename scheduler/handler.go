package scheduler

import (
	"context"
	"encoding/json"

	"github.com/cn-asukai/comfy-connector/queue"
)

type Handler func(ctx context.Context, job *queue.Job) (json.RawMessage, error)
