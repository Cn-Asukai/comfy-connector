package scheduler

import (
	"context"
	"encoding/json"

	"comfyui_connector/queue"
)

type Handler func(ctx context.Context, job *queue.Job) (json.RawMessage, error)
