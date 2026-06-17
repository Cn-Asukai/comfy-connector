package main

import (
	"comfyui_connector/comfyui"
	"comfyui_connector/queue"
	"comfyui_connector/queue/memory"
	"comfyui_connector/scheduler"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"time"
)

func main() {
	q := memory.NewMemoryQueue()
	client := comfyui.NewClient("http://127.0.0.1:8188")

	s := scheduler.NewScheduler(q,
		scheduler.WithHandler("comfyui.generate", func(ctx context.Context, job *queue.Job) (json.RawMessage, error) {
			slog.Info("handler executing", "job_id", job.ID)
			var wf map[string]any
			if err := json.Unmarshal(job.Payload, &wf); err != nil {
				return nil, err
			}
			result, err := client.GenerateImage(ctx, wf)
			if err != nil {
				return nil, err
			}
			return json.Marshal(result)
		}),
		scheduler.WithWorkerCount(2),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	go func() {
		slog.Info("scheduler started")
		if err := s.Start(ctx); err != nil {
			slog.Error("scheduler start failed", "error", err)
			os.Exit(1)
		}
		slog.Info("scheduler stopped")
	}()

	time.Sleep(100 * time.Millisecond)

	workflow := map[string]any{
		"3": map[string]any{
			"class_type": "KSampler",
		},
	}
	payload, _ := json.Marshal(workflow)

	job := &queue.Job{
		ID:          "job-001",
		HandlerName: "comfyui.generate",
		Priority:    10,
		Payload:     payload,
	}
	if err := s.Submit(ctx, job); err != nil {
		slog.Error("submit failed", "error", err)
		os.Exit(1)
	}

	slog.Info("job submitted", "job_id", job.ID)

	time.Sleep(2 * time.Second)

	queried, _ := q.Get(ctx, job.ID)
	if queried != nil {
		slog.Info("job status", "status", queried.Status)
	}

	<-ctx.Done()
	s.Stop()
}
