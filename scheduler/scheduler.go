package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cn-asukai/comfy-connector/queue"
)

const defaultDequeueTimeout = 5 * time.Second

type Handler func(ctx context.Context, job *queue.Job) (fmt.Stringer, error)

type Scheduler struct {
	queue          queue.Queue
	handlers       map[string]Handler
	workers        int
	dequeueTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	active atomic.Int64
}

func NewScheduler(q queue.Queue, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		queue:          q,
		handlers:       make(map[string]Handler),
		workers:        1,
		dequeueTimeout: defaultDequeueTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.workerLoop()
	}

	s.wg.Wait()
	return nil
}

func (s *Scheduler) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (s *Scheduler) Running() bool {
	return s.ctx != nil && s.ctx.Err() == nil
}

func (s *Scheduler) ActiveWorkers() int64 {
	return s.active.Load()
}

func (s *Scheduler) Submit(ctx context.Context, job *queue.Job) error {
	job.Status = queue.StatusPending
	job.CreatedAt = time.Now()
	return s.queue.Enqueue(ctx, job)
}

func (s *Scheduler) workerLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		job, err := s.queue.Dequeue(s.ctx, s.dequeueTimeout)
		if err != nil {
			slog.Error("dequeue error", "error", err)
			continue
		}
		if job == nil {
			continue
		}

		now := time.Now()
		job.Status = queue.StatusRunning
		job.StartedAt = &now

		s.active.Add(1)
		s.execute(job)
		s.active.Add(-1)
	}
}

func (s *Scheduler) execute(job *queue.Job) {
	slog.Info("executing job", "job_id", job.ID, "handler", job.HandlerName)

	execCtx, cancel := s.jobContext()
	defer cancel()

	handler, ok := s.handlers[job.HandlerName]
	if !ok {
		now := time.Now()
		job.Status = queue.StatusFailed
		job.Error = "handler not found: " + job.HandlerName
		job.DoneAt = &now
		slog.Error("handler not found", "job_id", job.ID, "handler", job.HandlerName)
		s.queue.Nack(s.ctx, job.ID, job.Error)
		return
	}

	result, err := handler(execCtx, job)

	now := time.Now()
	job.DoneAt = &now

	if err != nil {
		job.Status = queue.StatusFailed
		job.Error = err.Error()
		s.queue.Nack(s.ctx, job.ID, err.Error())
	} else {
		job.Status = queue.StatusCompleted
		job.Result = result
		s.queue.Ack(s.ctx, job.ID, result)
	}
}

func (s *Scheduler) jobContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(s.ctx)
}
