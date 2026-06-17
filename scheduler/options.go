package scheduler

import "time"

type SchedulerOption func(*Scheduler)

func WithHandler(name string, h Handler) SchedulerOption {
	return func(s *Scheduler) {
		s.handlers[name] = h
	}
}

func WithWorkerCount(n int) SchedulerOption {
	return func(s *Scheduler) {
		if n > 0 {
			s.workers = n
		}
	}
}

func WithDequeueTimeout(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		if d > 0 {
			s.dequeueTimeout = d
		}
	}
}
