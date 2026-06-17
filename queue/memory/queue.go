package memory

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/cn-asukai/comfy-connector/queue"
)

const defaultAgingFactor = 0.01

type MemoryQueue struct {
	mu   sync.Mutex
	jobs map[string]*queue.Job
	cond *sync.Cond

	pendingIDs  map[string]struct{}
	agingFactor float64
}

func NewMemoryQueue() *MemoryQueue {
	m := &MemoryQueue{
		jobs:        make(map[string]*queue.Job),
		pendingIDs:  make(map[string]struct{}),
		agingFactor: defaultAgingFactor,
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *MemoryQueue) Enqueue(ctx context.Context, job *queue.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.jobs[job.ID]; exists {
		return queue.ErrJobDuplicate
	}

	j := *job
	m.jobs[job.ID] = &j
	m.pendingIDs[job.ID] = struct{}{}
	m.cond.Signal()
	return nil
}

func (m *MemoryQueue) Dequeue(ctx context.Context, timeout time.Duration) (*queue.Job, error) {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if job := m.popPending(); job != nil {
			return job, nil
		}

		if timeout < 0 {
			return nil, nil
		}

		if timeout > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil, nil
			}
			time.AfterFunc(remaining, func() { m.cond.Broadcast() })
		}

		m.cond.Wait()
	}
}

func (m *MemoryQueue) popPending() *queue.Job {
	if len(m.pendingIDs) == 0 {
		return nil
	}

	now := time.Now()
	type item struct {
		job *queue.Job
		ep  float64
	}
	var items []item
	for id := range m.pendingIDs {
		job := m.jobs[id]
		items = append(items, item{job, m.effectivePriority(job, now)})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ep != items[j].ep {
			return items[i].ep > items[j].ep
		}
		return items[i].job.CreatedAt.Before(items[j].job.CreatedAt)
	})

	job := items[0].job
	delete(m.pendingIDs, job.ID)
	return job
}

func (m *MemoryQueue) effectivePriority(job *queue.Job, now time.Time) float64 {
	wait := now.Sub(job.CreatedAt).Seconds()
	return float64(job.Priority) + wait*m.agingFactor
}

func (m *MemoryQueue) Ack(ctx context.Context, jobID string, result json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[jobID]
	if !ok {
		return queue.ErrJobNotFound
	}
	if job.Status != queue.StatusRunning {
		return queue.ErrJobNotRunning
	}
	return nil
}

func (m *MemoryQueue) Nack(ctx context.Context, jobID string, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[jobID]
	if !ok {
		return queue.ErrJobNotFound
	}
	if job.Status != queue.StatusRunning {
		return queue.ErrJobNotRunning
	}
	return nil
}

func (m *MemoryQueue) Get(ctx context.Context, jobID string) (*queue.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[jobID]
	if !ok {
		return nil, queue.ErrJobNotFound
	}

	j := *job
	return &j, nil
}

func (m *MemoryQueue) Cancel(ctx context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.jobs[jobID]
	if !ok {
		return queue.ErrJobNotFound
	}

	delete(m.pendingIDs, jobID)
	return nil
}

func (m *MemoryQueue) Size(ctx context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.pendingIDs), nil
}
