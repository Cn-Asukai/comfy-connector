package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cn-asukai/comfy-connector/queue"
	"github.com/cn-asukai/comfy-connector/scheduler"
)

type mockQueue struct {
	mu      sync.Mutex
	jobs    map[string]*queue.Job
	jobCh   chan *queue.Job
	ackCh   chan string
	nackCh  chan string

	dequeueErr error
}

func newMockQueue() *mockQueue {
	return &mockQueue{
		jobs:   make(map[string]*queue.Job),
		jobCh:  make(chan *queue.Job, 100),
		ackCh:  make(chan string, 100),
		nackCh: make(chan string, 100),
	}
}

func (m *mockQueue) Enqueue(ctx context.Context, job *queue.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j := *job
	m.jobs[job.ID] = &j
	return nil
}

func (m *mockQueue) Dequeue(ctx context.Context, timeout time.Duration) (*queue.Job, error) {
	if m.dequeueErr != nil {
		return nil, m.dequeueErr
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case job := <-m.jobCh:
		if job != nil {
			m.mu.Lock()
			m.jobs[job.ID] = job
			m.mu.Unlock()
		}
		return job, nil
	}
}

func (m *mockQueue) Ack(ctx context.Context, jobID string, result string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.jobs[jobID]
	if !ok {
		return queue.ErrJobNotFound
	}
	m.ackCh <- jobID
	return nil
}

func (m *mockQueue) Nack(ctx context.Context, jobID string, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.jobs[jobID]
	if !ok {
		return queue.ErrJobNotFound
	}
	m.nackCh <- jobID
	return nil
}

func (m *mockQueue) Get(ctx context.Context, jobID string) (*queue.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[jobID]
	if !ok {
		return nil, queue.ErrJobNotFound
	}
	j := *job
	return &j, nil
}

func (m *mockQueue) Cancel(ctx context.Context, jobID string) error {
	return nil
}

func (m *mockQueue) Size(ctx context.Context) (int, error) {
	return 0, nil
}

func TestNewScheduler_Defaults(t *testing.T) {
	mq := newMockQueue()
	s := scheduler.NewScheduler(mq)

	assert.False(t, s.Running())
	assert.Equal(t, int64(0), s.ActiveWorkers())
}

func TestNewScheduler_WithWorkerCount(t *testing.T) {
	blockingHandler := func(done <-chan struct{}) scheduler.Handler {
		return func(ctx context.Context, job *queue.Job) (string, error) {
			select {
			case <-done:
				return "ok", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}

	t.Run("positive_count", func(t *testing.T) {
		mq := newMockQueue()
		done := make(chan struct{})
		s := scheduler.NewScheduler(mq,
			scheduler.WithWorkerCount(3),
			scheduler.WithDequeueTimeout(50*time.Millisecond),
			scheduler.WithHandlerFunc("test", blockingHandler(done)),
		)

		go func() { _ = s.Start(context.Background()) }()
		defer s.Stop()

		for i := 0; i < 3; i++ {
			mq.jobCh <- &queue.Job{ID: fmt.Sprintf("job-%d", i), HandlerName: "test"}
		}

		assert.Eventually(t, func() bool { return s.ActiveWorkers() == 3 }, 1*time.Second, 10*time.Millisecond)

		for i := 0; i < 3; i++ {
			done <- struct{}{}
		}
		for i := 0; i < 3; i++ {
			<-mq.ackCh
		}
	})

	t.Run("negative_keeps_default", func(t *testing.T) {
		mq := newMockQueue()
		done := make(chan struct{})
		s := scheduler.NewScheduler(mq,
			scheduler.WithWorkerCount(-1),
			scheduler.WithDequeueTimeout(50*time.Millisecond),
			scheduler.WithHandlerFunc("test", blockingHandler(done)),
		)

		go func() { _ = s.Start(context.Background()) }()
		defer s.Stop()

		for i := 0; i < 2; i++ {
			mq.jobCh <- &queue.Job{ID: fmt.Sprintf("job-%d", i), HandlerName: "test"}
		}

		assert.Eventually(t, func() bool { return s.ActiveWorkers() == 1 }, 1*time.Second, 10*time.Millisecond)
		assert.Never(t, func() bool { return s.ActiveWorkers() >= 2 }, 200*time.Millisecond, 20*time.Millisecond)

		done <- struct{}{}
		<-mq.ackCh
		done <- struct{}{}
		<-mq.ackCh
	})

	t.Run("zero_keeps_default", func(t *testing.T) {
		mq := newMockQueue()
		done := make(chan struct{})
		s := scheduler.NewScheduler(mq,
			scheduler.WithWorkerCount(0),
			scheduler.WithDequeueTimeout(50*time.Millisecond),
			scheduler.WithHandlerFunc("test", blockingHandler(done)),
		)

		go func() { _ = s.Start(context.Background()) }()
		defer s.Stop()

		mq.jobCh <- &queue.Job{ID: "job-1", HandlerName: "test"}
		mq.jobCh <- &queue.Job{ID: "job-2", HandlerName: "test"}

		assert.Eventually(t, func() bool { return s.ActiveWorkers() == 1 }, 1*time.Second, 10*time.Millisecond)
		assert.Never(t, func() bool { return s.ActiveWorkers() >= 2 }, 200*time.Millisecond, 20*time.Millisecond)

		done <- struct{}{}
		<-mq.ackCh
		done <- struct{}{}
		<-mq.ackCh
	})
}

func TestNewScheduler_WithDequeueTimeout(t *testing.T) {
	mq := newMockQueue()
	s := scheduler.NewScheduler(mq, scheduler.WithDequeueTimeout(1*time.Second))
	require.NotNil(t, s)
}

func TestNewScheduler_WithHandlerFunc(t *testing.T) {
	mq := newMockQueue()
	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandlerFunc("test", func(ctx context.Context, job *queue.Job) (string, error) {
			return "from_handler_func", nil
		}),
	)

	go func() { _ = s.Start(context.Background()) }()
	defer s.Stop()

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	mq.jobCh <- job

	select {
	case <-mq.ackCh:
		assert.Equal(t, queue.StatusCompleted, job.Status)
		assert.Equal(t, "from_handler_func", job.Result)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

func TestNewScheduler_WithHandler(t *testing.T) {
	mq := newMockQueue()
	h := &testHandler{name: "test", result: "from_handler_interface"}
	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandler("test", h),
	)

	go func() { _ = s.Start(context.Background()) }()
	defer s.Stop()

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	mq.jobCh <- job

	select {
	case <-mq.ackCh:
		assert.Equal(t, queue.StatusCompleted, job.Status)
		assert.Equal(t, "from_handler_interface", job.Result)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

type testHandler struct {
	name   string
	result string
}

func (h *testHandler) GetHandlerName() string { return h.name }

func (h *testHandler) Execute(ctx context.Context, job *queue.Job) (string, error) {
	return h.result, nil
}

func TestSubmit(t *testing.T) {
	mq := newMockQueue()
	s := scheduler.NewScheduler(mq)

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	err := s.Submit(context.Background(), job)

	assert.NoError(t, err)
	assert.Equal(t, queue.StatusPending, job.Status)
	assert.False(t, job.CreatedAt.IsZero())
}

func TestWorkerLoop_NormalExecution(t *testing.T) {
	mq := newMockQueue()

	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandlerFunc("test", func(ctx context.Context, job *queue.Job) (string, error) {
			return "success", nil
		}),
	)

	go func() {
		_ = s.Start(context.Background())
	}()

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	mq.jobCh <- job

	select {
	case ackID := <-mq.ackCh:
		assert.Equal(t, "job-1", ackID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	assert.Equal(t, queue.StatusCompleted, job.Status)
	assert.Equal(t, "success", job.Result)
	assert.NotNil(t, job.StartedAt)
	assert.NotNil(t, job.DoneAt)

	s.Stop()
}

func TestWorkerLoop_HandlerError(t *testing.T) {
	mq := newMockQueue()

	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandlerFunc("test", func(ctx context.Context, job *queue.Job) (string, error) {
			return "", errors.New("handler failed")
		}),
	)

	go func() {
		_ = s.Start(context.Background())
	}()

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	mq.jobCh <- job

	select {
	case nackID := <-mq.nackCh:
		assert.Equal(t, "job-1", nackID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	assert.Equal(t, queue.StatusFailed, job.Status)
	assert.Equal(t, "handler failed", job.Error)
	assert.NotNil(t, job.DoneAt)

	s.Stop()
}

func TestWorkerLoop_HandlerNotFound(t *testing.T) {
	mq := newMockQueue()

	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Start(ctx)
	}()

	job := &queue.Job{ID: "job-1", HandlerName: "nonexistent"}
	mq.jobCh <- job

	select {
	case nackID := <-mq.nackCh:
		assert.Equal(t, "job-1", nackID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	assert.Equal(t, queue.StatusFailed, job.Status)
	assert.Equal(t, "handler not found: nonexistent", job.Error)

	cancel()
	wg.Wait()
}

func TestWorkerLoop_NilJob(t *testing.T) {
	mq := newMockQueue()

	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandlerFunc("test", func(ctx context.Context, job *queue.Job) (string, error) {
			return "ok", nil
		}),
	)

	go func() { _ = s.Start(context.Background()) }()
	defer s.Stop()

	mq.jobCh <- nil
	time.Sleep(100 * time.Millisecond)

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	mq.jobCh <- job

	select {
	case <-mq.ackCh:
		assert.Equal(t, queue.StatusCompleted, job.Status)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack — scheduler may be stuck on nil job")
	}
}

func TestWorkerLoop_DequeueError(t *testing.T) {
	mq := newMockQueue()
	mq.dequeueErr = errors.New("connection refused")

	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandlerFunc("test", func(ctx context.Context, job *queue.Job) (string, error) {
			return "ok", nil
		}),
	)

	go func() { _ = s.Start(context.Background()) }()
	defer s.Stop()

	mq.dequeueErr = nil

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	mq.jobCh <- job

	select {
	case <-mq.ackCh:
		assert.Equal(t, queue.StatusCompleted, job.Status)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack — scheduler did not recover from dequeue error")
	}
}

func TestWorkerLoop_ConcurrentWorkers(t *testing.T) {
	mq := newMockQueue()

	var mu sync.Mutex
	concurrent := 0
	maxConcurrent := 0
	started := make(chan struct{})
	done := make(chan struct{})

	s := scheduler.NewScheduler(mq,
		scheduler.WithWorkerCount(3),
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandlerFunc("test", func(ctx context.Context, job *queue.Job) (string, error) {
			mu.Lock()
			concurrent++
			if concurrent > maxConcurrent {
				maxConcurrent = concurrent
			}
			mu.Unlock()
			started <- struct{}{}
			<-done
			mu.Lock()
			concurrent--
			mu.Unlock()
			return "ok", nil
		}),
	)

	go func() { _ = s.Start(context.Background()) }()
	defer s.Stop()

	for i := 0; i < 3; i++ {
		mq.jobCh <- &queue.Job{ID: fmt.Sprintf("job-%d", i), HandlerName: "test"}
	}

	for i := 0; i < 3; i++ {
		<-started
	}
	assert.Equal(t, 3, maxConcurrent)
	assert.Eventually(t, func() bool { return s.ActiveWorkers() == 3 }, 1*time.Second, 10*time.Millisecond)

	for i := 0; i < 3; i++ {
		done <- struct{}{}
	}
	for i := 0; i < 3; i++ {
		<-mq.ackCh
	}
}

func TestStartStop(t *testing.T) {
	mq := newMockQueue()
	s := scheduler.NewScheduler(mq, scheduler.WithDequeueTimeout(50*time.Millisecond))

	assert.False(t, s.Running())

	go func() {
		_ = s.Start(context.Background())
	}()

	assert.Eventually(t, func() bool { return s.Running() }, 1*time.Second, 10*time.Millisecond)

	s.Stop()

	assert.Eventually(t, func() bool { return !s.Running() }, 1*time.Second, 10*time.Millisecond)
}

func TestActiveWorkers(t *testing.T) {
	mq := newMockQueue()

	handlerRunning := make(chan struct{})
	handlerDone := make(chan struct{})

	s := scheduler.NewScheduler(mq,
		scheduler.WithDequeueTimeout(50*time.Millisecond),
		scheduler.WithHandlerFunc("test", func(ctx context.Context, job *queue.Job) (string, error) {
			close(handlerRunning)
			<-handlerDone
			return "ok", nil
		}),
	)

	go func() {
		_ = s.Start(context.Background())
	}()

	job := &queue.Job{ID: "job-1", HandlerName: "test"}
	mq.jobCh <- job

	<-handlerRunning
	assert.Eventually(t, func() bool { return s.ActiveWorkers() == 1 }, 1*time.Second, 10*time.Millisecond)

	close(handlerDone)

	select {
	case <-mq.ackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	assert.Eventually(t, func() bool { return s.ActiveWorkers() == 0 }, 1*time.Second, 10*time.Millisecond)

	s.Stop()
}
