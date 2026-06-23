package memory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cn-asukai/comfy-connector/queue"
	"github.com/cn-asukai/comfy-connector/queue/memory"
)

func TestEnqueue(t *testing.T) {
	t.Run("normal enqueue increases size", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		err := q.Enqueue(ctx, &queue.Job{ID: "job-1", Priority: 5})
		require.NoError(t, err)

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, size)
	})

	t.Run("duplicate ID returns ErrJobDuplicate", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()
		job := &queue.Job{ID: "job-1"}

		err := q.Enqueue(ctx, job)
		require.NoError(t, err)

		err = q.Enqueue(ctx, job)
		assert.ErrorIs(t, err, queue.ErrJobDuplicate)
	})

	t.Run("enqueue stores a defensive copy", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()
		job := &queue.Job{ID: "job-1", Priority: 5}

		err := q.Enqueue(ctx, job)
		require.NoError(t, err)

		job.Priority = 999

		got, err := q.Get(ctx, "job-1")
		require.NoError(t, err)
		assert.Equal(t, 5, got.Priority)
	})
}

func TestDequeue(t *testing.T) {
	t.Run("empty queue with timeout=-1 returns nil immediately", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		job, err := q.Dequeue(ctx, -1)
		assert.NoError(t, err)
		assert.Nil(t, job)
	})

	t.Run("empty queue with positive timeout returns nil after timeout", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		start := time.Now()
		job, err := q.Dequeue(ctx, 100*time.Millisecond)
		elapsed := time.Since(start)

		assert.NoError(t, err)
		assert.Nil(t, job)
		assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	})

	t.Run("empty queue with context cancel returns ctx.Err", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		_, err := q.Dequeue(ctx, 200*time.Millisecond)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("dequeues pending job and removes from pending set", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-1", Priority: 5}))

		job, err := q.Dequeue(ctx, -1)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, "job-1", job.ID)

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, size)
	})

	t.Run("higher priority dequeued first", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "low", Priority: 1}))
		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "high", Priority: 10}))

		job, err := q.Dequeue(ctx, -1)
		require.NoError(t, err)
		assert.Equal(t, "high", job.ID)

		job, err = q.Dequeue(ctx, -1)
		require.NoError(t, err)
		assert.Equal(t, "low", job.ID)
	})

	t.Run("same priority uses CreatedAt as tiebreaker", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "first", Priority: 0}))
		time.Sleep(50 * time.Millisecond)
		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "second", Priority: 0}))

		job, err := q.Dequeue(ctx, -1)
		require.NoError(t, err)
		assert.Equal(t, "first", job.ID)

		job, err = q.Dequeue(ctx, -1)
		require.NoError(t, err)
		assert.Equal(t, "second", job.ID)
	})
}

func TestAck(t *testing.T) {
	t.Run("existing running job returns nil", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-1", Status: queue.StatusRunning}))

		err := q.Ack(ctx, "job-1", "result-1")
		assert.NoError(t, err)
	})

	t.Run("non-existent job returns ErrJobNotFound", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		err := q.Ack(ctx, "nonexistent", "result")
		assert.ErrorIs(t, err, queue.ErrJobNotFound)
	})

	t.Run("non-running job returns ErrJobNotRunning", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-1", Status: queue.StatusPending}))

		err := q.Ack(ctx, "job-1", "result")
		assert.ErrorIs(t, err, queue.ErrJobNotRunning)
	})
}

func TestNack(t *testing.T) {
	t.Run("existing running job returns nil", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-1", Status: queue.StatusRunning}))

		err := q.Nack(ctx, "job-1", "some error")
		assert.NoError(t, err)
	})

	t.Run("non-existent job returns ErrJobNotFound", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		err := q.Nack(ctx, "nonexistent", "reason")
		assert.ErrorIs(t, err, queue.ErrJobNotFound)
	})

	t.Run("non-running job returns ErrJobNotRunning", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-1", Status: queue.StatusPending}))

		err := q.Nack(ctx, "job-1", "reason")
		assert.ErrorIs(t, err, queue.ErrJobNotRunning)
	})
}

func TestGet(t *testing.T) {
	t.Run("existing job returns defensive copy", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-1", Priority: 5}))

		got, err := q.Get(ctx, "job-1")
		require.NoError(t, err)
		assert.Equal(t, "job-1", got.ID)
		assert.Equal(t, 5, got.Priority)

		got.Priority = 999

		got2, err := q.Get(ctx, "job-1")
		require.NoError(t, err)
		assert.Equal(t, 5, got2.Priority)
	})

	t.Run("non-existent job returns ErrJobNotFound", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		_, err := q.Get(ctx, "nonexistent")
		assert.ErrorIs(t, err, queue.ErrJobNotFound)
	})
}

func TestCancel(t *testing.T) {
	t.Run("existing pending job is removed from pending set", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-1"}))

		err := q.Cancel(ctx, "job-1")
		assert.NoError(t, err)

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, size)
	})

	t.Run("non-existent job returns ErrJobNotFound", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		err := q.Cancel(ctx, "nonexistent")
		assert.ErrorIs(t, err, queue.ErrJobNotFound)
	})
}

func TestSize(t *testing.T) {
	t.Run("empty queue returns 0", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, size)
	})

	t.Run("n pending jobs returns n", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			require.NoError(t, q.Enqueue(ctx, &queue.Job{ID: "job-" + string(rune('0'+i))}))
		}

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, size)
	})
}

func TestConcurrency(t *testing.T) {
	t.Run("concurrent enqueue and dequeue with -race", func(t *testing.T) {
		q := memory.NewMemoryQueue()
		ctx := context.Background()

		const numOps = 50
		var wg sync.WaitGroup

		received := make(map[string]bool)
		var mu sync.Mutex

		wg.Add(numOps)
		for i := 0; i < numOps; i++ {
			go func(id int) {
				defer wg.Done()
				_ = q.Enqueue(ctx, &queue.Job{
					ID:       "job-" + string(rune('A'+id)),
					Priority: id,
				})
			}(i)
		}
		wg.Wait()

		wg.Add(numOps)
		for i := 0; i < numOps; i++ {
			go func() {
				defer wg.Done()
				job, err := q.Dequeue(ctx, 100*time.Millisecond)
				if err != nil || job == nil {
					return
				}
				mu.Lock()
				received[job.ID] = true
				mu.Unlock()
			}()
		}
		wg.Wait()

		mu.Lock()
		assert.Len(t, received, numOps)
		mu.Unlock()
	})
}
