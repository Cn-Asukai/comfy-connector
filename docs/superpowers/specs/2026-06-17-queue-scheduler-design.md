# 队列与调度接口设计

> Version: 1.0 | 2026-06-17 | 方案 A — Queue + Scheduler 双接口分离

---

## 1. 概述

### 1.1 目的

为 `comfyui_connector` 设计一套实现无关的**队列**和**调度**接口。用户可自由选择底层存储（内存 / Redis / MySQL 等），上层调度器通过接口操作，不感知具体实现。

### 1.2 核心需求汇总

| 维度 | 决策 |
|------|------|
| 部署 | 单 worker / 多 worker 均支持，worker 数可配置 |
| 任务模型 | 通用抽象任务（ComfyUI 生成只是其中一个 handler） |
| 优先级 | 数值优先级 + 防饥饿（aging） |
| 重试 | 不自动重试，失败即标记 failed，上层自行决定重新入队 |
| 结果交付 | 回调函数 `OnJobComplete(ctx, job)` |
| 调度 | 仅 worker 分发（无延迟/定时/cron） |

### 1.3 架构

```
                        ┌─────────────────────────────┐
                        │        调用方                │
                        └────────────┬────────────────┘
                                     │ Enqueue(job)
                                     ▼
┌──────────────────────────────────────────────────────┐
│                    Queue 接口 (持久层)                 │
│  Enqueue / Dequeue / Ack / Nack / Get / Cancel /     │
│  Size                                                │
└──────────────────────┬───────────────────────────────┘
                       │ Dequeue (阻塞, 按优先级)
                       ▼
┌──────────────────────────────────────────────────────┐
│                 Scheduler (运行时)                     │
│  ┌──────────────────────────────────────────────┐   │
│  │              Worker Pool (N goroutines)       │   │
│  │  ┌──────┐  ┌──────┐       ┌──────┐          │   │
│  │  │ w[0] │  │ w[1] │  ...  │ w[N] │          │   │
│  │  └──┬───┘  └──┬───┘       └──┬───┘          │   │
│  │     │         │              │               │   │
│  │     ▼         ▼              ▼               │   │
│  │  s.handlers[job.HandlerName](ctx, job)        │   │
│  │     │         │              │               │   │
│  │     ▼         ▼              ▼               │   │
│  │  Ack/Nack ───────────────────────────► Queue │   │
│  │     │                                         │   │
│  │     ▼                                         │   │
│  │  OnJobComplete(ctx, job)                      │   │
│  └──────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

### 1.4 包结构

```
comfyui_connector/
├── comfyui/                # (已有) ComfyUI HTTP/WS 客户端
│   ├── client.go
│   ├── websocket.go
│   └── generate.go
├── queue/                  # 队列抽象层
│   ├── job.go              #   Job 结构体 & 状态常量 & 错误类型
│   ├── queue.go            #   Queue 接口定义
│   └── memory/             #   内存实现（默认，单机/dev）
│       └── queue.go
├── scheduler/              # 调度器
│   ├── handler.go          #   Handler 函数类型
│   ├── scheduler.go        #   Scheduler: worker pool + 分发循环
│   ├── callback.go         #   OnJobComplete 回调函数类型
│   └── options.go          #   函数式选项
└── main.go
```

---

## 2. Job 数据模型

### 2.1 结构体

```go
// queue/job.go

type JobStatus string

const (
    StatusPending   JobStatus = "pending"
    StatusRunning   JobStatus = "running"
    StatusCompleted JobStatus = "completed"
    StatusFailed    JobStatus = "failed"
    StatusCancelled JobStatus = "cancelled"
)

type Job struct {
    ID          string          `json:"id"`
    HandlerName string          `json:"handler_name"`
    Priority    int             `json:"priority"`
    Payload     json.RawMessage `json:"payload"`
    Status      JobStatus       `json:"status"`

    // 完成时填充
    Result json.RawMessage `json:"result,omitempty"`
    Error  string          `json:"error,omitempty"`

    // 执行者标识（worker ID）
    WorkerID string `json:"worker_id,omitempty"`

    // 时间戳
    CreatedAt time.Time  `json:"created_at"`
    StartedAt *time.Time `json:"started_at,omitempty"`
    DoneAt    *time.Time `json:"done_at,omitempty"`

    // 任务最长执行时间，0 或负数 = 无限制
    MaxRunTime time.Duration `json:"max_run_time,omitempty"`
}
```

### 2.2 状态机

```
  ┌───────┐
  │pending│ ──Cancel()──► cancelled
  └───┬───┘
      │ Dequeue()
      ▼
  ┌───────┐
  │running│
  └───┬───┘
      │ Ack()   ──► completed
      │ Nack()  ──► failed
```

- `pending` → `running`: 由 `Dequeue()` 自动触发
- `running` → `completed`: `Ack()` 触发
- `running` → `failed`: `Nack()` 触发
- `pending` → `cancelled`: `Cancel()` 触发
- 只有 `pending` 可取消，只有 `running` 可 Ack/Nack

### 2.3 查询语义

| 方法 | 可见状态 |
|------|----------|
| `Dequeue()` | 仅返回 `pending` |
| `Get()` | 所有状态 |
| `Size()` | 仅统计 `pending` |
| `Cancel()` | 仅对 `pending` 有效 |

---

## 3. Queue 接口

### 3.1 接口定义

```go
// queue/queue.go

type Queue interface {
    // Enqueue 添加任务，ID 必须唯一。Status 自动设为 pending，CreatedAt 自动设。
    // 若 ID 已存在返回 ErrJobDuplicate。
    Enqueue(ctx context.Context, job *Job) error

    // Dequeue 按（有效优先级降序, 入队时间升序）取出最高优先级任务。
    // 阻塞最多 timeout 时长。timeout=0 永久阻塞，timeout<0 非阻塞立即返回。
    // 无合适任务时返回 (nil, nil)，出错返回 error。
    // 取出后 Status 自动变为 running，StartedAt 设当前时间。
    Dequeue(ctx context.Context, timeout time.Duration) (*Job, error)

    // Ack 标记任务完成，写入 result。仅 running 状态有效，否则返回 ErrJobNotRunning。
    Ack(ctx context.Context, jobID string, result json.RawMessage) error

    // Nack 标记任务失败，写入错误原因。仅 running 状态有效，否则返回 ErrJobNotRunning。
    Nack(ctx context.Context, jobID string, reason string) error

    // Get 按 ID 查询任务，任意状态。不存在返回 (nil, ErrJobNotFound)。
    Get(ctx context.Context, jobID string) (*Job, error)

    // Cancel 取消 pending 任务，status 变为 cancelled。非 pending 返回 ErrJobNotPending。
    Cancel(ctx context.Context, jobID string) error

    // Size 返回 pending 状态的任务数量。
    Size(ctx context.Context) (int, error)
}
```

### 3.2 错误类型

```go
// queue/queue.go

var (
    ErrJobNotFound   = errors.New("job not found")
    ErrJobDuplicate  = errors.New("job ID already exists")
    ErrJobNotPending = errors.New("job is not in pending status")
    ErrJobNotRunning = errors.New("job is not in running status")
)
```

### 3.3 实现约定

1. **线程安全**：所有方法必须 goroutine-safe
2. **Context 响应**：方法实现应尊重 `ctx.Done()`，允许调用方超时/取消
3. **幂等性**：`Enqueue` 对重复 ID 返回错误；`Ack`/`Nack` 对非 running 返回错误，不产生副作用
4. **优先级**：排序键为 `(effective_priority DESC, created_at ASC)`，参见第 7 节防饥饿
5. **Dequeue 原子性**：取出 + 状态变更必须是原子的（见第 8 节 crash recovery）

---

## 4. Handler

### 4.1 函数类型

```go
// scheduler/handler.go

// Handler 执行任务，返回结果 JSON。
// ctx 继承 scheduler 生命周期 & job.MaxRunTime 超时。
type Handler func(ctx context.Context, job *Job) (json.RawMessage, error)
```

### 4.2 注册机制

Handler 通过 `WithHandler(name, fn)` 注册到 Scheduler 的注册表中。Job 通过 `HandlerName` 指定由哪个 Handler 执行：

```
Job.HandlerName = "comfyui.generate"  ──►  Scheduler 注册表查找  ──►  对应 Handler 执行
```

Job 和 Handler 通过名称解耦 —— Job 只存名称（可序列化），Handler 函数由 Scheduler 持有。

## 5. Scheduler 设计

### 5.1 结构体

```go
// scheduler/scheduler.go

type Scheduler struct {
    queue      Queue
    handlers   map[string]Handler
    workers    int
    onComplete OnJobComplete

    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
    active atomic.Int64
}
```

### 5.2 构造函数与选项

```go
type SchedulerOption func(*Scheduler)

// WithWorkerCount 设置 worker 协程数，默认 1
func WithWorkerCount(n int) SchedulerOption

// WithHandler 注册一个 handler，关联名称与 job.HandlerName 对应
func WithHandler(name string, h Handler) SchedulerOption

// WithOnComplete 设置任务完成回调，默认 nil（不回调）
func WithOnComplete(fn OnJobComplete) SchedulerOption

// WithDequeueTimeout 设置单次 Dequeue 阻塞超时，默认 5s
func WithDequeueTimeout(d time.Duration) SchedulerOption

func NewScheduler(queue Queue, opts ...SchedulerOption) *Scheduler
```

### 5.3 生命周期

```go
// Start 启动 N 个 worker 协程，阻塞直到 ctx.Done() 或 Stop() 被调用。
func (s *Scheduler) Start(ctx context.Context) error

// Stop 优雅关闭：取消内部 ctx，等待正在执行的任务完成。
func (s *Scheduler) Stop() error

// Running 返回是否正在运行。
func (s *Scheduler) Running() bool

// ActiveWorkers 返回当前正在执行任务的 worker 数量。
func (s *Scheduler) ActiveWorkers() int64
```

### 5.4 Worker 循环

```go
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
            log.Printf("[scheduler] dequeue error: %v", err)
            continue
        }
        if job == nil {
            continue // 超时，无任务
        }

        s.active.Add(1)
        s.execute(job)
        s.active.Add(-1)
    }
}
```

### 5.5 单次执行流程

```go
func (s *Scheduler) execute(job *Job) {
    log.Printf("[scheduler] executing job %s (handler=%s)", job.ID, job.HandlerName)

    // 1. 构建 job 级别 context（带 MaxRunTime 超时）
    execCtx := s.ctx
    if job.MaxRunTime > 0 {
        var cancel context.CancelFunc
        execCtx, cancel = context.WithTimeout(s.ctx, job.MaxRunTime)
        defer cancel()
    }

    // 2. 查找 handler
    handler, ok := s.handlers[job.HandlerName]
    if !ok {
        log.Printf("[scheduler] handler %s not found for job %s", job.HandlerName, job.ID)
        s.queue.Nack(s.ctx, job.ID, "handler not found: "+job.HandlerName)
        return
    }

    // 3. 执行 handler
    result, err := handler(execCtx, job)

    // 4. 更新队列状态
    if err != nil {
        if nackErr := s.queue.Nack(s.ctx, job.ID, err.Error()); nackErr != nil {
            log.Printf("[scheduler] nack error: %v", nackErr)
        }
        job.Status = StatusFailed
        job.Error = err.Error()
    } else {
        if ackErr := s.queue.Ack(s.ctx, job.ID, result); ackErr != nil {
            log.Printf("[scheduler] ack error: %v", ackErr)
        }
        job.Status = StatusCompleted
        job.Result = result
    }

    // 5. 调用回调函数
    if s.onComplete != nil {
        s.onComplete(s.ctx, job)
    }
}
```

### 5.6 Submit 便捷方法

```go
// Submit 将任务入队并返回 job。等价于直接调用 Queue.Enqueue。
func (s *Scheduler) Submit(ctx context.Context, job *Job) error
```

---

## 6. 回调

### 6.1 函数类型

```go
// scheduler/callback.go

// OnJobComplete 任务完成（成功或失败）时由 Scheduler 调用。
// job.Status / job.Result / job.Error 已填充完成状态。
// 回调在 worker goroutine 内执行，耗时操作应自行起 goroutine。
type OnJobComplete func(ctx context.Context, job *Job)
```

### 6.2 设计要点

| 特性 | 说明 |
|------|------|
| 注入位置 | Scheduler 级别，通过 `WithOnComplete(fn)` 设置 |
| 调用时机 | Ack 或 Nack 成功后，job 状态已落盘 |
| 调用线程 | 当前 worker goroutine |
| 错误处理 | 回调返回 error 仅打日志，不影响 job 状态（已完成） |
| 重试 | 回调函数自行决定，Scheduler 不重试 |
| 默认值 | nil，设置后才调用 |

### 6.3 示例

```go
s := scheduler.NewScheduler(q,
    scheduler.WithHandler("comfyui.generate", h.generate),
    scheduler.WithOnComplete(func(ctx context.Context, job *queue.Job) {
        switch job.Status {
        case queue.StatusCompleted:
            log.Printf("job %s done: %s", job.ID, job.Result)
        case queue.StatusFailed:
            log.Printf("job %s failed: %s", job.ID, job.Error)
        }
    }),
)

// 需要 HTTP 回调的用户，在 OnJobComplete 内自行实现：
s := scheduler.NewScheduler(q,
    scheduler.WithHandler("comfyui.generate", h.generate),
    scheduler.WithOnComplete(func(ctx context.Context, job *queue.Job) {
        payload := map[string]any{
            "job_id": job.ID,
            "status": job.Status,
            "result": job.Result,
            "error":  job.Error,
        }
        b, _ := json.Marshal(payload)
        http.Post("http://myapp/callback", "application/json", bytes.NewReader(b))
    }),
)
```

---

## 7. 防饥饿策略

### 7.1 问题

纯优先级排序下，高优先级任务持续涌入会导致低优先级任务永远得不到执行（饥饿）。

### 7.2 方案：Aging（优先级老化）

等待时间越长的任务，其**有效优先级**逐渐提升：

```
effective_priority = priority + (wait_duration_seconds × aging_factor)

wait_duration = now - created_at
aging_factor  = 队列实现参数 (默认 0.01/s, 即等待 100s 后等级提升 1)
```

### 7.3 队列实现层约定

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `aging_factor` | 0.01 | 每秒优先级增量 |
| `max_effective_priority` | `math.MaxInt` | 有效优先级上限 |

Dequeue 排序键：`(effective_priority DESC, created_at ASC)`

### 7.4 实现示例（内存队列）

```go
type memoryItem struct {
    job   *Job
    index int
}

func (m *MemoryQueue) Dequeue(ctx context.Context, timeout time.Duration) (*Job, error) {
    // ...
    now := time.Now()
    for pq.Len() > 0 {
        item := heap.Pop(&pq).(*memoryItem)
        waitSec := now.Sub(item.job.CreatedAt).Seconds()
        effectivePriority := item.job.Priority + int(waitSec * m.agingFactor)
        // 返回 effectivePriority 最高的
    }
}
```

### 7.5 Redis / MySQL 实现要点

- **Redis**：ZSET member 在取出时计算有效优先级，或定期扫描 `created_at` 更新 score
- **MySQL**：ORDER BY 中使用计算表达式：

```sql
SELECT * FROM jobs
WHERE status = 'pending'
ORDER BY (priority + TIMESTAMPDIFF(SECOND, created_at, NOW()) * 0.01) DESC,
         created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED
```

---

## 8. Crash Recovery

### 8.1 已知限制

方案 A 中 `Dequeue()` 将 job 状态变为 `running` 后，若 worker 崩溃：
- Job 卡在 `running` 状态，无法被其他 worker 取出
- 没有内建 lease（租约）/ visibility timeout 机制

### 8.2 影响

| 场景 | 影响 |
|------|------|
| 内存队列 | 进程崩溃后所有 running 状态丢失（内存实现仅适用于 dev/单机） |
| Redis 队列 | running job 数据仍在，但无法自动恢复 |
| MySQL 队列 | running job 数据仍在，但无法自动恢复 |
| Worker goroutine panic | 同一进程内可通过 recover 处理，影响可控 |

### 8.3 建议演进路径

v1 接受此限制。v2 增加可选方法：

```go
// Claim 替代 Dequeue，支持租约
Claim(ctx context.Context, timeout time.Duration, lease time.Duration) (*Job, error)

// Renew 续租（用于长时间运行的任务）
Renew(ctx context.Context, jobID string, lease time.Duration) error

// ReapStaleJobs 回收超时租约的 running 任务，退回 pending
ReapStaleJobs(ctx context.Context, maxLeaseDuration time.Duration) ([]string, error)
```

---

## 9. 内存队列实现（参考实现）

### 9.1 核心结构

```go
// queue/memory/queue.go

type MemoryQueue struct {
    mu          sync.Mutex
    jobs        map[string]*Job
    heap        *priorityHeap
    cond        *sync.Cond       // 用于阻塞 Dequeue
    closed      bool
    agingFactor float64
}

func NewMemoryQueue() *MemoryQueue
func NewMemoryQueueWithOptions(agingFactor float64) *MemoryQueue
```

### 9.2 实现要点

| 方法 | 策略 |
|------|------|
| `Enqueue` | 检查重复 ID → 写入 map → `heap.Push` → `cond.Signal()` |
| `Dequeue` | 加锁 → for heap 为空且未超时: `cond.Wait` → 计算有效优先级 → `heap.Pop` → 更新 status |
| `Ack` | 加锁 → 校验 status=running → 更新 status/result/DoneAt |
| `Nack` | 同 Ack |
| `Get` | 加锁 → map 查找 |
| `Cancel` | 加锁 → 校验 status=pending → 更新 status → heap 中标记移除（惰性删除） |
| `Size` | 加锁 → 统计 status=pending |

### 9.3 优先级堆

```go
type priorityHeap struct {
    items       []*memoryItem
    agingFactor float64
}

func (h *priorityHeap) Less(i, j int) bool {
    // effective_priority 高的优先；相同则创建早的优先
    epI := h.effectivePriority(h.items[i].job)
    epJ := h.effectivePriority(h.items[j].job)
    if epI != epJ {
        return epI > epJ
    }
    return h.items[i].job.CreatedAt.Before(h.items[j].job.CreatedAt)
}
```

---

## 10. 使用示例

### 10.1 基本用法 — 内存队列 + 单 worker

```go
package main

import (
    "comfyui_connector/comfyui"
    "comfyui_connector/queue"
    "comfyui_connector/queue/memory"
    "comfyui_connector/scheduler"
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
)

type ComfyUIGenerateHandler struct {
    client *comfyui.Client
}

func (h *ComfyUIGenerateHandler) generate(ctx context.Context, job *queue.Job) (json.RawMessage, error) {
    var workflow map[string]any
    if err := json.Unmarshal(job.Payload, &workflow); err != nil {
        return nil, err
    }
    result, err := h.client.GenerateImage(ctx, workflow)
    if err != nil {
        return nil, err
    }
    return json.Marshal(result)
}

func main() {
    q := memory.NewMemoryQueue()
    h := &ComfyUIGenerateHandler{
        client: comfyui.NewClient("http://127.0.0.1:8188"),
    }

    s := scheduler.NewScheduler(q,
        scheduler.WithHandler("comfyui.generate", h.generate),
        scheduler.WithWorkerCount(2),
        scheduler.WithOnComplete(func(ctx context.Context, job *queue.Job) {
            switch job.Status {
            case queue.StatusCompleted:
                log.Printf("job %s completed: %s", job.ID, job.Result)
            case queue.StatusFailed:
                log.Printf("job %s failed: %s", job.ID, job.Error)
            }
        }),
    )

    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
    defer cancel()

    go func() {
        log.Println("scheduler started")
        if err := s.Start(ctx); err != nil {
            log.Fatal(err)
        }
        log.Println("scheduler stopped")
    }()

    // 提交任务
    workflow := map[string]any{ /* ... */ }
    payload, _ := json.Marshal(workflow)
    job := &queue.Job{
        ID:          "job-001",
        HandlerName: "comfyui.generate",
        Priority:    10,
        Payload:     payload,
    }
    if err := s.Submit(ctx, job); err != nil {
        log.Fatal(err)
    }

    <-ctx.Done()
    s.Stop()
}
```

### 10.2 无回调模式

```go
s := scheduler.NewScheduler(q,
    scheduler.WithHandler("comfyui.generate", h.generate),
    scheduler.WithWorkerCount(1),
)
// 不传 WithOnComplete，默认 nil，不触发任何回调

// 调用方通过 queue.Get() 轮询结果：
job, _ := q.Get(ctx, "job-001")
if job.Status == queue.StatusCompleted {
    log.Printf("result: %s", job.Result)
}
```

---

## 11. 未来 Backend 扩展

### 11.1 Redis (`queue/redis`)

| 方法 | 实现 |
|------|------|
| `Enqueue` | `HSET job:{id}` + `ZADD queue:pending {score} {id}` |
| `Dequeue` | Lua script: `ZPOPMAX queue:pending` → `HSET status=running` → return |
| `Ack` | `HSET status=completed result=... done_at=...` |
| `Get` | `HGETALL job:{id}` |
| `Size` | `ZCARD queue:pending` |

Score 计算：`effective_priority * 1e12 + (MaxInt - unix_nano/1000)` 以支持精确排序。

### 11.2 MySQL (`queue/mysql`)

单表设计：

```sql
CREATE TABLE queue_jobs (
    id          VARCHAR(64) PRIMARY KEY,
    handler_name VARCHAR(64) NOT NULL,
    priority    INT NOT NULL DEFAULT 0,
    payload     JSON NOT NULL,
    status      ENUM('pending','running','completed','failed','cancelled') NOT NULL,
    result      JSON,
    error       TEXT,
    worker_id   VARCHAR(64) DEFAULT '',
    created_at  DATETIME(3) NOT NULL,
    started_at  DATETIME(3),
    done_at     DATETIME(3),
    max_run_time_ms BIGINT DEFAULT 0,

    INDEX idx_dequeue (status, priority DESC, created_at ASC)
);
```

Dequeue 使用 `SELECT ... FOR UPDATE SKIP LOCKED`：

```sql
SELECT * FROM queue_jobs
WHERE status = 'pending'
ORDER BY (priority + TIMESTAMPDIFF(SECOND, created_at, NOW()) * 0.01) DESC, created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

---

## 12. 依赖项

新增依赖：

```
github.com/google/uuid  (已有 — 生成 Job ID)
标准库                 (sync, container/heap, net/http, context, encoding/json)
```

无需引入其他第三方库。Redis/MySQL backend 作为独立子包，各自引入对应驱动。

---

## 附录 A：接口方法速查

| 接口 | 方法 | 说明 |
|------|------|------|
| `Queue` | `Enqueue` | 入队 |
| | `Dequeue` | 阻塞出队（按优先级） |
| | `Ack` | 确认完成 |
| | `Nack` | 标记失败 |
| | `Get` | 按 ID 查询 |
| | `Cancel` | 取消 pending 任务 |
| | `Size` | pending 数量 |
| `Handler` | 函数类型 `func(ctx, *Job) (json.RawMessage, error)` | 执行任务 |
| ~~`CallbackNotifier`~~ | ~~`Notify`~~ | ~~发送回调~~ |
| `OnJobComplete` | 函数类型 `func(ctx, *Job)` | Scheduler 级别回调 |

## 附录 B：状态转换矩阵

| 当前状态 → | pending | running | completed | failed | cancelled |
|------------|---------|---------|-----------|--------|-----------|
| Enqueue | ✅ | | | | |
| Dequeue | ✅ | | | | |
| Ack | | ✅ | | | |
| Nack | | ✅ | | | |
| Cancel | ✅ | | | | |
