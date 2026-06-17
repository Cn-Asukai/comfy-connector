# 队列与调度接口设计

> Version: 3.0 | 2026-06-17 | 方案 A — Queue + Scheduler 双接口分离

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
| 结果交付 | 调用方通过 `Queue.Get()` 轮询结果 |
| 调度 | 仅 worker 分发（无延迟/定时/cron） |

### 1.3 核心设计

```
Job (纯数据，可序列化)                   Scheduler (持有 handler 注册表)
┌──────────────────────────┐            ┌──────────────────────────────┐
│ ID                       │            │ handlers: map[string]Handler │
│ HandlerName  ─────────────┼── lookup ──►│  "comfyui.generate" → fn1   │
│ Priority                 │            │  "image.resize"     → fn2   │
│ Payload                  │            │  ...                         │
│ Status, Result, Error    │            └──────────────────────────────┘
└──────────────────────────┘
```

- **Job**：纯数据，所有字段可 JSON 序列化。`HandlerName` 是指向 handler 注册表的键
- **Scheduler**：通过 `WithHandler(name, fn)` 注册 handler，运行时按 `job.HandlerName` 查找执行
- Handler 和 Job 通过名称解耦 —— Job 存可序列化的名称，handler 函数由 Scheduler 持有

### 1.4 架构

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
                       │ Dequeue → *Job
                       ▼
┌──────────────────────────────────────────────────────┐
│                 Scheduler (运行时)                     │
│  ┌──────────────────────────────────────────────┐   │
│  │  handlers: map[string]Handler                │   │
│  │                                              │   │
│  │              Worker Pool (N goroutines)       │   │
│  │  ┌──────┐  ┌──────┐       ┌──────┐          │   │
│  │  │ w[0] │  │ w[1] │  ...  │ w[N] │          │   │
│  │  └──┬───┘  └──┬───┘       └──┬───┘          │   │
│  │     │         │              │               │   │
│  │     ▼         ▼              ▼               │   │
│  │  handler := s.handlers[job.HandlerName]      │   │
│  │  handler(ctx, job)                           │   │
│  │     │         │              │               │   │
│  │     ▼         ▼              ▼               │   │
│  │  Ack/Nack ───────────────────────────► Queue │
│  └──────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

### 1.5 包结构

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
│   ├── scheduler.go        #   Scheduler: worker pool + handler 注册表
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

- `HandlerName` 是纯字符串，指向 Scheduler 注册表中的 handler
- Job 完全可序列化，切换 Redis/MySQL 后端无需改动结构

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
    // Enqueue 添加任务，Job.Status 自动设为 pending，Job.CreatedAt 自动设。
    // 若 Job.ID 已存在返回 ErrJobDuplicate。
    Enqueue(ctx context.Context, job *Job) error

    // Dequeue 按（有效优先级降序, 入队时间升序）阻塞取出任务。
    // timeout=0 永久阻塞，timeout<0 非阻塞立即返回。
    // 无合适任务时返回 (nil, nil)。取出后 Job.Status 自动变为 running。
    Dequeue(ctx context.Context, timeout time.Duration) (*Job, error)

    // Ack 标记任务完成，写入 result。仅 running 状态有效。
    Ack(ctx context.Context, jobID string, result json.RawMessage) error

    // Nack 标记任务失败，写入错误原因。仅 running 状态有效。
    Nack(ctx context.Context, jobID string, reason string) error

    // Get 按 ID 查询任务，任意状态。不存在返回 (nil, ErrJobNotFound)。
    Get(ctx context.Context, jobID string) (*Job, error)

    // Cancel 取消 pending 任务。非 pending 返回 ErrJobNotPending。
    Cancel(ctx context.Context, jobID string) error

    // Size 返回 pending 状态的任务数量。
    Size(ctx context.Context) (int, error)
}
```

### 3.2 错误类型

```go
var (
    ErrJobNotFound   = errors.New("job not found")
    ErrJobDuplicate  = errors.New("job ID already exists")
    ErrJobNotPending = errors.New("job is not in pending status")
    ErrJobNotRunning = errors.New("job is not in running status")
)
```

### 3.3 实现约定

1. **线程安全**：所有方法必须 goroutine-safe
2. **Context 响应**：方法实现应尊重 `ctx.Done()`
3. **幂等性**：`Enqueue` 重复 ID 报错；`Ack`/`Nack` 非 running 报错
4. **优先级**：排序键为 `(effective_priority DESC, created_at ASC)`，参见第 6 节
5. **Dequeue 原子性**：取出 + 状态变更必须是原子操作

---

## 4. Handler

```go
// scheduler/handler.go

// Handler 执行任务，返回结果 JSON。
// ctx 继承 scheduler 生命周期 & job.MaxRunTime 超时。
type Handler func(ctx context.Context, job *Job) (json.RawMessage, error)
```

Handler 定义在 `scheduler` 包，通过 `WithHandler(name, fn)` 注册到 Scheduler。`job.HandlerName` 决定执行时查找哪个 handler。

---

## 5. Scheduler 设计

### 5.1 结构体

```go
// scheduler/scheduler.go

type Scheduler struct {
    queue    Queue
    handlers map[string]Handler
    workers  int

    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
    active atomic.Int64
}
```

### 5.2 构造函数与选项

```go
type SchedulerOption func(*Scheduler)

// WithHandler 注册 handler，名称与 job.HandlerName 对应
func WithHandler(name string, h Handler) SchedulerOption

// WithWorkerCount 设置 worker 协程数，默认 1
func WithWorkerCount(n int) SchedulerOption

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
        errMsg := "handler not found: " + job.HandlerName
        log.Printf("[scheduler] %s for job %s", errMsg, job.ID)
        s.queue.Nack(s.ctx, job.ID, errMsg)
        return
    }

    // 3. 执行 handler
    result, err := handler(execCtx, job)

    // 4. 更新队列状态
    if err != nil {
        s.queue.Nack(s.ctx, job.ID, err.Error())
        job.Status = StatusFailed
        job.Error = err.Error()
    } else {
        s.queue.Ack(s.ctx, job.ID, result)
        job.Status = StatusCompleted
        job.Result = result
    }
}
```

### 5.6 Submit 便捷方法

```go
// Submit 将 Job 入队。等价于直接调用 Queue.Enqueue。
func (s *Scheduler) Submit(ctx context.Context, job *Job) error
```

---

## 6. 防饥饿策略

### 7.1 问题

纯优先级排序下，高优先级任务持续涌入会导致低优先级任务永远得不到执行。

### 7.2 方案：Aging（优先级老化）

```
effective_priority = priority + (wait_duration_seconds × aging_factor)

wait_duration = now - created_at
aging_factor  = 队列实现参数 (默认 0.01/s)
```

### 7.3 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `aging_factor` | 0.01 | 每秒优先级增量 |
| `max_effective_priority` | `math.MaxInt` | 上限 |

Dequeue 排序键：`(effective_priority DESC, created_at ASC)`

---

## 7. Crash Recovery

### 8.1 已知限制

`Dequeue()` 将 job 状态变为 `running` 后，若 worker 崩溃，job 卡在 `running` 状态无法恢复。

### 8.2 影响

| 场景 | 影响 |
|------|------|
| 内存队列 | 进程崩溃后 running 状态丢失 |
| Redis/MySQL | running job 数据在但无法自动恢复 |
| Worker goroutine panic | 同进程可通过 recover 处理 |

### 8.3 演进路径

v2 增加 optional 方法：

```go
Claim(ctx, timeout, lease) (*Job, error)       // 替代 Dequeue，支持租约
Renew(ctx, jobID, lease) error                 // 续租
ReapStaleJobs(ctx, maxLease) ([]string, error) // 回收过期 running 任务
```

---

## 8. 内存队列实现（参考实现）

### 9.1 核心结构

```go
// queue/memory/queue.go

type MemoryQueue struct {
    mu          sync.Mutex
    jobs        map[string]*Job
    heap        *priorityHeap
    cond        *sync.Cond
    closed      bool
    agingFactor float64
}

func NewMemoryQueue() *MemoryQueue
```

### 9.2 实现要点

| 方法 | 策略 |
|------|------|
| `Enqueue` | 检查重复 → 写入 map → `heap.Push` → `cond.Signal()` |
| `Dequeue` | 加锁 → cond.Wait 直到有 item → 计算有效优先级 → Pop → 返回 `*Job` |
| `Ack` | 加锁 → 校验 status=running → 更新 Job |
| `Nack` | 同 Ack |
| `Get` | 加锁 → map 查找 |
| `Cancel` | 加锁 → 校验 status=pending → 更新 status |
| `Size` | 加锁 → 统计 pending |

### 9.3 优先级堆

```go
func (h *priorityHeap) Less(i, j int) bool {
    epI := h.effectivePriority(h.items[i])
    epJ := h.effectivePriority(h.items[j])
    if epI != epJ {
        return epI > epJ
    }
    return h.items[i].CreatedAt.Before(h.items[j].CreatedAt)
}
```

---

## 9. 使用示例

### 9.1 基本用法

```go
q := memory.NewMemoryQueue()
client := comfyui.NewClient("http://127.0.0.1:8188")

s := scheduler.NewScheduler(q,
    scheduler.WithHandler("comfyui.generate", func(ctx context.Context, job *queue.Job) (json.RawMessage, error) {
        var wf map[string]any
        json.Unmarshal(job.Payload, &wf)
        result, err := client.GenerateImage(ctx, wf)
        if err != nil {
            return nil, err
        }
        return json.Marshal(result)
    }),
    scheduler.WithWorkerCount(2),
)

go s.Start(ctx)
defer s.Stop()

// 提交任务
job := &queue.Job{
    ID:          "job-001",
    HandlerName: "comfyui.generate",
    Priority:    10,
    Payload:     payload,
}
s.Submit(ctx, job)
```

### 9.2 多 handler 注册

```go
s := scheduler.NewScheduler(q,
    scheduler.WithHandler("comfyui.generate", generateHandler),
    scheduler.WithHandler("image.resize",    resizeHandler),
    scheduler.WithHandler("thumbnail",       thumbHandler),
    scheduler.WithWorkerCount(4),
)
```

### 9.3 查询结果

```go
s := scheduler.NewScheduler(q, scheduler.WithWorkerCount(1))

job, _ := q.Get(ctx, "job-001")
if job.Status == queue.StatusCompleted {
    log.Printf("result: %s", job.Result)
}
```

---

## 10. 未来 Backend 扩展

### 10.1 Redis (`queue/redis`)

| 方法 | 实现 |
|------|------|
| `Enqueue` | `HSET job:{id}` + `ZADD queue:pending {score} {id}` |
| `Dequeue` | Lua: `ZPOPMAX` → `HSET status=running` → return Job |
| `Ack` | `HSET status=completed result=...` |
| `Get` | `HGETALL job:{id}` |
| `Size` | `ZCARD queue:pending` |

Job 全部字段（含 `handler_name`）存入 Hash，Handler 通过 Scheduler 的注册表解析。

### 10.2 MySQL (`queue/mysql`)

```sql
CREATE TABLE queue_jobs (
    id           VARCHAR(64) PRIMARY KEY,
    handler_name VARCHAR(64) NOT NULL,
    priority     INT NOT NULL DEFAULT 0,
    payload      JSON NOT NULL,
    status       ENUM('pending','running','completed','failed','cancelled') NOT NULL,
    result       JSON,
    error        TEXT,
    worker_id    VARCHAR(64) DEFAULT '',
    created_at   DATETIME(3) NOT NULL,
    started_at   DATETIME(3),
    done_at      DATETIME(3),
    max_run_time_ms BIGINT DEFAULT 0,
    INDEX idx_dequeue (status, priority DESC, created_at ASC)
);
```

Dequeue:
```sql
SELECT * FROM queue_jobs
WHERE status = 'pending'
ORDER BY (priority + TIMESTAMPDIFF(SECOND, created_at, NOW()) * 0.01) DESC, created_at ASC
LIMIT 1 FOR UPDATE SKIP LOCKED;
```

---

## 11. 依赖项

```
标准库 (sync, container/heap, context, encoding/json)
github.com/google/uuid  (已有 — 生成 Job ID)
```

---

## 附录 A：类型速查

| 类型 | 定义 | 说明 |
|------|------|------|
| `Job` | `struct` — 纯数据 | Queue 存储单位，可序列化 |
| `Handler` | `func(ctx, *Job) (json.RawMessage, error)` | 执行函数，通过 WithHandler 注册 |
| `Queue` | `interface` — 7 methods | 队列抽象 |

## 附录 B：Queue 方法速查

| 方法 | 入参 | 出参 |
|------|------|------|
| `Enqueue` | `*Job` | `error` |
| `Dequeue` | `time.Duration` | `(*Job, error)` |
| `Ack` | `string, json.RawMessage` | `error` |
| `Nack` | `string, string` | `error` |
| `Get` | `string` | `(*Job, error)` |
| `Cancel` | `string` | `error` |
| `Size` | — | `(int, error)` |

## 附录 C：状态转换矩阵

| 操作 | pending | running | completed | failed | cancelled |
|------|---------|---------|-----------|--------|-----------|
| Enqueue | ✅ | | | | |
| Dequeue | ✅ | | | | |
| Ack | | ✅ | | | |
| Nack | | ✅ | | | |
| Cancel | ✅ | | | | |
