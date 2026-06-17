# 队列与调度接口设计

> Version: 2.0 | 2026-06-17 | 方案 A — Queue + Scheduler 双接口分离

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

### 1.3 核心抽象：Job 与 Work 分离

```
Job (纯数据，可序列化)              Work (运行时单元，不持久化)
┌──────────────────────────┐       ┌──────────────────────────┐
│ ID, Priority, Payload    │       │  Job     *Job            │
│ Status, Result, Error    │  +    │  Handler func(...) error │
│ CreatedAt, DoneAt        │       └──────────────────────────┘
└──────────────────────────┘
```

- **Job**：纯数据结构，所有字段可 JSON 序列化，是 Queue 存储和 Query 的基本单位
- **Work**：`{*Job, Handler}` 捆绑体，Queue 的 Enqueue/Dequeue 单位，仅在内存中传递

好处：Job 不依赖不可序列化的函数类型，未来切换到 Redis/MySQL 时 Job 结构无需改动；Handler 通过 Work 携带，不污染数据模型。

### 1.4 架构

```
                        ┌─────────────────────────────┐
                        │        调用方                │
                        └────────────┬────────────────┘
                                     │ Enqueue(work)   work = {Job, Handler}
                                     ▼
┌──────────────────────────────────────────────────────┐
│                    Queue 接口 (持久层)                 │
│  Enqueue(*Work) / Dequeue()→*Work                    │
│  Ack / Nack / Get / Cancel / Size                    │
└──────────────────────┬───────────────────────────────┘
                       │ Dequeue → *Work
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
│  │  work.Handler(ctx, work.Job)                 │   │
│  │     │         │              │               │   │
│  │     ▼         ▼              ▼               │   │
│  │  Ack/Nack(work.Job.ID) ───────────► Queue    │   │
│  │     │                                         │   │
│  │     ▼                                         │   │
│  │  OnJobComplete(ctx, work.Job)                 │   │
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
│   ├── work.go             #   Work = {Job, Handler} 运行时捆绑体
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

## 2. Job（纯数据）

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
    ID       string          `json:"id"`
    Priority int             `json:"priority"`
    Payload  json.RawMessage `json:"payload"`
    Status   JobStatus       `json:"status"`

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

**注意**：Job 不包含 Handler 字段。Handler 通过 Work 携带（见下一节）。

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

## 3. Work（运行时捆绑体）

### 3.1 定义

```go
// queue/work.go

// Work 是 Job 和 Handler 的捆绑体。Job 负责数据，Handler 负责执行。
// Queue.Enqueue/Dequeue 以 Work 为操作单位；Handler 不持久化。
type Work struct {
    Job     *Job
    Handler Handler
}
```

- 调用方构造 `Work{Job: job, Handler: fn}` 提交给 Queue
- Scheduler Dequeue 后直接调用 `work.Handler(ctx, work.Job)`
- 对于持久化后端（Redis/MySQL），Handler 仅在 Enqueue→Dequeue 期间由内存持有，不会序列化

### 3.2 与 Queue 的关系

```
Enqueue(*Work) ──► Queue 内部存储 Job 数据 + 临时持有 Handler
                          │
                          ▼ Dequeue() → *Work  （Job + Handler 一起返回）
```

---

## 4. Queue 接口

### 4.1 接口定义

```go
// queue/queue.go

type Queue interface {
    // Enqueue 提交 Work，Job.Status 自动设为 pending，Job.CreatedAt 自动设。
    // 若 Job.ID 已存在返回 ErrJobDuplicate。
    Enqueue(ctx context.Context, work *Work) error

    // Dequeue 按（有效优先级降序, 入队时间升序）阻塞取出 Work。
    // timeout=0 永久阻塞，timeout<0 非阻塞立即返回。
    // 无合适任务时返回 (nil, nil)。取出后 Job.Status 自动变为 running。
    Dequeue(ctx context.Context, timeout time.Duration) (*Work, error)

    // Ack 标记任务完成，写入 result。仅 running 状态有效。
    Ack(ctx context.Context, jobID string, result json.RawMessage) error

    // Nack 标记任务失败，写入错误原因。仅 running 状态有效。
    Nack(ctx context.Context, jobID string, reason string) error

    // Get 按 ID 查询任务（纯 Job），任意状态。不存在返回 (nil, ErrJobNotFound)。
    Get(ctx context.Context, jobID string) (*Job, error)

    // Cancel 取消 pending 任务。非 pending 返回 ErrJobNotPending。
    Cancel(ctx context.Context, jobID string) error

    // Size 返回 pending 状态的任务数量。
    Size(ctx context.Context) (int, error)
}
```

### 4.2 方法操作矩阵

| 方法 | 入参 | 出参 | 操作对象 |
|------|------|------|----------|
| `Enqueue` | `*Work` | `error` | Work（Job 数据 + Handler） |
| `Dequeue` | timeout | `(*Work, error)` | Work（Job 数据 + Handler） |
| `Ack` | jobID, result | `error` | Job 的状态/Result |
| `Nack` | jobID, reason | `error` | Job 的状态/Error |
| `Get` | jobID | `(*Job, error)` | Job 数据（不含 Handler） |
| `Cancel` | jobID | `error` | Job 的状态 |
| `Size` | — | `(int, error)` | pending Job 计数 |

### 4.3 错误类型

```go
// queue/queue.go

var (
    ErrJobNotFound   = errors.New("job not found")
    ErrJobDuplicate  = errors.New("job ID already exists")
    ErrJobNotPending = errors.New("job is not in pending status")
    ErrJobNotRunning = errors.New("job is not in running status")
)
```

### 4.4 实现约定

1. **线程安全**：所有方法必须 goroutine-safe
2. **Context 响应**：方法实现应尊重 `ctx.Done()`
3. **幂等性**：`Enqueue` 重复 ID 报错；`Ack`/`Nack` 非 running 报错
4. **优先级**：排序键为 `(effective_priority DESC, created_at ASC)`，参见第 8 节
5. **Dequeue 原子性**：取出 + 状态变更必须是原子操作

---

## 5. Handler

```go
// scheduler/handler.go

// Handler 执行任务，返回结果 JSON。
// ctx 继承 scheduler 生命周期 & job.MaxRunTime 超时。
type Handler func(ctx context.Context, job *Job) (json.RawMessage, error)
```

Handler 定义在 `scheduler` 包，通过 `Work` 携带到 Queue。Handler 不持久化——内存队列直接持有函数指针；持久化后端自行处理重建。

---

## 6. Scheduler 设计

### 6.1 结构体

```go
// scheduler/scheduler.go

type Scheduler struct {
    queue      Queue
    workers    int
    onComplete OnJobComplete

    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
    active atomic.Int64
}
```

### 6.2 构造函数与选项

```go
type SchedulerOption func(*Scheduler)

func WithWorkerCount(n int) SchedulerOption
func WithOnComplete(fn OnJobComplete) SchedulerOption
func WithDequeueTimeout(d time.Duration) SchedulerOption

func NewScheduler(queue Queue, opts ...SchedulerOption) *Scheduler
```

### 6.3 生命周期

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

### 6.4 Worker 循环

```go
func (s *Scheduler) workerLoop() {
    defer s.wg.Done()
    for {
        select {
        case <-s.ctx.Done():
            return
        default:
        }

        work, err := s.queue.Dequeue(s.ctx, s.dequeueTimeout)
        if err != nil {
            log.Printf("[scheduler] dequeue error: %v", err)
            continue
        }
        if work == nil {
            continue // 超时，无任务
        }

        s.active.Add(1)
        s.execute(work)
        s.active.Add(-1)
    }
}
```

### 6.5 单次执行流程

```go
func (s *Scheduler) execute(work *Work) {
    job := work.Job
    log.Printf("[scheduler] executing job %s", job.ID)

    // 1. 构建 job 级别 context（带 MaxRunTime 超时）
    execCtx := s.ctx
    if job.MaxRunTime > 0 {
        var cancel context.CancelFunc
        execCtx, cancel = context.WithTimeout(s.ctx, job.MaxRunTime)
        defer cancel()
    }

    // 2. 执行 handler
    result, err := work.Handler(execCtx, job)

    // 3. 更新队列状态
    if err != nil {
        s.queue.Nack(s.ctx, job.ID, err.Error())
        job.Status = StatusFailed
        job.Error = err.Error()
    } else {
        s.queue.Ack(s.ctx, job.ID, result)
        job.Status = StatusCompleted
        job.Result = result
    }

    // 4. 调用回调
    if s.onComplete != nil {
        s.onComplete(s.ctx, job)
    }
}
```

### 6.6 Submit 便捷方法

```go
// Submit 将 Work 入队。等价于直接调用 Queue.Enqueue。
func (s *Scheduler) Submit(ctx context.Context, work *Work) error
```

---

## 7. 回调

### 7.1 函数类型

```go
// scheduler/callback.go

type OnJobComplete func(ctx context.Context, job *Job)
```

### 7.2 设计要点

| 特性 | 说明 |
|------|------|
| 注入位置 | Scheduler 级别，通过 `WithOnComplete(fn)` 设置 |
| 调用时机 | Ack 或 Nack 成功后 |
| 调用线程 | 当前 worker goroutine |
| 入参 | `*Job`（含 Status/Result/Error 完成状态） |
| 默认值 | nil |

### 7.3 示例

```go
s := scheduler.NewScheduler(q,
    scheduler.WithOnComplete(func(ctx context.Context, job *queue.Job) {
        switch job.Status {
        case queue.StatusCompleted:
            log.Printf("job %s done: %s", job.ID, job.Result)
        case queue.StatusFailed:
            log.Printf("job %s failed: %s", job.ID, job.Error)
        }
    }),
)
```

---

## 8. 防饥饿策略

### 8.1 问题

纯优先级排序下，高优先级任务持续涌入会导致低优先级任务永远得不到执行。

### 8.2 方案：Aging（优先级老化）

```
effective_priority = priority + (wait_duration_seconds × aging_factor)

wait_duration = now - created_at
aging_factor  = 队列实现参数 (默认 0.01/s)
```

### 8.3 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `aging_factor` | 0.01 | 每秒优先级增量 |
| `max_effective_priority` | `math.MaxInt` | 上限 |

Dequeue 排序键：`(effective_priority DESC, created_at ASC)`

---

## 9. Crash Recovery

### 9.1 已知限制

`Dequeue()` 将 job 状态变为 `running` 后，若 worker 崩溃，job 卡在 `running` 状态无法恢复。

### 9.2 影响

| 场景 | 影响 |
|------|------|
| 内存队列 | 进程崩溃后所有 running 状态丢失 |
| Redis/MySQL | running job 数据仍在但无法自动恢复 |
| Worker goroutine panic | 同进程可通过 recover 处理 |

### 9.3 演进路径

v2 增加 optional 方法：

```go
Claim(ctx, timeout, lease) (*Work, error)      // 替代 Dequeue，支持租约
Renew(ctx, jobID, lease) error                 // 续租
ReapStaleJobs(ctx, maxLease) ([]string, error) // 回收过期 running 任务
```

---

## 10. 内存队列实现（参考实现）

### 10.1 核心结构

```go
// queue/memory/queue.go

type MemoryQueue struct {
    mu          sync.Mutex
    items       map[string]*Work      // jobID → Work
    heap        *priorityHeap
    cond        *sync.Cond
    closed      bool
    agingFactor float64
}

func NewMemoryQueue() *MemoryQueue
```

### 10.2 实现要点

| 方法 | 策略 |
|------|------|
| `Enqueue` | 检查重复 → 存 Work 到 items + heap → `cond.Signal()` |
| `Dequeue` | 加锁 → cond.Wait 直到有 item → 计算有效优先级 → Pop → 返回 Work |
| `Ack` | 加锁 → 校验 status=running → 更新 Job |
| `Nack` | 同 Ack |
| `Get` | 加锁 → 返回 `items[id].Job`（纯 Job，不含 Handler） |
| `Cancel` | 加锁 → 校验 status=pending → 更新 status |
| `Size` | 加锁 → 统计 pending |

### 10.3 优先级堆

```go
func (h *priorityHeap) Less(i, j int) bool {
    epI := h.effectivePriority(h.items[i].Job)
    epJ := h.effectivePriority(h.items[j].Job)
    if epI != epJ {
        return epI > epJ
    }
    return h.items[i].Job.CreatedAt.Before(h.items[j].Job.CreatedAt)
}
```

---

## 11. 使用示例

### 11.1 基本用法

```go
q := memory.NewMemoryQueue()
client := comfyui.NewClient("http://127.0.0.1:8188")

s := scheduler.NewScheduler(q,
    scheduler.WithWorkerCount(2),
    scheduler.WithOnComplete(func(ctx context.Context, job *queue.Job) {
        log.Printf("job %s: status=%s result=%s", job.ID, job.Status, job.Result)
    }),
)

go s.Start(ctx)
defer s.Stop()

// 提交任务 —— Handler 通过 Work 携带
work := &queue.Work{
    Job: &queue.Job{
        ID:       "job-001",
        Priority: 10,
        Payload:  payload,
    },
    Handler: func(ctx context.Context, job *queue.Job) (json.RawMessage, error) {
        var wf map[string]any
        json.Unmarshal(job.Payload, &wf)
        result, err := client.GenerateImage(ctx, wf)
        if err != nil {
            return nil, err
        }
        return json.Marshal(result)
    },
}
s.Submit(ctx, work)
```

### 11.2 无回调模式

```go
s := scheduler.NewScheduler(q, scheduler.WithWorkerCount(1))

job, _ := q.Get(ctx, "job-001")
if job.Status == queue.StatusCompleted {
    log.Printf("result: %s", job.Result)
}
```

---

## 12. 未来 Backend 扩展

### 12.1 与 Work 的关系

```
Memory:   Enqueue(Work) → 持有 Handler 指针
Redis:    Enqueue(Work) → 存 Job 到 Hash/ZSet, Handler 在内存中挂靠
MySQL:    Enqueue(Work) → INSERT Job, Handler 在内存中挂靠
```

持久化后端 Enqueue 时将 `work.Job` 持久化，`work.Handler` 暂存在内存的映射表中。Dequeue 时从映射表取出 Handler 重新组装 `Work`。

### 12.2 Redis (`queue/redis`)

| 方法 | 实现 |
|------|------|
| `Enqueue` | `HSET job:{id}` + `ZADD queue:pending {score} {id}` + 内存 map[id]Handler |
| `Dequeue` | Lua: `ZPOPMAX` → `HSET status=running` → 从内存 map 取 Handler 组装 Work |
| `Ack` | `HSET status=completed result=...` + 清理内存 map |
| `Get` | `HGETALL job:{id}` |

### 12.3 MySQL (`queue/mysql`)

```sql
CREATE TABLE queue_jobs (
    id          VARCHAR(64) PRIMARY KEY,
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

Dequeue:
```sql
SELECT * FROM queue_jobs
WHERE status = 'pending'
ORDER BY (priority + TIMESTAMPDIFF(SECOND, created_at, NOW()) * 0.01) DESC, created_at ASC
LIMIT 1 FOR UPDATE SKIP LOCKED;
```

---

## 13. 依赖项

新增依赖：

```
标准库 (sync, container/heap, context, encoding/json, net/http)
github.com/google/uuid  (已有 — 生成 Job ID)
```

---

## 附录 A：类型速查

| 类型 | 定义 | 说明 |
|------|------|------|
| `Job` | `struct` — 纯数据 | Queue 存储单位，可序列化 |
| `Work` | `struct {*Job, Handler}` | Enqueue/Dequeue 单位，运行时捆绑 |
| `Handler` | `func(ctx, *Job) (json.RawMessage, error)` | 执行函数 |
| `OnJobComplete` | `func(ctx, *Job)` | Scheduler 级别回调 |
| `Queue` | `interface` — 7 methods | 队列抽象 |

## 附录 B：Queue 方法速查

| 方法 | 入参 | 出参 |
|------|------|------|
| `Enqueue` | `*Work` | `error` |
| `Dequeue` | `time.Duration` | `(*Work, error)` |
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
