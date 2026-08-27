# 04 API 与接口设计

本文档定义两类接口：**库 API**（Go 公开签名，供业务代码与中间件调用）与 **HTTP API**（监控/管理，供前端与运维调用）。文档中的签名是设计约定，实现阶段保持兼容。

## 1. 包结构与公开符号

```
breaker（核心库）       middleware（集成层）     server（服务层）
  Breaker                 Handler / Options       Server / New / Run
  Registry                WithResourceFunc        路由见 §4
  Config / Option         WithTimeout
  State / Snapshot        WithFallback
  Event / EventBus        WithDefaultBody
  Metrics / Semaphore     WithStateHeader
  Fallback / FallbackFunc
  Execute / ExecuteWithResult
```

## 2. 库 API（Go）

### 2.1 状态与结果

```go
type State int32

const (
    StateClosed   State = iota // 关闭：正常放行
    StateOpen                  // 打开：熔断拒绝
    StateHalfOpen              // 半开：探测中
)

func (s State) String() string          // "closed" / "open" / "half_open"
func (s State) Color() string           // 前端配色辅助："green" / "red" / "yellow"

type ResultType int32

const (
    ResultSucceeded          ResultType = iota // 成功
    ResultFailed                               // 调用失败（业务错误）
    ResultTimeout                              // 超时
    ResultRejectedByBreaker                    // 熔断拒绝
    ResultRejectedByConcurrency                // 并发拒绝
)

type Result struct {
    Type      ResultType
    Err       error
    LatencyMs int64
    StartedAt time.Time
}
```

### 2.2 熔断器主入口

```go
// Execute 在熔断保护下执行 fn，返回其结果或降级结果/错误。
func (b *Breaker) Execute(ctx context.Context, fn func(ctx context.Context) (interface{}, error)) (interface{}, error)

// ExecuteWithResult 额外返回请求结果明细（含拒绝原因），供中间件记录响应头。
func (b *Breaker) ExecuteWithResult(ctx context.Context, fn func(ctx context.Context) (interface{}, error)) (interface{}, *Result, error)

// 只读查询
func (b *Breaker) State() State
func (b *Breaker) Snapshot() Snapshot
func (b *Breaker) Config() Config

// 管理操作
func (b *Breaker) UpdateConfig(cfg Config) error   // 动态配置
func (b *Breaker) Reset()                          // 重置窗口与累计指标
func (b *Breaker) ForceState(s State)              // 手动切换状态（演示/测试）
func (b *Breaker) TriggerProbe() bool              // 手动触发一次探测（Half-Open 下有效）
```

### 2.3 配置

```go
type Config struct {
    WindowSize        int           // 窗口桶数，默认 10
    BucketDuration    time.Duration // 桶时长，默认 1s
    ErrorThreshold    float64       // 错误率阈值，默认 0.5
    MinRequests       int64         // 最小请求数，默认 5
    SleepWindow       time.Duration // 冷却时间，默认 5s
    ProbeCount        int           // 半开探测并发，默认 1
    SuccessThreshold  int           // 连续成功恢复阈值，默认 1
    MaxConcurrency    int           // 最大并发，默认 100
    AcquireTimeout    time.Duration // 信号量获取超时，默认 0（不等待）
    CallTimeout       time.Duration // 调用超时，默认 3s
    Fallback          Fallback      // 降级策略（默认 ReturnErr）
    Retry             RetryPolicy   // 重试策略（默认不重试）
    EnableResultEvent bool          // 是否发布单请求事件，默认 false
    MetricSnapshotSec int           // 周期指标快照间隔，默认 5
}

func DefaultConfig() Config
func (c Config) Validate() error    // 阈值 ∈ [0,1]、窗口 ≥ 1、并发 ≥ 1 等
```

### 2.3.1 重试策略

```go
type RetryPredicate func(error) bool

type RetryPolicy struct {
    MaxAttempts  int           // 总尝试次数，含首次调用；默认 1
    InitialDelay time.Duration // 首次重试等待时间
    MaxDelay     time.Duration // 退避等待上限，0 表示不设上限
    Multiplier   float64       // 退避倍数，最小 1
    Retryable    RetryPredicate // 必须由调用方判断错误是否可安全重试
}

func DefaultRetryPolicy() RetryPolicy
func WithRetryPolicy(policy RetryPolicy) Option
```

重试仅适用于幂等或已实现去重的业务操作；每次 `Execute` 无论内部尝试多少次，滑动窗口只记录最终结果。

### 2.4 构造选项（Option 模式）

```go
type Option func(*options)

func WithConfig(cfg Config) Option
func WithErrorThreshold(v float64) Option
func WithWindowSize(n int) Option
func WithBucketDuration(d time.Duration) Option
func WithSleepWindow(d time.Duration) Option
func WithMinRequests(n int64) Option
func WithProbeCount(n int) Option
func WithSuccessThreshold(n int) Option
func WithMaxConcurrency(n int) Option
func WithCallTimeout(d time.Duration) Option
func WithAcquireTimeout(d time.Duration) Option
func WithFallback(fb Fallback) Option
func WithFallbackFunc(fn FallbackFunc) Option
func WithFallbackValue(v interface{}) Option
func WithEventListener(fn func(Event)) Option
func WithResultEvents(enabled bool) Option
```

### 2.5 注册中心

```go
type Registry struct{ /* 私有字段 */ }

func NewRegistry() *Registry

func (r *Registry) Get(name string) (*Breaker, bool)
func (r *Registry) GetOrCreate(name string, opts ...Option) *Breaker
func (r *Registry) List() []*Breaker
func (r *Registry) Remove(name string) bool
func (r *Registry) ResetAll()
func (r *Registry) UpdateConfigAll(cfg Config) error
func (r *Registry) Subscribe(fn func(Event))
```

### 2.6 信号量

```go
type Semaphore struct{ /* 私有字段 */ }

func NewSemaphore(max int) *Semaphore
func (s *Semaphore) Acquire(timeout time.Duration) bool
func (s *Semaphore) TryAcquire() bool
func (s *Semaphore) Release()
func (s *Semaphore) Count() int64    // 当前占用
func (s *Semaphore) Max() int
```

### 2.7 降级策略

```go
type FallbackType int32

const (
    FallbackReturnErr    FallbackType = iota // 直接返回错误
    FallbackDefaultValue                     // 返回默认值
    FallbackCustomFunc                       // 执行自定义兜底函数
)

type Reason int32

const (
    ReasonBreakerOpen      Reason = iota // 熔断
    ReasonConcurrencyLimit               // 并发隔离
    ReasonTimeout                        // 超时
    ReasonCallFailed                     // 调用失败
)

type Fallback struct {
    Type    FallbackType
    Value   interface{}
    Func    FallbackFunc
}

func (f Fallback) Execute(reason Reason, res *Result) (interface{}, error)
```

### 2.8 事件

```go
type EventType int32 // EventStateChanged / EventRequestResult / EventBreakerCreated / EventBreakerRemoved / EventConfigChanged / EventMetricSnapshot

type Event struct {
    Type     EventType
    Resource string
    Time     time.Time
    Data     interface{}
}

type Listener func(Event)

func (b *Breaker) Subscribe(fn Listener)
func (b *Breaker) SubscribeType(t EventType, fn Listener)
func (b *Breaker) RecentEvents(n int) []Event
```

### 2.9 指标快照

```go
type Snapshot struct {
    Resource    string
    State       State
    Config      Config
    Window      WindowSnapshot     // 窗口聚合 + 桶明细
    Metrics     MetricsSnapshot    // 累计指标
    OpenedAt    time.Time          // 最近熔断时间
    StateChangedAt time.Time
}

type WindowSnapshot struct {
    Total      int64
    Succeeded  int64
    Failed     int64
    Timeouts   int64
    RejectedC  int64
    RejectedB  int64
    ErrorRate  float64
    Buckets    []BucketSnapshot    // 按时间正序，供前端柱状图
}

type BucketSnapshot struct {
    StartAt    time.Time
    Succeeded  int64
    Failed     int64
    Timeouts   int64
    RejectedC  int64
    RejectedB  int64
}

type MetricsSnapshot struct {
    TotalRequests       int64
    Succeeded           int64
    Failed              int64
    Timeouts            int64
    RejectedByBreaker   int64
    RejectedByConcurrency int64
    ProbeSuccess        int64
    ProbeFailed         int64
    InFlight            int64
    AvgLatencyMs        float64
    P95LatencyMs        int64
}
```

### 2.10 错误定义

```go
var (
    ErrBreakerOpen         = errors.New("breaker: circuit breaker is open")
    ErrConcurrencyLimit    = errors.New("breaker: concurrency limit exceeded")
    ErrTimeout             = errors.New("breaker: call timeout")
    ErrCallFailed          = errors.New("breaker: call failed")
    ErrConfigInvalid       = errors.New("breaker: invalid config")
    ErrBreakerNotFound     = errors.New("breaker: not found")
    ErrFallbackFailed      = errors.New("breaker: fallback failed")
)

func IsBreakerOpen(err error) bool     // 错误分类辅助（errors.Is）
func IsRejected(err error) bool        // 是否为"拒绝类"错误（熔断/并发）
```

## 3. 中间件 API（middleware 包）

```go
// New 生成中间件；registry 提供熔断器实例。
func New(registry *breaker.Registry, opts ...Option) *Handler

type Handler struct{ /* 私有字段 */ }

func (h *Handler) Wrap(next http.Handler) http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) // 可作根 Handler

type Option func(*options)

// 资源名提取：默认取 r.URL.Path（去掉首斜杠）；可自定义。
func WithResourceFunc(fn func(r *http.Request) string) Option
func WithCallTimeout(d time.Duration) Option
func WithFallback(fb breaker.Fallback) Option
func WithDefaultBody(body []byte) Option          // 降级响应体，默认 "service degraded"
func WithStatusOnDegrade(code int) Option          // 降级响应状态码，默认 503
func WithStateHeader(enable bool) Option           // 输出 X-Breaker-State 响应头，默认 true
```

**中间件行为契约**：
- 请求分类：`2xx/3xx` 成功；`5xx` 失败；`4xx` 按配置（默认视为成功，可配置视为失败）；调用超时按超时处理。
- 降级时写 `WithStatusOnDegrade` 状态码与 `WithDefaultBody` 响应体，并附 `X-Breaker-State` / `X-Breaker-Reason` 响应头。

## 4. HTTP API（server 包）

统一前缀 `/api`，全部 JSON；错误响应格式：`{"error": "message"}`。

### 4.1 监控接口

| 方法 | 路径 | 说明 | 响应 |
| --- | --- | --- | --- |
| GET | `/api/breakers` | 熔断器列表（摘要） | `BreakerSummary[]` |
| GET | `/api/breakers/{name}` | 单实例详情（窗口桶明细 + 指标 + 配置） | `BreakerDetail` |
| GET | `/api/metrics` | 全局聚合指标 | `GlobalMetrics` |
| GET | `/api/events?since={ts}&limit={n}` | 事件列表（时间倒序） | `EventView[]` |
| GET | `/api/health` | 存活探针与熔断器状态汇总 | `HealthResponse` |

### 4.2 管理接口

| 方法 | 路径 | 说明 | 请求体 |
| --- | --- | --- | --- |
| PUT | `/api/breakers/{name}/config` | 动态更新配置 | `ConfigView`（部分字段可缺省） |
| POST | `/api/breakers/{name}/reset` | 重置窗口与指标 | — |
| PUT | `/api/breakers/config` | 对全部实例下发部分配置；每个实例未指定字段保持原值 | `ConfigView` |
| POST | `/api/breakers/reset` | 重置全部实例的窗口与累计指标，不改变状态 | — |
| POST | `/api/breakers/{name}/state` | 手动切换状态 | `{"state":"closed\|open\|half_open"}` |
| POST | `/api/breakers/{name}/probe` | 手动触发探测 | — |

### 4.3 演示接口（cmd/demo 提供）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/demo/simulate` | 注入故障：`{"resource":"payOrder","failRate":0.8,"latencyMs":200,"seconds":15}` |
| POST | `/api/demo/stop` | 停止全部注入 |
| GET | `/api/demo/status` | 当前注入状态 |

### 4.4 JSON 契约示例

**GET /api/breakers** → 200

```json
[
  {
    "resource": "payOrder",
    "state": "open",
    "state_changed_at": "2025-01-01T10:00:05Z",
    "error_rate": 0.83,
    "total": 120,
    "succeeded": 20,
    "failed": 100,
    "rejected": 60,
    "in_flight": 3,
    "sleep_window_ms": 5000
  }
]
```

**GET /api/breakers/payOrder** → 200

```json
{
  "resource": "payOrder",
  "state": "half_open",
  "config": {
    "window_size": 10,
    "bucket_duration_ms": 1000,
    "error_threshold": 0.5,
    "min_requests": 5,
    "sleep_window_ms": 5000,
    "probe_count": 1,
    "success_threshold": 1,
    "max_concurrency": 100,
    "call_timeout_ms": 3000
  },
  "window": {
    "total": 100, "succeeded": 30, "failed": 70,
    "timeouts": 10, "rejected_c": 12, "rejected_b": 40,
    "error_rate": 0.7,
    "buckets": [
      {"start": "2025-01-01T09:59:51Z", "succeeded": 2, "failed": 8, "timeouts": 1, "rejected_c": 1, "rejected_b": 0}
    ]
  },
  "metrics": {
    "total_requests": 1024, "succeeded": 800, "failed": 224,
    "timeouts": 30, "rejected_by_breaker": 120, "rejected_by_concurrency": 40,
    "probe_success": 3, "probe_failed": 2,
    "in_flight": 0, "avg_latency_ms": 45.2, "p95_latency_ms": 210
  },
  "opened_at": "2025-01-01T10:00:05Z"
}
```

**PUT /api/breakers/payOrder/config** 请求体 → 200

```json
{ "error_threshold": 0.6, "sleep_window_ms": 3000, "max_concurrency": 50 }
```

**GET /api/events** → 200

```json
[
  {"type": "state_changed", "resource": "payOrder", "time": "2025-01-01T10:00:05Z",
   "data": {"from": "closed", "to": "open", "reason": "error_rate_exceeded"}},
  {"type": "config_changed", "resource": "payOrder", "time": "2025-01-01T10:00:02Z",
   "data": {"key": "error_threshold", "old": "0.5", "new": "0.6"}}
]
```

### 4.5 状态码约定

| 场景 | 状态码 |
| --- | --- |
| 成功 | 200 |
| 参数不合法 | 400 |
| 资源不存在 | 404 |
| 配置非法 | 422 |
| 内部错误 | 500 |
| 业务请求被熔断/并发拒绝（经中间件） | 503（可配置） |

## 5. server 包公开符号

```go
type Server struct{ /* 私有字段 */ }

func New(registry *breaker.Registry, opts ...Option) *Server

type Option func(*options)
func WithAddr(addr string) Option                 // 默认 ":8080"
func WithStaticDir(dir string) Option             // 开发期从磁盘加载 web/
func WithEmbedFS(fs embed.FS, path string) Option // 生产期 embed 加载
func WithMetricsInterval(d time.Duration) Option  // 指标快照事件周期

func (s *Server) Handler() http.Handler           // 供测试/复用
func (s *Server) Run(ctx context.Context) error   // 阻塞启动，context 取消时优雅关闭
```

## 6. 兼容性约定

- 库 API 稳定：实现阶段不得改变本文档公开签名（可新增）。
- HTTP API 字段名使用 snake_case，时间统一 RFC3339 UTC。
- 配置缺省字段表示"不修改"（管理接口部分更新语义）。
