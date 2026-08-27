# go-breaker — Go 熔断降级组件

基于 Go 语言实现的熔断降级组件（Circuit Breaker & Degradation Component）。

以「熔断器库」为核心，提供三态状态机、基于滑动窗口的错误率统计、半开探测与信号量并发隔离，并配套 HTTP 中间件、监控/管理 API 与一个轻量 Web Dashboard 前端，可用于生产环境的服务治理场景（如微服务调用保护、第三方依赖保护）。

> 当前阶段：**可运行实现**。核心库、中间件、管理 API、Web Dashboard、演示应用和自动化测试均已完成。

---

## 1. 项目定位

| 项目 | 说明 |
| --- | --- |
| 项目名称 | go-breaker |
| 技术栈 | Go（标准库 net/http + 原生并发原语），前端为原生 HTML/CSS/JS |
| 核心能力 | 三态熔断状态机、滑动窗口错误率统计、半开探测、并发隔离、多级降级 |
| 配套能力 | 熔断器注册中心、动态配置、事件监听、指标采集、HTTP 中间件、监控/管理 API、Web Dashboard |
| 代码规模约束 | Go 代码（不含测试）约 **2,000 ~ 2,200 行**；Go 代码文件 **21 ~ 24 个** |

## 快速开始

```bash
# 运行测试与静态检查
make check

# 启动 Dashboard（默认 http://localhost:8080）
make run

# 构建二进制
make build

# 生成 tar.gz 发布包
make package
```

也可以直接运行：`go run ./cmd/demo -addr :8080`。

## 2. 核心功能特性

- **三态熔断状态机**：关闭（Closed）→ 打开（Open）→ 半开（Half-Open）→ 关闭/打开，状态迁移规则可配置。
- **滑动窗口错误率统计**：时间桶（Bucket）按秒划分，窗口内聚合成功/失败计数，实时计算错误率与请求量。
- **半开探测**：熔断冷却期结束后放行少量探测请求，按连续成功次数决定恢复或再次熔断。
- **并发隔离**：基于信号量的最大并发控制，超限请求快速失败，防止故障在调用链上放大。
- **多级降级策略**：熔断降级、并发超限降级、超时降级、失败降级，支持 Fallback 回调与默认值降级。
- **安全重试策略**：支持调用方定义可重试错误，按指数退避进行有限重试，且只记录最终结果。
- **熔断器注册中心**：按资源名（如 `getUser`、`payOrder`）注册与管理多个熔断器实例。
- **动态配置**：运行期调整阈值、冷却时间、窗口大小等参数，无需重启。
- **批量运维控制**：可对注册中心内全部熔断器下发部分配置，或一键清空运行指标。
- **事件与指标**：状态变更事件、请求结果事件、周期指标快照，可挂载自定义监听器。
- **HTTP 中间件**：一行接入标准库 `net/http` 服务。
- **监控/管理 API**：REST 风格接口，输出熔断器状态与指标（JSON）。
- **Web Dashboard**：单页前端，可视化状态、指标与滑动窗口，支持故障演示与手动控制。

## 3. 文档目录

| 文档 | 说明 |
| --- | --- |
| [docs/01-需求规格说明书.md](docs/01-需求规格说明书.md) | 背景、功能需求（FR-01 ~ FR-14）、非功能需求与业务逻辑详述 |
| [docs/02-总体架构设计.md](docs/02-总体架构设计.md) | 分层架构、模块划分、目录结构、关键数据流与设计决策 |
| [docs/03-核心算法设计.md](docs/03-核心算法设计.md) | 状态机转换表、滑动窗口算法、半开探测、并发隔离、降级策略 |
| [docs/04-API与接口设计.md](docs/04-API与接口设计.md) | 库公开 API（Go 签名）、配置结构、事件接口、HTTP API 契约 |
| [docs/05-前端界面设计.md](docs/05-前端界面设计.md) | Dashboard 布局、功能模块、交互流程与数据接口契约 |
| [docs/06-代码规模与文件规划.md](docs/06-代码规模与文件规划.md) | 22 个 Go 文件的行数预算表、规模控制策略与校验方法 |
| [docs/07-开发计划与测试方案.md](docs/07-开发计划与测试方案.md) | 里程碑、任务拆分、测试用例清单与验收标准 |

## 4. 目录结构

```
go-breaker/
├── go.mod                  # module go-breaker
├── README.md
├── docs/                   # 项目文档（当前阶段产物）
├── breaker/                # 熔断器核心库（14 个文件）
│   ├── breaker.go          # 熔断器主类型与请求执行入口
│   ├── state.go            # 状态定义与迁移
│   ├── config.go           # 配置结构
│   ├── options.go          # 构造选项（Option 模式）
│   ├── errors.go           # 错误类型定义
│   ├── bucket.go           # 时间桶
│   ├── window.go           # 滑动窗口接口
│   ├── sliding_window.go   # 滑动窗口实现
│   ├── semaphore.go        # 并发隔离信号量
│   ├── result.go           # 结果记录与分类
│   ├── metrics.go          # 指标聚合
│   ├── event.go            # 事件定义与监听器
│   ├── registry.go         # 熔断器注册中心
│   └── fallback.go         # 降级策略
├── middleware/             # 集成层（2 个文件）
│   ├── middleware.go       # HTTP 中间件
│   └── http.go             # 请求分类与错误提取辅助
├── server/                 # 服务层（4 个文件）
│   ├── server.go           # HTTP 服务装配
│   ├── handlers.go         # API 处理函数
│   ├── metrics.go          # 指标快照输出
│   └── api_models.go       # API 数据结构
├── cmd/demo/               # 示例应用（2 个文件）
│   ├── main.go             # 演示服务入口
│   └── mock.go             # 模拟上游服务
└── web/                    # 前端（不计入 Go 规模）
    ├── index.html          # Dashboard 页面
    ├── style.css           # 样式
    └── app.js              # 交互逻辑
```

## 5. 快速开始（规划）

> 以下为设计规划，命令在实现阶段生效。

```bash
# 1. 启动演示服务（含 Dashboard 与模拟上游）
go run ./cmd/demo

# 2. 打开 Dashboard
open http://localhost:8080

# 3. 查看熔断器列表与指标
curl http://localhost:8080/api/breakers
curl http://localhost:8080/api/metrics

# 对全部熔断器下发部分配置，或清空全部运行指标
curl -X PUT http://localhost:8080/api/breakers/config -d '{"max_concurrency":64}'
curl -X POST http://localhost:8080/api/breakers/reset

# 4. 向模拟服务持续注入失败，观察熔断器从 Closed → Open → Half-Open
curl "http://localhost:8080/api/demo/simulate?name=payOrder&failRate=80&seconds=15"
```

## 6. 核心概念速览

| 概念 | 说明 |
| --- | --- |
| 资源（Resource） | 被保护的调用目标，以字符串名称标识，如 `getUser` |
| 熔断器（Breaker） | 某个资源对应的熔断保护单元，持有独立的状态机与滑动窗口 |
| 状态（State） | Closed / Open / Half-Open 三态 |
| 滑动窗口（Sliding Window） | 按时间桶组织的最近 N 秒请求统计 |
| 错误率（Error Rate） | 窗口内 `失败数 / (成功数 + 失败数)` |
| 半开探测（Probe） | Open 冷却结束后放行的受限探测请求 |
| 并发隔离（Isolation） | 信号量限制同一资源的最大并发执行数 |
| 降级（Fallback） | 拒绝/异常时返回的兜底结果或替代执行逻辑 |

## 7. 规模约束自检

- Go 代码行数（不含 `_test.go`）：预算 **2,115 行**，区间 **[2,000, 2,200]** ✔
- Go 代码文件数：**22 个**，区间 **(20, 25)** ✔
- 前端：3 个静态文件（HTML/CSS/JS），不参与 Go 规模统计 ✔

详细预算见 [docs/06-代码规模与文件规划.md](docs/06-代码规模与文件规划.md)。
