# go-breaker 评测说明

`go-breaker` 是一个包含熔断器核心库、HTTP 中间件、管理 API、Web Dashboard 和演示服务的 Go 项目。项目没有第三方 Go 模块或前端构建依赖，Dashboard 静态资源通过 `embed.FS` 编译进二进制。

## 本机验证

```bash
go mod download
go build ./...
go test ./...
go test -race ./...
go run ./cmd/demo -addr :8080
```

服务启动后访问 `http://localhost:8080/`，健康检查地址为 `http://localhost:8080/api/health`。

## Docker 评测镜像

```bash
./build_benzhi_docker.sh go-breaker linux/arm64
./build_benzhi_docker.sh go-breaker linux/amd64
docker run --rm -it go-breaker:latest
```

容器保留完整 Go 工具链，进入容器后可离线执行：

```bash
go build ./...
go test ./...
go run ./cmd/demo -addr :8080
```
