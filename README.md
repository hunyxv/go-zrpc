# go-zrpc

一个基于 ZeroMQ 的高性能 RPC 框架，支持插件式中间件架构。

## 特性

- **四种调用模式**：请求-响应、流式请求、流式响应、双向流式
- **插件式中间件**：日志、认证、恢复、链路追踪、指标收集
- **服务发现**：支持 etcd、Consul、ZooKeeper
- **高性能**：基于 ZeroMQ 和 msgpack 序列化
- **流式支持**：原生支持双向流式通信
- **链路追踪**：内置 OpenTelemetry 支持

## 快速开始

### 安装

```bash
go get github.com/example/go-zrpc
```

### 服务端示例

```go
package main

import (
    "context"
    "log"
    
    "github.com/example/go-zrpc"
    "github.com/example/go-zrpc/middleware"
)

// 定义服务接口
type Greeter interface {
    SayHello(ctx context.Context, name string) (string, error)
}

// 服务实现
type GreeterImpl struct{}

func (g *GreeterImpl) SayHello(ctx context.Context, name string) (string, error) {
    return "Hello " + name + "!", nil
}

func main() {
    // 创建服务端
    srv := zrpc.NewServer(
        zrpc.WithAddress(":8080"),
    )
    
    // 注册中间件
    srv.Use(middleware.Recovery())
    srv.Use(middleware.Logging())
    
    // 注册服务
    srv.Register("Greeter", &GreeterImpl{}, (*Greeter)(nil))
    
    // 启动服务
    log.Fatal(srv.Run())
}
```

### 客户端示例

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/example/go-zrpc"
)

type Greeter interface {
    SayHello(ctx context.Context, name string) (string, error)
}

func main() {
    // 创建客户端
    cli, err := zrpc.NewClient("tcp://localhost:8080")
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()
    
    // 创建代理
    var greeter Greeter
    cli.Proxy("Greeter", &greeter)
    
    // 调用 RPC
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    resp, err := greeter.SayHello(ctx, "World")
    if err != nil {
        log.Fatal(err)
    }
    log.Println(resp) // Hello World!
}
```

## 中间件

### 内置中间件

```go
// 恢复中间件 - 捕获 panic
srv.Use(middleware.Recovery())

// 日志中间件
srv.Use(middleware.Logging())

// 认证中间件
srv.Use(middleware.Auth(func(ctx context.Context, token string) error {
    // 验证 token
    return nil
}))

// 链路追踪中间件
srv.Use(middleware.Tracing())

// 指标中间件
srv.Use(middleware.Metrics())
```

### 自定义中间件

```go
func MyMiddleware() zrpc.Middleware {
    return func(next zrpc.Handler) zrpc.Handler {
        return func(ctx context.Context, req interface{}) (interface{}, error) {
            // 前置处理
            log.Println("before request")
            
            resp, err := next(ctx, req)
            
            // 后置处理
            log.Println("after request")
            
            return resp, err
        }
    }
}

srv.Use(MyMiddleware())
```

## 流式 RPC

### 流式响应

```go
type Streamer interface {
    StreamData(ctx context.Context, req *Request, stream zrpc.Stream) error
}

func (s *StreamerImpl) StreamData(ctx context.Context, req *Request, stream zrpc.Stream) error {
    for i := 0; i < 10; i++ {
        if err := stream.Send(&Data{Value: i}); err != nil {
            return err
        }
    }
    return nil
}
```

### 双向流式

```go
type BidirectionalStreamer interface {
    Chat(ctx context.Context, stream zrpc.BidiStream) error
}

func (s *Impl) Chat(ctx context.Context, stream zrpc.BidiStream) error {
    for {
        msg, err := stream.Recv()
        if err == io.EOF {
            return nil
        }
        if err != nil {
            return err
        }
        
        // 处理并响应
        if err := stream.Send(&Response{...}); err != nil {
            return err
        }
    }
}
```

## 服务发现

### etcd

```go
import "github.com/example/go-zrpc/registry"

// 服务端注册
reg, err := registry.NewEtcdRegistry([]string{"localhost:2379"})
srv := zrpc.NewServer(
    zrpc.WithRegistry(reg),
)

// 客户端发现
disc, err := registry.NewEtcdDiscovery([]string{"localhost:2379"})
cli, err := zrpc.NewClientWithDiscovery(disc, "service-name")
```

### Consul

```go
reg, err := registry.NewConsulRegistry("localhost:8500")
disc, err := registry.NewConsulDiscovery("localhost:8500")
```

## 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Proxy     │  │ Load Balance│  │   Retry/Circuit     │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
└─────────┼────────────────┼────────────────────┼─────────────┘
          │                │                    │
┌─────────┼────────────────┼────────────────────┼─────────────┐
│         │           Middleware Chain            │             │
│  ┌──────┴────────────────┴────────────────────┴──────┐      │
│  │              Logging/Auth/Tracing/Metrics          │      │
│  └──────────────────────┬─────────────────────────────┘      │
│                         │                                    │
│  ┌──────────────────────┴─────────────────────────────┐      │
│  │              Codec (msgpack/json/...)              │      │
│  └──────────────────────┬─────────────────────────────┘      │
│                         │                                    │
│  ┌──────────────────────┴─────────────────────────────┐      │
│  │              ZeroMQ Transport Layer                │      │
│  └────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        Server Layer                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Router    │  │   Method    │  │   Reflection        │  │
│  │             │  │   Registry  │  │   Invoker           │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## License

MIT License