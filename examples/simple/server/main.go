package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
	"github.com/example/go-zrpc/middleware"
)

// Greeter 服务接口
type Greeter interface {
	SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error)
}

// HelloRequest 请求
type HelloRequest struct {
	Name string `msgpack:"name"`
}

// HelloResponse 响应
type HelloResponse struct {
	Message string `msgpack:"message"`
}

// GreeterImpl 服务实现
type GreeterImpl struct{}

// SayHello 实现
func (g *GreeterImpl) SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
	return &HelloResponse{
		Message: fmt.Sprintf("Hello, %s!", req.Name),
	}, nil
}

func main() {
	// 创建服务端
	srv := zrpc.NewServer(
		zrpc.WithAddress("tcp://0.0.0.0:8080"),
	)

	// 添加中间件
	srv.Use(middleware.Recovery())
	srv.Use(middleware.Logging())

	// 注册服务
	if err := srv.Register("Greeter", &GreeterImpl{}, (*Greeter)(nil)); err != nil {
		log.Fatal(err)
	}

	log.Println("Server starting on :8080")
	if err := srv.Run(); err != nil {
		log.Fatal(err)
	}

	// 等待中断
	time.Sleep(1 * time.Hour)
}
