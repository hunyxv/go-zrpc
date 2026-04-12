package main

import (
	"context"
	"fmt"
	"log"

	"github.com/example/go-zrpc"
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
	// 创建服务端 - 使用 127.0.0.1
	srv := zrpc.NewServer(
		zrpc.WithAddress("tcp://127.0.0.1:8080"),
	)

	// 注册服务
	if err := srv.Register("Greeter", &GreeterImpl{}, (*Greeter)(nil)); err != nil {
		log.Fatal(err)
	}

	log.Println("Server starting on 127.0.0.1:8080")
	if err := srv.Run(); err != nil {
		log.Fatal(err)
	}

	// 阻塞
	select {}
}
