package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
)

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
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 直接调用 RPC（不使用代理）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &HelloRequest{Name: "World"}
	resp := &HelloResponse{}

	if err := cli.Call(ctx, "Greeter", "SayHello", req, resp); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Response:", resp.Message)
}
