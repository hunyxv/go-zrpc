package main

import (
	"context"
	"fmt"
	"log"
	"time"

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

func main() {
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 创建代理
	var greeter Greeter
	if err := cli.Proxy("Greeter", &greeter); err != nil {
		log.Fatal(err)
	}

	// 调用 RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := greeter.SayHello(ctx, &HelloRequest{Name: "World"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Response:", resp.Message)
}
