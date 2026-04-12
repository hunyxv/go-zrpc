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

// GreeterProxy 代理结构体 - 方法签名必须与服务端一致
type GreeterProxy struct {
	SayHello func(ctx context.Context, req *HelloRequest) (*HelloResponse, error)
}

func main() {
	log.Println("Connecting to server...")
	
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8080")
	if err != nil {
		log.Fatal("Failed to create client:", err)
	}
	defer cli.Close()
	
	log.Println("Client created successfully")

	// 创建代理对象
	proxy := &GreeterProxy{}

	// 装饰代理 - 将函数字段替换为 RPC 调用
	log.Println("Decorating proxy...")
	if err := cli.Decorator("Greeter", proxy, 0); err != nil {
		log.Fatal("Failed to decorate:", err)
	}
	log.Println("Proxy decorated successfully")

	// 调用 RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Println("Calling SayHello...")
	resp, err := proxy.SayHello(ctx, &HelloRequest{Name: "World"})
	if err != nil {
		log.Fatal("RPC failed:", err)
	}

	fmt.Println("Response:", resp.Message)
}
