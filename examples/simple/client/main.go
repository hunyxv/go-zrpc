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

func main() {
	log.Println("Connecting to server...")
	
	// 创建客户端 - 使用 127.0.0.1
	cli, err := zrpc.NewClient("tcp://127.0.0.1:8080")
	if err != nil {
		log.Fatal("Failed to create client:", err)
	}
	defer cli.Close()
	
	log.Println("Client created successfully")

	// 直接调用 RPC（不使用代理）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &HelloRequest{Name: "World"}
	resp := &HelloResponse{}

	log.Println("Calling SayHello...")
	if err := cli.Call(ctx, "Greeter", "SayHello", req, resp); err != nil {
		log.Fatal("RPC failed:", err)
	}

	fmt.Println("Response:", resp.Message)
}
