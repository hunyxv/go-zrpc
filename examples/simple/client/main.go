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
	SayHello(ctx context.Context, name string) (string, error)
}

// GreeterProxy 代理结构体
type GreeterProxy struct {
	SayHello func(ctx context.Context, name string) (string, error)
}

func main() {
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 创建代理对象
	proxy := &GreeterProxy{}

	// 装饰代理 - 将函数字段替换为 RPC 调用
	if err := cli.Decorator("Greeter", proxy, 0); err != nil {
		log.Fatal(err)
	}

	// 调用 RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := proxy.SayHello(ctx, "World")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Response:", resp)
}
