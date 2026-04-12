package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
)

// AddRequest 加法请求
type AddRequest struct {
	A int `msgpack:"a"`
	B int `msgpack:"b"`
}

// AddResponse 加法响应
type AddResponse struct {
	Result int `msgpack:"result"`
}

// MultiplyRequest 乘法请求
type MultiplyRequest struct {
	A int `msgpack:"a"`
	B int `msgpack:"b"`
}

// MultiplyResponse 乘法响应
type MultiplyResponse struct {
	Result int `msgpack:"result"`
}

// CalculatorProxy 代理结构体
type CalculatorProxy struct {
	Add      func(ctx context.Context, req *AddRequest) (*AddResponse, error)
	Multiply func(ctx context.Context, req *MultiplyRequest) (*MultiplyResponse, error)
}

func main() {
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8082")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 创建代理对象
	proxy := &CalculatorProxy{}

	// 装饰代理
	if err := cli.Decorator("Calculator", proxy, 0); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试加法
	addResp, err := proxy.Add(ctx, &AddRequest{A: 10, B: 20})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("10 + 20 = %d\n", addResp.Result)

	// 测试乘法
	mulResp, err := proxy.Multiply(ctx, &MultiplyRequest{A: 6, B: 7})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("6 * 7 = %d\n", mulResp.Result)
}
