package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
)

// Calculator 计算器服务接口
type Calculator interface {
	Add(ctx context.Context, req *AddRequest) (*AddResponse, error)
	Multiply(ctx context.Context, req *MultiplyRequest) (*MultiplyResponse, error)
}

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

func main() {
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8082")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 创建代理
	var calc Calculator
	if err := cli.Proxy("Calculator", &calc); err != nil {
		log.Fatal(err)
	}

	// 调用 RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试加法
	addResp, err := calc.Add(ctx, &AddRequest{A: 10, B: 20})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("10 + 20 = %d\n", addResp.Result)

	// 测试乘法
	mulResp, err := calc.Multiply(ctx, &MultiplyRequest{A: 6, B: 7})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("6 * 7 = %d\n", mulResp.Result)
}
