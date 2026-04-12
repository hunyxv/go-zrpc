package main

import (
	"context"
	"log"

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

// CalculatorImpl 计算器服务实现
type CalculatorImpl struct{}

// Add 加法
func (c *CalculatorImpl) Add(ctx context.Context, req *AddRequest) (*AddResponse, error) {
	return &AddResponse{Result: req.A + req.B}, nil
}

// Multiply 乘法
func (c *CalculatorImpl) Multiply(ctx context.Context, req *MultiplyRequest) (*MultiplyResponse, error) {
	return &MultiplyResponse{Result: req.A * req.B}, nil
}

func main() {
	// 创建服务端
	srv := zrpc.NewServer(
		zrpc.WithAddress("tcp://0.0.0.0:8082"),
	)

	// 注册服务
	if err := srv.Register("Calculator", &CalculatorImpl{}, (*Calculator)(nil)); err != nil {
		log.Fatal(err)
	}

	log.Println("Calculator Server starting on :8082")
	if err := srv.Run(); err != nil {
		log.Fatal(err)
	}

	select {}
}
