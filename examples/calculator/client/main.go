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

func main() {
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8082")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试加法
	addReq := &AddRequest{A: 10, B: 20}
	addResp := &AddResponse{}
	if err := cli.Call(ctx, "Calculator", "Add", addReq, addResp); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("10 + 20 = %d\n", addResp.Result)

	// 测试乘法
	mulReq := &MultiplyRequest{A: 6, B: 7}
	mulResp := &MultiplyResponse{}
	if err := cli.Call(ctx, "Calculator", "Multiply", mulReq, mulResp); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("6 * 7 = %d\n", mulResp.Result)
}
