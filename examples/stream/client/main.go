package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
)

// StreamRequest 流式请求
type StreamRequest struct {
	Count int `msgpack:"count"`
}

// StreamResponse 流式响应
type StreamResponse struct {
	Index   int    `msgpack:"index"`
	Message string `msgpack:"message"`
}

// StreamerProxy 代理结构体 - 使用与服务端一致的签名
type StreamerProxy struct {
	ServerStream func(ctx context.Context, req *StreamRequest) (zrpc.Stream, error)
}

func main() {
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8081")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 创建代理对象
	proxy := &StreamerProxy{}

	// 装饰代理
	if err := cli.Decorator("Streamer", proxy, 0); err != nil {
		log.Fatal(err)
	}

	// 流式调用 - 使用同名方法调用
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := proxy.ServerStream(ctx, &StreamRequest{Count: 10})
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	// 接收流式响应
	for {
		var resp StreamResponse
		if err := stream.Recv(&resp); err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatal(err)
		}
		fmt.Printf("Received: Index=%d, Message=%s\n", resp.Index, resp.Message)
	}

	fmt.Println("Stream completed")
}
