package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
)

// Streamer 流式服务接口
type Streamer interface {
	ServerStream(ctx context.Context, req *StreamRequest) (*StreamResponse, error)
}

// StreamRequest 流式请求
type StreamRequest struct {
	Count int `msgpack:"count"`
}

// StreamResponse 流式响应
type StreamResponse struct {
	Index   int    `msgpack:"index"`
	Message string `msgpack:"message"`
}

func main() {
	// 创建客户端
	cli, err := zrpc.NewClient("tcp://localhost:8081")
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 流式调用
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := cli.StreamCall(ctx, "Streamer", "ServerStream", &StreamRequest{Count: 10})
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
