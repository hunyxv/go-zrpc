package main

import (
	"context"
	"fmt"
	"log"

	"github.com/example/go-zrpc"
)

// Streamer 流式服务接口
type Streamer interface {
	// ServerStream 服务端流式：客户端发送一个请求，服务端返回多个响应
	ServerStream(ctx context.Context, req *StreamRequest, stream zrpc.Stream) error
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

// StreamerImpl 流式服务实现
type StreamerImpl struct{}

// ServerStream 服务端流式实现
func (s *StreamerImpl) ServerStream(ctx context.Context, req *StreamRequest, stream zrpc.Stream) error {
	for i := 0; i < req.Count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp := &StreamResponse{
			Index:   i,
			Message: fmt.Sprintf("Message #%d", i),
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// 创建服务端
	srv := zrpc.NewServer(
		zrpc.WithAddress("tcp://0.0.0.0:8081"),
	)

	// 注册服务
	if err := srv.Register("Streamer", &StreamerImpl{}, (*Streamer)(nil)); err != nil {
		log.Fatal(err)
	}

	log.Println("Stream Server starting on :8081")
	if err := srv.Run(); err != nil {
		log.Fatal(err)
	}

	select {}
}
