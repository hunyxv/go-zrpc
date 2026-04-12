package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/example/go-zrpc"
)

// StreamService is the streaming service
type StreamService struct{}

// StreamRequest is a streaming request
type StreamRequest struct {
	Data string `msgpack:"data"`
}

// StreamResponse is a streaming response
type StreamResponse struct {
	Result string `msgpack:"result"`
}

// ServerStream implements server-side streaming
func (s *StreamService) ServerStream(ctx context.Context, req *StreamRequest, stream *zrpc.Stream) error {
	// Send multiple responses
	for i := 0; i < 5; i++ {
		resp := &StreamResponse{
			Result: fmt.Sprintf("Server response %d: %s", i, req.Data),
		}
		
		data, err := zrpc.Marshal(resp)
		if err != nil {
			return err
		}
		
		if err := stream.SendData(data); err != nil {
			return err
		}
		
		time.Sleep(100 * time.Millisecond)
	}
	
	return stream.SendEnd()
}

// ClientStream implements client-side streaming
func (s *StreamService) ClientStream(ctx context.Context, stream *zrpc.Stream) (*StreamResponse, error) {
	var allData string
	
	for {
		var req StreamRequest
		if err := stream.Recv(&req); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		
		allData += req.Data + " "
		log.Printf("Received: %s", req.Data)
	}
	
	return &StreamResponse{
		Result: "Received: " + allData,
	}, nil
}

// BidirectionalStream implements bidirectional streaming
func (s *StreamService) BidirectionalStream(ctx context.Context, stream *zrpc.Stream) error {
	for {
		var req StreamRequest
		if err := stream.Recv(&req); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		
		resp := &StreamResponse{
			Result: "Echo: " + req.Data,
		}
		
		data, err := zrpc.Marshal(resp)
		if err != nil {
			return err
		}
		
		if err := stream.SendData(data); err != nil {
			return err
		}
	}
}

func main() {
	// Create and start server
	go func() {
		server := zrpc.NewServer()
		
		// Register streaming service
		desc := &zrpc.ServiceDesc{
			ServiceName: "StreamService",
			HandlerType: (*StreamService)(nil),
			Streams: []zrpc.StreamDesc{
				{
					StreamName:    "ServerStream",
					ServerStreams: true,
				},
				{
					StreamName:    "ClientStream",
					ClientStreams: true,
				},
				{
					StreamName:    "BidirectionalStream",
					ClientStreams: true,
					ServerStreams: true,
				},
			},
		}
		
		if err := server.RegisterService(desc, &StreamService{}); err != nil {
			log.Fatalf("Failed to register service: %v", err)
		}
		
		if err := server.Listen("tcp://localhost:5556"); err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}
		
		log.Println("Stream server started on tcp://localhost:5556")
		if err := server.Serve(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()
	
	// Wait for server to start
	time.Sleep(1 * time.Second)
	
	// Create client
	client := zrpc.NewClient()
	defer client.Close()
	
	// Test server streaming
	fmt.Println("=== Server Streaming ===")
	stream, err := client.NewStream(context.Background(), "StreamService", "ServerStream", zrpc.WithAddress("tcp://localhost:5556"))
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	
	// Send initial request
	req := &StreamRequest{Data: "Hello"}
	if err := stream.SendData(mustMarshal(req)); err != nil {
		log.Fatalf("Failed to send: %v", err)
	}
	
	// Receive responses
	for {
		var resp StreamResponse
		if err := stream.Recv(&resp); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Failed to receive: %v", err)
		}
		fmt.Printf("Received: %s\n", resp.Result)
	}
	
	stream.Close()
	fmt.Println("Streaming example completed")
}

func mustMarshal(v interface{}) []byte {
	data, err := zrpc.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
