package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
)

// GreeterService is the service interface
type GreeterService struct{}

// HelloRequest is the request for Hello
type HelloRequest struct {
	Name string `msgpack:"name"`
}

// HelloResponse is the response for Hello
type HelloResponse struct {
	Message string `msgpack:"message"`
}

// Hello implements the Greeter service
func (s *GreeterService) Hello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
	return &HelloResponse{
		Message: fmt.Sprintf("Hello, %s!", req.Name),
	}, nil
}

// GreeterServiceDesc is the service descriptor
var GreeterServiceDesc = &zrpc.ServiceDesc{
	ServiceName: "Greeter",
	HandlerType: (*GreeterService)(nil),
	Methods: []zrpc.MethodDesc{
		{
			MethodName: "Hello",
			Handler:    nil,
			Mode:       zrpc.Unary,
		},
	},
}

func main() {
	// Create and start server
	go func() {
		server := zrpc.NewServer()
		
		// Register service
		if err := server.RegisterService(GreeterServiceDesc, &GreeterService{}); err != nil {
			log.Fatalf("Failed to register service: %v", err)
		}
		
		// Listen
		if err := server.Listen("tcp://localhost:5555"); err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}
		
		log.Println("Server started on tcp://localhost:5555")
		if err := server.Serve(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()
	
	// Wait for server to start
	time.Sleep(1 * time.Second)
	
	// Create client
	client := zrpc.NewClient()
	defer client.Close()
	
	// Make RPC call
	ctx := context.Background()
	req := &HelloRequest{Name: "World"}
	resp := &HelloResponse{}
	
	if err := client.Invoke(ctx, "Greeter", "Hello", req, resp, zrpc.WithAddress("tcp://localhost:5555")); err != nil {
		log.Fatalf("RPC failed: %v", err)
	}
	
	fmt.Printf("Response: %s\n", resp.Message)
}
