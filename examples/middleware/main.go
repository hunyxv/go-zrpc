package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/example/go-zrpc"
	"github.com/example/go-zrpc/middleware"
)

// SecureService is a service that requires authentication
type SecureService struct{}

// SecureRequest is a secure request
type SecureRequest struct {
	Data string `msgpack:"data"`
}

// SecureResponse is a secure response
type SecureResponse struct {
	Result string `msgpack:"result"`
}

// SecureMethod implements a secure method
func (s *SecureService) SecureMethod(ctx context.Context, req *SecureRequest) (*SecureResponse, error) {
	// Get user info from context (set by auth middleware)
	userID := middleware.UserIDFromContext(ctx)
	role := middleware.UserRoleFromContext(ctx)
	
	return &SecureResponse{
		Result: fmt.Sprintf("Hello %s (role: %s), your data: %s", userID, role, req.Data),
	}, nil
}

// PublicMethod implements a public method
func (s *SecureService) PublicMethod(ctx context.Context, req *SecureRequest) (*SecureResponse, error) {
	return &SecureResponse{
		Result: fmt.Sprintf("Public response: %s", req.Data),
	}, nil
}

func main() {
	// Create auth middleware with custom validator
	authConfig := middleware.DefaultAuthConfig(func(ctx context.Context, token string) (context.Context, error) {
		// Simple token validation
		validTokens := map[string]struct {
			UserID string
			Role   string
		}{
			"token123": {UserID: "user1", Role: "admin"},
			"token456": {UserID: "user2", Role: "user"},
		}
		
		info, ok := validTokens[token]
		if !ok {
			return nil, fmt.Errorf("invalid token")
		}
		
		ctx = middleware.WithUserID(ctx, info.UserID)
		ctx = middleware.WithUserRole(ctx, info.Role)
		return ctx, nil
	})
	authConfig.SkipMethods = []string{"PublicMethod"}
	
	// Create middleware chain
	chain := middleware.NewServerMiddlewareChain()
	chain.Use(middleware.RecoveryMiddleware())
	chain.Use(middleware.LoggingMiddleware())
	chain.Use(middleware.AuthMiddlewareWithConfig(authConfig))
	
	// Create server with middleware
	server := zrpc.NewServer(zrpc.WithMiddleware(chain))
	
	// Register service
	desc := &zrpc.ServiceDesc{
		ServiceName: "SecureService",
		HandlerType: (*SecureService)(nil),
		Methods: []zrpc.MethodDesc{
			{MethodName: "SecureMethod", Mode: zrpc.Unary},
			{MethodName: "PublicMethod", Mode: zrpc.Unary},
		},
	}
	
	if err := server.RegisterService(desc, &SecureService{}); err != nil {
		log.Fatalf("Failed to register service: %v", err)
	}
	
	// Start server
	go func() {
		if err := server.Listen("tcp://localhost:5557"); err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}
		
		log.Println("Middleware server started on tcp://localhost:5557")
		if err := server.Serve(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()
	
	// Wait for server to start
	time.Sleep(1 * time.Second)
	
	// Create client
	client := zrpc.NewClient()
	defer client.Close()
	
	// Test public method (no auth required)
	fmt.Println("=== Public Method (no auth) ===")
	publicReq := &SecureRequest{Data: "public data"}
	publicResp := &SecureResponse{}
	
	if err := client.Invoke(context.Background(), "SecureService", "PublicMethod", publicReq, publicResp, zrpc.WithAddress("tcp://localhost:5557")); err != nil {
		log.Printf("Public method failed: %v", err)
	} else {
		fmt.Printf("Public response: %s\n", publicResp.Result)
	}
	
	// Test secure method without auth (should fail)
	fmt.Println("\n=== Secure Method (no auth) ===")
	secureReq := &SecureRequest{Data: "secure data"}
	secureResp := &SecureResponse{}
	
	if err := client.Invoke(context.Background(), "SecureService", "SecureMethod", secureReq, secureResp, zrpc.WithAddress("tcp://localhost:5557")); err != nil {
		fmt.Printf("Expected failure: %v\n", err)
	}
	
	// Test secure method with auth
	fmt.Println("\n=== Secure Method (with auth) ===")
	ctx := zrpc.WithMetadata(context.Background(), map[string]string{
		"authorization": "Bearer token123",
	})
	
	if err := client.Invoke(ctx, "SecureService", "SecureMethod", secureReq, secureResp, zrpc.WithAddress("tcp://localhost:5557")); err != nil {
		log.Printf("Secure method failed: %v", err)
	} else {
		fmt.Printf("Secure response: %s\n", secureResp.Result)
	}
	
	// Test with different user
	fmt.Println("\n=== Secure Method (different user) ===")
	ctx2 := zrpc.WithMetadata(context.Background(), map[string]string{
		"authorization": "Bearer token456",
	})
	
	if err := client.Invoke(ctx2, "SecureService", "SecureMethod", secureReq, secureResp, zrpc.WithAddress("tcp://localhost:5557")); err != nil {
		log.Printf("Secure method failed: %v", err)
	} else {
		fmt.Printf("Secure response: %s\n", secureResp.Result)
	}
	
	fmt.Println("\nMiddleware example completed")
}
