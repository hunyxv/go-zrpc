package middleware

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// Recovery 恢复中间件
func Recovery() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (resp interface{}, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
					resp = nil
				}
			}()
			return next(ctx, req)
		}
	}
}

// Logging 日志中间件
func Logging() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			start := time.Now()
			
			// 从 context 获取方法名（如果有）
			method, _ := ctx.Value("method").(string)
			
			resp, err := next(ctx, req)
			
			duration := time.Since(start)
			
			if err != nil {
				fmt.Printf("[ZRPC] %s | ERROR | %v | %v\n", method, duration, err)
			} else {
				fmt.Printf("[ZRPC] %s | SUCCESS | %v\n", method, duration)
			}
			
			return resp, err
		}
	}
}

// AuthFunc 认证函数类型
type AuthFunc func(ctx context.Context, token string) error

// Auth 认证中间件
func Auth(fn AuthFunc) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			// 从 context 获取 token
			token, _ := ctx.Value("token").(string)
			
			if err := fn(ctx, token); err != nil {
				return nil, fmt.Errorf("auth failed: %w", err)
			}
			
			return next(ctx, req)
		}
	}
}

// Timeout 超时中间件
func Timeout(timeout time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			
			return next(ctx, req)
		}
	}
}

// Metrics 指标中间件（简单实现）
func Metrics() Middleware {
	var (
		totalRequests int64
		totalErrors   int64
		totalDuration int64
	)
	
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			start := time.Now()
			
			totalRequests++
			
			resp, err := next(ctx, req)
			
			duration := time.Since(start)
			totalDuration += int64(duration)
			
			if err != nil {
				totalErrors++
			}
			
			// 这里可以接入 Prometheus 等监控系统
			avgDuration := time.Duration(totalDuration / totalRequests)
			fmt.Printf("[Metrics] total: %d, errors: %d, avg: %v\n", 
				totalRequests, totalErrors, avgDuration)
			
			return resp, err
		}
	}
}
