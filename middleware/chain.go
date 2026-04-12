package middleware

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/example/go-zrpc"
)

// Chain 中间件链
type Chain struct {
	middlewares []zrpc.Middleware
}

// NewChain 创建中间件链
func NewChain(mws ...zrpc.Middleware) *Chain {
	return &Chain{middlewares: mws}
}

// Then 链式调用
func (c *Chain) Then(final zrpc.Handler) zrpc.Handler {
	h := final
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		h = c.middlewares[i](h)
	}
	return h
}

// Append 添加中间件
func (c *Chain) Append(mws ...zrpc.Middleware) *Chain {
	c.middlewares = append(c.middlewares, mws...)
	return c
}

// Recovery 恢复中间件
func Recovery() zrpc.Middleware {
	return func(next zrpc.Handler) zrpc.Handler {
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
func Logging() zrpc.Middleware {
	return func(next zrpc.Handler) zrpc.Handler {
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

// LoggingWithLogger 使用自定义日志的日志中间件
func LoggingWithLogger(logger zrpc.Logger) zrpc.Middleware {
	return func(next zrpc.Handler) zrpc.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			start := time.Now()
			
			method, _ := ctx.Value("method").(string)
			
			resp, err := next(ctx, req)
			
			duration := time.Since(start)
			
			if err != nil {
				logger.Error("rpc call failed",
					"method", method,
					"duration", duration,
					"error", err,
				)
			} else {
				logger.Info("rpc call success",
					"method", method,
					"duration", duration,
				)
			}
			
			return resp, err
		}
	}
}

// AuthFunc 认证函数类型
type AuthFunc func(ctx context.Context, token string) error

// Auth 认证中间件
func Auth(fn AuthFunc) zrpc.Middleware {
	return func(next zrpc.Handler) zrpc.Handler {
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
func Timeout(timeout time.Duration) zrpc.Middleware {
	return func(next zrpc.Handler) zrpc.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			
			return next(ctx, req)
		}
	}
}

// Metrics 指标中间件（简单实现）
func Metrics() zrpc.Middleware {
	var (
		totalRequests int64
		totalErrors   int64
		totalDuration int64
	)
	
	return func(next zrpc.Handler) zrpc.Handler {
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
