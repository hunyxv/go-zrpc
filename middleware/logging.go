package middleware

import (
	"context"
	"fmt"
	"time"
)

// LoggerFunc is the custom logger function type
type LoggerFunc func(format string, args ...any)

// defaultLogger wraps fmt.Printf to match LoggerFunc signature
func defaultLogger(format string, args ...any) {
	fmt.Printf(format, args...)
}

// LoggingConfig holds configuration for logging middleware
type LoggingConfig struct {
	// EnableRequestLogging enables logging of request details
	EnableRequestLogging bool
	// EnableResponseLogging enables logging of response details
	EnableResponseLogging bool
	// EnableErrorLogging enables logging of errors
	EnableErrorLogging bool
	// Logger is the custom logger function
	Logger LoggerFunc
}

// DefaultLoggingConfig returns the default logging configuration
func DefaultLoggingConfig() *LoggingConfig {
	return &LoggingConfig{
		EnableRequestLogging:  true,
		EnableResponseLogging: true,
		EnableErrorLogging:    true,
		Logger:                defaultLogger,
	}
}

// LoggingMiddlewareWithConfig creates a logging middleware with custom configuration
func LoggingMiddlewareWithConfig(config *LoggingConfig) Middleware {
	if config == nil {
		config = DefaultLoggingConfig()
	}
	if config.Logger == nil {
		config.Logger = defaultLogger
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			start := time.Now()
			
			if config.EnableRequestLogging {
				config.Logger("[zRPC] Request started at %s\n", start.Format(time.RFC3339))
			}
			
			resp, err := next(ctx, req)
			
			duration := time.Since(start)
			
			if err != nil {
				if config.EnableErrorLogging {
					config.Logger("[zRPC] Request failed after %v: %v\n", duration, err)
				}
			} else {
				if config.EnableResponseLogging {
					config.Logger("[zRPC] Request completed in %v\n", duration)
				}
			}
			
			return resp, err
		}
	}
}

// RequestLogger is an interface for custom request logging
type RequestLogger interface {
	LogRequest(ctx context.Context, req interface{})
	LogResponse(ctx context.Context, resp interface{}, duration time.Duration, err error)
}

// LoggingMiddlewareWithLogger creates a logging middleware with a custom logger
func LoggingMiddlewareWithLogger(logger RequestLogger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			start := time.Now()
			
			if logger != nil {
				logger.LogRequest(ctx, req)
			}
			
			resp, err := next(ctx, req)
			
			if logger != nil {
				logger.LogResponse(ctx, resp, time.Since(start), err)
			}
			
			return resp, err
		}
	}
}
