package middleware

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
)

// RecoveryConfig holds configuration for recovery middleware
type RecoveryConfig struct {
	// RecoverFunc is called when a panic is recovered
	RecoverFunc func(ctx context.Context, req interface{}, err interface{}) error
	// LogStackTrace enables logging of stack traces
	LogStackTrace bool
	// Logger is the custom logger function
	Logger func(format string, args ...interface{})
}

// DefaultRecoveryConfig returns the default recovery configuration
func DefaultRecoveryConfig() *RecoveryConfig {
	return &RecoveryConfig{
		RecoverFunc:   defaultRecoverFunc,
		LogStackTrace: true,
		Logger:        log.Printf,
	}
}

// defaultRecoverFunc is the default panic recovery function
func defaultRecoverFunc(ctx context.Context, req interface{}, err interface{}) error {
	return fmt.Errorf("panic recovered: %v", err)
}

// RecoveryMiddlewareWithConfig creates a recovery middleware with custom configuration
func RecoveryMiddlewareWithConfig(config *RecoveryConfig) Middleware {
	if config == nil {
		config = DefaultRecoveryConfig()
	}
	if config.RecoverFunc == nil {
		config.RecoverFunc = defaultRecoverFunc
	}
	if config.Logger == nil {
		config.Logger = log.Printf
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (resp interface{}, err error) {
			defer func() {
				if r := recover(); r != nil {
					if config.LogStackTrace {
						config.Logger("[zRPC] Panic recovered: %v\n%s", r, string(debug.Stack()))
					} else {
						config.Logger("[zRPC] Panic recovered: %v", r)
					}

					err = config.RecoverFunc(ctx, req, r)
					resp = nil
				}
			}()

			return next(ctx, req)
		}
	}
}

// PanicHandler is a function that handles panics
type PanicHandler func(ctx context.Context, req interface{}, err interface{}) error

// RecoveryMiddlewareWithHandler creates a recovery middleware with a custom panic handler
func RecoveryMiddlewareWithHandler(handler PanicHandler) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (resp interface{}, err error) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[zRPC] Panic recovered: %v\n%s", r, string(debug.Stack()))
					err = handler(ctx, req, r)
					resp = nil
				}
			}()

			return next(ctx, req)
		}
	}
}
