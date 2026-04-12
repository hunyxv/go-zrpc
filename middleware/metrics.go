package middleware

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector is the interface for metrics collection
type MetricsCollector interface {
	// RecordRequest records a request metric
	RecordRequest(service, method string, duration time.Duration, err error)
	// RecordActiveRequests records the number of active requests
	RecordActiveRequests(service string, count int64)
}

// MetricsConfig holds configuration for metrics middleware
type MetricsConfig struct {
	// Collector is the metrics collector
	Collector MetricsCollector
	// ServiceName is the name of the service
	ServiceName string
}

// DefaultMetricsConfig returns the default metrics configuration
func DefaultMetricsConfig(collector MetricsCollector) *MetricsConfig {
	return &MetricsConfig{
		Collector:   collector,
		ServiceName: "zrpc",
	}
}

// MetricsMiddleware creates a metrics collection middleware
func MetricsMiddleware(config *MetricsConfig) Middleware {
	if config == nil || config.Collector == nil {
		panic("metrics collector cannot be nil")
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			start := time.Now()

			// Get service and method from context
			service := config.ServiceName
			if s, ok := ctx.Value("service").(string); ok {
				service = s
			}

			method := "unknown"
			if m, ok := ctx.Value("method").(string); ok {
				method = m
			}

			// Record active request
			config.Collector.RecordActiveRequests(service, 1)
			defer config.Collector.RecordActiveRequests(service, -1)

			// Call the handler
			resp, err := next(ctx, req)

			// Record request metric
			duration := time.Since(start)
			config.Collector.RecordRequest(service, method, duration, err)

			return resp, err
		}
	}
}

// SimpleMetricsCollector is a simple in-memory metrics collector
type SimpleMetricsCollector struct {
	mu        sync.RWMutex
	requests  map[string]*RequestMetrics
}

// RequestMetrics holds metrics for a specific service/method
type RequestMetrics struct {
	Service         string
	Method          string
	TotalRequests   uint64
	SuccessRequests uint64
	FailedRequests  uint64
	TotalDuration   time.Duration
	ActiveRequests  int64
}

// NewSimpleMetricsCollector creates a new simple metrics collector
func NewSimpleMetricsCollector() *SimpleMetricsCollector {
	return &SimpleMetricsCollector{
		requests: make(map[string]*RequestMetrics),
	}
}

// RecordRequest records a request metric
func (c *SimpleMetricsCollector) RecordRequest(service, method string, duration time.Duration, err error) {
	key := fmt.Sprintf("%s/%s", service, method)

	c.mu.Lock()
	defer c.mu.Unlock()

	metrics, ok := c.requests[key]
	if !ok {
		metrics = &RequestMetrics{
			Service: service,
			Method:  method,
		}
		c.requests[key] = metrics
	}

	atomic.AddUint64(&metrics.TotalRequests, 1)
	metrics.TotalDuration += duration

	if err != nil {
		atomic.AddUint64(&metrics.FailedRequests, 1)
	} else {
		atomic.AddUint64(&metrics.SuccessRequests, 1)
	}
}

// RecordActiveRequests records the number of active requests
func (c *SimpleMetricsCollector) RecordActiveRequests(service string, delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	metrics, ok := c.requests[service]
	if !ok {
		metrics = &RequestMetrics{
			Service: service,
		}
		c.requests[service] = metrics
	}

	atomic.AddInt64(&metrics.ActiveRequests, delta)
}

// GetMetrics returns metrics for a specific service/method
func (c *SimpleMetricsCollector) GetMetrics(service, method string) *RequestMetrics {
	key := fmt.Sprintf("%s/%s", service, method)

	c.mu.RLock()
	defer c.mu.RUnlock()

	if metrics, ok := c.requests[key]; ok {
		// Return a copy
		return &RequestMetrics{
			Service:         metrics.Service,
			Method:          metrics.Method,
			TotalRequests:   atomic.LoadUint64(&metrics.TotalRequests),
			SuccessRequests: atomic.LoadUint64(&metrics.SuccessRequests),
			FailedRequests:  atomic.LoadUint64(&metrics.FailedRequests),
			TotalDuration:   metrics.TotalDuration,
			ActiveRequests:  atomic.LoadInt64(&metrics.ActiveRequests),
		}
	}
	return nil
}

// GetAllMetrics returns all recorded metrics
func (c *SimpleMetricsCollector) GetAllMetrics() map[string]*RequestMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*RequestMetrics, len(c.requests))
	for k, v := range c.requests {
		result[k] = &RequestMetrics{
			Service:         v.Service,
			Method:          v.Method,
			TotalRequests:   atomic.LoadUint64(&v.TotalRequests),
			SuccessRequests: atomic.LoadUint64(&v.SuccessRequests),
			FailedRequests:  atomic.LoadUint64(&v.FailedRequests),
			TotalDuration:   v.TotalDuration,
			ActiveRequests:  atomic.LoadInt64(&v.ActiveRequests),
		}
	}
	return result
}

// AverageDuration returns the average duration for a service/method
func (m *RequestMetrics) AverageDuration() time.Duration {
	if m.TotalRequests == 0 {
		return 0
	}
	return m.TotalDuration / time.Duration(m.TotalRequests)
}

// SuccessRate returns the success rate as a percentage
func (m *RequestMetrics) SuccessRate() float64 {
	if m.TotalRequests == 0 {
		return 0
	}
	return float64(m.SuccessRequests) / float64(m.TotalRequests) * 100
}
