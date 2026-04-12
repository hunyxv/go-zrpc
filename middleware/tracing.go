package middleware

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/example/go-zrpc"
)

// Tracing 链路追踪中间件
func Tracing() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			tracer := otel.GetTracerProvider().Tracer(tracerName)
			
			// 从 context 获取方法名
			method, _ := ctx.Value("method").(string)
			if method == "" {
				method = "unknown"
			}
			
			// 创建 span
			ctx, span := tracer.Start(ctx, method)
			defer span.End()
			
			// 设置属性
			span.SetAttributes(attribute.String("rpc.method", method))
			
			resp, err := next(ctx, req)
			
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
				span.RecordError(err)
			} else {
				span.SetStatus(codes.Ok, "success")
			}
			
			return resp, err
		}
	}
}

// TracingWithCarrier 支持跨服务传播的链路追踪中间件
func TracingWithCarrier(carrier propagation.TextMapCarrier) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			// 从 carrier 中提取 trace context
			propagator := otel.GetTextMapPropagator()
			ctx = propagator.Extract(ctx, carrier)
			
			return Tracing()(next)(ctx, req)
		}
	}
}

// InjectTracingContext 注入追踪上下文到 carrier
func InjectTracingContext(ctx context.Context, carrier propagation.TextMapCarrier) {
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, carrier)
}
