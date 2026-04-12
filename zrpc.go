package zrpc

import (
	"context"
	"errors"
	"reflect"
)

var (
	// ErrServerClosed 服务器已关闭
	ErrServerClosed = errors.New("zrpc: server closed")
	// ErrMethodNotFound 方法未找到
	ErrMethodNotFound = errors.New("zrpc: method not found")
	// ErrInvalidServer 无效的服务器
	ErrInvalidServer = errors.New("zrpc: invalid server")
	// ErrNotImplements 未实现接口
	ErrNotImplements = errors.New("zrpc: not implements interface")
	// ErrTooFewReturn 返回值太少
	ErrTooFewReturn = errors.New("zrpc: too few return values")
	// ErrInvalidResultType 无效的返回类型
	ErrInvalidResultType = errors.New("zrpc: last return value must be error")
	// ErrInvalidParamType 无效的参数类型
	ErrInvalidParamType = errors.New("zrpc: first param must be context.Context")
	// ErrTooFewParam 参数太少
	ErrTooFewParam = errors.New("zrpc: too few parameters")

	// errorType 是 error 接口的反射类型
	errorType = reflect.TypeOf((*error)(nil)).Elem()
)

// FuncMode 函数调用模式
type FuncMode int

const (
	// ReqRep 请求-响应模式
	ReqRep FuncMode = iota
	// StreamReqRep 流式请求模式（客户端流式）
	StreamReqRep
	// ReqStreamRep 流式响应模式（服务端流式）
	ReqStreamRep
	// Stream 双向流式模式
	Stream
)

func (fm FuncMode) String() string {
	switch fm {
	case ReqRep:
		return "ReqRep"
	case StreamReqRep:
		return "StreamReqRep"
	case ReqStreamRep:
		return "ReqStreamRep"
	case Stream:
		return "Stream"
	}
	return "Unknown"
}

// Handler 处理函数类型
type Handler func(ctx context.Context, req interface{}) (interface{}, error)

// Middleware 中间件类型
type Middleware func(next Handler) Handler

// Stream 流式接口
type Stream interface {
	Send(msg interface{}) error
}

// BidiStream 双向流式接口
type BidiStream interface {
	Send(msg interface{}) error
	Recv(msg interface{}) error
}

// Message 消息接口
type Message interface {
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// Logger 日志接口
type Logger interface {
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
}

// defaultLogger 默认日志实现
type defaultLogger struct{}

func (l *defaultLogger) Debug(msg string, fields ...interface{}) {}
func (l *defaultLogger) Info(msg string, fields ...interface{})  {}
func (l *defaultLogger) Warn(msg string, fields ...interface{})  {}
func (l *defaultLogger) Error(msg string, fields ...interface{}) {}

var defaultLog Logger = &defaultLogger{}

// SetDefaultLogger 设置默认日志
func SetDefaultLogger(l Logger) {
	defaultLog = l
}
