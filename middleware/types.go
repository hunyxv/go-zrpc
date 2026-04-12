package middleware

import (
	"context"
)

// Handler 处理函数类型
type Handler func(ctx context.Context, req interface{}) (interface{}, error)

// Middleware 中间件类型
type Middleware func(next Handler) Handler

// Chain 中间件链
type Chain struct {
	middlewares []Middleware
}

// NewChain 创建中间件链
func NewChain(mws ...Middleware) *Chain {
	return &Chain{middlewares: mws}
}

// Then 链式调用
func (c *Chain) Then(final Handler) Handler {
	h := final
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		h = c.middlewares[i](h)
	}
	return h
}

// Append 添加中间件
func (c *Chain) Append(mws ...Middleware) *Chain {
	c.middlewares = append(c.middlewares, mws...)
	return c
}
