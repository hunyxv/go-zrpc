package zrpc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Client RPC客户端
type Client struct {
	address   string
	identity  string
	transport Transport
	codec     Codec
	logger    Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
	closed int32

	// 等待响应
	pending     map[string]*callFuture
	streams     map[string]*clientStreamSession
	heartbeatInterval time.Duration
}

// callFuture 调用未来
type callFuture struct {
	msgID    string
	done     chan struct{}
	response interface{}
	err      error
}

// clientStreamSession 客户端流会话
type clientStreamSession struct {
	msgID    string
	recvChan chan *Packet
	stream   interface{}
}

// NewClient 创建客户端
func NewClient(address string, opts ...ClientOption) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		address:          address,
		identity:         "cli-" + uuid.New().String(),
		codec:            DefaultCodec,
		logger:           defaultLog,
		ctx:              ctx,
		cancel:           cancel,
		pending:          make(map[string]*callFuture),
		streams:          make(map[string]*clientStreamSession),
		heartbeatInterval: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(c)
	}

	// 创建传输层
	transport, err := NewZMQTransport(c.identity, address, false)
	if err != nil {
		return nil, err
	}
	c.transport = transport

	// 启动接收循环
	c.wg.Add(1)
	go c.receive()

	// 启动心跳
	c.wg.Add(1)
	go c.heartbeat()

	return c, nil
}

// ClientOption 客户端选项
type ClientOption func(*Client)

// WithClientLogger 设置客户端日志
func WithClientLogger(logger Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithClientCodec 设置客户端编解码器
func WithClientCodec(codec Codec) ClientOption {
	return func(c *Client) {
		c.codec = codec
	}
}

// WithHeartbeatInterval 设置心跳间隔
func WithHeartbeatInterval(interval time.Duration) ClientOption {
	return func(c *Client) {
		c.heartbeatInterval = interval
	}
}

// receive 接收循环
func (c *Client) receive() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		_, data, err := c.transport.Recv()
		if err != nil {
			if atomic.LoadInt32(&c.closed) == 1 {
				return
			}
			c.logger.Error("receive error", "error", err)
			continue
		}
		if data == nil {
			continue
		}

		// 解析数据包
		packet, err := UnmarshalPacket(data)
		if err != nil {
			c.logger.Error("unmarshal packet error", "error", err)
			continue
		}

		c.handlePacket(packet)
	}
}

// handlePacket 处理数据包
func (c *Client) handlePacket(packet *Packet) {
	switch packet.Type {
	case PacketResponse, PacketError:
		c.handleResponse(packet)
	case PacketStream:
		c.handleStreamData(packet)
	case PacketStreamEnd:
		c.handleStreamEnd(packet)
	case PacketHeartbeat:
		// 心跳响应，忽略
	}
}

// handleResponse 处理响应
func (c *Client) handleResponse(packet *Packet) {
	c.mu.Lock()
	future, ok := c.pending[packet.ID]
	c.mu.Unlock()

	if !ok {
		return
	}

	if packet.Type == PacketError {
		future.err = fmt.Errorf(packet.Error)
	} else {
		future.response = packet.Data
	}

	close(future.done)
}

// handleStreamData 处理流式数据
func (c *Client) handleStreamData(packet *Packet) {
	c.mu.RLock()
	session, ok := c.streams[packet.ID]
	c.mu.RUnlock()

	if !ok {
		return
	}

	select {
	case session.recvChan <- packet:
	default:
		c.logger.Warn("stream recv channel full", "msgID", packet.ID)
	}
}

// handleStreamEnd 处理流式结束
func (c *Client) handleStreamEnd(packet *Packet) {
	c.mu.RLock()
	session, ok := c.streams[packet.ID]
	c.mu.RUnlock()

	if !ok {
		return
	}

	close(session.recvChan)
}

// heartbeat 心跳循环
func (c *Client) heartbeat() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			packet := NewHeartbeatPacket()
			data, _ := packet.Marshal()
			if err := c.transport.Send("", data); err != nil {
				c.logger.Error("heartbeat error", "error", err)
			}
		}
	}
}

// Call 同步调用
func (c *Client) Call(ctx context.Context, service, method string, req interface{}, resp interface{}) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("client closed")
	}

	// 序列化请求
	reqData, err := c.codec.Marshal(req)
	if err != nil {
		return err
	}

	// 创建调用
	msgID := uuid.New().String()
	packet := NewRequestPacket(msgID, service, service+"/"+method, reqData)
	data, err := packet.Marshal()
	if err != nil {
		return err
	}

	// 注册等待
	future := &callFuture{
		msgID: msgID,
		done:  make(chan struct{}),
	}
	c.mu.Lock()
	c.pending[msgID] = future
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, msgID)
		c.mu.Unlock()
	}()

	// 发送请求
	if err := c.transport.Send("", data); err != nil {
		return err
	}

	// 等待响应
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-future.done:
		if future.err != nil {
			return future.err
		}
		if resp != nil && future.response != nil {
			return c.codec.Unmarshal(future.response.([]byte), resp)
		}
		return nil
	}
}

// StreamCall 流式调用
func (c *Client) StreamCall(ctx context.Context, service, method string, req interface{}) (Stream, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, fmt.Errorf("client closed")
	}

	// 序列化请求
	reqData, err := c.codec.Marshal(req)
	if err != nil {
		return nil, err
	}

	// 创建调用
	msgID := uuid.New().String()
	packet := NewRequestPacket(msgID, service, service+"/"+method, reqData)
	data, err := packet.Marshal()
	if err != nil {
		return nil, err
	}

	// 创建流
	stream := newClientStream(ctx, c.transport, msgID)

	// 注册流会话
	session := &clientStreamSession{
		msgID:    msgID,
		recvChan: make(chan *Packet, 10),
		stream:   stream,
	}
	c.mu.Lock()
	c.streams[msgID] = session
	c.mu.Unlock()

	// 发送请求
	if err := c.transport.Send("", data); err != nil {
		return nil, err
	}

	return stream, nil
}

// BidiStreamCall 双向流式调用
func (c *Client) BidiStreamCall(ctx context.Context, service, method string) (BidiStream, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, fmt.Errorf("client closed")
	}

	// 创建调用
	msgID := uuid.New().String()
	packet := NewRequestPacket(msgID, service, service+"/"+method, nil)
	data, err := packet.Marshal()
	if err != nil {
		return nil, err
	}

	// 创建双向流
	recvChan := make(chan *Packet, 10)
	stream := newBidiStream(ctx, c.transport, msgID, recvChan)

	// 注册流会话
	session := &clientStreamSession{
		msgID:    msgID,
		recvChan: recvChan,
		stream:   stream,
	}
	c.mu.Lock()
	c.streams[msgID] = session
	c.mu.Unlock()

	// 发送请求
	if err := c.transport.Send("", data); err != nil {
		return nil, err
	}

	return stream, nil
}

// Proxy 创建代理
func (c *Client) Proxy(service string, i interface{}) error {
	return c.decorator(service, i, 0)
}

// ProxyWithRetry 创建带重试的代理
func (c *Client) ProxyWithRetry(service string, i interface{}, retry int) error {
	return c.decorator(service, i, retry)
}

// decorator 装饰器实现
func (c *Client) decorator(service string, i interface{}, retry int) error {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("invalid interface pointer")
	}

	v = v.Elem()
	t := v.Type()

	// 创建代理结构体
	proxyValue := reflect.New(t)

	// 为每个方法创建代理函数
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		methodName := method.Name

		// 创建代理函数
		fn := c.createProxyFunc(service, methodName, method.Type, retry)
		proxyValue.Elem().FieldByName(method.Name).Set(fn)
	}

	v.Set(proxyValue.Elem())
	return nil
}

// createProxyFunc 创建代理函数
func (c *Client) createProxyFunc(service, method string, fnType reflect.Type, retry int) reflect.Value {
	return reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		// 第一个参数是 context
		ctx := args[0].Interface().(context.Context)

		// 第二个参数是请求
		var req interface{}
		if len(args) > 1 {
			req = args[1].Interface()
		}

		// 创建返回值
		numOut := fnType.NumOut()
		results := make([]reflect.Value, numOut)

		// 最后一个返回值是 error
		errIdx := numOut - 1
		for i := 0; i < errIdx; i++ {
			results[i] = reflect.Zero(fnType.Out(i))
		}

		// 调用
		var resp interface{}
		if numOut > 1 {
			resp = reflect.New(fnType.Out(0)).Interface()
		}

		var callErr error
		for i := 0; i <= retry; i++ {
			callErr = c.Call(ctx, service, method, req, resp)
			if callErr == nil {
				break
			}
			if i < retry {
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			}
		}

		if callErr != nil {
			results[errIdx] = reflect.ValueOf(callErr)
		} else {
			results[errIdx] = reflect.Zero(errorType)
			if numOut > 1 && resp != nil {
				results[0] = reflect.ValueOf(resp).Elem()
			}
		}

		return results
	})
}

// Close 关闭客户端
func (c *Client) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}

	c.cancel()
	c.wg.Wait()

	return c.transport.Close()
}

// Identity 获取客户端标识
func (c *Client) Identity() string {
	return c.identity
}
