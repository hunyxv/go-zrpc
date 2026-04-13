package zrpc

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Client RPC客户端
type Client struct {
	address           string
	identity          string
	transport         Transport
	codec             Codec
	logger            Logger

	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	mu                sync.RWMutex
	closed            int32

	// 等待响应
	pending           map[string]*callFuture
	streams           map[string]*clientStreamSession
	heartbeatInterval time.Duration
}

// callFuture 调用未来
type callFuture struct {
	msgID      string
	done       chan struct{}
	response   interface{}
	err        error
}

// clientStreamSession 客户端流会话
type clientStreamSession struct {
	msgID      string
	recvChan   chan *Packet
	stream     interface{}
}

// NewClient 创建客户端
func NewClient(address string, opts ...ClientOption) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		address:           address,
		identity:          "cli-" + uuid.New().String(),
		codec:             DefaultCodec,
		logger:            defaultLog,
		ctx:               ctx,
		cancel:            cancel,
		pending:           make(map[string]*callFuture),
		streams:           make(map[string]*clientStreamSession),
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
	c.logger.Info("sending request", "msgID", msgID, "method", packet.Method)
	if err := c.transport.Send("", data); err != nil {
		return err
	}

	// 等待响应
	c.logger.Info("waiting for response", "msgID", msgID)
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

// Proxy 创建代理（使用代理结构体）
func (c *Client) Proxy(service string, proxy interface{}) error {
	return c.Decorator(service, proxy, 0)
}

// ProxyWithRetry 创建带重试的代理（使用代理结构体）
func (c *Client) ProxyWithRetry(service string, proxy interface{}, retry int) error {
	return c.Decorator(service, proxy, retry)
}

// Decorator 装饰器实现 - 支持代理结构体，自动识别流式方法
// 流式模式检测规则（服务端和客户端签名一致）：
// 1. StreamReqRep（客户端流式）：参数中包含 Stream/BidiStream/io.Reader/io.ReadWriteCloser 类型，返回值中无流式类型
// 2. ReqStreamRep（服务端流式）：参数中无流式类型，返回值中包含 Stream/BidiStream/io.WriteCloser/io.ReadWriteCloser 类型
// 3. BidiStream（双向流式）：参数和返回值中都包含流式类型
func (c *Client) Decorator(service string, proxy interface{}, retry int) error {
	v := reflect.ValueOf(proxy)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("proxy must be a non-nil pointer")
	}

	v = v.Elem()
	t := v.Type()

	// 确保是结构体
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("proxy must point to a struct")
	}

	// 为每个字段创建代理函数
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		// 只处理函数字段
		if field.Kind() != reflect.Func {
			continue
		}

		methodName := fieldType.Name
		fnType := field.Type()

		// 检测是否是流式方法
		inMode := c.detectStreamModeInParams(fnType)
		outMode := c.detectStreamModeInReturns(fnType)

		// 确定最终模式
		var mode FuncMode = ReqRep
		switch {
		case inMode == BidiStreamMode || outMode == BidiStreamMode || (inMode != ReqRep && outMode != ReqRep):
			mode = BidiStreamMode
		case inMode == StreamReqRep && outMode == ReqRep:
			mode = StreamReqRep
		case inMode == ReqRep && outMode == ReqStreamRep:
			mode = ReqStreamRep
		case inMode == StreamReqRep && outMode == ReqStreamRep:
			// 参数里有流请求，返回值里有流响应，也是双向流
			mode = BidiStreamMode
		}

		// 创建代理函数
		var proxyFn reflect.Value
		switch mode {
		case StreamReqRep:
			proxyFn = c.createStreamReqRepProxyFunc(service, methodName, fnType, retry)
		case ReqStreamRep:
			proxyFn = c.createReqStreamRepProxyFunc(service, methodName, fnType, retry)
		case BidiStreamMode:
			proxyFn = c.createBidiStreamProxyFunc(service, methodName, fnType, retry)
		default:
			proxyFn = c.createProxyFunc(service, methodName, fnType, retry)
		}
		field.Set(proxyFn)
	}

	return nil
}

// detectStreamModeInParams 检测参数中的流式模式
func (c *Client) detectStreamModeInParams(fnType reflect.Type) FuncMode {
	mode := ReqRep
	numIn := fnType.NumIn()

	for i := 1; i < numIn; i++ {
		pt := fnType.In(i)

		if pt.Implements(bidiStreamType) || pt.Implements(reflect.TypeOf((*io.ReadWriteCloser)(nil)).Elem()) {
			return BidiStreamMode
		}
		if pt.Implements(streamType) {
			// Stream 接口，如果同时实现了 Reader 则是 StreamReqRep
			if pt.Implements(readerType) {
				mode = StreamReqRep
			} else if pt.Implements(writeCloserType) {
				if mode == StreamReqRep {
					return BidiStreamMode
				}
				mode = ReqStreamRep
			}
		}
		if pt.Implements(readerType) {
			mode = StreamReqRep
		}
		if pt.Implements(writeCloserType) {
			if mode == StreamReqRep {
				return BidiStreamMode
			}
			mode = ReqStreamRep
		}
	}

	return mode
}

// detectStreamModeInReturns 检测返回值中的流式模式
func (c *Client) detectStreamModeInReturns(fnType reflect.Type) FuncMode {
	mode := ReqRep
	numOut := fnType.NumOut()

	for i := 0; i < numOut-1; i++ { // 跳过最后一个 error
		rt := fnType.Out(i)

		if rt.Implements(bidiStreamType) || rt.Implements(reflect.TypeOf((*io.ReadWriteCloser)(nil)).Elem()) {
			return BidiStreamMode
		}
		if rt.Implements(streamType) {
			if rt.Implements(readerType) || rt.Implements(reflect.TypeOf((*io.Reader)(nil)).Elem()) {
				// 返回值中可读的 stream 表示服务端流式（响应流）
				mode = ReqStreamRep
			} else if rt.Implements(writeCloserType) {
				if mode == ReqStreamRep {
					return BidiStreamMode
				}
				mode = StreamReqRep
			}
		}
		if rt.Implements(readerType) {
			mode = ReqStreamRep
		}
		if rt.Implements(writeCloserType) {
			if mode == ReqStreamRep {
				return BidiStreamMode
			}
			mode = StreamReqRep
		}
	}

	return mode
}

// createProxyFunc 创建普通代理函数 (ReqRep 模式)
func (c *Client) createProxyFunc(service, method string, fnType reflect.Type, retry int) reflect.Value {
	return reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		ctx := args[0].Interface().(context.Context)

		var req interface{}
		if len(args) > 1 {
			req = args[1].Interface()
		}

		numOut := fnType.NumOut()
		results := make([]reflect.Value, numOut)
		errIdx := numOut - 1
		for i := 0; i < errIdx; i++ {
			results[i] = reflect.Zero(fnType.Out(i))
		}

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

// createStreamReqRepProxyFunc 创建客户端流式代理函数 (StreamReqRep 模式)
// 参数中包含 StreamReq 对象，客户端通过它发送流数据
func (c *Client) createStreamReqRepProxyFunc(service, method string, fnType reflect.Type, retry int) reflect.Value {
	return reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		ctx := args[0].Interface().(context.Context)
		numOut := fnType.NumOut()
		results := make([]reflect.Value, numOut)
		errIdx := numOut - 1
		for i := 0; i < errIdx; i++ {
			results[i] = reflect.Zero(fnType.Out(i))
		}

		// 找到参数中的 stream
		streamIdx := -1
		for i := 1; i < len(args); i++ {
			pt := fnType.In(i)
			if pt.Implements(streamType) || pt.Implements(bidiStreamType) || pt.Implements(readerType) {
				streamIdx = i
				break
			}
		}

		if streamIdx == -1 {
			results[errIdx] = reflect.ValueOf(fmt.Errorf("no stream request parameter found"))
			return results
		}

		msgID := uuid.New().String()

		// 确定 stream 类型
		streamTypeIn := fnType.In(streamIdx)
		var reqStream interface{}

		if streamTypeIn.Implements(readerType) && !streamTypeIn.Implements(streamType) {
			// 纯 io.Reader，不支持 Send，需要包装
			results[errIdx] = reflect.ValueOf(fmt.Errorf("io.Reader not supported for stream request, use zrpc.Stream or similar"))
			return results
		}

		// 创建客户端发送流
		clientStreamObj := newClientStream(ctx, c.transport, msgID)
		reqStream = clientStreamObj

		// 设置 stream 参数
		args[streamIdx] = reflect.ValueOf(reqStream)

		// 序列化请求（非 stream 参数）
		var reqData []byte
		for i := 1; i < len(args); i++ {
			if i != streamIdx {
				req := args[i].Interface()
				var err error
				reqData, err = c.codec.Marshal(req)
				if err != nil {
					results[errIdx] = reflect.ValueOf(err)
					return results
				}
				break
			}
		}

		// 发送请求
		packet := NewRequestPacket(msgID, service, service+"/"+method, reqData)
		data, _ := packet.Marshal()
		if err := c.transport.Send("", data); err != nil {
			results[errIdx] = reflect.ValueOf(err)
			return results
		}

		// 等待最终响应
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

		select {
		case <-ctx.Done():
			results[errIdx] = reflect.ValueOf(ctx.Err())
		case <-future.done:
			if future.err != nil {
				results[errIdx] = reflect.ValueOf(future.err)
			} else {
				results[errIdx] = reflect.Zero(errorType)
				if numOut > 1 && future.response != nil {
					respType := fnType.Out(0)
					resp := reflect.New(respType).Interface()
					if err := c.codec.Unmarshal(future.response.([]byte), resp); err != nil {
						results[errIdx] = reflect.ValueOf(err)
					} else {
						results[0] = reflect.ValueOf(resp).Elem()
					}
				}
			}
		}

		return results
	})
}

// createReqStreamRepProxyFunc 创建服务端流式代理函数 (ReqStreamRep 模式)
// 响应中包含 StreamResp 对象，客户端通过它接收流数据
func (c *Client) createReqStreamRepProxyFunc(service, method string, fnType reflect.Type, retry int) reflect.Value {
	return reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		ctx := args[0].Interface().(context.Context)
		numOut := fnType.NumOut()
		results := make([]reflect.Value, numOut)
		errIdx := numOut - 1

		for i := 0; i < errIdx; i++ {
			results[i] = reflect.Zero(fnType.Out(i))
		}

		// 找到返回值中的 stream 位置
		streamOutIdx := -1
		for i := 0; i < errIdx; i++ {
			rt := fnType.Out(i)
			if rt.Implements(streamType) || rt.Implements(bidiStreamType) || rt.Implements(readerType) {
				streamOutIdx = i
				break
			}
		}

		if streamOutIdx == -1 {
			results[errIdx] = reflect.ValueOf(fmt.Errorf("no stream response return value found"))
			return results
		}

		msgID := uuid.New().String()

		// 创建接收通道
		recvChan := make(chan *Packet, 10)
		session := &clientStreamSession{
			msgID:    msgID,
			recvChan: recvChan,
		}
		c.mu.Lock()
		c.streams[msgID] = session
		c.mu.Unlock()

		// 创建客户端接收流
		streamTypeOut := fnType.Out(streamOutIdx)
		var respStream interface{}

		if streamTypeOut.Implements(readerType) && !streamTypeOut.Implements(streamType) {
			// 返回 io.Reader
			reader := &clientStreamReader{
				ctx:      ctx,
				recvChan: recvChan,
				codec:    c.codec,
			}
			respStream = reader
		} else {
			// 返回 zrpc.Stream
			stream := newClientRecvStream(ctx, recvChan, c.codec)
			respStream = stream
		}

		results[streamOutIdx] = reflect.ValueOf(respStream)

		// 序列化请求
		var reqData []byte
		if len(args) > 1 {
			req := args[1].Interface()
			var err error
			reqData, err = c.codec.Marshal(req)
			if err != nil {
				results[errIdx] = reflect.ValueOf(err)
				return results
			}
		}

		// 发送请求
		packet := NewRequestPacket(msgID, service, service+"/"+method, reqData)
		data, _ := packet.Marshal()
		if err := c.transport.Send("", data); err != nil {
			results[errIdx] = reflect.ValueOf(err)
			return results
		}

		results[errIdx] = reflect.Zero(errorType)
		return results
	})
}

// createBidiStreamProxyFunc 创建双向流式代理函数 (BidiStreamMode)
// 参数中有 StreamReq，返回值中有 StreamResp
func (c *Client) createBidiStreamProxyFunc(service, method string, fnType reflect.Type, retry int) reflect.Value {
	return reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		ctx := args[0].Interface().(context.Context)
		numOut := fnType.NumOut()
		results := make([]reflect.Value, numOut)
		errIdx := numOut - 1

		for i := 0; i < errIdx; i++ {
			results[i] = reflect.Zero(fnType.Out(i))
		}

		// 找到参数中的 stream
		streamInIdx := -1
		for i := 1; i < len(args); i++ {
			pt := fnType.In(i)
			if pt.Implements(streamType) || pt.Implements(bidiStreamType) || pt.Implements(readerType) {
				streamInIdx = i
				break
			}
		}

		// 找到返回值中的 stream
		streamOutIdx := -1
		for i := 0; i < errIdx; i++ {
			rt := fnType.Out(i)
			if rt.Implements(streamType) || rt.Implements(bidiStreamType) || rt.Implements(readerType) {
				streamOutIdx = i
				break
			}
		}

		msgID := uuid.New().String()

		// 创建接收通道
		recvChan := make(chan *Packet, 10)
		session := &clientStreamSession{
			msgID:    msgID,
			recvChan: recvChan,
		}
		c.mu.Lock()
		c.streams[msgID] = session
		c.mu.Unlock()

		// 创建双向流（同时支持 Send 和 Recv）
		bidi := newBidiStream(ctx, c.transport, msgID, recvChan, c.codec)

		// 设置参数和返回值
		if streamInIdx != -1 {
			args[streamInIdx] = reflect.ValueOf(bidi)
		}
		if streamOutIdx != -1 {
			results[streamOutIdx] = reflect.ValueOf(bidi)
		}

		// 序列化请求（非 stream 参数）
		var reqData []byte
		for i := 1; i < len(args); i++ {
			if i != streamInIdx {
				req := args[i].Interface()
				var err error
				reqData, err = c.codec.Marshal(req)
				if err != nil {
					results[errIdx] = reflect.ValueOf(err)
					return results
				}
				break
			}
		}

		// 发送请求
		packet := NewRequestPacket(msgID, service, service+"/"+method, reqData)
		data, _ := packet.Marshal()
		if err := c.transport.Send("", data); err != nil {
			results[errIdx] = reflect.ValueOf(err)
			return results
		}

		results[errIdx] = reflect.Zero(errorType)
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
