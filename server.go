package zrpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ServerOption 服务端选项
type ServerOption func(*Server)

// WithAddress 设置监听地址
func WithAddress(address string) ServerOption {
	return func(s *Server) {
		s.address = address
	}
}

// WithLogger 设置日志
func WithLogger(logger Logger) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

// WithCodec 设置编解码器
func WithCodec(codec Codec) ServerOption {
	return func(s *Server) {
		s.codec = codec
	}
}

// WithMiddleware 添加中间件
func WithMiddleware(mw ...Middleware) ServerOption {
	return func(s *Server) {
		s.middlewares = append(s.middlewares, mw...)
	}
}

// Server RPC服务端
type Server struct {
	address     string
	identity    string
	transport   Transport
	registry    *Registry
	codec       Codec
	logger      Logger
	middlewares []Middleware

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
	running bool
	
	// 流式会话管理
	streams map[string]*streamSession
}

// streamSession 流式会话
type streamSession struct {
	msgID    string
	method   *Method
	stream   interface{}
	recvChan chan *Packet
}

// NewServer 创建服务端
func NewServer(opts ...ServerOption) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	
	s := &Server{
		address:  ":8080",
		identity: uuid.New().String(),
		registry: NewRegistry(),
		codec:    DefaultCodec,
		logger:   defaultLog,
		ctx:      ctx,
		cancel:   cancel,
		streams:  make(map[string]*streamSession),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Register 注册服务
func (s *Server) Register(name string, instance interface{}, convention interface{}) error {
	return s.registry.Register(name, instance, convention)
}

// Use 添加中间件
func (s *Server) Use(mw ...Middleware) {
	s.middlewares = append(s.middlewares, mw...)
}

// Run 启动服务
func (s *Server) Run() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	// 创建传输层
	transport, err := NewZMQTransport(s.identity, s.address, true)
	if err != nil {
		return err
	}
	s.transport = transport

	s.logger.Info("server started", "address", s.address, "identity", s.identity)

	// 启动处理循环
	s.wg.Add(1)
	go s.serve()

	return nil
}

// serve 服务主循环
func (s *Server) serve() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		from, data, err := s.transport.Recv()
		if err != nil {
			s.logger.Error("receive error", "error", err)
			continue
		}
		if data == nil {
			continue // 超时，继续
		}

		// 解析数据包
		packet, err := UnmarshalPacket(data)
		if err != nil {
			s.logger.Error("unmarshal packet error", "error", err)
			continue
		}

		// 处理数据包
		s.wg.Add(1)
		go func(from string, packet *Packet) {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("panic recovered", "panic", r, "stack", string(debug.Stack()))
				}
			}()
			s.handlePacket(from, packet)
		}(from, packet)
	}
}

// handlePacket 处理数据包
func (s *Server) handlePacket(from string, packet *Packet) {
	switch packet.Type {
	case PacketRequest:
		s.handleRequest(from, packet)
	case PacketStream:
		s.handleStreamData(packet)
	case PacketStreamEnd:
		s.handleStreamEnd(packet)
	case PacketHeartbeat:
		// 心跳响应
		resp := NewHeartbeatPacket()
		resp.ID = packet.ID
		data, _ := resp.Marshal()
		s.transport.Send(from, data)
	default:
		s.logger.Warn("unknown packet type", "type", packet.Type)
	}
}

// handleRequest 处理请求
func (s *Server) handleRequest(from string, packet *Packet) {
	// 查找方法
	method, ok := s.registry.GetMethod(packet.Method)
	if !ok {
		s.sendError(from, packet.ID, ErrMethodNotFound)
		return
	}

	// 根据调用模式处理
	switch method.Mode {
	case ReqRep:
		s.handleReqRep(from, packet, method)
	case StreamReqRep:
		s.handleStreamReqRep(from, packet, method)
	case ReqStreamRep:
		s.handleReqStreamRep(from, packet, method)
	case Stream:
		s.handleBidiStream(from, packet, method)
	}
}

// handleReqRep 处理请求-响应
func (s *Server) handleReqRep(from string, packet *Packet, method *Method) {
	ctx := s.ctx
	
	// 构建参数
	params, err := s.buildParams(ctx, method, packet, nil)
	if err != nil {
		s.sendError(from, packet.ID, err)
		return
	}

	// 构建处理链
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return s.invoke(method, params)
	}

	// 应用中间件
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		handler = s.middlewares[i](handler)
	}

	// 执行
	resp, err := handler(ctx, packet.Data)
	if err != nil {
		s.sendError(from, packet.ID, err)
		return
	}

	// 序列化响应
	respData, err := s.codec.Marshal(resp)
	if err != nil {
		s.sendError(from, packet.ID, err)
		return
	}

	// 发送响应
	respPacket := NewResponsePacket(packet.ID, respData)
	respData, _ = respPacket.Marshal()
	s.transport.Send(from, respData)
}

// handleStreamReqRep 处理流式请求
func (s *Server) handleStreamReqRep(from string, packet *Packet, method *Method) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// 创建流式读取器
	recvChan := make(chan *Packet, 10)
	reader := newStreamReader(ctx, recvChan)

	// 保存会话
	session := &streamSession{
		msgID:    packet.ID,
		method:   method,
		recvChan: recvChan,
	}
	s.mu.Lock()
	s.streams[packet.ID] = session
	s.mu.Unlock()

	// 处理初始请求
	params, err := s.buildParams(ctx, method, packet, reader)
	if err != nil {
		s.sendError(from, packet.ID, err)
		return
	}

	// 等待流式数据结束
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.streams, packet.ID)
			s.mu.Unlock()
			reader.Close()
		}()

		resp, err := s.invoke(method, params)
		if err != nil {
			s.sendError(from, packet.ID, err)
			return
		}

		respData, _ := s.codec.Marshal(resp)
		respPacket := NewResponsePacket(packet.ID, respData)
		respData, _ = respPacket.Marshal()
		s.transport.Send(from, respData)
	}()
}

// handleReqStreamRep 处理流式响应
func (s *Server) handleReqStreamRep(from string, packet *Packet, method *Method) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// 创建流式发送器
	stream := newServerStream(ctx, s.transport, packet.ID)
	writeCloser := &serverWriteCloser{stream}

	// 构建参数
	params, err := s.buildParams(ctx, method, packet, writeCloser)
	if err != nil {
		s.sendError(from, packet.ID, err)
		return
	}

	// 执行
	go func() {
		defer stream.Close()

		resp, err := s.invoke(method, params)
		if err != nil {
			s.sendError(from, packet.ID, err)
			return
		}

		// 发送最终响应
		respData, _ := s.codec.Marshal(resp)
		respPacket := NewResponsePacket(packet.ID, respData)
		respData, _ = respPacket.Marshal()
		s.transport.Send(from, respData)
	}()
}

// handleBidiStream 处理双向流式
func (s *Server) handleBidiStream(from string, packet *Packet, method *Method) {
	ctx, cancel := context.WithCancel(s.ctx)

	// 创建双向流
	recvChan := make(chan *Packet, 10)
	stream := newBidiStream(ctx, s.transport, packet.ID, recvChan)

	// 保存会话
	session := &streamSession{
		msgID:    packet.ID,
		method:   method,
		stream:   stream,
		recvChan: recvChan,
	}
	s.mu.Lock()
	s.streams[packet.ID] = session
	s.mu.Unlock()

	// 构建参数
	params, err := s.buildParams(ctx, method, packet, stream)
	if err != nil {
		s.sendError(from, packet.ID, err)
		return
	}

	// 执行
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.streams, packet.ID)
			s.mu.Unlock()
			stream.Close()
			cancel()
		}()

		_, err := s.invoke(method, params)
		if err != nil {
			s.sendError(from, packet.ID, err)
		}
	}()
}

// handleStreamData 处理流式数据
func (s *Server) handleStreamData(packet *Packet) {
	s.mu.RLock()
	session, ok := s.streams[packet.ID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	select {
	case session.recvChan <- packet:
	default:
		s.logger.Warn("stream recv channel full", "msgID", packet.ID)
	}
}

// handleStreamEnd 处理流式结束
func (s *Server) handleStreamEnd(packet *Packet) {
	s.mu.RLock()
	session, ok := s.streams[packet.ID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	close(session.recvChan)
}

// buildParams 构建参数
func (s *Server) buildParams(ctx context.Context, method *Method, packet *Packet, stream interface{}) ([]reflect.Value, error) {
	params := make([]reflect.Value, len(method.ParamTypes))

	// 第一个参数是 context
	params[0] = reflect.ValueOf(ctx)

	// 解析请求数据
	if len(method.ParamTypes) > 1 && packet.Data != nil {
		// 非流式参数
		idx := 1
		for i := 1; i < len(method.ParamTypes); i++ {
			pt := method.ParamTypes[i]
			
			// 检查是否是流式类型
			if pt.Implements(streamType) || pt.Implements(bidiStreamType) ||
			   pt.Implements(reflect.TypeOf((*io.Reader)(nil)).Elem()) ||
			   pt.Implements(reflect.TypeOf((*io.WriteCloser)(nil)).Elem()) {
				params[i] = reflect.ValueOf(stream)
				continue
			}

			// 普通参数，从 packet.Data 反序列化
			// 简化处理：假设只有一个非流式参数
			if idx == 1 {
				val := reflect.New(pt)
				if err := s.codec.Unmarshal(packet.Data, val.Interface()); err != nil {
					return nil, err
				}
				params[i] = val.Elem()
			}
			idx++
		}
	}

	return params, nil
}

// invoke 调用方法
func (s *Server) invoke(method *Method, params []reflect.Value) (interface{}, error) {
	results := method.Func.Call(params)

	// 检查错误
	lastResult := results[len(results)-1]
	if !lastResult.IsNil() {
		err := lastResult.Interface().(error)
		return nil, err
	}

	// 返回结果（如果有的话）
	if len(results) > 1 {
		return results[0].Interface(), nil
	}

	return nil, nil
}

// sendError 发送错误响应
func (s *Server) sendError(to, msgID string, err error) {
	packet := NewErrorPacket(msgID, err)
	data, _ := packet.Marshal()
	s.transport.Send(to, data)
}

// Close 关闭服务
func (s *Server) Close() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.cancel()
	s.wg.Wait()

	if s.transport != nil {
		return s.transport.Close()
	}

	return nil
}

// Identity 获取服务标识
func (s *Server) Identity() string {
	return s.identity
}

// getLocalIP 获取本地IP
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}
