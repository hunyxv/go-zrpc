package zrpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// serverStream 服务端流实现
type serverStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	transport Transport
	msgID    string
	seq      uint64
	closed   int32
	mu       sync.Mutex
}

// newServerStream 创建服务端流
func newServerStream(ctx context.Context, transport Transport, msgID string) *serverStream {
	ctx, cancel := context.WithCancel(ctx)
	return &serverStream{
		ctx:       ctx,
		cancel:    cancel,
		transport: transport,
		msgID:     msgID,
	}
}

// Send 发送消息
func (s *serverStream) Send(msg interface{}) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.ErrClosedPipe
	}

	data, err := DefaultCodec.Marshal(msg)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	packet := NewStreamPacket(s.msgID, s.seq, data)
	packetData, err := packet.Marshal()
	if err != nil {
		return err
	}

	return s.transport.Send("", packetData)
}

// Close 关闭流
func (s *serverStream) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	s.seq++
	packet := NewStreamEndPacket(s.msgID, s.seq)
	packetData, err := packet.Marshal()
	if err != nil {
		return err
	}

	err = s.transport.Send("", packetData)
	s.cancel()
	return err
}

// Context 返回上下文
func (s *serverStream) Context() context.Context {
	return s.ctx
}

// serverWriteCloser 服务端 WriteCloser 实现
type serverWriteCloser struct {
	*serverStream
}

// Write 实现 io.Writer
func (w *serverWriteCloser) Write(p []byte) (n int, err error) {
	if err := w.Send(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close 实现 io.Closer
func (w *serverWriteCloser) Close() error {
	return w.serverStream.Close()
}

// clientStream 客户端流实现
type clientStream struct {
	ctx       context.Context
	cancel    context.CancelFunc
	transport Transport
	msgID     string
	seq       uint64
	closed    int32
	mu        sync.Mutex
}

// newClientStream 创建客户端流
func newClientStream(ctx context.Context, transport Transport, msgID string) *clientStream {
	ctx, cancel := context.WithCancel(ctx)
	return &clientStream{
		ctx:       ctx,
		cancel:    cancel,
		transport: transport,
		msgID:     msgID,
	}
}

// Send 发送消息
func (s *clientStream) Send(msg interface{}) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.ErrClosedPipe
	}

	data, err := DefaultCodec.Marshal(msg)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	packet := NewStreamPacket(s.msgID, s.seq, data)
	packetData, err := packet.Marshal()
	if err != nil {
		return err
	}

	return s.transport.Send("", packetData)
}

// Close 关闭流
func (s *clientStream) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	s.seq++
	packet := NewStreamEndPacket(s.msgID, s.seq)
	packetData, err := packet.Marshal()
	if err != nil {
		return err
	}

	err = s.transport.Send("", packetData)
	s.cancel()
	return err
}

// Context 返回上下文
func (s *clientStream) Context() context.Context {
	return s.ctx
}

// bidiStream 双向流实现
type bidiStream struct {
	ctx       context.Context
	cancel    context.CancelFunc
	transport Transport
	msgID     string
	seq       uint64
	recvChan  chan *Packet
	closed    int32
	mu        sync.Mutex
}

// newBidiStream 创建双向流
func newBidiStream(ctx context.Context, transport Transport, msgID string, recvChan chan *Packet) *bidiStream {
	ctx, cancel := context.WithCancel(ctx)
	return &bidiStream{
		ctx:       ctx,
		cancel:    cancel,
		transport: transport,
		msgID:     msgID,
		recvChan:  recvChan,
	}
}

// Send 发送消息
func (s *bidiStream) Send(msg interface{}) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.ErrClosedPipe
	}

	data, err := DefaultCodec.Marshal(msg)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	packet := NewStreamPacket(s.msgID, s.seq, data)
	packetData, err := packet.Marshal()
	if err != nil {
		return err
	}

	return s.transport.Send("", packetData)
}

// Recv 接收消息
func (s *bidiStream) Recv(msg interface{}) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case packet := <-s.recvChan:
		if packet == nil {
			return io.EOF
		}
		if packet.Type == PacketStreamEnd {
			return io.EOF
		}
		return DefaultCodec.Unmarshal(packet.Data, msg)
	}
}

// Close 关闭流
func (s *bidiStream) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	s.seq++
	packet := NewStreamEndPacket(s.msgID, s.seq)
	packetData, err := packet.Marshal()
	if err != nil {
		return err
	}

	err = s.transport.Send("", packetData)
	s.cancel()
	return err
}

// Context 返回上下文
func (s *bidiStream) Context() context.Context {
	return s.ctx
}

// streamReader 流式读取器
type streamReader struct {
	ctx      context.Context
	recvChan chan *Packet
	buffer   []byte
	closed   bool
}

// newStreamReader 创建流式读取器
func newStreamReader(ctx context.Context, recvChan chan *Packet) *streamReader {
	return &streamReader{
		ctx:      ctx,
		recvChan: recvChan,
	}
}

// Read 实现 io.Reader
func (r *streamReader) Read(p []byte) (n int, err error) {
	if r.closed && len(r.buffer) == 0 {
		return 0, io.EOF
	}

	if len(r.buffer) > 0 {
		n = copy(p, r.buffer)
		r.buffer = r.buffer[n:]
		return n, nil
	}

	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case packet := <-r.recvChan:
		if packet == nil || packet.Type == PacketStreamEnd {
			r.closed = true
			return 0, io.EOF
		}
		r.buffer = packet.Data
		n = copy(p, r.buffer)
		r.buffer = r.buffer[n:]
		return n, nil
	}
}

// Close 实现 io.Closer
func (r *streamReader) Close() error {
	r.closed = true
	return nil
}
