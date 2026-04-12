package zrpc

import (
	"fmt"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Transport 传输层接口
type Transport interface {
	Send(to string, data []byte) error
	Recv() (from string, data []byte, err error)
	Close() error
}

// ZMQTransport ZeroMQ 传输实现
type ZMQTransport struct {
	socket   *zmq.Socket
	identity string
	endpoint string
	poller   *zmq.Poller
	closed   bool
	mu       sync.Mutex
}

// NewZMQTransport 创建 ZeroMQ 传输
func NewZMQTransport(identity, endpoint string, isServer bool) (*ZMQTransport, error) {
	var socketType zmq.Type
	if isServer {
		socketType = zmq.ROUTER
	} else {
		socketType = zmq.DEALER
	}

	socket, err := zmq.NewSocket(socketType)
	if err != nil {
		return nil, fmt.Errorf("create socket failed: %w", err)
	}

	// 设置身份
	if identity != "" {
		socket.SetIdentity(identity)
	}

	// 设置 socket 选项
	socket.SetRouterMandatory(1)
	socket.SetSndtimeo(5 * time.Second)
	socket.SetRcvtimeo(time.Second)
	socket.SetLinger(0)

	if isServer {
		err = socket.Bind(endpoint)
	} else {
		err = socket.Connect(endpoint)
	}
	if err != nil {
		socket.Close()
		return nil, fmt.Errorf("bind/connect failed: %w", err)
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	return &ZMQTransport{
		socket:   socket,
		identity: identity,
		endpoint: endpoint,
		poller:   poller,
	}, nil
}

// Send 发送数据
func (t *ZMQTransport) Send(to string, data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transport closed")
	}

	fmt.Printf("[ZMQ] Sending %d bytes to '%s'\n", len(data), to)

	// 对于 ROUTER socket，需要发送目标 identity
	if to != "" {
		_, err := t.socket.SendMessage(to, data)
		return err
	}

	_, err := t.socket.SendMessage(data)
	return err
}

// Recv 接收数据
func (t *ZMQTransport) Recv() (from string, data []byte, err error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return "", nil, fmt.Errorf("transport closed")
	}
	t.mu.Unlock()

	// 使用 poller 检查是否有数据
	sockets, err := t.poller.Poll(100 * time.Millisecond)
	if err != nil {
		return "", nil, err
	}

	if len(sockets) == 0 {
		return "", nil, nil // 超时，没有数据
	}

	// 接收消息
	frames, err := t.socket.RecvMessage(0)
	if err != nil {
		return "", nil, err
	}
	fmt.Printf("[ZMQ] Received %d frames\n", len(frames))

	// 对于 ROUTER socket，第一个 frame 是发送者 identity
	// 对于 DEALER socket，直接是数据
	socketType, _ := t.socket.GetType()
	if socketType == zmq.ROUTER {
		if len(frames) < 2 {
			return "", nil, fmt.Errorf("invalid message format")
		}
		return frames[0], []byte(frames[len(frames)-1]), nil
	}

	// DEALER socket
	if len(frames) < 1 {
		return "", nil, fmt.Errorf("invalid message format")
	}
	return "", []byte(frames[len(frames)-1]), nil
}

// Close 关闭传输
func (t *ZMQTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true
	return t.socket.Close()
}

// GetEndpoint 获取端点地址
func (t *ZMQTransport) GetEndpoint() string {
	return t.endpoint
}

// GetIdentity 获取身份标识
func (t *ZMQTransport) GetIdentity() string {
	return t.identity
}
