package zrpc

import (
	"github.com/vmihailenco/msgpack/v5"
)

// PacketType 数据包类型
type PacketType byte

const (
	// PacketRequest 请求包
	PacketRequest PacketType = iota + 1
	// PacketResponse 响应包
	PacketResponse
	// PacketStream 流式数据包
	PacketStream
	// PacketStreamEnd 流式结束包
	PacketStreamEnd
	// PacketError 错误包
	PacketError
	// PacketHeartbeat 心跳包
	PacketHeartbeat
)

// Header 消息头
type Header map[string]string

// Get 获取头部值
func (h Header) Get(key string) string {
	if h == nil {
		return ""
	}
	return h[key]
}

// Set 设置头部值
func (h Header) Set(key, value string) {
	h[key] = value
}

// Packet 数据包
type Packet struct {
	Type       PacketType `msgpack:"t"`
	ID         string     `msgpack:"id"`
	Service    string     `msgpack:"svc"`
	Method     string     `msgpack:"method"`
	Header     Header     `msgpack:"hdr"`
	Data       []byte     `msgpack:"data"`
	SequenceID uint64     `msgpack:"seq"`
	Error      string     `msgpack:"err"`
}

// NewRequestPacket 创建请求包
func NewRequestPacket(id, service, method string, data []byte) *Packet {
	return &Packet{
		Type:    PacketRequest,
		ID:      id,
		Service: service,
		Method:  method,
		Header:  make(Header),
		Data:    data,
	}
}

// NewResponsePacket 创建响应包
func NewResponsePacket(id string, data []byte) *Packet {
	return &Packet{
		Type:   PacketResponse,
		ID:     id,
		Header: make(Header),
		Data:   data,
	}
}

// NewErrorPacket 创建错误包
func NewErrorPacket(id string, err error) *Packet {
	return &Packet{
		Type:   PacketError,
		ID:     id,
		Header: make(Header),
		Error:  err.Error(),
	}
}

// NewStreamPacket 创建流式数据包
func NewStreamPacket(id string, seq uint64, data []byte) *Packet {
	return &Packet{
		Type:       PacketStream,
		ID:         id,
		Header:     make(Header),
		Data:       data,
		SequenceID: seq,
	}
}

// NewStreamEndPacket 创建流式结束包
func NewStreamEndPacket(id string, seq uint64) *Packet {
	return &Packet{
		Type:       PacketStreamEnd,
		ID:         id,
		Header:     make(Header),
		SequenceID: seq,
	}
}

// NewHeartbeatPacket 创建心跳包
func NewHeartbeatPacket() *Packet {
	return &Packet{
		Type:   PacketHeartbeat,
		Header: make(Header),
	}
}

// Marshal 序列化
func (p *Packet) Marshal() ([]byte, error) {
	return msgpack.Marshal(p)
}

// UnmarshalPacket 反序列化
func UnmarshalPacket(data []byte) (*Packet, error) {
	p := &Packet{}
	err := msgpack.Unmarshal(data, p)
	return p, err
}
