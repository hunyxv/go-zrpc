package zrpc

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Codec 编解码器接口
type Codec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
	Name() string
}

// MsgpackCodec msgpack 编解码器
type MsgpackCodec struct{}

// Marshal 序列化
func (c *MsgpackCodec) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Unmarshal 反序列化
func (c *MsgpackCodec) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// Name 返回编解码器名称
func (c *MsgpackCodec) Name() string {
	return "msgpack"
}

// DefaultCodec 默认编解码器
var DefaultCodec Codec = &MsgpackCodec{}

// SetDefaultCodec 设置默认编解码器
func SetDefaultCodec(c Codec) {
	DefaultCodec = c
}
