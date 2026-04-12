package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ServiceInstance 服务实例
type ServiceInstance struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Address  string            `json:"address"`
	Port     int               `json:"port"`
	Metadata map[string]string `json:"metadata"`
	Version  string            `json:"version"`
}

// Registry 服务注册接口
type Registry interface {
	// Register 注册服务
	Register(ctx context.Context, instance *ServiceInstance) error
	// Deregister 注销服务
	Deregister(ctx context.Context, instance *ServiceInstance) error
}

// Discovery 服务发现接口
type Discovery interface {
	// GetService 获取服务实例
	GetService(ctx context.Context, name string) ([]*ServiceInstance, error)
	// Watch 监听服务变化
	Watch(ctx context.Context, name string) (chan []*ServiceInstance, error)
}

// RegisterDiscovery 组合接口
type RegisterDiscovery interface {
	Registry
	Discovery
}

// simpleRegistry 简单内存注册表（用于测试）
type simpleRegistry struct {
	services map[string][]*ServiceInstance
}

// NewSimpleRegistry 创建简单注册表
func NewSimpleRegistry() RegisterDiscovery {
	return &simpleRegistry{
		services: make(map[string][]*ServiceInstance),
	}
}

func (r *simpleRegistry) Register(ctx context.Context, instance *ServiceInstance) error {
	r.services[instance.Name] = append(r.services[instance.Name], instance)
	return nil
}

func (r *simpleRegistry) Deregister(ctx context.Context, instance *ServiceInstance) error {
	instances := r.services[instance.Name]
	for i, inst := range instances {
		if inst.ID == instance.ID {
			r.services[instance.Name] = append(instances[:i], instances[i+1:]...)
			break
		}
	}
	return nil
}

func (r *simpleRegistry) GetService(ctx context.Context, name string) ([]*ServiceInstance, error) {
	return r.services[name], nil
}

func (r *simpleRegistry) Watch(ctx context.Context, name string) (chan []*ServiceInstance, error) {
	ch := make(chan []*ServiceInstance, 1)
	ch <- r.services[name]
	return ch, nil
}

// ServiceKey 生成服务键
func ServiceKey(name, id string) string {
	return fmt.Sprintf("/zrpc/services/%s/%s", name, id)
}

// EncodeInstance 编码服务实例
func EncodeInstance(instance *ServiceInstance) ([]byte, error) {
	return json.Marshal(instance)
}

// DecodeInstance 解码服务实例
func DecodeInstance(data []byte) (*ServiceInstance, error) {
	instance := &ServiceInstance{}
	err := json.Unmarshal(data, instance)
	return instance, err
}
