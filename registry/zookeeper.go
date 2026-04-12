package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ZookeeperRegistry implements the Registry interface using ZooKeeper
// Note: This is a placeholder implementation. To use ZooKeeper, add:
//   go get github.com/go-zookeeper/zk
// and implement the full version.
type ZookeeperRegistry struct {
	ttl      time.Duration
	services map[string]*ServiceInstance
	mu       sync.RWMutex
	stopCh   chan struct{}
}

// NewZookeeperRegistry creates a new ZooKeeper registry placeholder
func NewZookeeperRegistry(servers []string) (*ZookeeperRegistry, error) {
	_ = servers // used in full implementation
	return &ZookeeperRegistry{
		ttl:      30 * time.Second,
		services: make(map[string]*ServiceInstance),
		stopCh:   make(chan struct{}),
	}, nil
}

// Register registers a service with ZooKeeper
func (r *ZookeeperRegistry) Register(ctx context.Context, instance *ServiceInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[instance.Name] = instance
	return nil
}

// Deregister deregisters a service from ZooKeeper
func (r *ZookeeperRegistry) Deregister(ctx context.Context, instance *ServiceInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.services, instance.Name)
	return nil
}

// GetService gets a service from ZooKeeper
func (r *ZookeeperRegistry) GetService(ctx context.Context, name string) ([]*ServiceInstance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if svc, ok := r.services[name]; ok {
		return []*ServiceInstance{svc}, nil
	}
	return nil, fmt.Errorf("service not found: %s", name)
}

// Watch watches for service changes
func (r *ZookeeperRegistry) Watch(ctx context.Context, name string) (chan []*ServiceInstance, error) {
	_ = name
	ch := make(chan []*ServiceInstance, 1)
	return ch, nil
}

// encodeInstance encodes a service instance to JSON
func encodeInstance(instance *ServiceInstance) ([]byte, error) {
	return json.Marshal(instance)
}

// decodeInstance decodes a service instance from JSON
func decodeInstance(data []byte) (*ServiceInstance, error) {
	instance := &ServiceInstance{}
	err := json.Unmarshal(data, instance)
	return instance, err
}
