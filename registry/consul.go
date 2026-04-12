package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ConsulRegistry implements the Registry interface using Consul
// Note: This is a placeholder implementation. To use Consul, add:
//   go get github.com/hashicorp/consul/api
// and implement the full version.
type ConsulRegistry struct {
	ttl      time.Duration
	services map[string]*ServiceInstance
	mu       sync.RWMutex
	stopCh   chan struct{}
}

// NewConsulRegistry creates a new Consul registry placeholder
func NewConsulRegistry(address string) (*ConsulRegistry, error) {
	_ = address // used in full implementation
	return &ConsulRegistry{
		ttl:      30 * time.Second,
		services: make(map[string]*ServiceInstance),
		stopCh:   make(chan struct{}),
	}, nil
}

// Register registers a service with Consul
func (r *ConsulRegistry) Register(ctx context.Context, instance *ServiceInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[instance.Name] = instance
	return nil
}

// Deregister deregisters a service from Consul
func (r *ConsulRegistry) Deregister(ctx context.Context, instance *ServiceInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.services, instance.Name)
	return nil
}

// GetService gets a service from Consul
func (r *ConsulRegistry) GetService(ctx context.Context, name string) ([]*ServiceInstance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if svc, ok := r.services[name]; ok {
		return []*ServiceInstance{svc}, nil
	}
	return nil, fmt.Errorf("service not found: %s", name)
}

// Watch watches for service changes
func (r *ConsulRegistry) Watch(ctx context.Context, name string) (chan []*ServiceInstance, error) {
	_ = name
	ch := make(chan []*ServiceInstance, 1)
	return ch, nil
}
