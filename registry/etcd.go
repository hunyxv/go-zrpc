package registry

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// EtcdRegistry implements the Registry interface using etcd
// Note: This is a placeholder implementation. To use etcd, add:
//   go get go.etcd.io/etcd/client/v3
// and implement the full version.
type EtcdRegistry struct {
	ttl      time.Duration
	services map[string]*ServiceInstance
	mu       sync.RWMutex
	stopCh   chan struct{}
}

// NewEtcdRegistry creates a new etcd registry placeholder
func NewEtcdRegistry(endpoints []string) (*EtcdRegistry, error) {
	_ = endpoints // used in full implementation
	return &EtcdRegistry{
		ttl:      30 * time.Second,
		services: make(map[string]*ServiceInstance),
		stopCh:   make(chan struct{}),
	}, nil
}

// Register registers a service with etcd
func (r *EtcdRegistry) Register(ctx context.Context, instance *ServiceInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[instance.Name] = instance
	return nil
}

// Deregister deregisters a service from etcd
func (r *EtcdRegistry) Deregister(ctx context.Context, instance *ServiceInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.services, instance.Name)
	return nil
}

// GetService gets a service from etcd
func (r *EtcdRegistry) GetService(ctx context.Context, name string) ([]*ServiceInstance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if svc, ok := r.services[name]; ok {
		return []*ServiceInstance{svc}, nil
	}
	return nil, fmt.Errorf("service not found: %s", name)
}

// Watch watches for service changes
func (r *EtcdRegistry) Watch(ctx context.Context, name string) (chan []*ServiceInstance, error) {
	_ = name
	ch := make(chan []*ServiceInstance, 1)
	return ch, nil
}
