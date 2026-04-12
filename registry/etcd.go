package zrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdRegistry implements the Registry interface using etcd
type EtcdRegistry struct {
	client   *clientv3.Client
	ttl      time.Duration
	services map[string]*ServiceInfo
	leases   map[string]clientv3.LeaseID
	mu       sync.RWMutex
	stopCh   chan struct{}
}

// NewEtcdRegistry creates a new etcd registry
func NewEtcdRegistry(endpoints []string, timeout, ttl time.Duration) (*EtcdRegistry, error) {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if ttl == 0 {
		ttl = 30 * time.Second
	}

	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	r := &EtcdRegistry{
		client:   client,
		ttl:      ttl,
		services: make(map[string]*ServiceInfo),
		leases:   make(map[string]clientv3.LeaseID),
		stopCh:   make(chan struct{}),
	}

	return r, nil
}

// Register registers a service with etcd
func (r *EtcdRegistry) Register(service *ServiceInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Create lease
	lease, err := r.client.Grant(context.Background(), int64(r.ttl.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	// Marshal service info
	data, err := json.Marshal(service)
	if err != nil {
		return fmt.Errorf("failed to marshal service info: %w", err)
	}

	// Create key
	key := r.serviceKey(service.Name, service.Address)

	// Put with lease
	_, err = r.client.Put(context.Background(), key, string(data), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	// Keep lease alive
	keepAliveCh, err := r.client.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		return fmt.Errorf("failed to keep lease alive: %w", err)
	}

	// Start goroutine to handle keepalive responses
	go func() {
		for {
			select {
			case <-keepAliveCh:
				// Lease kept alive
			case <-r.stopCh:
				return
			}
		}
	}()

	r.services[service.Name] = service
	r.leases[service.Name] = lease.ID

	return nil
}

// Deregister deregisters a service from etcd
func (r *EtcdRegistry) Deregister(serviceName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	service, ok := r.services[serviceName]
	if !ok {
		return nil
	}

	key := r.serviceKey(serviceName, service.Address)

	// Delete the key
	_, err := r.client.Delete(context.Background(), key)
	if err != nil {
		return fmt.Errorf("failed to deregister service: %w", err)
	}

	// Revoke lease
	if leaseID, ok := r.leases[serviceName]; ok {
		_, err = r.client.Revoke(context.Background(), leaseID)
		if err != nil {
			return fmt.Errorf("failed to revoke lease: %w", err)
		}
		delete(r.leases, serviceName)
	}

	delete(r.services, serviceName)

	return nil
}

// Discover discovers a service by name
func (r *EtcdRegistry) Discover(serviceName string) (*ServiceInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := r.servicePrefix(serviceName)

	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to discover service: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("service not found: %s", serviceName)
	}

	// Return the first available instance
	for _, kv := range resp.Kvs {
		var service ServiceInfo
		if err := json.Unmarshal(kv.Value, &service); err != nil {
			continue
		}
		return &service, nil
	}

	return nil, fmt.Errorf("no valid service instances found for: %s", serviceName)
}

// Watch watches for service changes
func (r *EtcdRegistry) Watch(serviceName string) (chan []*ServiceInfo, error) {
	watchCh := make(chan []*ServiceInfo, 10)
	prefix := r.servicePrefix(serviceName)

	// Start watching
	watcher := r.client.Watch(context.Background(), prefix, clientv3.WithPrefix())

	go func() {
		defer close(watchCh)

		// Send initial list
		services, err := r.listServices(serviceName)
		if err == nil {
			watchCh <- services
		}

		for resp := range watcher {
			if resp.Err() != nil {
				continue
			}

			// Get updated list
			services, err := r.listServices(serviceName)
			if err == nil {
				watchCh <- services
			}
		}
	}()

	return watchCh, nil
}

// listServices lists all instances of a service
func (r *EtcdRegistry) listServices(serviceName string) ([]*ServiceInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := r.servicePrefix(serviceName)

	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	services := make([]*ServiceInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var service ServiceInfo
		if err := json.Unmarshal(kv.Value, &service); err != nil {
			continue
		}
		services = append(services, &service)
	}

	return services, nil
}

// Close closes the registry connection
func (r *EtcdRegistry) Close() error {
	close(r.stopCh)

	// Revoke all leases
	r.mu.Lock()
	for serviceName, leaseID := range r.leases {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r.client.Revoke(ctx, leaseID)
		cancel()
		delete(r.leases, serviceName)
	}
	r.mu.Unlock()

	return r.client.Close()
}

// serviceKey returns the etcd key for a service instance
func (r *EtcdRegistry) serviceKey(serviceName, address string) string {
	return fmt.Sprintf("/zrpc/services/%s/%s", serviceName, address)
}

// servicePrefix returns the etcd prefix for a service
func (r *EtcdRegistry) servicePrefix(serviceName string) string {
	return fmt.Sprintf("/zrpc/services/%s/", serviceName)
}
