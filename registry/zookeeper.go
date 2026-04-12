package zrpc

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

// ZookeeperRegistry implements the Registry interface using ZooKeeper
type ZookeeperRegistry struct {
	conn     *zk.Conn
	ttl      time.Duration
	services map[string]*ServiceInfo
	mu       sync.RWMutex
	stopCh   chan struct{}
}

// NewZookeeperRegistry creates a new ZooKeeper registry
func NewZookeeperRegistry(servers []string, timeout, ttl time.Duration) (*ZookeeperRegistry, error) {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if ttl == 0 {
		ttl = 30 * time.Second
	}

	conn, _, err := zk.Connect(servers, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to zookeeper: %w", err)
	}

	r := &ZookeeperRegistry{
		conn:     conn,
		ttl:      ttl,
		services: make(map[string]*ServiceInfo),
		stopCh:   make(chan struct{}),
	}

	// Create base path
	if err := r.ensurePath("/zrpc/services"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create base path: %w", err)
	}

	return r, nil
}

// Register registers a service with ZooKeeper
func (r *ZookeeperRegistry) Register(service *ServiceInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Create service path
	servicePath := fmt.Sprintf("/zrpc/services/%s", service.Name)
	if err := r.ensurePath(servicePath); err != nil {
		return fmt.Errorf("failed to create service path: %w", err)
	}

	// Marshal service info
	data, err := json.Marshal(service)
	if err != nil {
		return fmt.Errorf("failed to marshal service info: %w", err)
	}

	// Create ephemeral node for service instance
	instancePath := fmt.Sprintf("%s/%s", servicePath, service.Address)
	_, err = r.conn.Create(instancePath, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		// Node exists, try to update
		_, err = r.conn.Set(instancePath, data, -1)
	}
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	r.services[service.Name] = service

	return nil
}

// Deregister deregisters a service from ZooKeeper
func (r *ZookeeperRegistry) Deregister(serviceName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	service, ok := r.services[serviceName]
	if !ok {
		return nil
	}

	instancePath := fmt.Sprintf("/zrpc/services/%s/%s", serviceName, service.Address)

	// Delete the node
	if err := r.conn.Delete(instancePath, -1); err != nil && err != zk.ErrNoNode {
		return fmt.Errorf("failed to deregister service: %w", err)
	}

	delete(r.services, serviceName)

	return nil
}

// Discover discovers a service by name
func (r *ZookeeperRegistry) Discover(serviceName string) (*ServiceInfo, error) {
	servicePath := fmt.Sprintf("/zrpc/services/%s", serviceName)

	children, _, err := r.conn.Children(servicePath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, fmt.Errorf("service not found: %s", serviceName)
		}
		return nil, fmt.Errorf("failed to discover service: %w", err)
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no instances found for service: %s", serviceName)
	}

	// Return the first available instance
	for _, child := range children {
		data, _, err := r.conn.Get(fmt.Sprintf("%s/%s", servicePath, child))
		if err != nil {
			continue
		}

		var service ServiceInfo
		if err := json.Unmarshal(data, &service); err != nil {
			continue
		}

		return &service, nil
	}

	return nil, fmt.Errorf("no valid service instances found for: %s", serviceName)
}

// Watch watches for service changes
func (r *ZookeeperRegistry) Watch(serviceName string) (chan []*ServiceInfo, error) {
	watchCh := make(chan []*ServiceInfo, 10)
	servicePath := fmt.Sprintf("/zrpc/services/%s", serviceName)

	// Start watching in a goroutine
	go func() {
		defer close(watchCh)

		for {
			// Get children and watch
			children, _, eventCh, err := r.conn.ChildrenW(servicePath)
			if err != nil {
				if err == zk.ErrNoNode {
					// Wait for node to be created
					time.Sleep(1 * time.Second)
					continue
				}
				return
			}

			// Get service info for each child
			services := make([]*ServiceInfo, 0, len(children))
			for _, child := range children {
				data, _, err := r.conn.Get(fmt.Sprintf("%s/%s", servicePath, child))
				if err != nil {
					continue
				}

				var service ServiceInfo
				if err := json.Unmarshal(data, &service); err != nil {
					continue
				}

				services = append(services, &service)
			}

			select {
			case watchCh <- services:
			case <-r.stopCh:
				return
			}

			// Wait for next event
			select {
			case <-eventCh:
				// Children changed, loop will fetch new list
			case <-r.stopCh:
				return
			}
		}
	}()

	return watchCh, nil
}

// Close closes the registry connection
func (r *ZookeeperRegistry) Close() error {
	close(r.stopCh)

	// Deregister all services
	r.mu.Lock()
	for serviceName := range r.services {
		r.Deregister(serviceName)
	}
	r.mu.Unlock()

	r.conn.Close()
	return nil
}

// ensurePath ensures that the given path exists
func (r *ZookeeperRegistry) ensurePath(path string) error {
	parts := strings.Split(path, "/")
	current := ""

	for _, part := range parts {
		if part == "" {
			continue
		}

		current = current + "/" + part
		exists, _, err := r.conn.Exists(current)
		if err != nil {
			return err
		}

		if !exists {
			_, err = r.conn.Create(current, []byte{}, 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				return err
			}
		}
	}

	return nil
}
