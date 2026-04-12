package zrpc

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

// ConsulRegistry implements the Registry interface using Consul
type ConsulRegistry struct {
	client   *api.Client
	ttl      time.Duration
	services map[string]*ServiceInfo
	agents   map[string]string // service name -> check ID
	mu       sync.RWMutex
	stopCh   chan struct{}
}

// NewConsulRegistry creates a new Consul registry
func NewConsulRegistry(addresses []string, timeout, ttl time.Duration) (*ConsulRegistry, error) {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if ttl == 0 {
		ttl = 30 * time.Second
	}

	config := api.DefaultConfig()
	if len(addresses) > 0 {
		config.Address = addresses[0]
	}
	config.WaitTime = timeout

	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %w", err)
	}

	r := &ConsulRegistry{
		client:   client,
		ttl:      ttl,
		services: make(map[string]*ServiceInfo),
		agents:   make(map[string]string),
		stopCh:   make(chan struct{}),
	}

	return r, nil
}

// Register registers a service with Consul
func (r *ConsulRegistry) Register(service *ServiceInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Create service registration
	registration := &api.AgentServiceRegistration{
		ID:      fmt.Sprintf("%s-%s", service.Name, service.Address),
		Name:    service.Name,
		Address: service.Address,
		Tags:    []string{"zrpc", "v" + service.Version},
		Meta:    service.Metadata,
		Check: &api.AgentServiceCheck{
			TTL:                            r.ttl.String(),
			DeregisterCriticalServiceAfter: "1m",
		},
	}

	// Parse address to get port
	if host, port, err := net.SplitHostPort(service.Address); err == nil {
		registration.Address = host
		if p, err := strconv.Atoi(port); err == nil {
			registration.Port = p
		}
	}

	// Register service
	if err := r.client.Agent().ServiceRegister(registration); err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	// Start TTL check updater
	checkID := "service:" + registration.ID
	r.agents[service.Name] = checkID

	go r.keepAlive(checkID)

	r.services[service.Name] = service

	return nil
}

// keepAlive keeps the service check alive
func (r *ConsulRegistry) keepAlive(checkID string) {
	ticker := time.NewTicker(r.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.client.Agent().UpdateTTL(checkID, "", api.HealthPassing); err != nil {
				// Log error but continue trying
				continue
			}
		case <-r.stopCh:
			return
		}
	}
}

// Deregister deregisters a service from Consul
func (r *ConsulRegistry) Deregister(serviceName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	service, ok := r.services[serviceName]
	if !ok {
		return nil
	}

	serviceID := fmt.Sprintf("%s-%s", serviceName, service.Address)

	if err := r.client.Agent().ServiceDeregister(serviceID); err != nil {
		return fmt.Errorf("failed to deregister service: %w", err)
	}

	delete(r.agents, serviceName)
	delete(r.services, serviceName)

	return nil
}

// Discover discovers a service by name
func (r *ConsulRegistry) Discover(serviceName string) (*ServiceInfo, error) {
	services, _, err := r.client.Health().Service(serviceName, "", true, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to discover service: %w", err)
	}

	if len(services) == 0 {
		return nil, fmt.Errorf("service not found: %s", serviceName)
	}

	// Return the first healthy instance
	entry := services[0]
	address := entry.Service.Address
	if entry.Service.Port > 0 {
		address = fmt.Sprintf("%s:%d", address, entry.Service.Port)
	}

	return &ServiceInfo{
		Name:     entry.Service.Service,
		Address:  address,
		Version:  entry.Service.Meta["version"],
		Metadata: entry.Service.Meta,
	}, nil
}

// Watch watches for service changes
func (r *ConsulRegistry) Watch(serviceName string) (chan []*ServiceInfo, error) {
	watchCh := make(chan []*ServiceInfo, 10)

	// Poll for changes since watch.Parse is not available
	go func() {
		defer close(watchCh)
		
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		
		var lastServices []*ServiceInfo
		
		for {
			select {
			case <-ticker.C:
				services, _, err := r.client.Health().Service(serviceName, "", true, nil)
				if err != nil {
					continue
				}

				currentServices := make([]*ServiceInfo, 0, len(services))
				for _, entry := range services {
					if entry.Checks.AggregatedStatus() != api.HealthPassing {
						continue
					}

					address := entry.Service.Address
					if entry.Service.Port > 0 {
						address = fmt.Sprintf("%s:%d", address, entry.Service.Port)
					}

					currentServices = append(currentServices, &ServiceInfo{
						Name:     entry.Service.Service,
						Address:  address,
						Version:  entry.Service.Meta["version"],
						Metadata: entry.Service.Meta,
					})
				}
				
				// Only send if changed
				if !servicesEqual(lastServices, currentServices) {
					lastServices = currentServices
					select {
					case watchCh <- currentServices:
					case <-r.stopCh:
						return
					}
				}
				
			case <-r.stopCh:
				return
			}
		}
	}()

	return watchCh, nil
}

// servicesEqual checks if two service lists are equal
func servicesEqual(a, b []*ServiceInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Address != b[i].Address {
			return false
		}
	}
	return true
}

// Close closes the registry connection
func (r *ConsulRegistry) Close() error {
	close(r.stopCh)

	// Deregister all services
	r.mu.Lock()
	for serviceName := range r.services {
		r.Deregister(serviceName)
	}
	r.mu.Unlock()

	return nil
}