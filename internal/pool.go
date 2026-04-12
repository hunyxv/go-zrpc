package internal

import (
	"fmt"
	"sync"
	"time"
)

// Pool represents a connection pool
type Pool struct {
	size        int
	ttl         time.Duration
	factory     Factory
	connections map[string]*poolBucket
	mu          sync.RWMutex
	closed      bool
}

// Factory is a function that creates a new connection
type Factory func(address string) (interface{}, error)

// poolBucket holds connections for a specific address
type poolBucket struct {
	address     string
	connections []interface{}
	inUse       map[interface{}]bool
	mu          sync.Mutex
}

// NewPool creates a new connection pool
func NewPool(size int, ttl time.Duration, factory Factory) *Pool {
	return &Pool{
		size:        size,
		ttl:         ttl,
		factory:     factory,
		connections: make(map[string]*poolBucket),
	}
}

// Get gets a connection from the pool
func (p *Pool) Get(address string) (interface{}, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("pool is closed")
	}
	p.mu.RUnlock()

	p.mu.Lock()
	bucket, ok := p.connections[address]
	if !ok {
		bucket = &poolBucket{
			address:     address,
			connections: make([]interface{}, 0, p.size),
			inUse:       make(map[interface{}]bool),
		}
		p.connections[address] = bucket
	}
	p.mu.Unlock()

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	// Try to find an available connection
	for len(bucket.connections) > 0 {
		conn := bucket.connections[0]
		bucket.connections = bucket.connections[1:]
		bucket.inUse[conn] = true
		return conn, nil
	}

	// Create a new connection if pool is not full
	if len(bucket.inUse) < p.size {
		conn, err := p.factory(address)
		if err != nil {
			return nil, err
		}
		bucket.inUse[conn] = true
		return conn, nil
	}

	return nil, fmt.Errorf("pool exhausted for address: %s", address)
}

// Put returns a connection to the pool
func (p *Pool) Put(address string, conn interface{}) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return
	}
	bucket, ok := p.connections[address]
	p.mu.RUnlock()

	if !ok {
		return
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	delete(bucket.inUse, conn)
	
	// Only return to pool if not full
	if len(bucket.connections) < p.size {
		bucket.connections = append(bucket.connections, conn)
	}
}

// Close closes the pool and all connections
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	buckets := make([]*poolBucket, 0, len(p.connections))
	for _, bucket := range p.connections {
		buckets = append(buckets, bucket)
	}
	p.mu.Unlock()

	// Close all connections
	for _, bucket := range buckets {
		bucket.mu.Lock()
		for conn := range bucket.inUse {
			if closer, ok := conn.(interface{ Close() error }); ok {
				closer.Close()
			}
		}
		for _, conn := range bucket.connections {
			if closer, ok := conn.(interface{ Close() error }); ok {
				closer.Close()
			}
		}
		bucket.mu.Unlock()
	}
}

// Stats returns pool statistics
func (p *Pool) Stats() map[string]PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]PoolStats)
	for address, bucket := range p.connections {
		bucket.mu.Lock()
		stats[address] = PoolStats{
			Available: len(bucket.connections),
			InUse:     len(bucket.inUse),
			MaxSize:   p.size,
		}
		bucket.mu.Unlock()
	}

	return stats
}

// PoolStats holds pool statistics
type PoolStats struct {
	Available int
	InUse     int
	MaxSize   int
}

// PoolConn wraps a pooled connection
type PoolConn struct {
	Conn    interface{}
	pool    *Pool
	address string
	closed  bool
	mu      sync.Mutex
}

// NewPoolConn creates a new pooled connection wrapper
func NewPoolConn(conn interface{}, pool *Pool, address string) *PoolConn {
	return &PoolConn{
		Conn:    conn,
		pool:    pool,
		address: address,
	}
}

// Close returns the connection to the pool
func (c *PoolConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.pool.Put(c.address, c.Conn)
	return nil
}

// Release permanently closes the connection
func (c *PoolConn) Release() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	if closer, ok := c.Conn.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
