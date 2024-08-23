package teststore

import (
	"context"
	"sync"

	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

// SwarmInMemoryStore represents an in-memory key-value store for Swarm.
type SwarmInMemoryStore struct {
	data map[string]swarm.Chunk // Change to use string as the key
	mu   sync.RWMutex
}

// NewSwarmInMemoryStore creates a new in-memory key-value store.
func NewSwarmInMemoryStore() *SwarmInMemoryStore {
	return &SwarmInMemoryStore{
		data: make(map[string]swarm.Chunk), // Initialize the map with string keys
	}
}

// Put stores the given chunk in the store.
func (s *SwarmInMemoryStore) Put(ctx context.Context, chunk swarm.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := chunk.Address().String() // Convert the address to a string
	s.data[key] = chunk
	return nil
}

// Get retrieves the chunk associated with the given address.
func (s *SwarmInMemoryStore) Get(ctx context.Context, address swarm.Address) (swarm.Chunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := address.String() // Convert the address to a string
	chunk, exists := s.data[key]
	if !exists {
		return nil, storage.ErrNotFound
	}

	return chunk, nil
}

// Close closes the in-memory store (no-op for this simple implementation).
func (s *SwarmInMemoryStore) Close() error {
	// No resources to clean up in this simple implementation
	return nil
}
