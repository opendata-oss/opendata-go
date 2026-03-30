package objstore

import (
	"context"
	"fmt"
	"sync"
)

type memObject struct {
	data    []byte
	version int64
}

// InMemory is an in-memory ObjectStore implementation for testing.
type InMemory struct {
	mu      sync.Mutex
	objects map[string]*memObject
}

// NewInMemory creates a new in-memory object store.
func NewInMemory() *InMemory {
	return &InMemory{objects: make(map[string]*memObject)}
}

// Get implements ObjectStore.
func (m *InMemory) Get(_ context.Context, path string) (GetResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	obj, ok := m.objects[path]
	if !ok {
		return GetResult{}, ErrNotFound
	}
	data := make([]byte, len(obj.data))
	copy(data, obj.data)
	return GetResult{
		Data:    data,
		Version: Version{ETag: fmt.Sprintf("%d", obj.version)},
	}, nil
}

// Put implements ObjectStore.
func (m *InMemory) Put(_ context.Context, path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	stored := make([]byte, len(data))
	copy(stored, data)

	obj, ok := m.objects[path]
	if ok {
		obj.data = stored
		obj.version++
	} else {
		m.objects[path] = &memObject{data: stored, version: 1}
	}
	return nil
}

// PutIfMatch implements ObjectStore.
func (m *InMemory) PutIfMatch(_ context.Context, path string, data []byte, version *Version) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	stored := make([]byte, len(data))
	copy(stored, data)

	obj, ok := m.objects[path]

	if version == nil {
		// Create mode: object must not exist.
		if ok {
			return ErrPreconditionFailed
		}
		m.objects[path] = &memObject{data: stored, version: 1}
		return nil
	}

	// Update mode: version must match.
	if !ok {
		return ErrPreconditionFailed
	}
	expected := fmt.Sprintf("%d", obj.version)
	if version.ETag != expected {
		return ErrPreconditionFailed
	}
	obj.data = stored
	obj.version++
	return nil
}

// Delete implements ObjectStore.
func (m *InMemory) Delete(_ context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, path)
	return nil
}
