package glue

import (
	"fmt"
	"maps"
)

// BatchGetDevEndpoints retrieves multiple dev endpoints by name.
func (b *InMemoryBackend) BatchGetDevEndpoints(names []string) ([]*DevEndpoint, []string) {
	b.mu.RLock("BatchGetDevEndpoints")
	defer b.mu.RUnlock()

	found := make([]*DevEndpoint, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		dep, ok := b.devEndpoints.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		cp := *dep
		found = append(found, &cp)
	}

	return found, missing
}

// AddDevEndpointInternal adds a dev endpoint directly to the backend without validation.
func (b *InMemoryBackend) AddDevEndpointInternal(dep *DevEndpoint) {
	b.mu.Lock("AddDevEndpointInternal")
	defer b.mu.Unlock()

	cp := *dep
	b.devEndpoints.Put(&cp)
}

func (b *InMemoryBackend) UpdateDevEndpoint(name string, args map[string]string) error {
	b.mu.Lock("UpdateDevEndpoint")
	defer b.mu.Unlock()

	dep, ok := b.devEndpoints.Get(name)
	if !ok {
		return fmt.Errorf("dev endpoint %q not found: %w", name, ErrNotFound)
	}
	if dep.Arguments == nil {
		dep.Arguments = make(map[string]string)
	}
	maps.Copy(dep.Arguments, args)

	return nil
}

// CreateDevEndpoint creates a new Glue dev endpoint.
func (b *InMemoryBackend) CreateDevEndpoint(name string) (*DevEndpoint, error) {
	b.mu.Lock("CreateDevEndpoint")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrValidation
	}

	if b.devEndpoints.Has(name) {
		return nil, ErrAlreadyExists
	}

	dep := &DevEndpoint{
		EndpointName: name,
		Status:       stateReady,
	}
	b.devEndpoints.Put(dep)

	cp := *dep

	return &cp, nil
}

// GetDevEndpoint retrieves a Glue dev endpoint by name.
func (b *InMemoryBackend) GetDevEndpoint(name string) (*DevEndpoint, error) {
	b.mu.RLock("GetDevEndpoint")
	defer b.mu.RUnlock()

	dep, ok := b.devEndpoints.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *dep

	return &cp, nil
}

// GetAllDevEndpoints returns all dev endpoints sorted by name.
func (b *InMemoryBackend) GetAllDevEndpoints() []*DevEndpoint {
	b.mu.RLock("GetAllDevEndpoints")
	defer b.mu.RUnlock()

	src := b.devEndpoints.Snapshot()
	out := make([]*DevEndpoint, 0, len(src))
	for _, dep := range src {
		cp := *dep
		out = append(out, &cp)
	}

	return out
}

// DeleteDevEndpoint deletes a Glue dev endpoint by name.
func (b *InMemoryBackend) DeleteDevEndpoint(name string) error {
	b.mu.Lock("DeleteDevEndpoint")
	defer b.mu.Unlock()

	if !b.devEndpoints.Has(name) {
		return ErrNotFound
	}

	b.devEndpoints.Delete(name)

	return nil
}
