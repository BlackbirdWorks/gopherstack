package servicediscovery

import "time"

// NamespaceCount returns the number of namespaces in the backend (test use only).
func NamespaceCount(b *InMemoryBackend) int {
	b.mu.RLock("NamespaceCount")
	defer b.mu.RUnlock()

	return b.namespaces.Len()
}

// ServiceCount returns the number of services in the backend (test use only).
func ServiceCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceCount")
	defer b.mu.RUnlock()

	return b.services.Len()
}

// InstanceCount returns the number of instances in the backend (test use only).
func InstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return b.instances.Len()
}

// OperationCount returns the number of operations in the backend (test use only).
func OperationCount(b *InMemoryBackend) int {
	b.mu.RLock("OperationCount")
	defer b.mu.RUnlock()

	return b.operations.Len()
}

// ServiceAttributeCount returns the number of service attribute entries (test use only).
func ServiceAttributeCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceAttributeCount")
	defer b.mu.RUnlock()

	return len(b.serviceAttributes)
}

// HandlerOpsLen returns the number of supported operations (test use only).
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// SetDeterministicIDs enables counter-based ID generation for deterministic test output.
func SetDeterministicIDs(b *InMemoryBackend) {
	b.deterministicIDs = true
}

// AddNamespaceInternal directly seeds a namespace into the backend (test use only).
func AddNamespaceInternal(b *InMemoryBackend, ns *Namespace) {
	b.mu.Lock("AddNamespaceInternal")
	defer b.mu.Unlock()

	b.namespaces.Put(ns)
}

// AddServiceInternal directly seeds a service into the backend (test use only).
func AddServiceInternal(b *InMemoryBackend, svc *Service) {
	b.mu.Lock("AddServiceInternal")
	defer b.mu.Unlock()

	b.services.Put(svc)
}

// AddInstanceInternal directly seeds an instance into the backend (test use only).
func AddInstanceInternal(b *InMemoryBackend, inst *Instance) {
	b.mu.Lock("AddInstanceInternal")
	defer b.mu.Unlock()

	b.instances.Put(inst)
}

// NewNamespaceForTest creates a Namespace value for seeding in tests.
func NewNamespaceForTest(id, name, nsType string) *Namespace {
	return &Namespace{
		ID:        id,
		ARN:       "arn:aws:servicediscovery:us-east-1:000000000000:namespace/" + id,
		Name:      name,
		Type:      nsType,
		CreatedAt: time.Now(),
	}
}

// NewServiceForTest creates a Service value for seeding in tests.
func NewServiceForTest(id, name, namespaceID string) *Service {
	return &Service{
		ID:          id,
		ARN:         "arn:aws:servicediscovery:us-east-1:000000000000:service/" + id,
		Name:        name,
		NamespaceID: namespaceID,
		CreatedAt:   time.Now(),
	}
}

// NewServiceWithCustomHealthForTest creates a Service with HealthCheckCustomConfig for seeding.
func NewServiceWithCustomHealthForTest(id, name, namespaceID string) *Service {
	return &Service{
		ID:                      id,
		ARN:                     "arn:aws:servicediscovery:us-east-1:000000000000:service/" + id,
		Name:                    name,
		NamespaceID:             namespaceID,
		HealthCheckCustomConfig: &HealthCheckCustomConfig{FailureThreshold: 1},
		CreatedAt:               time.Now(),
	}
}

// NewInstanceForTest creates an Instance value for seeding in tests.
func NewInstanceForTest(id, serviceID string, attrs map[string]string) *Instance {
	return &Instance{
		ID:         id,
		ServiceID:  serviceID,
		Attributes: attrs,
	}
}
