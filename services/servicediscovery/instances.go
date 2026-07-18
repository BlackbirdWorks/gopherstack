package servicediscovery

import (
	"fmt"
	"sort"
	"time"
)

// RegisterInstance registers an instance to a service.
func (b *InMemoryBackend) RegisterInstance(serviceID, instanceID string, attrs map[string]string) (string, error) {
	b.mu.Lock("RegisterInstance")
	defer b.mu.Unlock()

	if !b.services.Has(serviceID) {
		return "", fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	inst := &Instance{
		ID:         instanceID,
		ServiceID:  serviceID,
		Attributes: copyAttrs(attrs),
	}
	b.instances.Put(inst)

	b.instanceRevision++

	now := time.Now()
	opID := b.nextOpID()
	b.operations.Put(&Operation{
		ID:         opID,
		Type:       operationTypeRegisterInstance,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeInstance: instanceID, typeService: serviceID},
		CreateDate: now,
		UpdateDate: now,
	})

	return opID, nil
}

// DeregisterInstance deregisters an instance from a service.
func (b *InMemoryBackend) DeregisterInstance(serviceID, instanceID string) (string, error) {
	b.mu.Lock("DeregisterInstance")
	defer b.mu.Unlock()

	key := instanceKey(serviceID, instanceID)
	if !b.instances.Has(key) {
		return "", fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	b.instances.Delete(key)
	delete(b.instanceHealthStatuses, key)

	b.instanceRevision++

	now := time.Now()
	opID := b.nextOpID()
	b.operations.Put(&Operation{
		ID:         opID,
		Type:       operationTypeDeregisterInstance,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeInstance: instanceID, typeService: serviceID},
		CreateDate: now,
		UpdateDate: now,
	})

	return opID, nil
}

// GetInstance returns a registered instance.
func (b *InMemoryBackend) GetInstance(serviceID, instanceID string) (*Instance, error) {
	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	key := instanceKey(serviceID, instanceID)
	inst, ok := b.instances.Get(key)

	if !ok {
		return nil, fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	cp := *inst
	cp.Attributes = copyAttrs(inst.Attributes)

	return &cp, nil
}

// ListInstances returns all instances registered to a service.
func (b *InMemoryBackend) ListInstances(serviceID string) ([]Instance, error) {
	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	if !b.services.Has(serviceID) {
		return nil, fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	insts := b.instancesByService.Get(serviceID)
	result := make([]Instance, 0, len(insts))

	for _, inst := range insts {
		cp := *inst
		cp.Attributes = copyAttrs(inst.Attributes)
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// GetInstancesHealthStatus returns the health status for instances in a service.
// If instanceIDs is non-empty, only those instances are included.
// Instances without a recorded status default to HEALTHY.
func (b *InMemoryBackend) GetInstancesHealthStatus(serviceID string, instanceIDs []string) (map[string]string, error) {
	b.mu.RLock("GetInstancesHealthStatus")
	defer b.mu.RUnlock()

	if !b.services.Has(serviceID) {
		return nil, fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	filter := make(map[string]struct{}, len(instanceIDs))
	for _, id := range instanceIDs {
		filter[id] = struct{}{}
	}

	statuses := make(map[string]string)
	insts := b.instancesByService.Get(serviceID)

	for _, inst := range insts {
		instID := inst.ID

		if len(filter) > 0 {
			if _, ok := filter[instID]; !ok {
				continue
			}
		}

		key := instanceKey(serviceID, instID)
		status := b.instanceHealthStatuses[key]
		if status == "" {
			status = instanceHealthStatusHealthy
		}

		statuses[instID] = status
	}

	return statuses, nil
}

// UpdateInstanceCustomHealthStatus sets a custom health status for an instance.
// Returns CustomHealthNotFound if the service has no HealthCheckCustomConfig.
func (b *InMemoryBackend) UpdateInstanceCustomHealthStatus(serviceID, instanceID, status string) error {
	b.mu.Lock("UpdateInstanceCustomHealthStatus")
	defer b.mu.Unlock()

	if status != instanceHealthStatusHealthy && status != instanceHealthStatusUnhealthy {
		return fmt.Errorf(
			"%w: status must be %s or %s",
			ErrInvalidInput,
			instanceHealthStatusHealthy,
			instanceHealthStatusUnhealthy,
		)
	}

	svc, ok := b.services.Get(serviceID)
	if !ok {
		return fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	key := instanceKey(serviceID, instanceID)
	if !b.instances.Has(key) {
		return fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	if svc.HealthCheckCustomConfig == nil {
		return fmt.Errorf(
			"%w: service %s has no custom health check configured",
			ErrCustomHealthNotFound,
			serviceID,
		)
	}

	b.instanceHealthStatuses[key] = status

	return nil
}
