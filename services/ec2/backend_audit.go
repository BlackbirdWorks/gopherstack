package ec2

import "fmt"

// ModifyInstancePlacement updates the placement attributes (tenancy,
// availability zone, placement group, affinity) of an existing instance. Only
// non-empty fields are applied. Returns ErrInstanceNotFound if the instance
// does not exist.
func (b *InMemoryBackend) ModifyInstancePlacement(
	instanceID string,
	placement InstancePlacement,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstancePlacement")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if placement.Tenancy != "" {
		inst.Placement.Tenancy = placement.Tenancy
	}
	if placement.AvailabilityZone != "" {
		inst.Placement.AvailabilityZone = placement.AvailabilityZone
	}
	if placement.GroupName != "" {
		inst.Placement.GroupName = placement.GroupName
	}
	if placement.Affinity != "" {
		inst.Placement.Affinity = placement.Affinity
	}

	cp := *inst

	return &cp, nil
}

// ModifyInstanceCPUOptions updates the CPU options (core count, threads per
// core) of an existing instance. Only positive values are applied. Returns
// ErrInstanceNotFound if the instance does not exist.
func (b *InMemoryBackend) ModifyInstanceCPUOptions(
	instanceID string,
	opts InstanceCPUOptions,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceCPUOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if opts.CoreCount > 0 {
		inst.CPUOptions.CoreCount = opts.CoreCount
	}
	if opts.ThreadsPerCore > 0 {
		inst.CPUOptions.ThreadsPerCore = opts.ThreadsPerCore
	}

	cp := *inst

	return &cp, nil
}

// ModifyInstanceMaintenanceOptions updates the maintenance options
// (auto-recovery) of an existing instance. Returns ErrInstanceNotFound if the
// instance does not exist.
func (b *InMemoryBackend) ModifyInstanceMaintenanceOptions(
	instanceID string,
	opts InstanceMaintenanceOptions,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceMaintenanceOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if opts.AutoRecovery != "" {
		inst.MaintenanceOptions.AutoRecovery = opts.AutoRecovery
	}

	cp := *inst

	return &cp, nil
}

// ModifyInstanceNetworkPerformanceOptions updates the network performance
// options (bandwidth weighting) of an existing instance. Returns
// ErrInstanceNotFound if the instance does not exist.
func (b *InMemoryBackend) ModifyInstanceNetworkPerformanceOptions(
	instanceID string,
	opts InstanceNetworkPerformanceOptions,
) (*Instance, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceNetworkPerformanceOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if opts.BandwidthWeighting != "" {
		inst.NetworkPerformanceOptions.BandwidthWeighting = opts.BandwidthWeighting
	}

	cp := *inst

	return &cp, nil
}
