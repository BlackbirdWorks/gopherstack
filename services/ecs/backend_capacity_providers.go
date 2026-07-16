package ecs

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrCapacityProviderNotFound is returned when a capacity provider does not exist.
var ErrCapacityProviderNotFound = awserr.New(
	"CapacityProviderNotFoundException",
	awserr.ErrNotFound,
)

// ErrCapacityProviderAlreadyExists is returned when a capacity provider already exists.
var ErrCapacityProviderAlreadyExists = awserr.New(
	"CapacityProviderAlreadyExistsException",
	awserr.ErrAlreadyExists,
)

// builtinCapacityProviders returns a synthesized CapacityProvider for FARGATE or
// FARGATE_SPOT, which are managed by AWS and do not require explicit creation.
func builtinCapacityProvider(name string) *CapacityProvider {
	switch name {
	case launchTypeFargate, "FARGATE_SPOT":
		return &CapacityProvider{
			Name:                name,
			Status:              statusActive,
			CapacityProviderArn: "arn:aws:ecs:::capacity-provider/" + name,
		}
	default:
		return nil
	}
}

// findCapacityProviderLocked returns the map key and pointer for a capacity provider by name or ARN.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) findCapacityProviderLocked(nameOrArn string) (string, *CapacityProvider) {
	if cp, ok := b.capacityProviders.Get(nameOrArn); ok {
		return nameOrArn, cp
	}

	for _, cp := range b.capacityProviders.All() {
		if cp.CapacityProviderArn == nameOrArn {
			return cp.Name, cp
		}
	}

	return "", nil
}

// CreateCapacityProvider creates a new capacity provider.
func (b *InMemoryBackend) CreateCapacityProvider(
	input CreateCapacityProviderInput,
) (*CapacityProvider, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCapacityProvider")
	defer b.mu.Unlock()

	if b.capacityProviders.Has(input.Name) {
		return nil, fmt.Errorf("%w: %s", ErrCapacityProviderAlreadyExists, input.Name)
	}

	cp := &CapacityProvider{
		CreatedAt: time.Now(),
		CapacityProviderArn: fmt.Sprintf(
			"arn:aws:ecs:%s:%s:capacity-provider/%s", b.region, b.accountID, input.Name,
		),
		Name:                     input.Name,
		Status:                   statusActive,
		AutoScalingGroupProvider: input.AutoScalingGroupProvider,
		Tags:                     copyTags(input.Tags),
	}

	b.capacityProviders.Put(cp)

	out := *cp
	out.Tags = copyTags(cp.Tags)

	return &out, nil
}

// DeleteCapacityProvider deletes a capacity provider by name or ARN.
func (b *InMemoryBackend) DeleteCapacityProvider(nameOrArn string) (*CapacityProvider, error) {
	b.mu.Lock("DeleteCapacityProvider")
	defer b.mu.Unlock()

	key, cp := b.findCapacityProviderLocked(nameOrArn)
	if cp == nil {
		return nil, fmt.Errorf("%w: %s", ErrCapacityProviderNotFound, nameOrArn)
	}

	b.capacityProviders.Delete(key)

	out := *cp

	return &out, nil
}

// DescribeCapacityProviders returns capacity providers, optionally filtered by name/ARN.
// Names/ARNs that don't resolve to a known or built-in capacity provider are reported in
// the returned Failures slice (reason MISSING), matching AWS's DescribeCapacityProviders
// behavior of partial success rather than failing the whole call.
func (b *InMemoryBackend) DescribeCapacityProviders(
	nameOrArns []string,
) ([]CapacityProvider, []Failure, error) {
	b.mu.RLock("DescribeCapacityProviders")
	defer b.mu.RUnlock()

	if len(nameOrArns) == 0 {
		all := b.capacityProviders.All()
		out := make([]CapacityProvider, 0, len(all))
		for _, cp := range all {
			c := *cp
			c.Tags = copyTags(cp.Tags)
			out = append(out, c)
		}

		return out, nil, nil
	}

	out := make([]CapacityProvider, 0, len(nameOrArns))
	failures := make([]Failure, 0, len(nameOrArns))

	for _, ref := range nameOrArns {
		_, cp := b.findCapacityProviderLocked(ref)
		if cp == nil {
			// Fall back to built-in FARGATE / FARGATE_SPOT providers.
			builtin := builtinCapacityProvider(ref)
			if builtin == nil {
				failures = append(failures, Failure{
					Arn:    ref,
					Reason: statusMissing,
					Detail: fmt.Sprintf("capacity provider %s not found", ref),
				})

				continue
			}

			out = append(out, *builtin)

			continue
		}

		c := *cp
		c.Tags = copyTags(cp.Tags)
		out = append(out, c)
	}

	return out, failures, nil
}

// UpdateCapacityProvider updates tags or status of a capacity provider.
func (b *InMemoryBackend) UpdateCapacityProvider(
	input UpdateCapacityProviderInput,
) (*CapacityProvider, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateCapacityProvider")
	defer b.mu.Unlock()

	_, cp := b.findCapacityProviderLocked(input.Name)
	if cp == nil {
		return nil, fmt.Errorf("%w: %s", ErrCapacityProviderNotFound, input.Name)
	}

	if input.Status != "" {
		cp.Status = input.Status
	}

	if input.AutoScalingGroupProvider != nil {
		cp.AutoScalingGroupProvider = input.AutoScalingGroupProvider
	}

	if input.Tags != nil {
		cp.Tags = copyTags(input.Tags)
	}

	out := *cp
	out.Tags = copyTags(cp.Tags)

	return &out, nil
}

// AddCapacityProviderInternal adds a capacity provider directly (seed helper for tests).
func (b *InMemoryBackend) AddCapacityProviderInternal(cp *CapacityProvider) {
	b.mu.Lock("AddCapacityProviderInternal")
	defer b.mu.Unlock()

	c := *cp
	c.Tags = copyTags(cp.Tags)
	b.capacityProviders.Put(&c)
}
