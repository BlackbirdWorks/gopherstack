package lambda

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Capacity providers ---

// CreateCapacityProvider creates a new Lambda capacity provider.
func (b *InMemoryBackend) CreateCapacityProvider(
	input *CreateCapacityProviderInput,
) (*CapacityProvider, error) {
	b.mu.Lock("CreateCapacityProvider")
	defer b.mu.Unlock()

	if _, exists := b.capacityProviders.Get(input.CapacityProviderName); exists {
		return nil, ErrFunctionAlreadyExists
	}

	now := time.Now().UTC().Format(time.RFC3339)
	cp := &CapacityProvider{
		Name:                          input.CapacityProviderName,
		CapacityProviderArn:           buildCapacityProviderARN(b.region, b.accountID, input.CapacityProviderName),
		PermissionsConfig:             input.PermissionsConfig,
		VpcConfig:                     input.VpcConfig,
		CapacityProviderScalingConfig: input.CapacityProviderScalingConfig,
		InstanceRequirements:          input.InstanceRequirements,
		KmsKeyArn:                     input.KmsKeyArn,
		PropagateTags:                 input.PropagateTags,
		TelemetryConfig:               input.TelemetryConfig,
		State:                         CapacityProviderStateActive,
		LastModified:                  now,
	}

	b.capacityProviders.Put(cp)

	return cp, nil
}

// GetCapacityProvider retrieves a capacity provider by name.
func (b *InMemoryBackend) GetCapacityProvider(name string) (*CapacityProvider, error) {
	b.mu.RLock("GetCapacityProvider")
	defer b.mu.RUnlock()

	cp, ok := b.capacityProviders.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	return cp, nil
}

// DeleteCapacityProvider removes a capacity provider by name.
func (b *InMemoryBackend) DeleteCapacityProvider(name string) error {
	b.mu.Lock("DeleteCapacityProvider")
	defer b.mu.Unlock()

	if _, ok := b.capacityProviders.Get(name); !ok {
		return ErrFunctionNotFound
	}

	b.capacityProviders.Delete(name)

	return nil
}

// UpdateCapacityProvider updates an existing capacity provider.
func (b *InMemoryBackend) UpdateCapacityProvider(
	name string,
	input *UpdateCapacityProviderInput,
) (*CapacityProvider, error) {
	b.mu.Lock("UpdateCapacityProvider")
	defer b.mu.Unlock()

	cp, ok := b.capacityProviders.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if input.CapacityProviderScalingConfig != nil {
		cp.CapacityProviderScalingConfig = input.CapacityProviderScalingConfig
	}

	if input.PropagateTags != nil {
		cp.PropagateTags = input.PropagateTags
	}

	if input.TelemetryConfig != nil {
		cp.TelemetryConfig = input.TelemetryConfig
	}

	cp.LastModified = time.Now().UTC().Format(time.RFC3339)
	b.capacityProviders.Put(cp)

	return cp, nil
}

// ListCapacityProviders returns all capacity providers.
func (b *InMemoryBackend) ListCapacityProviders() []*CapacityProvider {
	b.mu.RLock("ListCapacityProviders")
	defer b.mu.RUnlock()

	cps := b.capacityProviders.All()

	sort.Slice(cps, func(i, j int) bool {
		return cps[i].Name < cps[j].Name
	})

	return cps
}

// SeedCapacityProviderFunctionVersions assigns the given function-version ARNs to
// the named capacity provider. AWS exposes no public assignment API in this
// emulator's surface, so this internal helper is the only way to populate the
// assignments observed by ListFunctionVersionsByCapacityProvider (primarily for
// tests). It returns ErrFunctionNotFound if the provider does not exist.
func (b *InMemoryBackend) SeedCapacityProviderFunctionVersions(
	name string,
	versions ...string,
) error {
	b.mu.Lock("SeedCapacityProviderFunctionVersions")
	defer b.mu.Unlock()

	cp, ok := b.capacityProviders.Get(name)
	if !ok {
		return ErrFunctionNotFound
	}

	cp.AssignedFunctionVersions = append(cp.AssignedFunctionVersions, versions...)

	return nil
}

// ListFunctionVersionsByCapacityProvider returns a page of function-version ARNs
// assigned to the named capacity provider. It returns ErrFunctionNotFound if the
// provider does not exist. Assignments are populated only via the internal
// SeedCapacityProviderFunctionVersions helper, since AWS exposes no public
// assignment API in this emulator's surface.
func (b *InMemoryBackend) ListFunctionVersionsByCapacityProvider(
	name, marker string,
	maxItems int,
) (page.Page[string], error) {
	b.mu.RLock("ListFunctionVersionsByCapacityProvider")
	defer b.mu.RUnlock()

	cp, ok := b.capacityProviders.Get(name)
	if !ok {
		return page.Page[string]{}, ErrFunctionNotFound
	}

	versions := make([]string, len(cp.AssignedFunctionVersions))
	copy(versions, cp.AssignedFunctionVersions)
	sort.Strings(versions)

	return page.New(versions, marker, maxItems, lambdaDefaultMaxItems), nil
}
