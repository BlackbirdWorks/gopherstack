package lambda

import (
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Capacity providers ---

// cloneCapacityProvider copies cp so a caller can't race Update/Seed, which
// mutate the stored provider in place.
func cloneCapacityProvider(cp *CapacityProvider) *CapacityProvider {
	out := *cp

	return &out
}

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
		State:                         CapacityProviderStatePending,
		LastModified:                  now,
	}

	b.capacityProviders.Put(cp)

	return cloneCapacityProvider(cp), nil
}

// settleCapacityProviderLocked completes the Pending to Active transition the
// first time a provider is observed after creation. Caller holds b.mu for write.
func settleCapacityProviderLocked(cp *CapacityProvider) {
	if cp.State == CapacityProviderStatePending {
		cp.State = CapacityProviderStateActive
	}
}

// GetCapacityProvider retrieves a capacity provider by name.
func (b *InMemoryBackend) GetCapacityProvider(name string) (*CapacityProvider, error) {
	b.mu.Lock("GetCapacityProvider")
	defer b.mu.Unlock()

	cp, ok := b.capacityProviders.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	settleCapacityProviderLocked(cp)

	return cloneCapacityProvider(cp), nil
}

// DeleteCapacityProvider removes a capacity provider by name and returns the
// deleted provider's state. DeleteCapacityProviderOutput.CapacityProvider is
// required on the wire (api_op_DeleteCapacityProvider.go:44-46), so the
// caller must have the pre-deletion snapshot to echo back.
func (b *InMemoryBackend) DeleteCapacityProvider(name string) (*CapacityProvider, error) {
	b.mu.Lock("DeleteCapacityProvider")
	defer b.mu.Unlock()

	cp, ok := b.capacityProviders.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if len(b.capacityProviderUsersLocked(cp.CapacityProviderArn)) > 0 {
		return nil, ErrCapacityProviderInUse
	}

	b.capacityProviders.Delete(name)

	out := cloneCapacityProvider(cp)
	out.State = CapacityProviderStateDeleting

	return out, nil
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

	settleCapacityProviderLocked(cp)

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

	return cloneCapacityProvider(cp), nil
}

// ListCapacityProviders returns one name-ordered page, optionally narrowed to a State.
func (b *InMemoryBackend) ListCapacityProviders(state, marker string, maxItems int) page.Page[*CapacityProvider] {
	b.mu.Lock("ListCapacityProviders")
	defer b.mu.Unlock()

	out := make([]*CapacityProvider, 0)

	for _, cp := range b.capacityProviders.All() {
		settleCapacityProviderLocked(cp)

		if state == "" || cp.State == state {
			out = append(out, cloneCapacityProvider(cp))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	return page.New(out, marker, maxItems, lambdaDefaultMaxItems)
}

// CapacityProviderFunctionVersion is one function version using a capacity provider.
type CapacityProviderFunctionVersion struct {
	FunctionArn string
	State       string
}

// capacityProviderUsersLocked returns every function ($LATEST) and published
// version whose CapacityProviderConfig references cpArn, ordered by ARN.
func (b *InMemoryBackend) capacityProviderUsersLocked(cpArn string) []CapacityProviderFunctionVersion {
	var out []CapacityProviderFunctionVersion

	for _, fn := range b.functions.All() {
		if !referencesCapacityProvider(fn.CapacityProviderConfig, cpArn) {
			continue
		}

		out = append(out, CapacityProviderFunctionVersion{
			FunctionArn: fn.FunctionArn + ":" + versionLatest,
			State:       string(fn.State),
		})

		for _, v := range b.versions[fn.FunctionName] {
			if referencesCapacityProvider(v.CapacityProviderConfig, cpArn) {
				out = append(out, CapacityProviderFunctionVersion{FunctionArn: v.FunctionArn, State: string(v.State)})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].FunctionArn < out[j].FunctionArn })

	return out
}

func referencesCapacityProvider(cfg *CapacityProviderConfig, cpArn string) bool {
	return cfg != nil && cfg.ManagedInstances != nil && cfg.ManagedInstances.CapacityProviderArn == cpArn
}

// capacityProviderNameFromARN extracts the provider name from a capacity provider ARN.
func capacityProviderNameFromARN(cpArn string) (string, bool) {
	_, name, ok := strings.Cut(cpArn, ":capacity-provider:")

	return name, ok && name != ""
}

// CapacityProviderExists reports whether cpArn names an existing capacity provider.
func (b *InMemoryBackend) CapacityProviderExists(cpArn string) bool {
	name, ok := capacityProviderNameFromARN(cpArn)
	if !ok {
		return false
	}

	b.mu.RLock("CapacityProviderExists")
	defer b.mu.RUnlock()

	cp, found := b.capacityProviders.Get(name)

	return found && cp.CapacityProviderArn == cpArn
}

// ListFunctionVersionsByCapacityProvider returns a page of the function versions
// whose CapacityProviderConfig references the named capacity provider, along
// with the provider's ARN. It returns ErrFunctionNotFound for an unknown provider.
func (b *InMemoryBackend) ListFunctionVersionsByCapacityProvider(
	name, marker string,
	maxItems int,
) (page.Page[CapacityProviderFunctionVersion], string, error) {
	b.mu.RLock("ListFunctionVersionsByCapacityProvider")
	defer b.mu.RUnlock()

	cp, ok := b.capacityProviders.Get(name)
	if !ok {
		return page.Page[CapacityProviderFunctionVersion]{}, "", ErrFunctionNotFound
	}

	users := b.capacityProviderUsersLocked(cp.CapacityProviderArn)

	return page.New(users, marker, maxItems, lambdaDefaultMaxItems), cp.CapacityProviderArn, nil
}
