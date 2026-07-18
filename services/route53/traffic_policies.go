package route53

import (
	"fmt"
	"sort"
)

const (
	trafficPolicyIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"
	trafficPolicyIDLength = 36
)

func randomTrafficPolicyID() string {
	return randomID(trafficPolicyIDChars, trafficPolicyIDLength)
}

// CreateTrafficPolicy creates a new traffic policy.
func (b *InMemoryBackend) CreateTrafficPolicy(
	name, document, comment string,
) (*TrafficPolicy, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	if document == "" {
		return nil, fmt.Errorf("%w: document is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateTrafficPolicy")
	defer b.mu.Unlock()

	for _, versions := range b.trafficPolicies {
		if len(versions) > 0 && versions[0].Name == name {
			return nil, fmt.Errorf(
				"%w: traffic policy with name %s already exists",
				ErrTrafficPolicyAlreadyExists,
				name,
			)
		}
	}

	id := randomTrafficPolicyID()
	tp := &TrafficPolicy{
		ID:       id,
		Name:     name,
		Document: document,
		Comment:  comment,
		Type:     "DNS",
		Version:  1,
	}

	b.trafficPolicies[id] = []*TrafficPolicy{tp}

	cp := *tp

	return &cp, nil
}

// CreateTrafficPolicyVersion creates a new version of an existing traffic policy.
func (b *InMemoryBackend) CreateTrafficPolicyVersion(
	id, document, comment string,
) (*TrafficPolicy, error) {
	if document == "" {
		return nil, fmt.Errorf("%w: document is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateTrafficPolicyVersion")
	defer b.mu.Unlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	latest := versions[len(versions)-1]
	newVersion := &TrafficPolicy{
		ID:       id,
		Name:     latest.Name,
		Document: document,
		Comment:  comment,
		Type:     latest.Type,
		Version:  latest.Version + 1,
	}

	b.trafficPolicies[id] = append(b.trafficPolicies[id], newVersion)

	cp := *newVersion

	return &cp, nil
}

// DeleteTrafficPolicy deletes a specific version of a traffic policy.
// If it is the last version, the entire policy is removed.
func (b *InMemoryBackend) DeleteTrafficPolicy(id string, version int32) error {
	b.mu.Lock("DeleteTrafficPolicy")
	defer b.mu.Unlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	idx := -1
	for i, tp := range versions {
		if tp.Version == version {
			idx = i

			break
		}
	}

	if idx == -1 {
		return fmt.Errorf(
			"%w: traffic policy %s version %d not found",
			ErrTrafficPolicyNotFound,
			id,
			version,
		)
	}

	for _, inst := range b.trafficPolicyInstances.All() {
		if inst.TrafficPolicyID == id && inst.TrafficPolicyVersion == version {
			return fmt.Errorf(
				"%w: traffic policy %s version %d is still in use by instance %s",
				ErrTrafficPolicyInUse,
				id,
				version,
				inst.ID,
			)
		}
	}

	if len(versions) == 1 {
		delete(b.trafficPolicies, id)

		return nil
	}

	b.trafficPolicies[id] = append(versions[:idx], versions[idx+1:]...)

	return nil
}

// GetTrafficPolicy returns a specific version of a traffic policy.
func (b *InMemoryBackend) GetTrafficPolicy(id string, version int32) (*TrafficPolicy, error) {
	b.mu.RLock("GetTrafficPolicy")
	defer b.mu.RUnlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	for _, tp := range versions {
		if tp.Version == version {
			cp := *tp

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: traffic policy %s version %d not found",
		ErrTrafficPolicyNotFound,
		id,
		version,
	)
}

// UpdateTrafficPolicyComment updates the comment on a specific version of a traffic policy.
func (b *InMemoryBackend) UpdateTrafficPolicyComment(
	id string, version int32, comment string,
) (*TrafficPolicy, error) {
	b.mu.Lock("UpdateTrafficPolicyComment")
	defer b.mu.Unlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	for _, tp := range versions {
		if tp.Version == version {
			tp.Comment = comment
			cp := *tp

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: traffic policy %s version %d not found",
		ErrTrafficPolicyNotFound,
		id,
		version,
	)
}

// ListTrafficPolicies returns the latest version of each traffic policy with its version count.
func (b *InMemoryBackend) ListTrafficPolicies() ([]*TrafficPolicySummary, error) {
	b.mu.RLock("ListTrafficPolicies")
	defer b.mu.RUnlock()

	result := make([]*TrafficPolicySummary, 0, len(b.trafficPolicies))
	for _, versions := range b.trafficPolicies {
		if len(versions) == 0 {
			continue
		}

		latest := versions[len(versions)-1]
		cp := *latest
		result = append(result, &TrafficPolicySummary{
			TrafficPolicy: cp,
			VersionCount:  int32(len(versions)), //nolint:gosec // version count fits in int32
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// ListTrafficPolicyVersions returns all versions of a traffic policy.
func (b *InMemoryBackend) ListTrafficPolicyVersions(id string) ([]*TrafficPolicy, error) {
	b.mu.RLock("ListTrafficPolicyVersions")
	defer b.mu.RUnlock()

	versions, ok := b.trafficPolicies[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, id)
	}

	result := make([]*TrafficPolicy, len(versions))
	for i, tp := range versions {
		cp := *tp
		result[i] = &cp
	}

	return result, nil
}

// AddTrafficPolicyInternal adds a traffic policy directly into the backend for testing.
func (b *InMemoryBackend) AddTrafficPolicyInternal(tp TrafficPolicy) {
	b.mu.Lock("AddTrafficPolicyInternal")
	defer b.mu.Unlock()
	cp := tp
	b.trafficPolicies[tp.ID] = append(b.trafficPolicies[tp.ID], &cp)
}
