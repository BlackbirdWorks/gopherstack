package ssoadmin

import (
	"slices"
	"sort"
)

// DeleteApplicationAccessScope removes an access scope from an application.
func (b *InMemoryBackend) DeleteApplicationAccessScope(applicationArn, scope string) error {
	b.mu.Lock("DeleteApplicationAccessScope")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	scopes := b.applicationScopes[applicationArn]
	if _, ok := scopes[scope]; !ok {
		return ErrAccessScopeNotFound
	}
	delete(scopes, scope)

	return nil
}

// PutApplicationAccessScope adds or updates an access scope and its
// authorized target ARNs on an application. Real AWS "Put" semantics replace
// the full authorizedTargets list on each call rather than merging.
func (b *InMemoryBackend) PutApplicationAccessScope(applicationArn, scope string, authorizedTargets []string) error {
	b.mu.Lock("PutApplicationAccessScope")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationArn) {
		return ErrApplicationNotFound
	}
	if b.applicationScopes[applicationArn] == nil {
		b.applicationScopes[applicationArn] = make(map[string][]string)
	}
	b.applicationScopes[applicationArn][scope] = slices.Clone(authorizedTargets)

	return nil
}

// ListApplicationAccessScopes lists access scopes and their authorized
// targets on an application, sorted by scope name for deterministic output.
func (b *InMemoryBackend) ListApplicationAccessScopes(applicationArn string) ([]ScopeDetails, error) {
	b.mu.RLock("ListApplicationAccessScopes")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}

	scopes := b.applicationScopes[applicationArn]
	result := make([]ScopeDetails, 0, len(scopes))
	for scope, targets := range scopes {
		// Normalize nil to a non-nil empty slice; see GetApplicationAccessScope.
		result = append(result, ScopeDetails{Scope: scope, AuthorizedTargets: append([]string{}, targets...)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Scope < result[j].Scope })

	return result, nil
}

// GetApplicationAccessScope returns whether a specific access scope exists for an application.
func (b *InMemoryBackend) GetApplicationAccessScope(applicationArn, scope string) ([]string, error) {
	b.mu.RLock("GetApplicationAccessScope")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationArn) {
		return nil, ErrApplicationNotFound
	}
	targets, ok := b.applicationScopes[applicationArn][scope]
	if !ok {
		return nil, ErrAccessScopeNotFound
	}

	// Normalize nil to a non-nil empty slice so the wire response carries
	// AuthorizedTargets: [] rather than null when none were set.
	return append([]string{}, targets...), nil
}
