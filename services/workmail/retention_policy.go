package workmail

import (
	"fmt"
)

// --- Retention Policies ---

// PutRetentionPolicy creates or updates a retention policy for an org.
func (b *InMemoryBackend) PutRetentionPolicy(
	orgID, id, name, description string, folderConfigurations []*FolderConfiguration,
) error {
	b.mu.Lock("PutRetentionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if id == "" {
		id = newID()
	}
	b.retentionPolicies.Put(&RetentionPolicy{
		ID:                   id,
		Name:                 name,
		Description:          description,
		FolderConfigurations: folderConfigurations,
		orgID:                orgID,
	})

	return nil
}

// DeleteRetentionPolicy removes the retention policy from an org.
func (b *InMemoryBackend) DeleteRetentionPolicy(orgID, id string) error {
	b.mu.Lock("DeleteRetentionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	existing, ok := b.retentionPolicies.Get(orgID)
	if !ok || existing.ID != id {
		return fmt.Errorf("%w: retention policy %q not found", ErrNotFound, id)
	}
	b.retentionPolicies.Delete(orgID)

	return nil
}

// GetDefaultRetentionPolicy returns the retention policy for an org.
func (b *InMemoryBackend) GetDefaultRetentionPolicy(orgID string) (*RetentionPolicy, error) {
	b.mu.RLock("GetDefaultRetentionPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	pol, ok := b.retentionPolicies.Get(orgID)
	if !ok {
		return nil, fmt.Errorf("%w: no retention policy configured", ErrNotFound)
	}

	return pol, nil
}
