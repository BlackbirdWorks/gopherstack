package backup

import (
	"fmt"
	"sort"
	"time"
)

// PutProtectedResource adds or updates a protected resource record (called internally by StartBackupJob).
func (b *InMemoryBackend) PutProtectedResource(resourceArn, resourceType, vaultName string) {
	b.mu.Lock("PutProtectedResource")
	defer b.mu.Unlock()

	b.protectedResources.Put(&ProtectedResource{
		ResourceArn:     resourceArn,
		ResourceType:    resourceType,
		BackupVaultName: vaultName,
		LastBackupTime:  time.Now().UTC(),
	})
}

// DescribeProtectedResource returns a protected resource by ARN.
func (b *InMemoryBackend) DescribeProtectedResource(
	resourceArn string,
) (*ProtectedResource, error) {
	b.mu.RLock("DescribeProtectedResource")
	defer b.mu.RUnlock()

	pr, ok := b.protectedResources.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errProtectedResourceNotFound, resourceArn)
	}

	return pr, nil
}

// ListProtectedResources returns all protected resources.
func (b *InMemoryBackend) ListProtectedResources() []*ProtectedResource {
	b.mu.RLock("ListProtectedResources")
	defer b.mu.RUnlock()

	all := b.protectedResources.All()
	out := make([]*ProtectedResource, 0, len(all))
	for _, pr := range all {
		cp := *pr
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceArn < out[j].ResourceArn })

	return out
}

// ListProtectedResourcesByBackupVault returns protected resources for a vault.
func (b *InMemoryBackend) ListProtectedResourcesByBackupVault(
	vaultName string,
) []*ProtectedResource {
	b.mu.RLock("ListProtectedResourcesByBackupVault")
	defer b.mu.RUnlock()

	var out []*ProtectedResource
	for _, pr := range b.protectedResources.All() {
		if pr.BackupVaultName == vaultName {
			cp := *pr
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceArn < out[j].ResourceArn })

	return out
}

// ---- Restore Jobs ----
