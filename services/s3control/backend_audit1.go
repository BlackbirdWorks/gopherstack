package s3control

import (
	"fmt"
	"maps"
	"sort"
	"strings"
)

// StorageLensConfigMeta holds the synthesized metadata fields for a Storage Lens configuration.
// AWS returns these in ListStorageLensConfigurations but they are not always in the raw XML blob.
type StorageLensConfigMeta struct {
	AccountID      string `json:"accountID"`
	ID             string `json:"id"`
	StorageLensArn string `json:"storageLensArn"`
	HomeRegion     string `json:"homeRegion"`
	IsEnabled      bool   `json:"isEnabled"`
}

// GetStorageLensConfigMeta returns the metadata for a Storage Lens configuration.
func (b *InMemoryBackend) GetStorageLensConfigMeta(accountID, configName string) (*StorageLensConfigMeta, error) {
	b.mu.RLock("GetStorageLensConfigMeta")
	defer b.mu.RUnlock()

	key := accountID + ":" + configName
	if _, ok := b.storageLensConfigs[key]; !ok {
		return nil, errStorageLensConfigNotFound
	}

	meta, ok := b.storageLensConfigMetas[key]
	if !ok {
		return &StorageLensConfigMeta{
			AccountID:      accountID,
			ID:             configName,
			StorageLensArn: fmt.Sprintf("arn:aws:s3:%s:%s:storage-lens/%s", b.region, accountID, configName),
			HomeRegion:     b.region,
			IsEnabled:      true,
		}, nil
	}

	cp := *meta

	return &cp, nil
}

// PutStorageLensConfigMeta stores metadata for a Storage Lens configuration.
func (b *InMemoryBackend) PutStorageLensConfigMeta(accountID, configName string, isEnabled bool, homeRegion string) {
	b.mu.Lock("PutStorageLensConfigMeta")
	defer b.mu.Unlock()

	if homeRegion == "" {
		homeRegion = b.region
	}

	key := accountID + ":" + configName
	b.storageLensConfigMetas[key] = &StorageLensConfigMeta{
		AccountID:      accountID,
		ID:             configName,
		StorageLensArn: fmt.Sprintf("arn:aws:s3:%s:%s:storage-lens/%s", b.region, accountID, configName),
		HomeRegion:     homeRegion,
		IsEnabled:      isEnabled,
	}
}

// ListStorageLensConfigMeta returns metadata for all Storage Lens configurations for an account.
func (b *InMemoryBackend) ListStorageLensConfigMeta(accountID string) []*StorageLensConfigMeta {
	b.mu.RLock("ListStorageLensConfigMeta")
	defer b.mu.RUnlock()

	prefix := accountID + ":"
	var out []*StorageLensConfigMeta

	for k := range b.storageLensConfigs {
		if name, ok := strings.CutPrefix(k, prefix); ok {
			meta, found := b.storageLensConfigMetas[k]
			if found {
				cp := *meta
				out = append(out, &cp)
			} else {
				out = append(out, &StorageLensConfigMeta{
					AccountID:      accountID,
					ID:             name,
					StorageLensArn: fmt.Sprintf("arn:aws:s3:%s:%s:storage-lens/%s", b.region, accountID, name),
					HomeRegion:     b.region,
					IsEnabled:      true,
				})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// ---- StorageLensGroup tags ----

// GetStorageLensGroupTags returns tags for a Storage Lens group.
func (b *InMemoryBackend) GetStorageLensGroupTags(accountID, name string) (TagSet, error) {
	b.mu.RLock("GetStorageLensGroupTags")
	defer b.mu.RUnlock()

	if _, ok := b.storageLensGroups[accountID+":"+name]; !ok {
		return nil, errStorageLensGroupNotFound
	}

	tags := b.storageLensGroupTags[accountID+":"+name]
	if tags == nil {
		return TagSet{}, nil
	}

	cp := make(TagSet, len(tags))
	maps.Copy(cp, tags)

	return cp, nil
}

// PutStorageLensGroupTags sets tags on a Storage Lens group.
func (b *InMemoryBackend) PutStorageLensGroupTags(accountID, name string, tags TagSet) error {
	b.mu.Lock("PutStorageLensGroupTags")
	defer b.mu.Unlock()

	if _, ok := b.storageLensGroups[accountID+":"+name]; !ok {
		return errStorageLensGroupNotFound
	}

	cp := make(TagSet, len(tags))
	maps.Copy(cp, tags)
	b.storageLensGroupTags[accountID+":"+name] = cp

	return nil
}

// DeleteStorageLensGroupTags removes all tags from a Storage Lens group.
func (b *InMemoryBackend) DeleteStorageLensGroupTags(accountID, name string) error {
	b.mu.Lock("DeleteStorageLensGroupTags")
	defer b.mu.Unlock()

	if _, ok := b.storageLensGroups[accountID+":"+name]; !ok {
		return errStorageLensGroupNotFound
	}

	delete(b.storageLensGroupTags, accountID+":"+name)

	return nil
}

// ---- MRAP PublicAccessBlock ----

// SetMRAPPublicAccessBlock stores a public access block configuration for an MRAP.
func (b *InMemoryBackend) SetMRAPPublicAccessBlock(accountID, name string, pab PublicAccessBlock) error {
	b.mu.Lock("SetMRAPPublicAccessBlock")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	mrap, ok := b.mraps[key]
	if !ok {
		return fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	cp := pab
	mrap.PublicAccessBlock = &cp

	return nil
}

// GetMRAPPublicAccessBlock returns the public access block for an MRAP.
func (b *InMemoryBackend) GetMRAPPublicAccessBlock(accountID, name string) (*PublicAccessBlock, error) {
	b.mu.RLock("GetMRAPPublicAccessBlock")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	mrap, ok := b.mraps[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	if mrap.PublicAccessBlock == nil {
		return nil, ErrNotFound
	}

	cp := *mrap.PublicAccessBlock

	return &cp, nil
}

// ---- ListAccessGrantsInstances ----

// ListAllAccessGrantsInstances returns all Access Grants instances for an account.
func (b *InMemoryBackend) ListAllAccessGrantsInstances(accountID string) []*AccessGrantsInstance {
	b.mu.RLock("ListAllAccessGrantsInstances")
	defer b.mu.RUnlock()

	out := make([]*AccessGrantsInstance, 0)

	for _, inst := range b.accessGrantsInstances {
		if inst.AccountID == accountID {
			cp := *inst
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].AccessGrantsInstanceID < out[j].AccessGrantsInstanceID
	})

	return out
}

// ---- GetDataAccess credentials ----

// GetDataAccessCredentials returns mock temporary credentials and matched grant target for a data access request.
func (b *InMemoryBackend) GetDataAccessCredentials(
	accountID, target, permission string,
) (accessKeyID, secretAccessKey, sessionToken, expiration, matchedTarget string, err error) {
	b.mu.RLock("GetDataAccessCredentials")
	defer b.mu.RUnlock()

	if _, ok := b.accessGrantsInstances[accountID]; !ok {
		return "", "", "", "", "", awserrAccessGrantsInstanceNotFound
	}

	_ = target
	_ = permission

	ts := nowRFC3339()
	id := fmt.Sprintf("ASIA%s", strings.ToUpper(accountID[:8]))
	if len(accountID) < 8 {
		id = fmt.Sprintf("ASIA%s", strings.ToUpper(accountID))
	}

	return id,
		"mock-secret-access-key",
		"mock-session-token",
		ts,
		fmt.Sprintf("s3://%s/*", target),
		nil
}
