package s3control

import (
	"fmt"
	"maps"
	"sort"
	"strings"
)

// CreateStorageLensGroup creates an S3 Storage Lens group.
func (b *InMemoryBackend) CreateStorageLensGroup(accountID, name string) *StorageLensGroup {
	b.mu.Lock("CreateStorageLensGroup")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(arnFmtStorageLensGroup, b.region, accountID, name)

	grp := &StorageLensGroup{
		AccountID:           accountID,
		Name:                name,
		StorageLensGroupArn: arn,
		CreatedAt:           nowRFC3339(),
	}
	b.storageLensGroups.Put(grp)

	cp := *grp

	return &cp
}

// UpdateStorageLensGroupFilter stores the filter XML for an existing Storage Lens group.
func (b *InMemoryBackend) UpdateStorageLensGroupFilter(accountID, name, filter string) error {
	b.mu.Lock("UpdateStorageLensGroupFilter")
	defer b.mu.Unlock()

	grp, ok := b.storageLensGroups.Get(accountID + ":" + name)
	if !ok {
		return errStorageLensGroupNotFound
	}

	grp.Filter = filter

	return nil
}

// ---- Storage Lens Configuration ----

// GetStorageLensConfiguration retrieves a Storage Lens configuration.
func (b *InMemoryBackend) GetStorageLensConfiguration(accountID, configName string) (string, error) {
	b.mu.RLock("GetStorageLensConfiguration")
	defer b.mu.RUnlock()

	key := accountID + ":" + configName
	cfg, ok := b.storageLensConfigs[key]
	if !ok {
		return "", errStorageLensConfigNotFound
	}

	return cfg, nil
}

// PutStorageLensConfiguration stores a Storage Lens configuration.
func (b *InMemoryBackend) PutStorageLensConfiguration(accountID, configName, config string) error {
	b.mu.Lock("PutStorageLensConfiguration")
	defer b.mu.Unlock()

	b.storageLensConfigs[accountID+":"+configName] = config

	return nil
}

// DeleteStorageLensConfiguration removes a Storage Lens configuration.
func (b *InMemoryBackend) DeleteStorageLensConfiguration(accountID, configName string) error {
	b.mu.Lock("DeleteStorageLensConfiguration")
	defer b.mu.Unlock()

	key := accountID + ":" + configName
	delete(b.storageLensConfigs, key)
	delete(b.storageLensConfigTags, key)

	return nil
}

// GetStorageLensConfigurationTagging retrieves tags for a Storage Lens configuration.
func (b *InMemoryBackend) GetStorageLensConfigurationTagging(accountID, configName string) (TagSet, error) {
	b.mu.RLock("GetStorageLensConfigurationTagging")
	defer b.mu.RUnlock()

	key := accountID + ":" + configName
	if _, ok := b.storageLensConfigs[key]; !ok {
		return nil, errStorageLensConfigNotFound
	}

	tags := b.storageLensConfigTags[key]
	if tags == nil {
		return TagSet{}, nil
	}

	cp := make(TagSet, len(tags))
	maps.Copy(cp, tags)

	return cp, nil
}

// PutStorageLensConfigurationTagging sets tags on a Storage Lens configuration.
func (b *InMemoryBackend) PutStorageLensConfigurationTagging(accountID, configName string, tags TagSet) error {
	b.mu.Lock("PutStorageLensConfigurationTagging")
	defer b.mu.Unlock()

	key := accountID + ":" + configName
	cp := make(TagSet, len(tags))
	maps.Copy(cp, tags)
	b.storageLensConfigTags[key] = cp

	return nil
}

// DeleteStorageLensConfigurationTagging removes all tags from a Storage Lens configuration.
func (b *InMemoryBackend) DeleteStorageLensConfigurationTagging(accountID, configName string) error {
	b.mu.Lock("DeleteStorageLensConfigurationTagging")
	defer b.mu.Unlock()

	key := accountID + ":" + configName
	delete(b.storageLensConfigTags, key)

	return nil
}

// ListStorageLensConfigurations returns the names of all Storage Lens
// configurations for an account, sorted so the handler's index-based
// nextToken pagination (s3cPaginate) stays stable across calls -- Go map
// iteration order is unspecified.
func (b *InMemoryBackend) ListStorageLensConfigurations(accountID string) []string {
	b.mu.RLock("ListStorageLensConfigurations")
	defer b.mu.RUnlock()

	prefix := accountID + ":"
	var out []string

	for k := range b.storageLensConfigs {
		if name, ok := strings.CutPrefix(k, prefix); ok {
			out = append(out, name)
		}
	}

	sort.Strings(out)

	return out
}

// ---- Storage Lens Groups (additional CRUD) ----

// GetStorageLensGroup retrieves a Storage Lens group by name.
func (b *InMemoryBackend) GetStorageLensGroup(accountID, name string) (*StorageLensGroup, error) {
	b.mu.RLock("GetStorageLensGroup")
	defer b.mu.RUnlock()

	grp, ok := b.storageLensGroups.Get(accountID + ":" + name)
	if !ok {
		return nil, errStorageLensGroupNotFound
	}

	cp := *grp

	return &cp, nil
}

// UpdateStorageLensGroup updates a Storage Lens group (currently a no-op that confirms existence).
func (b *InMemoryBackend) UpdateStorageLensGroup(accountID, name string) (*StorageLensGroup, error) {
	b.mu.Lock("UpdateStorageLensGroup")
	defer b.mu.Unlock()

	grp, ok := b.storageLensGroups.Get(accountID + ":" + name)
	if !ok {
		return nil, errStorageLensGroupNotFound
	}

	cp := *grp

	return &cp, nil
}

// DeleteStorageLensGroup removes a Storage Lens group and cascade-cleans its
// generic resource tags.
func (b *InMemoryBackend) DeleteStorageLensGroup(accountID, name string) error {
	b.mu.Lock("DeleteStorageLensGroup")
	defer b.mu.Unlock()

	key := accountID + ":" + name

	grp, ok := b.storageLensGroups.Get(key)
	if !ok {
		return errStorageLensGroupNotFound
	}

	arn := grp.StorageLensGroupArn

	b.storageLensGroups.Delete(key)
	delete(b.resourceTags, arn)

	return nil
}

// ListStorageLensGroups returns all Storage Lens groups for an account,
// sorted by name so the handler's index-based nextToken pagination
// (s3cPaginate) stays stable across calls -- store.Table.All()'s iteration
// order is unspecified.
func (b *InMemoryBackend) ListStorageLensGroups(accountID string) []*StorageLensGroup {
	b.mu.RLock("ListStorageLensGroups")
	defer b.mu.RUnlock()

	var out []*StorageLensGroup

	for _, v := range b.storageLensGroups.All() {
		if v.AccountID == accountID {
			cp := *v
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}
