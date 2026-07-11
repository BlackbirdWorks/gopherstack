package s3control

import (
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// errReplicationNotFound is returned when no replication config is found for a bucket.
var errReplicationNotFound = awserr.New("ReplicationConfigurationNotFoundError", awserr.ErrNotFound)

// errStorageLensConfigNotFound is returned when a Storage Lens configuration is not found.
var errStorageLensConfigNotFound = awserr.New("NoSuchConfiguration", awserr.ErrNotFound)

// errStorageLensGroupNotFound is returned when a Storage Lens group is not found.
var errStorageLensGroupNotFound = awserr.New("NoSuchStorageLensGroup", awserr.ErrNotFound)

// ---- Bucket Replication ----

// GetBucketReplication retrieves the replication configuration for an Outposts bucket.
func (b *InMemoryBackend) GetBucketReplication(accountID, bucketName string) (string, error) {
	b.mu.RLock("GetBucketReplication")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	cfg, ok := b.bucketReplication[key]
	if !ok {
		return "", errReplicationNotFound
	}

	return cfg, nil
}

// PutBucketReplication stores a replication configuration for an Outposts bucket.
func (b *InMemoryBackend) PutBucketReplication(accountID, bucketName, config string) error {
	b.mu.Lock("PutBucketReplication")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	b.bucketReplication[key] = config

	return nil
}

// DeleteBucketReplication removes the replication configuration for an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketReplication(accountID, bucketName string) error {
	b.mu.Lock("DeleteBucketReplication")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	delete(b.bucketReplication, key)

	return nil
}

// ---- MRAP Routes (submit) ----

// SubmitMultiRegionAccessPointRoutes stores routing configuration for an MRAP.
func (b *InMemoryBackend) SubmitMultiRegionAccessPointRoutes(accountID, mrap, routes string) error {
	b.mu.Lock("SubmitMultiRegionAccessPointRoutes")
	defer b.mu.Unlock()

	key := accountID + ":" + mrap
	b.mrapRoutes[key] = routes

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

// ListStorageLensConfigurations returns the names of all Storage Lens configurations for an account.
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

// DeleteStorageLensGroup removes a Storage Lens group.
func (b *InMemoryBackend) DeleteStorageLensGroup(accountID, name string) error {
	b.mu.Lock("DeleteStorageLensGroup")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.storageLensGroups.Delete(key) {
		return errStorageLensGroupNotFound
	}

	return nil
}

// ListStorageLensGroups returns all Storage Lens groups for an account.
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

	return out
}

// ---- Resource Tags ----

// ListTagsForResource returns all tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.resourceTags[arn]
	if tags == nil {
		return map[string]string{}
	}

	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// TagResource adds or updates tags on the given ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.resourceTags[arn]
	if existing == nil {
		existing = make(map[string]string, len(tags))
	}

	maps.Copy(existing, tags)
	b.resourceTags[arn] = existing
}

// UntagResource removes specific tag keys from the given ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	tags := b.resourceTags[arn]
	if tags == nil {
		return
	}

	for _, k := range tagKeys {
		delete(tags, k)
	}
}
