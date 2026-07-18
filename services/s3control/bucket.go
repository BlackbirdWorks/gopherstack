package s3control

import (
	"fmt"
	"maps"
	"sort"
)

// CreateBucket creates an S3 Outposts bucket.
func (b *InMemoryBackend) CreateBucket(accountID, bucketName string) *OutpostsBucket {
	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(arnFmtOutpostsBucket, b.region, accountID, bucketName)

	bkt := &OutpostsBucket{
		AccountID: accountID,
		Name:      bucketName,
		BucketArn: arn,
		Location:  "/" + bucketName,
	}
	b.outpostsBuckets.Put(bkt)

	cp := *bkt

	return &cp
}

// ---- Outposts Bucket ----

// GetBucket returns an Outposts bucket.
func (b *InMemoryBackend) GetBucket(accountID, bucketName string) (*OutpostsBucket, error) {
	b.mu.RLock("GetBucket")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	bucket, ok := b.outpostsBuckets.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return bucket, nil
}

// DeleteBucket removes an Outposts bucket.
func (b *InMemoryBackend) DeleteBucket(accountID, bucketName string) error {
	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Delete(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return nil
}

// GetBucketLifecycleConfiguration returns lifecycle config for an Outposts bucket.
func (b *InMemoryBackend) GetBucketLifecycleConfiguration(
	accountID, bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketLifecycleConfiguration")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return "", fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return b.bucketLifecycle[key], nil
}

// PutBucketLifecycleConfiguration sets lifecycle config for an Outposts bucket.
func (b *InMemoryBackend) PutBucketLifecycleConfiguration(
	accountID, bucketName, lifecycleConfig string,
) error {
	b.mu.Lock("PutBucketLifecycleConfiguration")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketLifecycle[key] = lifecycleConfig

	return nil
}

// DeleteBucketLifecycleConfiguration removes lifecycle config from an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketLifecycleConfiguration(accountID, bucketName string) error {
	b.mu.Lock("DeleteBucketLifecycleConfiguration")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	delete(b.bucketLifecycle, key)

	return nil
}

// GetBucketPolicy returns the policy for an Outposts bucket.
func (b *InMemoryBackend) GetBucketPolicy(accountID, bucketName string) (string, error) {
	b.mu.RLock("GetBucketPolicy")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return "", fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return b.bucketPolicies[key], nil
}

// PutBucketPolicy sets the policy for an Outposts bucket.
func (b *InMemoryBackend) PutBucketPolicy(accountID, bucketName, policy string) error {
	b.mu.Lock("PutBucketPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketPolicies[key] = policy

	return nil
}

// DeleteBucketPolicy removes the policy from an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketPolicy(accountID, bucketName string) error {
	b.mu.Lock("DeleteBucketPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	delete(b.bucketPolicies, key)

	return nil
}

// GetBucketTagging returns tags for an Outposts bucket.
func (b *InMemoryBackend) GetBucketTagging(accountID, bucketName string) (TagSet, error) {
	b.mu.RLock("GetBucketTagging")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return nil, fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	tags := b.bucketTagging[key]
	if tags == nil {
		return TagSet{}, nil
	}
	cp := make(TagSet, len(tags))
	maps.Copy(cp, tags)

	return cp, nil
}

// PutBucketTagging sets tags on an Outposts bucket.
func (b *InMemoryBackend) PutBucketTagging(accountID, bucketName string, tags TagSet) error {
	b.mu.Lock("PutBucketTagging")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketTagging[key] = tags

	return nil
}

// DeleteBucketTagging removes all tags from an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketTagging(accountID, bucketName string) error {
	b.mu.Lock("DeleteBucketTagging")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	delete(b.bucketTagging, key)

	return nil
}

// GetBucketVersioning returns the versioning state for an Outposts bucket.
func (b *InMemoryBackend) GetBucketVersioning(accountID, bucketName string) (string, error) {
	b.mu.RLock("GetBucketVersioning")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return "", fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	state := b.bucketVersioning[key]
	if state == "" {
		state = "Suspended"
	}

	return state, nil
}

// PutBucketVersioning sets the versioning state for an Outposts bucket.
func (b *InMemoryBackend) PutBucketVersioning(accountID, bucketName, status string) error {
	b.mu.Lock("PutBucketVersioning")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if !b.outpostsBuckets.Has(key) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketVersioning[key] = status

	return nil
}

// ListRegionalBuckets lists Outposts buckets for an account.
func (b *InMemoryBackend) ListRegionalBuckets(accountID string) []*OutpostsBucket {
	b.mu.RLock("ListRegionalBuckets")
	defer b.mu.RUnlock()

	var out []*OutpostsBucket
	for _, bucket := range b.outpostsBuckets.All() {
		if bucket.AccountID == accountID {
			cp := *bucket
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

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
