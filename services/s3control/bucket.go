package s3control

import (
	"fmt"
	"maps"
	"sort"
)

// CreateBucket creates an S3 Outposts bucket.
//
// accountID here is whatever accountIDFromRequest(c) resolves to (see
// handler_bucket.go) -- almost always the literal "default", because a real
// aws-sdk-go-v2 CreateBucket request carries no AccountId at all. Confirmed
// against the installed aws-sdk-go-v2/service/s3control's CreateBucketInput:
// its members are Bucket/ACL/CreateBucketConfiguration/GrantFullControl/
// GrantRead/GrantReadACP/GrantWrite/GrantWriteACP/ObjectLockEnabledForBucket/
// OutpostId -- nothing account-related, unlike EVERY other Create* op in
// this service (CreateAccessPoint, CreateJob, CreateMultiRegionAccessPoint,
// CreateAccessGrant*, CreateStorageLensGroup, ... all bind AccountId to the
// X-Amz-Account-Id header). Real S3 on Outposts substitutes OutpostId for
// that role: an Outpost is provisioned to, and always addressed as belonging
// to, exactly one AWS account, so the bucket's owner is implied by which
// Outpost it's created on rather than by an explicit request field.
//
// GetBucket/DeleteBucket/ListRegionalBuckets (and every bucket sub-resource
// op: lifecycle/policy/tagging/versioning/replication) DO bind an AccountId
// to that same header -- but it's optional, and a real caller that
// configures one consistently across their whole session has no way to
// supply that same value on CreateBucket, since the field doesn't exist
// there. Storing this resource keyed by "accountID:bucketName" (as every
// sibling resource in this service correctly does, since their Create ops
// really do carry AccountId) would mean a bucket created via the real,
// headerless CreateBucket shape lands under the "default" partition while
// the same client's real, account-bearing Get/Delete/List calls look
// elsewhere and never find it -- see gopherstack-eje5.
//
// The fix: OutpostsBucket identity (outpostsBucketKeyFn, store_setup.go) and
// every piece of its sub-resource state (bucketLifecycle/bucketPolicies/
// bucketTagging/bucketVersioning/bucketReplication, all below) are keyed by
// bucket Name ALONE. AccountID is still recorded on the struct (it flows
// into BucketArn) but is no longer part of any lookup key in this file --
// it cannot be, without inventing a persistent OutpostId->account registry
// this service has no other reason to carry. This mirrors real S3's own
// bucket namespace, which is likewise globally unique by name rather than
// partitioned per caller.
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

// GetBucket returns an Outposts bucket. See the CreateBucket doc comment
// above for why lookup is by bucketName alone, not accountID+bucketName.
func (b *InMemoryBackend) GetBucket(bucketName string) (*OutpostsBucket, error) {
	b.mu.RLock("GetBucket")
	defer b.mu.RUnlock()

	bucket, ok := b.outpostsBuckets.Get(bucketName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return bucket, nil
}

// DeleteBucket removes an Outposts bucket and cascade-cleans every piece of
// state keyed off it (lifecycle, policy, tagging, versioning, replication,
// generic resource tags) so a delete/recreate cycle under the same name
// never resurfaces stale state from the deleted bucket. See the CreateBucket
// doc comment above for why lookup is by bucketName alone.
func (b *InMemoryBackend) DeleteBucket(bucketName string) error {
	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	bkt, ok := b.outpostsBuckets.Get(bucketName)
	if !ok {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	arn := bkt.BucketArn

	b.outpostsBuckets.Delete(bucketName)
	delete(b.bucketLifecycle, bucketName)
	delete(b.bucketPolicies, bucketName)
	delete(b.bucketTagging, bucketName)
	delete(b.bucketVersioning, bucketName)
	delete(b.bucketReplication, bucketName)
	delete(b.resourceTags, arn)

	return nil
}

// GetBucketLifecycleConfiguration returns lifecycle config for an Outposts bucket.
func (b *InMemoryBackend) GetBucketLifecycleConfiguration(bucketName string) (string, error) {
	b.mu.RLock("GetBucketLifecycleConfiguration")
	defer b.mu.RUnlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return "", fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return b.bucketLifecycle[bucketName], nil
}

// PutBucketLifecycleConfiguration sets lifecycle config for an Outposts bucket.
func (b *InMemoryBackend) PutBucketLifecycleConfiguration(bucketName, lifecycleConfig string) error {
	b.mu.Lock("PutBucketLifecycleConfiguration")
	defer b.mu.Unlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketLifecycle[bucketName] = lifecycleConfig

	return nil
}

// DeleteBucketLifecycleConfiguration removes lifecycle config from an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketLifecycleConfiguration(bucketName string) error {
	b.mu.Lock("DeleteBucketLifecycleConfiguration")
	defer b.mu.Unlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	delete(b.bucketLifecycle, bucketName)

	return nil
}

// GetBucketPolicy returns the policy for an Outposts bucket.
func (b *InMemoryBackend) GetBucketPolicy(bucketName string) (string, error) {
	b.mu.RLock("GetBucketPolicy")
	defer b.mu.RUnlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return "", fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return b.bucketPolicies[bucketName], nil
}

// PutBucketPolicy sets the policy for an Outposts bucket.
func (b *InMemoryBackend) PutBucketPolicy(bucketName, policy string) error {
	b.mu.Lock("PutBucketPolicy")
	defer b.mu.Unlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketPolicies[bucketName] = policy

	return nil
}

// DeleteBucketPolicy removes the policy from an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketPolicy(bucketName string) error {
	b.mu.Lock("DeleteBucketPolicy")
	defer b.mu.Unlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	delete(b.bucketPolicies, bucketName)

	return nil
}

// GetBucketTagging returns tags for an Outposts bucket.
func (b *InMemoryBackend) GetBucketTagging(bucketName string) (TagSet, error) {
	b.mu.RLock("GetBucketTagging")
	defer b.mu.RUnlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return nil, fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	tags := b.bucketTagging[bucketName]
	if tags == nil {
		return TagSet{}, nil
	}
	cp := make(TagSet, len(tags))
	maps.Copy(cp, tags)

	return cp, nil
}

// PutBucketTagging sets tags on an Outposts bucket.
func (b *InMemoryBackend) PutBucketTagging(bucketName string, tags TagSet) error {
	b.mu.Lock("PutBucketTagging")
	defer b.mu.Unlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketTagging[bucketName] = tags

	return nil
}

// DeleteBucketTagging removes all tags from an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketTagging(bucketName string) error {
	b.mu.Lock("DeleteBucketTagging")
	defer b.mu.Unlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	delete(b.bucketTagging, bucketName)

	return nil
}

// GetBucketVersioning returns the versioning state for an Outposts bucket.
func (b *InMemoryBackend) GetBucketVersioning(bucketName string) (string, error) {
	b.mu.RLock("GetBucketVersioning")
	defer b.mu.RUnlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return "", fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	state := b.bucketVersioning[bucketName]
	if state == "" {
		state = "Suspended"
	}

	return state, nil
}

// PutBucketVersioning sets the versioning state for an Outposts bucket.
func (b *InMemoryBackend) PutBucketVersioning(bucketName, status string) error {
	b.mu.Lock("PutBucketVersioning")
	defer b.mu.Unlock()

	if !b.outpostsBuckets.Has(bucketName) {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	b.bucketVersioning[bucketName] = status

	return nil
}

// ListRegionalBuckets lists every Outposts bucket. Real ListRegionalBucketsInput
// binds AccountId (optionally) too, but for the same reason GetBucket/
// DeleteBucket don't filter/key by it -- see the CreateBucket doc comment
// above -- there is no reliable per-account partition to filter against, so
// this returns every bucket regardless of the caller's AccountId header.
func (b *InMemoryBackend) ListRegionalBuckets() []*OutpostsBucket {
	b.mu.RLock("ListRegionalBuckets")
	defer b.mu.RUnlock()

	all := b.outpostsBuckets.All()
	out := make([]*OutpostsBucket, 0, len(all))
	for _, bucket := range all {
		cp := *bucket
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// ---- Bucket Replication ----

// GetBucketReplication retrieves the replication configuration for an Outposts bucket.
func (b *InMemoryBackend) GetBucketReplication(bucketName string) (string, error) {
	b.mu.RLock("GetBucketReplication")
	defer b.mu.RUnlock()

	cfg, ok := b.bucketReplication[bucketName]
	if !ok {
		return "", errReplicationNotFound
	}

	return cfg, nil
}

// PutBucketReplication stores a replication configuration for an Outposts bucket.
func (b *InMemoryBackend) PutBucketReplication(bucketName, config string) error {
	b.mu.Lock("PutBucketReplication")
	defer b.mu.Unlock()

	b.bucketReplication[bucketName] = config

	return nil
}

// DeleteBucketReplication removes the replication configuration for an Outposts bucket.
func (b *InMemoryBackend) DeleteBucketReplication(bucketName string) error {
	b.mu.Lock("DeleteBucketReplication")
	defer b.mu.Unlock()

	delete(b.bucketReplication, bucketName)

	return nil
}
