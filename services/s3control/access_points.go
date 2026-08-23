package s3control

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// CreateAccessPoint creates an S3 access point.
func (b *InMemoryBackend) CreateAccessPoint(accountID, name, bucket string) *AccessPoint {
	b.mu.Lock("CreateAccessPoint")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(arnFmtAccessPoint, b.region, accountID, name)

	// Guard against short accountIDs to prevent panics on slice operations.
	aliasPrefix := accountID
	if len(aliasPrefix) > aliasAccountIDMaxLen {
		aliasPrefix = aliasPrefix[:aliasAccountIDMaxLen]
	}

	alias := fmt.Sprintf("%s-%s-s3alias", name, aliasPrefix)

	ap := &AccessPoint{
		AccountID:      accountID,
		Name:           name,
		Bucket:         bucket,
		AccessPointArn: arn,
		Alias:          alias,
		NetworkOrigin:  "Internet",
		CreationDate:   nowRFC3339(),
	}
	b.accessPoints.Put(ap)

	cp := *ap

	return &cp
}

// SetAccessPointVpcConfig sets VPC configuration fields on an existing access point.
// NetworkOrigin is set to "VPC" when vpcID is non-empty, else "Internet".
// Alias is cleared for VPC access points (AWS does not emit an alias for VPC APs).
func (b *InMemoryBackend) SetAccessPointVpcConfig(accountID, name, vpcID, bucketAccountID string) error {
	b.mu.Lock("SetAccessPointVpcConfig")
	defer b.mu.Unlock()

	ap, ok := b.accessPoints.Get(accountID + ":" + name)
	if !ok {
		return errAccessPointNotFound
	}

	ap.VpcID = vpcID
	ap.BucketAccountID = bucketAccountID

	if vpcID != "" {
		ap.NetworkOrigin = "VPC"
		ap.Alias = ""
	} else {
		ap.NetworkOrigin = "Internet"
	}

	return nil
}

// GetAccessPoint retrieves an S3 access point by name.
func (b *InMemoryBackend) GetAccessPoint(accountID, name string) (*AccessPoint, error) {
	b.mu.RLock("GetAccessPoint")
	defer b.mu.RUnlock()

	ap, ok := b.accessPoints.Get(accountID + ":" + name)
	if !ok {
		return nil, errAccessPointNotFound
	}

	cp := *ap

	return &cp, nil
}

// DeleteAccessPoint removes an S3 access point and cascade-cleans every
// piece of state keyed off it (policy, scope, per-AP PublicAccessBlock,
// generic resource tags) so a delete/recreate cycle under the same name
// never resurfaces stale state from the deleted access point.
func (b *InMemoryBackend) DeleteAccessPoint(accountID, name string) error {
	b.mu.Lock("DeleteAccessPoint")
	defer b.mu.Unlock()

	key := accountID + ":" + name

	ap, ok := b.accessPoints.Get(key)
	if !ok {
		return errAccessPointNotFound
	}

	arn := ap.AccessPointArn

	b.accessPoints.Delete(key)
	delete(b.accessPointPolicies, key)
	delete(b.accessPointScopes, key)
	b.accessPointPABs.Delete(key)
	delete(b.resourceTags, arn)

	return nil
}

// ListAccessPoints returns all access points for an account.
func (b *InMemoryBackend) ListAccessPoints(accountID string) []*AccessPoint {
	b.mu.RLock("ListAccessPoints")
	defer b.mu.RUnlock()

	var out []*AccessPoint

	for _, v := range b.accessPoints.All() {
		if v.AccountID == accountID {
			cp := *v
			out = append(out, &cp)
		}
	}

	return out
}

// PutAccessPointPolicy stores a policy for an access point.
func (b *InMemoryBackend) PutAccessPointPolicy(accountID, name, policy string) error {
	b.mu.Lock("PutAccessPointPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return errAccessPointNotFound
	}

	b.accessPointPolicies[key] = policy

	return nil
}

// GetAccessPointPolicy retrieves the policy for an access point.
func (b *InMemoryBackend) GetAccessPointPolicy(accountID, name string) (string, error) {
	b.mu.RLock("GetAccessPointPolicy")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return "", errAccessPointNotFound
	}

	policy, ok := b.accessPointPolicies[key]
	if !ok {
		return "", errAccessPointPolicyNotFound
	}

	return policy, nil
}

// DeleteAccessPointPolicy removes the policy for an access point.
func (b *InMemoryBackend) DeleteAccessPointPolicy(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return errAccessPointNotFound
	}

	delete(b.accessPointPolicies, key)

	return nil
}

// GetAccessPointPolicyStatus reports whether an access point has a policy
// attached, mirroring the ObjectLambda/MRAP siblings' "policy set" heuristic
// (GetAccessPointPolicyStatusForObjectLambda, GetMultiRegionAccessPointPolicyStatus).
func (b *InMemoryBackend) GetAccessPointPolicyStatus(accountID, name string) (bool, error) {
	b.mu.RLock("GetAccessPointPolicyStatus")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return false, errAccessPointNotFound
	}

	return b.accessPointPolicies[key] != "", nil
}

// ---- Access Point Scope ----

// GetAccessPointScope returns the scope for an access point.
func (b *InMemoryBackend) GetAccessPointScope(accountID, name string) (string, error) {
	b.mu.RLock("GetAccessPointScope")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return "", awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)
	}

	return b.accessPointScopes[key], nil
}

// PutAccessPointScope sets the scope for an access point.
func (b *InMemoryBackend) PutAccessPointScope(accountID, name, scope string) error {
	b.mu.Lock("PutAccessPointScope")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)
	}
	b.accessPointScopes[key] = scope

	return nil
}

// DeleteAccessPointScope removes the scope for an access point.
func (b *InMemoryBackend) DeleteAccessPointScope(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointScope")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	delete(b.accessPointScopes, key)

	return nil
}

// ListAccessPointsForDirectoryBuckets returns access points for directory buckets.
func (b *InMemoryBackend) ListAccessPointsForDirectoryBuckets(accountID string) []*AccessPoint {
	b.mu.RLock("ListAccessPointsForDirectoryBuckets")
	defer b.mu.RUnlock()

	var out []*AccessPoint
	for _, ap := range b.accessPoints.All() {
		if ap.AccountID == accountID {
			cp := *ap
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// Per-AccessPoint PublicAccessBlock: internal storage plumbing, NOT
// separate AWS operations — aws-sdk-go-v2/service/s3control has no
// Get/Put/DeleteAccessPointPublicAccessBlock ops. PublicAccessBlockConfiguration
// is account-level only via Get/Put/DeletePublicAccessBlock, except for the
// inline field the real CreateAccessPoint request and GetAccessPoint
// response carry directly, which these methods back.

// GetAccessPointPublicAccessBlock returns the public access block configuration for an access point.
func (b *InMemoryBackend) GetAccessPointPublicAccessBlock(accountID, name string) (*PublicAccessBlock, error) {
	b.mu.RLock("GetAccessPointPublicAccessBlock")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return nil, fmt.Errorf("%w: %s", errAccessPointNotFound, name)
	}

	pab, ok := b.accessPointPABs.Get(key)
	if !ok {
		return nil, errAPPABNotFound
	}

	cp := *pab

	return &cp, nil
}

// PutAccessPointPublicAccessBlock sets the public access block configuration for an access point.
func (b *InMemoryBackend) PutAccessPointPublicAccessBlock(accountID, name string, cfg PublicAccessBlock) error {
	b.mu.Lock("PutAccessPointPublicAccessBlock")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return fmt.Errorf("%w: %s", errAccessPointNotFound, name)
	}

	cp := cfg
	cp.AccountID = accountID
	cp.APName = name
	b.accessPointPABs.Put(&cp)

	return nil
}

// DeleteAccessPointPublicAccessBlock removes the public access block configuration for an access point.
func (b *InMemoryBackend) DeleteAccessPointPublicAccessBlock(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointPublicAccessBlock")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.accessPoints.Has(key) {
		return fmt.Errorf("%w: %s", errAccessPointNotFound, name)
	}

	b.accessPointPABs.Delete(key)

	return nil
}

// ---- Seed helpers for testing ----

// AddAccessPointInternal creates an access point directly, for seeding test data.
func (b *InMemoryBackend) AddAccessPointInternal(accountID, name, bucket string) *AccessPoint {
	return b.CreateAccessPoint(accountID, name, bucket)
}
