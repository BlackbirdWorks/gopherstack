package s3control

import (
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// errBucketNotFound is returned when an Outposts bucket is not found.
var errBucketNotFound = awserr.New("NoSuchBucket", awserr.ErrNotFound)

// errJobNotFound is returned when a batch job is not found.
var errJobNotFound = awserr.New("NoSuchJob", awserr.ErrNotFound)

// errMRAPNotFound is returned when a Multi-Region Access Point is not found.
var errMRAPNotFound = awserr.New("NoSuchMultiRegionAccessPoint", awserr.ErrNotFound)

// errObjectLambdaAPNotFound is returned when an Object Lambda AP is not found.
var errObjectLambdaAPNotFound = awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)

// errStorageLensNotFound is returned when a Storage Lens config is not found.

// TagSet represents a key-value tag map.
type TagSet map[string]string

// ---- Job Tagging ----

// PutJobTagging sets tags on a batch job.
func (b *InMemoryBackend) PutJobTagging(accountID, jobID string, tags TagSet) error {
	b.mu.Lock("PutJobTagging")
	defer b.mu.Unlock()

	key := accountID + ":" + jobID
	if _, ok := b.batchJobs[key]; !ok {
		return fmt.Errorf("%w: %s", errJobNotFound, jobID)
	}
	b.jobTags[key] = tags

	return nil
}

// GetJobTagging returns tags for a batch job.
func (b *InMemoryBackend) GetJobTagging(accountID, jobID string) (TagSet, error) {
	b.mu.RLock("GetJobTagging")
	defer b.mu.RUnlock()

	key := accountID + ":" + jobID
	if _, ok := b.batchJobs[key]; !ok {
		return nil, fmt.Errorf("%w: %s", errJobNotFound, jobID)
	}

	tags := b.jobTags[key]
	if tags == nil {
		return TagSet{}, nil
	}
	cp := make(TagSet, len(tags))
	maps.Copy(cp, tags)

	return cp, nil
}

// DeleteJobTagging removes all tags from a batch job.
func (b *InMemoryBackend) DeleteJobTagging(accountID, jobID string) error {
	b.mu.Lock("DeleteJobTagging")
	defer b.mu.Unlock()

	key := accountID + ":" + jobID
	if _, ok := b.batchJobs[key]; !ok {
		return fmt.Errorf("%w: %s", errJobNotFound, jobID)
	}
	delete(b.jobTags, key)

	return nil
}

// ---- Access Grants Instance ----

// ListAccessGrantsInstances returns all Access Grants instances for the account.
func (b *InMemoryBackend) ListAccessGrantsInstances(accountID string) []*AccessGrantsInstance {
	b.mu.RLock("ListAccessGrantsInstances")
	defer b.mu.RUnlock()

	inst, ok := b.accessGrantsInstances[accountID]
	if !ok {
		return nil
	}

	return []*AccessGrantsInstance{inst}
}

func (b *InMemoryBackend) GetAccessGrantsInstance(accountID string) (*AccessGrantsInstance, error) {
	b.mu.RLock("GetAccessGrantsInstance")
	defer b.mu.RUnlock()

	inst, ok := b.accessGrantsInstances[accountID]
	if !ok {
		return nil, awserr.New("AccessGrantsInstanceNotExistsError", awserr.ErrNotFound)
	}

	return inst, nil
}

// DeleteAccessGrantsInstance removes the Access Grants instance.
func (b *InMemoryBackend) DeleteAccessGrantsInstance(accountID string) error {
	b.mu.Lock("DeleteAccessGrantsInstance")
	defer b.mu.Unlock()

	delete(b.accessGrantsInstances, accountID)

	return nil
}

// GetAccessGrantsInstanceResourcePolicy returns the resource policy for an AGI.
func (b *InMemoryBackend) GetAccessGrantsInstanceResourcePolicy(accountID string) (string, error) {
	b.mu.RLock("GetAccessGrantsInstanceResourcePolicy")
	defer b.mu.RUnlock()

	policy := b.accessGrantsInstancePolicies[accountID]

	return policy, nil
}

// PutAccessGrantsInstanceResourcePolicy sets the resource policy for an AGI.
func (b *InMemoryBackend) PutAccessGrantsInstanceResourcePolicy(accountID, policy string) {
	b.mu.Lock("PutAccessGrantsInstanceResourcePolicy")
	defer b.mu.Unlock()

	b.accessGrantsInstancePolicies[accountID] = policy
}

// DeleteAccessGrantsInstanceResourcePolicy removes the resource policy.
func (b *InMemoryBackend) DeleteAccessGrantsInstanceResourcePolicy(accountID string) {
	b.mu.Lock("DeleteAccessGrantsInstanceResourcePolicy")
	defer b.mu.Unlock()

	delete(b.accessGrantsInstancePolicies, accountID)
}

// DissociateAccessGrantsIdentityCenter removes the identity center association.
func (b *InMemoryBackend) DissociateAccessGrantsIdentityCenter(accountID string) {
	b.mu.Lock("DissociateAccessGrantsIdentityCenter")
	defer b.mu.Unlock()

	if inst, ok := b.accessGrantsInstances[accountID]; ok {
		inst.IdentityCenterArn = ""
	}
}

// GetAccessGrantsInstanceForPrefix returns the AGI for a given S3 prefix.
func (b *InMemoryBackend) GetAccessGrantsInstanceForPrefix(
	accountID, prefix string,
) (*AccessGrantsInstance, error) {
	b.mu.RLock("GetAccessGrantsInstanceForPrefix")
	defer b.mu.RUnlock()

	inst, ok := b.accessGrantsInstances[accountID]
	if !ok {
		return nil, awserr.New("AccessGrantsInstanceNotExistsError", awserr.ErrNotFound)
	}
	_ = prefix

	return inst, nil
}

// ---- Access Grants CRUD ----

// GetAccessGrant returns an access grant by ID.
func (b *InMemoryBackend) GetAccessGrant(accountID, grantID string) (*AccessGrant, error) {
	b.mu.RLock("GetAccessGrant")
	defer b.mu.RUnlock()

	key := accountID + ":" + grantID
	grant, ok := b.accessGrants[key]
	if !ok {
		return nil, awserr.New("NoSuchAccessGrant", awserr.ErrNotFound)
	}

	return grant, nil
}

// DeleteAccessGrant removes an access grant.
func (b *InMemoryBackend) DeleteAccessGrant(accountID, grantID string) error {
	b.mu.Lock("DeleteAccessGrant")
	defer b.mu.Unlock()

	key := accountID + ":" + grantID
	if _, ok := b.accessGrants[key]; !ok {
		return awserr.New("NoSuchAccessGrant", awserr.ErrNotFound)
	}
	delete(b.accessGrants, key)

	return nil
}

// ListAccessGrants returns all access grants for an account, optionally filtered by locationScope.
func (b *InMemoryBackend) ListAccessGrants(accountID, locationScope string) []*AccessGrant {
	b.mu.RLock("ListAccessGrants")
	defer b.mu.RUnlock()

	var out []*AccessGrant
	for _, g := range b.accessGrants {
		if g.AccountID != accountID {
			continue
		}
		if locationScope != "" && g.GrantScope != locationScope {
			continue
		}
		cp := *g
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccessGrantID < out[j].AccessGrantID })

	return out
}

// ListCallerAccessGrants returns access grants visible to the caller.
func (b *InMemoryBackend) ListCallerAccessGrants(accountID string) []*AccessGrant {
	return b.ListAccessGrants(accountID, "")
}

// GetAccessGrantsLocation returns an access grants location by ID.
func (b *InMemoryBackend) GetAccessGrantsLocation(
	accountID, locationID string,
) (*AccessGrantsLocation, error) {
	b.mu.RLock("GetAccessGrantsLocation")
	defer b.mu.RUnlock()

	key := accountID + ":" + locationID
	loc, ok := b.accessGrantsLocations[key]
	if !ok {
		return nil, awserr.New("NoSuchAccessGrantsLocation", awserr.ErrNotFound)
	}

	return loc, nil
}

// DeleteAccessGrantsLocation removes an access grants location.
func (b *InMemoryBackend) DeleteAccessGrantsLocation(accountID, locationID string) error {
	b.mu.Lock("DeleteAccessGrantsLocation")
	defer b.mu.Unlock()

	key := accountID + ":" + locationID
	if _, ok := b.accessGrantsLocations[key]; !ok {
		return awserr.New("NoSuchAccessGrantsLocation", awserr.ErrNotFound)
	}
	delete(b.accessGrantsLocations, key)

	return nil
}

// UpdateAccessGrantsLocation updates the IAM role ARN for a location.
func (b *InMemoryBackend) UpdateAccessGrantsLocation(
	accountID, locationID, iamRoleArn string,
) (*AccessGrantsLocation, error) {
	b.mu.Lock("UpdateAccessGrantsLocation")
	defer b.mu.Unlock()

	key := accountID + ":" + locationID
	loc, ok := b.accessGrantsLocations[key]
	if !ok {
		return nil, awserr.New("NoSuchAccessGrantsLocation", awserr.ErrNotFound)
	}
	loc.IAMRoleArn = iamRoleArn

	return loc, nil
}

// ListAccessGrantsLocations returns all locations for an account.
func (b *InMemoryBackend) ListAccessGrantsLocations(accountID string) []*AccessGrantsLocation {
	b.mu.RLock("ListAccessGrantsLocations")
	defer b.mu.RUnlock()

	var out []*AccessGrantsLocation
	for _, loc := range b.accessGrantsLocations {
		if loc.AccountID == accountID {
			cp := *loc
			out = append(out, &cp)
		}
	}
	sort.Slice(
		out,
		func(i, j int) bool { return out[i].AccessGrantsLocationID < out[j].AccessGrantsLocationID },
	)

	return out
}

// GetDataAccess returns a presigned URL for accessing data via access grants.
func (b *InMemoryBackend) GetDataAccess(accountID, target, permission string) (string, error) {
	b.mu.RLock("GetDataAccess")
	defer b.mu.RUnlock()

	// Verify the instance exists
	if _, ok := b.accessGrantsInstances[accountID]; !ok {
		return "", awserr.New("AccessGrantsInstanceNotExistsError", awserr.ErrNotFound)
	}
	_ = target
	_ = permission

	return "https://s3.amazonaws.com/presigned-mock-url", nil
}

// ---- Access Point Scope ----

// GetAccessPointScope returns the scope for an access point.
func (b *InMemoryBackend) GetAccessPointScope(accountID, name string) (string, error) {
	b.mu.RLock("GetAccessPointScope")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return "", awserr.New("NoSuchAccessPoint", awserr.ErrNotFound)
	}

	return b.accessPointScopes[key], nil
}

// PutAccessPointScope sets the scope for an access point.
func (b *InMemoryBackend) PutAccessPointScope(accountID, name, scope string) error {
	b.mu.Lock("PutAccessPointScope")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
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
	for _, ap := range b.accessPoints {
		if ap.AccountID == accountID {
			cp := *ap
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// ---- Object Lambda Access Points ----

// GetAccessPointForObjectLambda returns an Object Lambda access point.
func (b *InMemoryBackend) GetAccessPointForObjectLambda(
	accountID, name string,
) (*ObjectLambdaAccessPoint, error) {
	b.mu.RLock("GetAccessPointForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	ap, ok := b.objectLambdaAccessPoints[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return ap, nil
}

// DeleteAccessPointForObjectLambda removes an Object Lambda access point.
func (b *InMemoryBackend) DeleteAccessPointForObjectLambda(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.objectLambdaAccessPoints[key]; !ok {
		return fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}
	delete(b.objectLambdaAccessPoints, key)

	return nil
}

// ListAccessPointsForObjectLambda lists Object Lambda access points for an account.
func (b *InMemoryBackend) ListAccessPointsForObjectLambda(
	accountID string,
) []*ObjectLambdaAccessPoint {
	b.mu.RLock("ListAccessPointsForObjectLambda")
	defer b.mu.RUnlock()

	var out []*ObjectLambdaAccessPoint
	for _, ap := range b.objectLambdaAccessPoints {
		if ap.AccountID == accountID {
			cp := *ap
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// GetAccessPointPolicyForObjectLambda returns the policy for an Object Lambda AP.
func (b *InMemoryBackend) GetAccessPointPolicyForObjectLambda(
	accountID, name string,
) (string, error) {
	b.mu.RLock("GetAccessPointPolicyForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if _, ok := b.objectLambdaAccessPoints[key]; !ok {
		return "", fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return b.objectLambdaAPPolicies[key], nil
}

// PutAccessPointPolicyForObjectLambda sets the policy for an Object Lambda AP.
func (b *InMemoryBackend) PutAccessPointPolicyForObjectLambda(
	accountID, name, policy string,
) error {
	b.mu.Lock("PutAccessPointPolicyForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.objectLambdaAccessPoints[key]; !ok {
		return fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}
	b.objectLambdaAPPolicies[key] = policy

	return nil
}

// DeleteAccessPointPolicyForObjectLambda removes the policy from an Object Lambda AP.
func (b *InMemoryBackend) DeleteAccessPointPolicyForObjectLambda(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointPolicyForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	delete(b.objectLambdaAPPolicies, key)

	return nil
}

// GetAccessPointPolicyStatusForObjectLambda returns the policy status for an Object Lambda AP.
func (b *InMemoryBackend) GetAccessPointPolicyStatusForObjectLambda(
	accountID, name string,
) (bool, error) {
	b.mu.RLock("GetAccessPointPolicyStatusForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if _, ok := b.objectLambdaAccessPoints[key]; !ok {
		return false, fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return b.objectLambdaAPPolicies[key] != "", nil
}

// GetAccessPointConfigurationForObjectLambda returns the configuration for an Object Lambda AP.
func (b *InMemoryBackend) GetAccessPointConfigurationForObjectLambda(
	accountID, name string,
) (string, error) {
	b.mu.RLock("GetAccessPointConfigurationForObjectLambda")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if _, ok := b.objectLambdaAccessPoints[key]; !ok {
		return "", fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}

	return b.objectLambdaAPConfigs[key], nil
}

// PutAccessPointConfigurationForObjectLambda sets the configuration for an Object Lambda AP.
func (b *InMemoryBackend) PutAccessPointConfigurationForObjectLambda(
	accountID, name, config string,
) error {
	b.mu.Lock("PutAccessPointConfigurationForObjectLambda")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.objectLambdaAccessPoints[key]; !ok {
		return fmt.Errorf("%w: %s", errObjectLambdaAPNotFound, name)
	}
	b.objectLambdaAPConfigs[key] = config

	return nil
}

// ---- Outposts Bucket ----

// GetBucket returns an Outposts bucket.
func (b *InMemoryBackend) GetBucket(accountID, bucketName string) (*OutpostsBucket, error) {
	b.mu.RLock("GetBucket")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	bucket, ok := b.outpostsBuckets[key]
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
	if _, ok := b.outpostsBuckets[key]; !ok {
		return fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}
	delete(b.outpostsBuckets, key)

	return nil
}

// GetBucketLifecycleConfiguration returns lifecycle config for an Outposts bucket.
func (b *InMemoryBackend) GetBucketLifecycleConfiguration(
	accountID, bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketLifecycleConfiguration")
	defer b.mu.RUnlock()

	key := accountID + ":" + bucketName
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
		return "", fmt.Errorf("%w: %s", errBucketNotFound, bucketName)
	}

	return b.bucketPolicies[key], nil
}

// PutBucketPolicy sets the policy for an Outposts bucket.
func (b *InMemoryBackend) PutBucketPolicy(accountID, bucketName, policy string) error {
	b.mu.Lock("PutBucketPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + bucketName
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	if _, ok := b.outpostsBuckets[key]; !ok {
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
	for _, bucket := range b.outpostsBuckets {
		if bucket.AccountID == accountID {
			cp := *bucket
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// ---- MRAP ----

// DescribeMultiRegionAccessPointOperation returns the status of an MRAP async operation.
func (b *InMemoryBackend) DescribeMultiRegionAccessPointOperation(
	accountID, requestToken string,
) (*MultiRegionAccessPointRequest, error) {
	b.mu.RLock("DescribeMultiRegionAccessPointOperation")
	defer b.mu.RUnlock()

	// Try direct key lookup (bare token).
	if req, ok := b.mrapRequests[accountID+":"+requestToken]; ok {
		return req, nil
	}
	// Fall back to ARN match.
	for _, req := range b.mrapRequests {
		if req.AccountID == accountID && req.RequestTokenARN == requestToken {
			return req, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", errMRAPNotFound, requestToken)
}

// GetMultiRegionAccessPointPolicy returns the policy for an MRAP.
func (b *InMemoryBackend) GetMultiRegionAccessPointPolicy(accountID, name string) (string, error) {
	b.mu.RLock("GetMultiRegionAccessPointPolicy")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	mrapObj, ok := b.mraps[key]
	if !ok {
		return "", fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	return mrapObj.Policy, nil
}

// GetMultiRegionAccessPointPolicyStatus returns whether the MRAP policy is public.
func (b *InMemoryBackend) GetMultiRegionAccessPointPolicyStatus(
	accountID, name string,
) (bool, error) {
	b.mu.RLock("GetMultiRegionAccessPointPolicyStatus")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	mrapObj, ok := b.mraps[key]
	if !ok {
		return false, fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	return mrapObj.Policy != "", nil
}

// GetMultiRegionAccessPointRoutes returns the routing configuration for an MRAP.
func (b *InMemoryBackend) GetMultiRegionAccessPointRoutes(accountID, mrap string) (string, error) {
	b.mu.RLock("GetMultiRegionAccessPointRoutes")
	defer b.mu.RUnlock()

	key := accountID + ":" + mrap
	if _, ok := b.mraps[key]; !ok {
		return "", fmt.Errorf("%w: %s", errMRAPNotFound, mrap)
	}

	return b.mrapRoutes[key], nil
}
