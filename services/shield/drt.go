package shield

import (
	"fmt"
	"slices"
)

// maxDRTLogBuckets is the Shield Advanced limit on the number of S3 log buckets that can be
// associated with the DRT (Shield Response Team) at once.
const maxDRTLogBuckets = 10

// AssociateDRTLogBucket associates an S3 log bucket with the DRT. Per real AWS behavior, the SRT
// must already have an IAM role associated (via AssociateDRTRole) before a log bucket can be
// shared -- otherwise the real API returns NoAssociatedRoleException. Also enforces the
// documented 10-bucket cap via LimitsExceededException.
func (b *InMemoryBackend) AssociateDRTLogBucket(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("%w: LogBucket is required", ErrValidation)
	}

	b.mu.Lock("AssociateDRTLogBucket")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if b.drtAccess == nil || b.drtAccess.RoleArn == "" {
		return fmt.Errorf(
			"%w: AssociateDRTRole must be called before AssociateDRTLogBucket",
			ErrNoAssociatedRole,
		)
	}

	if slices.Contains(b.drtAccess.LogBucketList, bucket) {
		return nil
	}

	if len(b.drtAccess.LogBucketList) >= maxDRTLogBuckets {
		return fmt.Errorf("%w: Type=DRTLogBucketList, Limit=%d", ErrLimitExceeded, maxDRTLogBuckets)
	}

	b.drtAccess.LogBucketList = append(b.drtAccess.LogBucketList, bucket)

	return nil
}

// DisassociateDRTLogBucket removes an S3 log bucket from the DRT.
func (b *InMemoryBackend) DisassociateDRTLogBucket(bucket string) error {
	b.mu.Lock("DisassociateDRTLogBucket")
	defer b.mu.Unlock()

	if b.drtAccess == nil {
		return fmt.Errorf("%w: bucket %q not associated", ErrProtectionNotFound, bucket)
	}

	idx := slices.Index(b.drtAccess.LogBucketList, bucket)
	if idx < 0 {
		return fmt.Errorf("%w: bucket %q not associated with DRT", ErrProtectionNotFound, bucket)
	}

	b.drtAccess.LogBucketList = slices.Delete(b.drtAccess.LogBucketList, idx, idx+1)

	return nil
}

// AssociateDRTRole associates an IAM role with the DRT.
func (b *InMemoryBackend) AssociateDRTRole(roleARN string) error {
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	b.mu.Lock("AssociateDRTRole")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if b.drtAccess == nil {
		b.drtAccess = &DRTAccess{}
	}

	b.drtAccess.RoleArn = roleARN

	return nil
}

// DisassociateDRTRole removes the IAM role association from the DRT. Idempotent per AWS.
func (b *InMemoryBackend) DisassociateDRTRole() error {
	b.mu.Lock("DisassociateDRTRole")
	defer b.mu.Unlock()

	if b.drtAccess != nil {
		b.drtAccess.RoleArn = ""
	}

	return nil
}

// DescribeDRTAccess returns the current DRT access configuration.
func (b *InMemoryBackend) DescribeDRTAccess() *DRTAccess {
	b.mu.RLock("DescribeDRTAccess")
	defer b.mu.RUnlock()

	if b.drtAccess == nil {
		return &DRTAccess{LogBucketList: []string{}}
	}

	cp := *b.drtAccess
	cp.LogBucketList = append([]string(nil), b.drtAccess.LogBucketList...)

	return &cp
}
