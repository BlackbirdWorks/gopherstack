package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// PutObjectLockConfiguration stores the object lock configuration for a bucket.
func (b *InMemoryBackend) PutObjectLockConfiguration(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("PutObjectLockConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectLockConfiguration")
	defer bucket.mu.Unlock()

	if !bucket.ObjectLockEnabled {
		return ErrObjectLockNotEnabled
	}

	bucket.ObjectLockConfig = configXML

	return nil
}

// GetObjectLockConfiguration retrieves the object lock configuration for a bucket.
func (b *InMemoryBackend) GetObjectLockConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetObjectLockConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetObjectLockConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.ObjectLockConfig == "" {
		return "", ErrNoObjectLockConfig
	}

	return bucket.ObjectLockConfig, nil
}

// PutObjectRetention sets the retention mode and retain-until-date for a specific object version.
func (b *InMemoryBackend) PutObjectRetention(
	_ context.Context,
	bucketName, key string,
	versionID *string,
	mode string,
	retainUntil time.Time,
	bypassGovernance bool,
) error {
	b.mu.RLock("PutObjectRetention")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectRetention")
	defer bucket.mu.Unlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return findErr
	}

	if validateErr := validateRetentionChange(
		ver.RetentionMode, ver.RetainUntil, mode, retainUntil, bypassGovernance,
	); validateErr != nil {
		return validateErr
	}

	ver.RetentionMode = mode
	ver.RetainUntil = retainUntil

	return nil
}

// validateRetentionChange enforces S3's one-way retention ratchet.
//
// COMPLIANCE: the retain-until date may only be extended and the mode can
// never change, for any principal — real S3 has no bypass for COMPLIANCE
// mode (docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html:
// "When an object is locked in compliance mode, its retention mode can't be
// changed, and its retention period can't be shortened").
//
// GOVERNANCE: the retain-until date may be shortened, removed, or upgraded
// to COMPLIANCE; shortening or removing it requires bypassGovernance (the
// x-amz-bypass-governance-retention: true header). gopherstack has no IAM
// evaluator, so this header is the only gate checked here — real S3 also
// requires the s3:BypassGovernanceRetention permission.
func validateRetentionChange(
	oldMode string,
	oldUntil time.Time,
	newMode string,
	newUntil time.Time,
	bypassGovernance bool,
) error {
	if oldMode == "" || oldUntil.IsZero() || !time.Now().Before(oldUntil) {
		return nil
	}

	if oldMode == string(types.ObjectLockRetentionModeCompliance) {
		if newMode != string(types.ObjectLockRetentionModeCompliance) {
			return ErrRetentionModeDowngrade
		}

		if newUntil.Before(oldUntil) {
			return ErrRetentionPeriodShortened
		}

		return nil
	}

	if newUntil.Before(oldUntil) && !bypassGovernance {
		return ErrRetentionPeriodShortened
	}

	return nil
}

// GetObjectRetention returns the retention mode and retain-until-date for a specific object version.
func (b *InMemoryBackend) GetObjectRetention(
	_ context.Context,
	bucketName, key string,
	versionID *string,
) (string, time.Time, error) {
	b.mu.RLock("GetObjectRetention")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", time.Time{}, err
	}

	bucket.mu.RLock("GetObjectRetention")
	defer bucket.mu.RUnlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return "", time.Time{}, findErr
	}

	if ver.RetentionMode == "" {
		return "", time.Time{}, ErrNoSuchObjectLockConfig
	}

	return ver.RetentionMode, ver.RetainUntil, nil
}

// PutObjectLegalHold sets or clears the legal hold status for a specific object version.
func (b *InMemoryBackend) PutObjectLegalHold(
	_ context.Context,
	bucketName, key string,
	versionID *string,
	status string,
) error {
	b.mu.RLock("PutObjectLegalHold")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectLegalHold")
	defer bucket.mu.Unlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return findErr
	}

	ver.LegalHold = status == "ON"

	return nil
}

// GetObjectLegalHold returns the legal hold status for a specific object version.
func (b *InMemoryBackend) GetObjectLegalHold(
	_ context.Context,
	bucketName, key string,
	versionID *string,
) (string, error) {
	b.mu.RLock("GetObjectLegalHold")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetObjectLegalHold")
	defer bucket.mu.RUnlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return "", findErr
	}

	if ver.LegalHold {
		return "ON", nil
	}

	return "OFF", nil
}

// findObjectVersion returns the specified object version (or the latest version if versionID is nil).
// Must be called with at least a read lock on bucket.mu.
func findObjectVersion(
	bucket *StoredBucket,
	key string,
	versionID *string,
) (*StoredObjectVersion, error) {
	obj, exists := bucket.Objects[key]
	if !exists {
		return nil, ErrNoSuchKey
	}

	if versionID != nil && *versionID != "" {
		ver, ok := obj.Versions[*versionID]
		if !ok || ver.Deleted {
			return nil, ErrNoSuchKey
		}

		return ver, nil
	}

	// Find latest
	if obj.LatestVersionID != "" {
		ver := obj.Versions[obj.LatestVersionID]
		if ver != nil && !ver.Deleted {
			return ver, nil
		}
	}

	for _, ver := range obj.Versions {
		if ver.IsLatest && !ver.Deleted {
			return ver, nil
		}
	}

	return nil, ErrNoSuchKey
}
