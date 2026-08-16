package s3

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (b *InMemoryBackend) DeleteObject(
	ctx context.Context,
	input *s3.DeleteObjectInput,
) (*s3.DeleteObjectOutput, error) {
	bucketName := *input.Bucket

	var bucket *StoredBucket
	var err error
	func() {
		b.mu.RLock("DeleteObject")
		defer b.mu.RUnlock()

		bucket, err = b.getBucket(bucketName)
	}()

	if err != nil {
		return nil, err
	}

	var out *s3.DeleteObjectOutput
	func() {
		bucket.mu.Lock("DeleteObject")
		defer bucket.mu.Unlock()

		out, err = b.deleteObjectLocked(bucket, *input.Key, input.VersionId)
	}()

	if err != nil {
		return nil, err
	}

	// Clean up tags for the deleted version (not when a delete marker is added).
	if out.DeleteMarker == nil || !aws.ToBool(out.DeleteMarker) {
		vid := NullVersion
		if input.VersionId != nil && *input.VersionId != "" {
			vid = *input.VersionId
		}

		func() {
			b.mu.RLock("DeleteObject.checkTags")
			hasTags := len(b.tags) > 0
			b.mu.RUnlock()

			if hasTags {
				b.mu.Lock("DeleteObject.tags")
				if b.tags != nil {
					delete(b.tags, fmt.Sprintf("%s/%s/%s", bucketName, *input.Key, vid))
				}
				b.mu.Unlock()
			}
		}()
	}

	// Async delete-marker replication when versioning created a delete marker,
	// parented to the service context rather than the request context.
	if out.DeleteMarker != nil && aws.ToBool(out.DeleteMarker) && bucket.ReplicationConfig != "" {
		repCtx := b.replicationContext(ctx)
		key := *input.Key
		b.replicationWg.Go(func() {
			b.triggerDeleteMarkerReplication(repCtx, bucketName, key)
		})
	}

	return out, nil
}

// deleteObjectLocked performs a single-object deletion assuming bucket.mu is
// already held by the caller. It is used by both DeleteObject and DeleteObjects
// (which holds the lock for the entire batch to avoid per-object lock churn).
func (b *InMemoryBackend) deleteObjectLocked(
	bucket *StoredBucket,
	key string,
	versionID *string,
) (*s3.DeleteObjectOutput, error) {
	obj, exists := bucket.Objects[key]
	if !exists {
		// S3 spec: Delete on non-existent object is 204
		return &s3.DeleteObjectOutput{}, nil
	}

	if err := checkObjectLockForDelete(obj, versionID); err != nil {
		return nil, err
	}

	if versionID != nil && *versionID != "" {
		return deleteSpecificVersion(bucket, obj, key, versionID), nil
	}

	return deleteLatestVersion(bucket, obj, key), nil
}

// findLatestVersion returns the version with IsLatest set, or nil if none exists.
func findLatestVersion(versions map[string]*StoredObjectVersion) *StoredObjectVersion {
	for _, v := range versions {
		if v.IsLatest {
			return v
		}
	}

	return nil
}

// checkObjectLockForDelete returns ErrObjectLocked if the target version is under
// a legal hold or an active retention policy. Must be called with bucket.mu held.
// obj.mu is acquired internally to guard against concurrent PutObject calls that
// update LatestVersionID / Versions under obj.mu after releasing bucket.mu.
func checkObjectLockForDelete(obj *StoredObject, versionID *string) error {
	obj.mu.RLock("checkObjectLockForDelete")
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion

	switch {
	case versionID != nil && *versionID != "":
		ver = obj.Versions[*versionID]
	case obj.LatestVersionID != "":
		ver = obj.Versions[obj.LatestVersionID]
	default:
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil || ver.Deleted {
		return nil
	}

	if ver.LegalHold {
		return ErrInvalidObjectState
	}

	if ver.RetentionMode != "" && !ver.RetainUntil.IsZero() && time.Now().Before(ver.RetainUntil) {
		return ErrInvalidObjectState
	}

	return nil
}

// checkObjectLockForOverwrite returns ErrObjectLocked if the null version (current
// when versioning is not enabled) is under a legal hold or an active retention
// policy. This prevents PutObject from silently overwriting a protected object.
// Must be called with bucket.mu held.
// obj.mu is acquired internally to guard against concurrent PutObject calls.
func checkObjectLockForOverwrite(obj *StoredObject) error {
	obj.mu.RLock("checkObjectLockForOverwrite")
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion

	if obj.LatestVersionID != "" {
		ver = obj.Versions[obj.LatestVersionID]
	} else {
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil || ver.Deleted {
		return nil
	}

	if ver.LegalHold {
		return ErrObjectLocked
	}

	if ver.RetentionMode != "" && !ver.RetainUntil.IsZero() && time.Now().Before(ver.RetainUntil) {
		return ErrObjectLocked
	}

	return nil
}

// deleteSpecificVersion removes the specified version from the object.
// Must be called with bucket.mu held by the caller.
func deleteSpecificVersion(
	bucket *StoredBucket,
	obj *StoredObject,
	key string,
	versionID *string,
) *s3.DeleteObjectOutput {
	if _, ok := obj.Versions[*versionID]; !ok {
		return &s3.DeleteObjectOutput{}
	}

	// Acquire obj.mu to serialize the map deletion with concurrent readers
	// (GetObject/HeadObject use obj.mu.RLock after releasing bucket.mu).
	var empty bool
	func() {
		obj.mu.Lock("deleteSpecificVersion")
		defer obj.mu.Unlock()

		delete(obj.Versions, *versionID)
		empty = len(obj.Versions) == 0
	}()

	if empty {
		// Remove the now-empty object from the bucket map (still under bucket.mu).
		delete(bucket.Objects, key)
		obj.mu.Close()
	}

	return &s3.DeleteObjectOutput{VersionId: versionID}
}

// deleteLatestVersion deletes the latest version of an object (or marks it deleted if versioning is enabled).
// Must be called with bucket.mu held by the caller.
func deleteLatestVersion(
	bucket *StoredBucket,
	obj *StoredObject,
	key string,
) *s3.DeleteObjectOutput {
	// Delete latest (Versioning enabled -> add delete marker, Suspended -> delete null version)
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		newVersionID := newObjectVersionID()

		// Acquire obj.mu to serialize the version-map mutation with concurrent
		// readers that use obj.mu.RLock after releasing bucket.mu.
		func() {
			obj.mu.Lock("deleteLatestVersion")
			defer obj.mu.Unlock()

			for _, v := range obj.Versions {
				v.IsLatest = false
			}
			deleteMarker := &StoredObjectVersion{
				VersionID:    newVersionID,
				Key:          key,
				Deleted:      true,
				IsLatest:     true,
				LastModified: time.Now().UTC(),
			}
			obj.Versions[newVersionID] = deleteMarker
			obj.LatestVersionID = newVersionID // Update cache
		}()

		return &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(true),
			VersionId:    aws.String(newVersionID),
		}
	}

	// Suspended versioning: an unversioned DELETE removes only the existing
	// "null" version and inserts a "null" delete marker. Any non-null versions
	// created while versioning was enabled must remain retrievable by versionId.
	var isEmpty bool
	func() {
		obj.mu.Lock("deleteLatestVersion")
		defer obj.mu.Unlock()

		delete(obj.Versions, NullVersion)

		// If no prior (non-null) versions remain, the object disappears entirely —
		// matching the behaviour of a bucket that was never versioned.
		if len(obj.Versions) == 0 {
			isEmpty = true

			return
		}

		for _, v := range obj.Versions {
			v.IsLatest = false
		}
		deleteMarker := &StoredObjectVersion{
			VersionID:    NullVersion,
			Key:          key,
			Deleted:      true,
			IsLatest:     true,
			LastModified: time.Now().UTC(),
		}
		obj.Versions[NullVersion] = deleteMarker
		obj.LatestVersionID = NullVersion
	}()

	if isEmpty {
		delete(bucket.Objects, key)
		obj.mu.Close()

		return &s3.DeleteObjectOutput{}
	}

	return &s3.DeleteObjectOutput{
		DeleteMarker: aws.Bool(true),
		VersionId:    aws.String(NullVersion),
	}
}

func (b *InMemoryBackend) DeleteObjects(
	_ context.Context,
	input *s3.DeleteObjectsInput,
) (*s3.DeleteObjectsOutput, error) {
	out := &s3.DeleteObjectsOutput{
		Deleted: make([]types.DeletedObject, 0, len(input.Delete.Objects)),
		Errors:  make([]types.Error, 0, len(input.Delete.Objects)),
	}

	bucketName := *input.Bucket

	var bucket *StoredBucket
	var err error
	func() {
		b.mu.RLock("DeleteObjects")
		defer b.mu.RUnlock()

		bucket, err = b.getBucket(bucketName)
	}()

	if err != nil {
		for _, obj := range input.Delete.Objects {
			out.Errors = append(out.Errors, types.Error{
				Key:     obj.Key,
				Code:    aws.String("NoSuchBucket"),
				Message: aws.String(ErrNoSuchBucket.Error()),
			})
		}

		return out, nil
	}

	// Hold the bucket lock for the entire batch to avoid per-object lock churn
	// when deleting thousands of objects.
	var tagKeysToDelete []string

	func() {
		bucket.mu.Lock("DeleteObjects")
		defer bucket.mu.Unlock()

		for _, obj := range input.Delete.Objects {
			deleted, tagKey, delErr := b.deleteSingleObject(bucket, bucketName, obj)
			if delErr != nil {
				out.Errors = append(out.Errors, types.Error{
					Key:     obj.Key,
					Code:    aws.String("AccessDenied"),
					Message: aws.String(delErr.Error()),
				})

				continue
			}

			if tagKey != "" {
				tagKeysToDelete = append(tagKeysToDelete, tagKey)
			}

			out.Deleted = append(out.Deleted, deleted)
		}
	}()

	// Clean up tags after the bucket lock is released.
	b.cleanDeletedTags(tagKeysToDelete)

	return out, nil
}

func (b *InMemoryBackend) cleanDeletedTags(tagKeys []string) {
	if len(tagKeys) == 0 {
		return
	}

	b.mu.RLock("DeleteObjects.checkTags")
	hasTags := len(b.tags) > 0
	b.mu.RUnlock()

	if hasTags {
		b.mu.Lock("DeleteObjects.tags")
		if b.tags != nil {
			for _, k := range tagKeys {
				delete(b.tags, k)
			}
		}
		b.mu.Unlock()
	}
}

// deleteSingleObject deletes one object from the bucket and returns the deleted record,
// the tag key to clean up (if any), and any error. Must be called with bucket.mu held.
func (b *InMemoryBackend) deleteSingleObject(
	bucket *StoredBucket,
	bucketName string,
	obj types.ObjectIdentifier,
) (types.DeletedObject, string, error) {
	delOut, delErr := b.deleteObjectLocked(bucket, aws.ToString(obj.Key), obj.VersionId)
	if delErr != nil {
		return types.DeletedObject{}, "", delErr
	}

	tagKey := ""
	if delOut.DeleteMarker == nil || !aws.ToBool(delOut.DeleteMarker) {
		vid := NullVersion
		if obj.VersionId != nil && *obj.VersionId != "" {
			vid = *obj.VersionId
		}
		tagKey = fmt.Sprintf("%s/%s/%s", bucketName, aws.ToString(obj.Key), vid)
	}

	deleted := types.DeletedObject{
		Key:       obj.Key,
		VersionId: obj.VersionId,
	}
	if delOut.DeleteMarker != nil {
		deleted.DeleteMarker = delOut.DeleteMarker
	}
	if delOut.VersionId != nil {
		deleted.DeleteMarkerVersionId = delOut.VersionId
	}

	return deleted, tagKey, nil
}
