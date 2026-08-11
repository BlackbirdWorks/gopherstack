package s3

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func applyDelimiter(
	prefix, delimiter string,
	contents []types.Object,
) ([]types.Object, []types.CommonPrefix) {
	var filtered []types.Object
	var cpList []types.CommonPrefix
	seenPrefixes := make(map[string]struct{})

	for _, obj := range contents {
		key := aws.ToString(obj.Key)
		rest := key[len(prefix):]
		idx := strings.Index(rest, delimiter)

		if idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if _, seen := seenPrefixes[cp]; !seen {
				seenPrefixes[cp] = struct{}{}
				cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(cp)})
			}
		} else {
			filtered = append(filtered, obj)
		}
	}

	sort.Slice(cpList, func(i, j int) bool {
		return aws.ToString(cpList[i].Prefix) < aws.ToString(cpList[j].Prefix)
	})

	return filtered, cpList
}

func (b *InMemoryBackend) processListObjects(
	bucket *StoredBucket,
	input *s3.ListObjectsInput,
) ([]types.Object, []types.CommonPrefix, bool, string, int32) {
	// Snapshot object pointers under lock
	prefix := aws.ToString(input.Prefix)
	var objectSnapshots []*StoredObject
	func() {
		bucket.mu.RLock("ListObjects")
		defer bucket.mu.RUnlock()

		objectSnapshots = make([]*StoredObject, 0, len(bucket.Objects))
		for _, obj := range bucket.Objects {
			if strings.HasPrefix(obj.Key, prefix) {
				objectSnapshots = append(objectSnapshots, obj)
			}
		}
	}()

	// Process objects outside the bucket lock
	contents := b.processObjectSnapshots(objectSnapshots)

	sort.Slice(contents, func(i, j int) bool {
		return *contents[i].Key < *contents[j].Key
	})

	// Apply Marker using binary search for O(log n) seek instead of O(n) linear scan.
	marker := aws.ToString(input.Marker)
	if marker != "" {
		startIndex := sort.Search(len(contents), func(i int) bool {
			return *contents[i].Key > marker
		})
		if startIndex >= len(contents) {
			contents = nil
		} else {
			contents = contents[startIndex:]
		}
	}

	delimiter := aws.ToString(input.Delimiter)
	var cpList []types.CommonPrefix

	if delimiter != "" {
		contents, cpList = applyDelimiter(prefix, delimiter, contents)
	}

	maxKeys := int32(defaultMaxKeys)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}

	contents, cpList, isTruncated, nextMarker := b.truncateListResults(contents, cpList, maxKeys)

	return contents, cpList, isTruncated, nextMarker, maxKeys
}

func (b *InMemoryBackend) processObjectSnapshots(objectSnapshots []*StoredObject) []types.Object {
	contents := make([]types.Object, 0, len(objectSnapshots)) // #61: capacity hint
	for _, obj := range objectSnapshots {
		var latest *StoredObjectVersion
		func() {
			obj.mu.RLock("ListObjects")
			defer obj.mu.RUnlock()

			latestID := obj.LatestVersionID
			if latestID != "" {
				latest = obj.Versions[latestID]
			} else {
				// Fallback: scan for latest if not cached
				latest = findLatestVersion(obj.Versions)
			}
		}()

		if latest == nil || latest.Deleted {
			continue
		}

		var checksumAlgos []types.ChecksumAlgorithm
		if latest.ChecksumAlgorithm != "" {
			checksumAlgos = []types.ChecksumAlgorithm{latest.ChecksumAlgorithm}
		}

		sc := latest.StorageClass
		if sc == "" {
			sc = storageStandard
		}
		contents = append(contents, types.Object{
			Key:               aws.String(latest.Key),
			LastModified:      aws.Time(latest.LastModified),
			ETag:              aws.String(latest.ETag),
			Size:              aws.Int64(latest.Size),
			StorageClass:      types.ObjectStorageClass(sc),
			ChecksumAlgorithm: checksumAlgos,
			Owner: &types.Owner{
				ID:          aws.String(gopherstackName),
				DisplayName: aws.String(gopherstackName),
			},
		})
	}

	return contents
}

func (b *InMemoryBackend) ListObjects(
	_ context.Context,
	input *s3.ListObjectsInput,
) (*s3.ListObjectsOutput, error) {
	bucketName := *input.Bucket

	var bucket *StoredBucket
	var err error
	func() {
		b.mu.RLock("ListObjects")
		defer b.mu.RUnlock()

		bucket, err = b.getBucket(bucketName)
	}()

	if err != nil {
		return nil, err
	}

	contents, cpList, isTruncated, nextMarker, maxKeys := b.processListObjects(bucket, input)

	return &s3.ListObjectsOutput{
		Name:           input.Bucket,
		Prefix:         input.Prefix,
		Delimiter:      input.Delimiter,
		MaxKeys:        aws.Int32(maxKeys),
		Marker:         input.Marker,
		Contents:       contents,
		CommonPrefixes: cpList,
		IsTruncated:    aws.Bool(isTruncated),
		NextMarker:     aws.String(nextMarker),
	}, nil
}

func (b *InMemoryBackend) ListObjectsV2(
	ctx context.Context,
	input *s3.ListObjectsV2Input,
) (*s3.ListObjectsV2Output, error) {
	// Re-use ListObjects logic but handle V2 specific params
	marker := ""
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		marker = *input.ContinuationToken
	} else if input.StartAfter != nil && *input.StartAfter != "" {
		marker = *input.StartAfter
	}

	listOut, err := b.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:    input.Bucket,
		Prefix:    input.Prefix,
		MaxKeys:   input.MaxKeys,
		Delimiter: input.Delimiter,
		Marker:    aws.String(marker),
	})
	if err != nil {
		return nil, err
	}

	count64 := int64(len(listOut.Contents)) + int64(len(listOut.CommonPrefixes))
	count := int32(uint32(count64)) //nolint:gosec // intentional conversion for key count

	nextCont := ""
	if aws.ToBool(listOut.IsTruncated) {
		nextCont = aws.ToString(listOut.NextMarker)
	}

	return &s3.ListObjectsV2Output{
		Name:                  input.Bucket,
		Prefix:                input.Prefix,
		MaxKeys:               input.MaxKeys,
		Contents:              listOut.Contents,
		CommonPrefixes:        listOut.CommonPrefixes,
		KeyCount:              aws.Int32(count),
		IsTruncated:           listOut.IsTruncated,
		NextContinuationToken: aws.String(nextCont),
		ContinuationToken:     input.ContinuationToken,
		StartAfter:            input.StartAfter,
		Delimiter:             input.Delimiter,
	}, nil
}

// versionSnapshot holds the subset of StoredObjectVersion fields needed for
// listing. It is captured under the bucket lock and processed outside it.
type versionSnapshot struct {
	lastModified time.Time
	key          string
	versionID    string
	etag         string
	storageClass string
	size         int64
	isLatest     bool
	deleted      bool
}

func (b *InMemoryBackend) ListObjectVersions(
	_ context.Context,
	input *s3.ListObjectVersionsInput,
) (*s3.ListObjectVersionsOutput, error) {
	bucketName := *input.Bucket

	var bucket *StoredBucket
	var err error
	func() {
		b.mu.RLock("ListObjectVersions")
		defer b.mu.RUnlock()

		bucket, err = b.getBucket(bucketName)
	}()

	if err != nil {
		return nil, err
	}

	prefix := aws.ToString(input.Prefix)
	keyMarker := aws.ToString(input.KeyMarker)
	versionIDMarker := aws.ToString(input.VersionIdMarker)
	delimiter := aws.ToString(input.Delimiter)

	maxKeys := int32(defaultMaxKeys)
	if input.MaxKeys != nil && *input.MaxKeys > 0 {
		maxKeys = *input.MaxKeys
	}

	snapshots := b.snapshotVersions(bucket, prefix)

	// Sort: primary by key (ascending), secondary by LastModified (newest first).
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].key != snapshots[j].key {
			return snapshots[i].key < snapshots[j].key
		}

		return snapshots[i].lastModified.After(snapshots[j].lastModified)
	})

	snapshots = seekVersionMarker(snapshots, keyMarker, versionIDMarker)
	filteredSnapshots, commonPrefixes := applyVersionDelimiter(snapshots, prefix, delimiter)

	versions, deleteMarkers, isTruncated, nextKeyMarker, nextVersionIDMarker := buildVersionPage(
		filteredSnapshots,
		maxKeys,
	)

	var cpList []types.CommonPrefix
	for _, cp := range commonPrefixes {
		cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(cp)})
	}

	return &s3.ListObjectVersionsOutput{
		Name:                aws.String(bucketName),
		Prefix:              input.Prefix,
		KeyMarker:           input.KeyMarker,
		VersionIdMarker:     input.VersionIdMarker,
		MaxKeys:             aws.Int32(maxKeys),
		Delimiter:           input.Delimiter,
		IsTruncated:         aws.Bool(isTruncated),
		NextKeyMarker:       aws.String(nextKeyMarker),
		NextVersionIdMarker: aws.String(nextVersionIDMarker),
		Versions:            versions,
		DeleteMarkers:       deleteMarkers,
		CommonPrefixes:      cpList,
	}, nil
}

// snapshotVersions captures all versions from bucket.Objects that match prefix,
// under the bucket read lock.
func (b *InMemoryBackend) snapshotVersions(bucket *StoredBucket, prefix string) []versionSnapshot {
	var snapshots []versionSnapshot

	bucket.mu.RLock("ListObjectVersions")
	defer bucket.mu.RUnlock()

	for _, obj := range bucket.Objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		obj.mu.RLock("snapshotVersions-obj")

		for _, v := range obj.Versions {
			sc := v.StorageClass
			if sc == "" {
				sc = storageStandard
			}

			snapshots = append(snapshots, versionSnapshot{
				key:          v.Key,
				versionID:    v.VersionID,
				etag:         v.ETag,
				lastModified: v.LastModified,
				size:         v.Size,
				isLatest:     v.IsLatest,
				deleted:      v.Deleted,
				storageClass: sc,
			})
		}

		obj.mu.RUnlock()
	}

	return snapshots
}

// seekVersionMarker advances the snapshot slice past the (keyMarker, versionIDMarker) cursor.
func seekVersionMarker(
	snapshots []versionSnapshot,
	keyMarker, versionIDMarker string,
) []versionSnapshot {
	if keyMarker == "" {
		return snapshots
	}

	for i, s := range snapshots {
		if s.key > keyMarker {
			return snapshots[i:]
		}

		if s.key == keyMarker && versionIDMarker != "" && s.versionID == versionIDMarker {
			return snapshots[i+1:]
		}

		// Skip all versions of keyMarker when no versionIDMarker specified.
		if s.key == keyMarker && versionIDMarker == "" {
			continue
		}
	}

	return nil
}

// applyVersionDelimiter groups snapshot keys that share a common prefix
// (when delimiter is set) and returns the remaining non-grouped snapshots
// together with the sorted list of discovered common-prefix strings.
func applyVersionDelimiter(
	snapshots []versionSnapshot,
	prefix, delimiter string,
) ([]versionSnapshot, []string) {
	if delimiter == "" {
		return snapshots, nil
	}

	seenCommonPrefixes := make(map[string]struct{})
	var filtered []versionSnapshot
	var commonPrefixes []string

	for _, snap := range snapshots {
		rest := strings.TrimPrefix(snap.key, prefix)
		if idx := strings.Index(rest, delimiter); idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if _, seen := seenCommonPrefixes[cp]; !seen {
				seenCommonPrefixes[cp] = struct{}{}
				commonPrefixes = append(commonPrefixes, cp)
			}

			continue
		}

		filtered = append(filtered, snap)
	}

	return filtered, commonPrefixes
}

// buildVersionPage builds the Versions and DeleteMarkers page from snapshots,
// enforcing maxKeys. It returns the pagination flags for the next request.
func buildVersionPage(snapshots []versionSnapshot, maxKeys int32) (
	[]types.ObjectVersion,
	[]types.DeleteMarkerEntry,
	bool,
	string,
	string,
) {
	var versions []types.ObjectVersion
	var deleteMarkers []types.DeleteMarkerEntry
	count := int32(0)
	var lastKey, lastVersionID string

	for _, snap := range snapshots {
		if count >= maxKeys {
			// NextKeyMarker is the last key returned (follow-up uses key-marker=last).
			return versions, deleteMarkers, true, lastKey, lastVersionID
		}

		lastKey = snap.key
		lastVersionID = snap.versionID

		if snap.deleted {
			deleteMarkers = append(deleteMarkers, types.DeleteMarkerEntry{
				Key:          aws.String(snap.key),
				VersionId:    aws.String(snap.versionID),
				IsLatest:     aws.Bool(snap.isLatest),
				LastModified: aws.Time(snap.lastModified),
				Owner: &types.Owner{
					ID:          aws.String(gopherstackName),
					DisplayName: aws.String(gopherstackName),
				},
			})
		} else {
			versions = append(versions, types.ObjectVersion{
				Key:          aws.String(snap.key),
				VersionId:    aws.String(snap.versionID),
				IsLatest:     aws.Bool(snap.isLatest),
				LastModified: aws.Time(snap.lastModified),
				ETag:         aws.String(snap.etag),
				Size:         aws.Int64(snap.size),
				StorageClass: types.ObjectVersionStorageClass(snap.storageClass),
				Owner:        &types.Owner{ID: aws.String(gopherstackName), DisplayName: aws.String(gopherstackName)},
			})
		}

		count++
	}

	return versions, deleteMarkers, false, "", ""
}

func (b *InMemoryBackend) truncateListResults(
	contents []types.Object,
	cpList []types.CommonPrefix,
	maxKeys int32,
) ([]types.Object, []types.CommonPrefix, bool, string) {
	// AWS clamps MaxKeys to [0, 1000]; a zero value means return no objects.
	if maxKeys <= 0 {
		return nil, nil, len(contents)+len(cpList) > 0, ""
	}

	totalCount64 := int64(len(contents)) + int64(len(cpList))
	if totalCount64 <= int64(maxKeys) {
		return contents, cpList, false, ""
	}

	isTruncated := true
	var nextMarker string

	if int64(len(contents)) > int64(maxKeys) {
		nextMarker = aws.ToString(contents[maxKeys-1].Key)
		contents = contents[:maxKeys]
		cpList = nil
	} else {
		remaining := int64(maxKeys) - int64(len(contents))
		if remaining > 0 {
			// Some CommonPrefixes fit on this page; the marker is the last prefix
			// we return so the next page resumes after it.
			nextMarker = aws.ToString(cpList[remaining-1].Prefix)
			cpList = cpList[:remaining]
		} else {
			// The page is filled exactly by object keys with CommonPrefixes still
			// pending. Resume from the last returned key and defer the prefixes to
			// the next page, otherwise IsTruncated=true would carry an empty marker
			// and the client could never fetch the remaining prefixes.
			nextMarker = aws.ToString(contents[len(contents)-1].Key)
			cpList = nil
		}
	}

	return contents, cpList, isTruncated, nextMarker
}
