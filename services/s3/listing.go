package s3

import (
	"context"
	"encoding/base64"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// listObjectEntry is one lexicographically-ordered slot in a delimited
// listing: either a plain object (vi indexes the walk's versions) or a common-prefix group (vi == prefixEntry),
// never both. Keeping the two kinds in a single ordered slice (rather than
// two separately-truncated lists) is what lets truncateVersionEntries cut
// the page and compute NextMarker in the same order AWS actually returns
// results in.
type listObjectEntry struct {
	key string
	vi  int
}

const prefixEntry = -1

// listedVersion is the compact lock-free copy of a version that listings render.
type listedVersion struct {
	lastModified      time.Time
	restoreExpiry     time.Time
	key               string
	etag              string
	storageClass      string
	checksumAlgorithm types.ChecksumAlgorithm
	size              int64
}

// afterMarkerPredicate returns whether a key comes strictly after marker on
// a delimited listing. A plain object-key marker (the common case) only
// needs key > marker: resume right after it. But NextMarker can also be a
// CommonPrefix string -- and every CommonPrefix this package emits ends
// with delimiter (walkDelimitedLocked's `rest[:idx+len(delimiter)]`
// always keeps the delimiter) -- and a CommonPrefix marker means "the whole
// b/* subtree was already summarized and returned as one entry, not just
// keys up to some point." key > "b/" is true for every "b/..." key, so a
// plain > comparison resumes inside the very subtree the prior page already
// covered, re-emitting the same CommonPrefix on the next page (duplicated,
// not dropped -- the mirror-image bug from truncating without checking a
// prefix boundary). Excluding any key sharing that prefix fixes it, and is
// safe to apply only when marker itself ends with delimiter: an ordinary
// object-key marker not ending in delimiter must not use HasPrefix (e.g.
// marker "c" would wrongly exclude the unrelated later key "c2").
func afterMarkerPredicate(marker, delimiter string) func(key string) bool {
	if delimiter != "" && strings.HasSuffix(marker, delimiter) {
		return func(key string) bool {
			return key > marker && !strings.HasPrefix(key, marker)
		}
	}

	return func(key string) bool {
		return key > marker
	}
}

// seekPrefixMatchesLocked returns the objects whose key has prefix and comes
// at or after the marker/start-after/continuation-token cursor (afterMarker),
// in ascending key order. It binary-searches bucket.keyIndex straight to the
// first candidate instead of scanning every key in the bucket, then walks
// forward only while keys still have the prefix (a contiguous range in
// sorted order). limit caps the number of objects returned (-1 for
// unbounded); a delimited listing must pass -1 since CommonPrefix grouping
// needs every candidate up front. Caller must hold bucket.mu.
func seekPrefixMatchesLocked(
	bucket *StoredBucket,
	prefix string,
	afterMarker func(string) bool,
	limit int,
) []*StoredObject {
	keys := bucket.keyIndex
	start := sort.Search(len(keys), func(i int) bool {
		return keys[i] >= prefix && afterMarker(keys[i])
	})

	var matched []*StoredObject

	for i := start; i < len(keys); i++ {
		key := keys[i]
		if !strings.HasPrefix(key, prefix) {
			break
		}

		if obj, ok := bucket.Objects[key]; ok {
			matched = append(matched, obj)
		}

		if limit >= 0 && len(matched) >= limit {
			break
		}
	}

	return matched
}

func (b *InMemoryBackend) processListObjects(
	bucket *StoredBucket,
	input *s3.ListObjectsInput,
) ([]types.Object, []types.CommonPrefix, bool, string, int32) {
	prefix := aws.ToString(input.Prefix)
	delimiter := aws.ToString(input.Delimiter)
	marker := aws.ToString(input.Marker)

	maxKeys := int32(defaultMaxKeys)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}

	afterMarker := afterMarkerPredicate(marker, delimiter)

	if delimiter != "" {
		return b.processDelimitedListing(bucket, prefix, delimiter, afterMarker, maxKeys)
	}

	// Only maxKeys+1 objects are needed (one extra detects truncation).
	limit := 1
	if maxKeys > 0 {
		limit = int(maxKeys) + 1
	}

	var objectSnapshots []*StoredObject
	func() {
		bucket.mu.RLock("ListObjects")
		defer bucket.mu.RUnlock()

		objectSnapshots = seekPrefixMatchesLocked(bucket, prefix, afterMarker, limit)
	}()

	// Truncate before resolving versions so a page only pays the per-object
	// resolution cost for the keys actually returned.
	var isTruncated bool
	var nextMarker string
	if maxKeys <= 0 {
		isTruncated = len(objectSnapshots) > 0
		objectSnapshots = nil
	} else if int64(len(objectSnapshots)) > int64(maxKeys) {
		isTruncated = true
		nextMarker = objectSnapshots[maxKeys-1].Key
		objectSnapshots = objectSnapshots[:maxKeys]
	}

	versions := b.snapshotLatestVersions(objectSnapshots)

	return objectsFromVersions(versions), nil, isTruncated, nextMarker, maxKeys
}

func (b *InMemoryBackend) processDelimitedListing(
	bucket *StoredBucket,
	prefix, delimiter string,
	afterMarker func(string) bool,
	maxKeys int32,
) ([]types.Object, []types.CommonPrefix, bool, string, int32) {
	// One entry past maxKeys is enough to detect truncation.
	limit := 1
	if maxKeys > 0 {
		limit = int(maxKeys) + 1
	}

	var walk delimitedWalk
	func() {
		bucket.mu.RLock("ListObjects")
		defer bucket.mu.RUnlock()

		walk = walkDelimitedLocked(bucket, prefix, delimiter, afterMarker, limit)
	}()

	versions, cpList, isTruncated, nextMarker := truncateVersionEntries(walk, maxKeys)

	return objectsFromVersions(versions), cpList, isTruncated, nextMarker, maxKeys
}

// snapshotLatestVersions returns lock-free copies of each object's live latest version.
func (b *InMemoryBackend) snapshotLatestVersions(objectSnapshots []*StoredObject) []listedVersion {
	versions := make([]listedVersion, 0, len(objectSnapshots))
	for _, obj := range objectSnapshots {
		var v listedVersion
		if latestLiveSnapshot(obj, &v) {
			versions = append(versions, v)
		}
	}

	return versions
}

// latestLiveSnapshot copies obj's latest live version into out under obj's lock, reporting
// false for an absent version or delete marker; the copy guards against the janitor.
func latestLiveSnapshot(obj *StoredObject, out *listedVersion) bool {
	obj.mu.RLock("ListObjects")
	defer obj.mu.RUnlock()

	var latest *StoredObjectVersion
	if obj.LatestVersionID != "" {
		latest = obj.Versions[obj.LatestVersionID]
	} else {
		latest = findLatestVersion(obj.Versions)
	}

	if latest == nil || latest.Deleted {
		return false
	}

	*out = listedVersion{
		lastModified:      latest.LastModified,
		key:               latest.Key,
		etag:              latest.ETag,
		storageClass:      latest.StorageClass,
		checksumAlgorithm: latest.ChecksumAlgorithm,
		restoreExpiry:     latest.RestoreExpiry,
		size:              latest.Size,
	}

	return true
}

// delimitedWalk is the ordered entries of a delimited listing plus the object versions they index.
type delimitedWalk struct {
	entries  []listObjectEntry
	versions []listedVersion
}

// walkDelimitedLocked builds up to limit entries, skipping each CommonPrefix's key
// range. Caller holds bucket.mu (read).
func walkDelimitedLocked(
	bucket *StoredBucket,
	prefix, delimiter string,
	afterMarker func(string) bool,
	limit int,
) delimitedWalk {
	keys := bucket.keyIndex
	i := sort.Search(len(keys), func(i int) bool {
		return keys[i] >= prefix && afterMarker(keys[i])
	})

	entries := make([]listObjectEntry, 0, min(limit, len(keys)-i))
	var versions []listedVersion

	for i < len(keys) && len(entries) < limit {
		key := keys[i]
		if !strings.HasPrefix(key, prefix) {
			break
		}

		i++

		obj, ok := bucket.Objects[key]
		if !ok {
			continue
		}

		var snap listedVersion
		if !latestLiveSnapshot(obj, &snap) {
			continue
		}

		rest := key[len(prefix):]
		idx := strings.Index(rest, delimiter)

		if idx == -1 {
			versions = append(versions, snap)
			entries = append(entries, listObjectEntry{key: snap.key, vi: len(versions) - 1})

			continue
		}

		cp := prefix + rest[:idx+len(delimiter)]
		entries = append(entries, listObjectEntry{key: cp, vi: prefixEntry})

		from := i
		i = from + sort.Search(len(keys)-from, func(k int) bool {
			return !strings.HasPrefix(keys[from+k], cp)
		})
	}

	return delimitedWalk{entries: entries, versions: versions}
}

// listedObject holds one rendered object's pointees so a page needs one slab allocation, not one per field.
type listedObject struct {
	lastModified time.Time
	restoreAt    time.Time
	restore      types.RestoreStatus
	owner        types.Owner
	key          string
	etag         string
	ownerName    string
	algos        [1]types.ChecksumAlgorithm
	size         int64
	restoreFlag  bool
}

func fillObject(slot *listedObject, latest *listedVersion) types.Object {
	slot.key = latest.key
	slot.etag = latest.etag
	slot.size = latest.size
	slot.lastModified = latest.lastModified
	slot.ownerName = gopherstackName
	slot.owner = types.Owner{ID: &slot.ownerName, DisplayName: &slot.ownerName}

	var checksumAlgos []types.ChecksumAlgorithm
	if latest.checksumAlgorithm != "" {
		slot.algos[0] = latest.checksumAlgorithm
		checksumAlgos = slot.algos[:]
	}

	sc := latest.storageClass
	if sc == "" {
		sc = storageStandard
	}

	var restore *types.RestoreStatus
	if !latest.restoreExpiry.IsZero() {
		slot.restoreFlag = false
		slot.restoreAt = latest.restoreExpiry
		slot.restore = types.RestoreStatus{IsRestoreInProgress: &slot.restoreFlag, RestoreExpiryDate: &slot.restoreAt}
		restore = &slot.restore
	}

	return types.Object{
		RestoreStatus:     restore,
		Key:               &slot.key,
		LastModified:      &slot.lastModified,
		ETag:              &slot.etag,
		Size:              &slot.size,
		StorageClass:      types.ObjectStorageClass(sc),
		ChecksumAlgorithm: checksumAlgos,
		Owner:             &slot.owner,
	}
}

func objectsFromVersions(versions []listedVersion) []types.Object {
	slab := make([]listedObject, len(versions))
	contents := make([]types.Object, len(versions))

	for i := range versions {
		contents[i] = fillObject(&slab[i], &versions[i])
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
	delim := aws.ToString(input.Delimiter)
	if delim != "" && delim != "/" && b.IsDirectoryBucket(aws.ToString(input.Bucket)) {
		return nil, ErrDirectoryBucketDelimiter
	}

	// Re-use ListObjects logic but handle V2 specific params
	marker := ""
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		decoded, ok := decodeContinuationToken(*input.ContinuationToken)
		if !ok {
			return nil, ErrInvalidContinuationToken
		}

		marker = decoded
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
		nextCont = encodeContinuationToken(aws.ToString(listOut.NextMarker))
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
	lastModified      time.Time
	restoreExpiry     time.Time
	key               string
	versionID         string
	etag              string
	storageClass      string
	checksumAlgorithm string
	size              int64
	isLatest          bool
	deleted           bool
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

	snapshots = seekVersionMarker(snapshots, keyMarker, versionIDMarker, delimiter)
	entries := buildVersionEntries(snapshots, prefix, delimiter)

	versions, deleteMarkers, cpList, isTruncated, nextKeyMarker, nextVersionIDMarker := buildVersionPage(
		entries,
		maxKeys,
	)

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

// snapshotVersions captures all versions from bucket.Objects that match
// prefix, under the bucket read lock. It binary-searches bucket.keyIndex to
// the first candidate key instead of scanning every key in the bucket.
func (b *InMemoryBackend) snapshotVersions(bucket *StoredBucket, prefix string) []versionSnapshot {
	bucket.mu.RLock("ListObjectVersions")
	defer bucket.mu.RUnlock()

	keys := bucket.keyIndex
	start := sort.Search(len(keys), func(i int) bool {
		return keys[i] >= prefix
	})

	snapshots := make([]versionSnapshot, 0, len(keys)-start)

	for i := start; i < len(keys); i++ {
		key := keys[i]
		if !strings.HasPrefix(key, prefix) {
			break
		}

		obj, ok := bucket.Objects[key]
		if !ok {
			continue
		}

		obj.mu.RLock("snapshotVersions-obj")

		for _, v := range obj.Versions {
			sc := v.StorageClass
			if sc == "" {
				sc = storageStandard
			}

			snapshots = append(snapshots, versionSnapshot{
				key:               v.Key,
				versionID:         v.VersionID,
				etag:              v.ETag,
				lastModified:      v.LastModified,
				size:              v.Size,
				isLatest:          v.IsLatest,
				deleted:           v.Deleted,
				storageClass:      sc,
				checksumAlgorithm: string(v.ChecksumAlgorithm),
				restoreExpiry:     v.RestoreExpiry,
			})
		}

		obj.mu.RUnlock()
	}

	return snapshots
}

// seekVersionMarker advances the snapshot slice past the (keyMarker,
// versionIDMarker) cursor. When keyMarker itself is a CommonPrefix boundary
// (it ends with delimiter -- every CommonPrefix buildVersionEntries emits
// does, by construction), every version whose key falls under that prefix
// must also be skipped: it was already summarized and returned as that one
// CommonPrefix entry, not individually. A plain `key > keyMarker` alone
// would resume inside that same prefix's key range and re-emit the
// CommonPrefix on the next page (see
// TestListObjectVersions_DelimiterTruncation_BoundaryWalk).
func seekVersionMarker(
	snapshots []versionSnapshot,
	keyMarker, versionIDMarker, delimiter string,
) []versionSnapshot {
	if keyMarker == "" {
		return snapshots
	}

	skipWholePrefix := delimiter != "" && strings.HasSuffix(keyMarker, delimiter)

	for i, s := range snapshots {
		if skipWholePrefix {
			if s.key > keyMarker && !strings.HasPrefix(s.key, keyMarker) {
				return snapshots[i:]
			}

			continue
		}

		if s.key > keyMarker {
			return snapshots[i:]
		}

		if s.key == keyMarker && versionIDMarker != "" && s.versionID == versionIDMarker {
			return snapshots[i+1:]
		}

		// Skip all versions of keyMarker when no versionIDMarker specified.
	}

	return nil
}

// versionListEntry is one lexicographically-ordered slot in a delimited
// ListObjectVersions listing: either one version/delete-marker snapshot or
// one common-prefix group, never both. See listObjectEntry (ListObjects'
// analog) for why this must be a single ordered sequence rather than two
// separately-truncated lists: cutting them independently -- or, as this
// function's predecessor did, never truncating CommonPrefixes against
// maxKeys at all -- can drop or duplicate an entire common-prefix group
// across a page boundary.
type versionListEntry struct {
	snap   *versionSnapshot
	prefix string
}

// buildVersionEntries groups snapshots that share a common prefix (when
// delimiter is set) into ordered versionListEntry values, preserving the
// input's sorted order.
func buildVersionEntries(snapshots []versionSnapshot, prefix, delimiter string) []versionListEntry {
	entries := make([]versionListEntry, 0, len(snapshots))

	if delimiter == "" {
		for i := range snapshots {
			entries = append(entries, versionListEntry{snap: &snapshots[i]})
		}

		return entries
	}

	var lastCP string
	haveCP := false

	for i := range snapshots {
		snap := &snapshots[i]
		rest := strings.TrimPrefix(snap.key, prefix)

		if idx := strings.Index(rest, delimiter); idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if !haveCP || cp != lastCP {
				lastCP = cp
				haveCP = true
				entries = append(entries, versionListEntry{prefix: cp})
			}

			continue
		}

		entries = append(entries, versionListEntry{snap: snap})
	}

	return entries
}

// buildVersionPage cuts entries at maxKeys (already in true lexicographic
// order) and splits the retained prefix into the Versions/DeleteMarkers/
// CommonPrefixes wire lists, deriving NextKeyMarker/NextVersionIdMarker from
// the last entry actually included, whichever kind it is.
func buildVersionPage(entries []versionListEntry, maxKeys int32) (
	[]types.ObjectVersion,
	[]types.DeleteMarkerEntry,
	[]types.CommonPrefix,
	bool,
	string,
	string,
) {
	if maxKeys <= 0 {
		return nil, nil, nil, len(entries) > 0, "", ""
	}

	isTruncated := int64(len(entries)) > int64(maxKeys)
	page := entries

	var nextKeyMarker, nextVersionIDMarker string

	if isTruncated {
		page = entries[:maxKeys]

		last := page[len(page)-1]
		if last.snap != nil {
			nextKeyMarker = last.snap.key
			nextVersionIDMarker = last.snap.versionID
		} else {
			nextKeyMarker = last.prefix
		}
	}

	var versions []types.ObjectVersion
	var deleteMarkers []types.DeleteMarkerEntry
	var cpList []types.CommonPrefix

	for _, e := range page {
		if e.snap == nil {
			cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(e.prefix)})

			continue
		}

		snap := e.snap

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

			continue
		}

		var checksumAlgos []types.ChecksumAlgorithm
		if snap.checksumAlgorithm != "" {
			checksumAlgos = []types.ChecksumAlgorithm{types.ChecksumAlgorithm(snap.checksumAlgorithm)}
		}

		owner := types.Owner{ID: aws.String(gopherstackName), DisplayName: aws.String(gopherstackName)}

		var restore *types.RestoreStatus
		if !snap.restoreExpiry.IsZero() {
			restore = &types.RestoreStatus{
				IsRestoreInProgress: aws.Bool(false),
				RestoreExpiryDate:   aws.Time(snap.restoreExpiry),
			}
		}

		versions = append(versions, types.ObjectVersion{
			RestoreStatus:     restore,
			Key:               aws.String(snap.key),
			VersionId:         aws.String(snap.versionID),
			IsLatest:          aws.Bool(snap.isLatest),
			LastModified:      aws.Time(snap.lastModified),
			ETag:              aws.String(snap.etag),
			Size:              aws.Int64(snap.size),
			StorageClass:      types.ObjectVersionStorageClass(snap.storageClass),
			ChecksumAlgorithm: checksumAlgos,
			Owner:             &owner,
		})
	}

	return versions, deleteMarkers, cpList, isTruncated, nextKeyMarker, nextVersionIDMarker
}

// truncateVersionEntries cuts entries (already in true lexicographic key
// order -- objects and common-prefix groups interleaved, not two separately
// truncated lists) at maxKeys, splitting the retained prefix back into
// Contents/CommonPrefixes wire lists and deriving NextMarker from the last
// entry actually included, whichever kind it is.
//
// Cutting the two kinds independently (an earlier version of this function
// took every object first and only padded with CommonPrefixes if page room
// remained) can silently drop an entire common-prefix group: if the flat
// object keys before and after it both fit within maxKeys, the object-only
// cut takes both of them, sets NextMarker past the CommonPrefix's key range,
// and every future page's `key > marker` seek then skips that prefix
// forever -- not merely reordered, permanently missing from the listing.
func truncateVersionEntries(walk delimitedWalk, maxKeys int32) (
	[]listedVersion, []types.CommonPrefix, bool, string,
) {
	entries := walk.entries

	// AWS clamps MaxKeys to [0, 1000]; a zero value means return no objects.
	if maxKeys <= 0 {
		return nil, nil, len(entries) > 0, ""
	}

	isTruncated := int64(len(entries)) > int64(maxKeys)
	page := entries

	var nextMarker string

	if isTruncated {
		page = entries[:maxKeys]
		nextMarker = page[len(page)-1].key
	}

	versions := make([]listedVersion, 0, len(page))
	var cpList []types.CommonPrefix

	for _, e := range page {
		if e.vi != prefixEntry {
			versions = append(versions, walk.versions[e.vi])
		} else {
			cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(e.key)})
		}
	}

	return versions, cpList, isTruncated, nextMarker
}

const continuationTokenPrefix = "1"

func encodeContinuationToken(key string) string {
	return continuationTokenPrefix + base64.URLEncoding.EncodeToString([]byte(key))
}

func decodeContinuationToken(token string) (string, bool) {
	rest, ok := strings.CutPrefix(token, continuationTokenPrefix)
	if !ok {
		return "", false
	}

	raw, err := base64.URLEncoding.DecodeString(rest)
	if err != nil {
		return "", false
	}

	return string(raw), true
}
