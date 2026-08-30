package s3

import (
	"cmp"
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// listObjectEntry is one lexicographically-ordered slot in a delimited
// listing: either a plain object (version set) or a common-prefix group,
// never both. Keeping the two kinds in a single ordered slice (rather than
// two separately-truncated lists) is what lets truncateVersionEntries cut
// the page and compute NextMarker in the same order AWS actually returns
// results in.
type listObjectEntry struct {
	version *StoredObjectVersion
	prefix  string
}

// key returns the entry's sort/marker key: the object's Key, or the
// common-prefix string.
func (e listObjectEntry) key() string {
	if e.version != nil {
		return e.version.Key
	}

	return e.prefix
}

// afterMarkerPredicate returns whether a key comes strictly after marker on
// a delimited listing. A plain object-key marker (the common case) only
// needs key > marker: resume right after it. But NextMarker can also be a
// CommonPrefix string -- and every CommonPrefix this package emits ends
// with delimiter (applyDelimiterToVersions's `rest[:idx+len(delimiter)]`
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

func applyDelimiterToVersions(
	prefix, delimiter string,
	versions []*StoredObjectVersion,
) []listObjectEntry {
	entries := make([]listObjectEntry, 0, len(versions))
	var lastCP string
	haveCP := false

	for _, v := range versions {
		rest := v.Key[len(prefix):]
		idx := strings.Index(rest, delimiter)

		if idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if !haveCP || cp != lastCP {
				lastCP = cp
				haveCP = true
				entries = append(entries, listObjectEntry{prefix: cp})
			}
		} else {
			entries = append(entries, listObjectEntry{version: v})
		}
	}

	return entries
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

	slices.SortFunc(objectSnapshots, func(a, b *StoredObject) int {
		return cmp.Compare(a.Key, b.Key)
	})

	delimiter := aws.ToString(input.Delimiter)

	// Apply Marker using binary search for O(log n) seek instead of O(n) linear scan.
	marker := aws.ToString(input.Marker)
	if marker != "" {
		afterMarker := afterMarkerPredicate(marker, delimiter)
		startIndex := sort.Search(len(objectSnapshots), func(i int) bool {
			return afterMarker(objectSnapshots[i].Key)
		})
		if startIndex >= len(objectSnapshots) {
			objectSnapshots = nil
		} else {
			objectSnapshots = objectSnapshots[startIndex:]
		}
	}

	maxKeys := int32(defaultMaxKeys)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}

	// No delimiter: CommonPrefixes is always empty, so truncation is a plain
	// slice cut on the already-sorted, marker-seeked object list. Truncate
	// BEFORE resolving versions so a page request against a huge bucket
	// only pays the per-object version resolution cost for the keys actually returned.
	if delimiter == "" {
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

	// Delimiter grouping needs every matching key up front to compute
	// CommonPrefixes correctly. We filter and truncate the lightweight version
	// pointers first, so objectsFromVersions only allocates wire structs for the
	// elements actually returned on the page.
	versions := b.snapshotLatestVersions(objectSnapshots)
	entries := applyDelimiterToVersions(prefix, delimiter, versions)
	truncatedVersions, cpList, isTruncated, nextMarker := truncateVersionEntries(entries, maxKeys)

	return objectsFromVersions(truncatedVersions), cpList, isTruncated, nextMarker, maxKeys
}

// snapshotLatestVersions resolves each object's current (non-deleted) latest
// version under its own lock. It returns bare *StoredObjectVersion pointers
// rather than the wire-shaped types.Object so callers can sort/seek/truncate
// cheaply before paying for the per-object response allocation.
func (b *InMemoryBackend) snapshotLatestVersions(objectSnapshots []*StoredObject) []*StoredObjectVersion {
	versions := make([]*StoredObjectVersion, 0, len(objectSnapshots))
	for _, obj := range objectSnapshots {
		var snap *StoredObjectVersion
		func() {
			obj.mu.RLock("ListObjects")
			defer obj.mu.RUnlock()

			var latest *StoredObjectVersion

			latestID := obj.LatestVersionID
			if latestID != "" {
				latest = obj.Versions[latestID]
			} else {
				// Fallback: scan for latest if not cached
				latest = findLatestVersion(obj.Versions)
			}

			if latest == nil || latest.Deleted {
				return
			}

			// objectFromVersion reads StorageClass (and other scalar fields)
			// from the returned pointer well after this RUnlock -- the
			// lifecycle janitor's applyStorageClassTransitions mutates that
			// same live *StoredObjectVersion under obj.mu.Lock, so the
			// caller must work from a snapshot, not the live pointer (see
			// TestListObjects_RacesWithStorageClassTransition).
			v := *latest
			snap = &v
		}()

		if snap == nil {
			continue
		}

		versions = append(versions, snap)
	}

	return versions
}

func objectFromVersion(latest *StoredObjectVersion) types.Object {
	var checksumAlgos []types.ChecksumAlgorithm
	if latest.ChecksumAlgorithm != "" {
		checksumAlgos = []types.ChecksumAlgorithm{latest.ChecksumAlgorithm}
	}

	sc := latest.StorageClass
	if sc == "" {
		sc = storageStandard
	}

	return types.Object{
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
	}
}

func objectsFromVersions(versions []*StoredObjectVersion) []types.Object {
	contents := make([]types.Object, 0, len(versions))
	for _, v := range versions {
		contents = append(contents, objectFromVersion(v))
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
	lastModified      time.Time
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

// snapshotVersions captures all versions from bucket.Objects that match prefix,
// under the bucket read lock.
func (b *InMemoryBackend) snapshotVersions(bucket *StoredBucket, prefix string) []versionSnapshot {
	bucket.mu.RLock("ListObjectVersions")
	defer bucket.mu.RUnlock()

	snapshots := make([]versionSnapshot, 0, len(bucket.Objects))

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
				key:               v.Key,
				versionID:         v.VersionID,
				etag:              v.ETag,
				lastModified:      v.LastModified,
				size:              v.Size,
				isLatest:          v.IsLatest,
				deleted:           v.Deleted,
				storageClass:      sc,
				checksumAlgorithm: string(v.ChecksumAlgorithm),
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
		versions = append(versions, types.ObjectVersion{
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
func truncateVersionEntries(entries []listObjectEntry, maxKeys int32) (
	[]*StoredObjectVersion, []types.CommonPrefix, bool, string,
) {
	// AWS clamps MaxKeys to [0, 1000]; a zero value means return no objects.
	if maxKeys <= 0 {
		return nil, nil, len(entries) > 0, ""
	}

	isTruncated := int64(len(entries)) > int64(maxKeys)
	page := entries

	var nextMarker string

	if isTruncated {
		page = entries[:maxKeys]
		nextMarker = page[len(page)-1].key()
	}

	versions := make([]*StoredObjectVersion, 0, len(page))
	var cpList []types.CommonPrefix

	for _, e := range page {
		if e.version != nil {
			versions = append(versions, e.version)
		} else {
			cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(e.prefix)})
		}
	}

	return versions, cpList, isTruncated, nextMarker
}
