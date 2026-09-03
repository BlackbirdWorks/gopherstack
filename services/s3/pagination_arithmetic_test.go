package s3_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// walkListObjects drains ListObjects to completion with the given delimiter
// and maxKeys, returning every Content key and CommonPrefix seen across
// every page, in the order returned.
func walkListObjects(
	t *testing.T, backend *s3.InMemoryBackend, bucket, delimiter string, maxKeys int32, maxPages int,
) []string {
	t.Helper()

	var (
		got    []string
		marker string
	)

	for range maxPages + 1 {
		out, err := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
			Bucket:    aws.String(bucket),
			Delimiter: aws.String(delimiter),
			MaxKeys:   aws.Int32(maxKeys),
			Marker:    aws.String(marker),
		})
		require.NoError(t, err)

		for _, obj := range out.Contents {
			got = append(got, aws.ToString(obj.Key))
		}

		for _, cp := range out.CommonPrefixes {
			got = append(got, aws.ToString(cp.Prefix))
		}

		if !aws.ToBool(out.IsTruncated) {
			return got
		}

		next := aws.ToString(out.NextMarker)
		require.NotEmpty(t, next, "IsTruncated=true but NextMarker is empty")
		marker = next
	}

	t.Fatalf("ListObjects pagination did not terminate within %d pages (possible infinite loop)", maxPages)

	return nil
}

// TestListObjects_DelimiterTruncation_BoundaryWalk prices a common S3 key
// layout -- a flat key, a "folder" of keys grouped by delimiter, and another
// flat key that sorts after the folder ("a", "b/x", "c") -- with MaxKeys
// small enough to split the listing across two requests.
//
// ListObjects truncates Contents and CommonPrefixes as two independently-cut
// lists (services/s3/listing.go's truncateVersionResults): it fills the page
// from `versions` (non-grouped keys) first and only pads with CommonPrefixes
// if room remains, instead of cutting a single list interleaved in true
// lexicographic key order. With maxKeys=2 here, "a" and "c" both fit in
// `versions` before any CommonPrefix budget is considered, so page 1 returns
// {a, c} with NextMarker="c" -- skipping over "b/" (which sorts between them)
// entirely. Because Marker seeking on the next call is `key > marker`, a
// NextMarker of "c" also means every future call skips every key under "b/"
// permanently: the "b/" CommonPrefix is dropped from the whole listing, not
// just reordered.
func TestListObjects_DelimiterTruncation_BoundaryWalk(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "delim-trunc")
	mustPutObject(t, backend, "delim-trunc", "a", []byte("x"))
	mustPutObject(t, backend, "delim-trunc", "b/x", []byte("x"))
	mustPutObject(t, backend, "delim-trunc", "c", []byte("x"))

	got := walkListObjects(t, backend, "delim-trunc", "/", 2, 10)

	assert.Equal(t, []string{"a", "b/", "c"}, got,
		"a full walk across pages must reproduce every key/prefix in order, nothing dropped or duplicated")
}

// TestListObjects_DelimiterTruncation_LargerBoundaryWalk is the same shape
// as above but with several common-prefix groups interleaved among flat
// keys, at a page size that does not evenly divide the collection --
// exercising the general "concatenate every page, reproduce the collection
// exactly" check for the delimiter-truncation path.
func TestListObjects_DelimiterTruncation_LargerBoundaryWalk(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "delim-trunc-large")

	keys := []string{
		"a", "b/1", "b/2", "c", "d/1", "d/2", "d/3", "e", "f/1", "g",
	}
	for _, k := range keys {
		mustPutObject(t, backend, "delim-trunc-large", k, []byte("x"))
	}

	want := []string{"a", "b/", "c", "d/", "e", "f/", "g"}

	got := walkListObjects(t, backend, "delim-trunc-large", "/", 2, 10)

	assert.Equal(t, want, got,
		"a full walk across pages must reproduce every key/prefix in order, nothing dropped or duplicated")
}

// walkListObjectVersions drains ListObjectVersions to completion, returning
// every Version key and CommonPrefix seen across every page, in order.
func walkListObjectVersions(
	t *testing.T, backend *s3.InMemoryBackend, bucket, delimiter string, maxKeys int32, maxPages int,
) []string {
	t.Helper()

	var (
		got                        []string
		keyMarker, versionIDMarker string
	)

	for range maxPages + 1 {
		out, err := backend.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucket),
			Delimiter:       aws.String(delimiter),
			MaxKeys:         aws.Int32(maxKeys),
			KeyMarker:       aws.String(keyMarker),
			VersionIdMarker: aws.String(versionIDMarker),
		})
		require.NoError(t, err)

		for _, v := range out.Versions {
			got = append(got, aws.ToString(v.Key))
		}

		for _, cp := range out.CommonPrefixes {
			got = append(got, aws.ToString(cp.Prefix))
		}

		if !aws.ToBool(out.IsTruncated) {
			return got
		}

		keyMarker = aws.ToString(out.NextKeyMarker)
		versionIDMarker = aws.ToString(out.NextVersionIdMarker)
		require.NotEmpty(t, keyMarker, "IsTruncated=true but NextKeyMarker is empty")
	}

	t.Fatalf("ListObjectVersions pagination did not terminate within %d pages (possible infinite loop)", maxPages)

	return nil
}

// TestListObjectVersions_DelimiterTruncation_BoundaryWalk is the
// ListObjectVersions analog of TestListObjects_DelimiterTruncation_BoundaryWalk:
// CommonPrefixes here (services/s3/listing.go's applyVersionDelimiter +
// buildVersionPage) are computed as a wholly separate list from the
// maxKeys-truncated version snapshots, and are never truncated or counted
// toward maxKeys at all, while NextKeyMarker is derived only from the
// truncated version list -- ignoring where a CommonPrefix falls in true key
// order.
func TestListObjectVersions_DelimiterTruncation_BoundaryWalk(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ver-delim-trunc")
	mustPutObject(t, backend, "ver-delim-trunc", "a", []byte("x"))
	mustPutObject(t, backend, "ver-delim-trunc", "b/x", []byte("x"))
	mustPutObject(t, backend, "ver-delim-trunc", "c", []byte("x"))

	got := walkListObjectVersions(t, backend, "ver-delim-trunc", "/", 2, 10)

	assert.Equal(t, []string{"a", "b/", "c"}, got,
		"a full walk across pages must reproduce every key/prefix in order, nothing dropped or duplicated")
}

// TestListObjectVersions_DelimiterTruncation_LargerBoundaryWalk exercises
// the same fix with several groups and a page size that does not evenly
// divide the collection.
func TestListObjectVersions_DelimiterTruncation_LargerBoundaryWalk(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ver-delim-trunc-large")

	keys := []string{
		"a", "b/1", "b/2", "c", "d/1", "d/2", "d/3", "e", "f/1", "g",
	}
	for _, k := range keys {
		mustPutObject(t, backend, "ver-delim-trunc-large", k, []byte("x"))
	}

	want := []string{"a", "b/", "c", "d/", "e", "f/", "g"}

	got := walkListObjectVersions(t, backend, "ver-delim-trunc-large", "/", 2, 10)

	assert.Equal(t, want, got,
		"a full walk across pages must reproduce every key/prefix in order, nothing dropped or duplicated")
}

// walkListMultipartUploads drains ListMultipartUploads to completion,
// returning every Key seen across every page, in order.
func walkListMultipartUploads(
	t *testing.T, backend *s3.InMemoryBackend, bucket string, maxUploads int32, maxPages int,
) []string {
	t.Helper()

	var (
		got                       []string
		keyMarker, uploadIDMarker string
	)

	for range maxPages + 1 {
		out, err := backend.ListMultipartUploads(t.Context(), &sdk_s3.ListMultipartUploadsInput{
			Bucket:         aws.String(bucket),
			MaxUploads:     aws.Int32(maxUploads),
			KeyMarker:      aws.String(keyMarker),
			UploadIdMarker: aws.String(uploadIDMarker),
		})
		require.NoError(t, err)

		for _, u := range out.Uploads {
			got = append(got, aws.ToString(u.Key))
		}

		if !aws.ToBool(out.IsTruncated) {
			return got
		}

		keyMarker = aws.ToString(out.NextKeyMarker)
		uploadIDMarker = aws.ToString(out.NextUploadIdMarker)
		require.NotEmpty(t, keyMarker, "IsTruncated=true but NextKeyMarker is empty")
	}

	t.Fatalf("ListMultipartUploads pagination did not terminate within %d pages (possible infinite loop)", maxPages)

	return nil
}

// TestListMultipartUploads_BoundaryWalk_NoDelimiter proves ListMultipartUploads
// no longer drops the one upload straddling every page boundary.
// truncateUploads (services/s3/multipart.go) encoded NextKeyMarker/
// NextUploadIdMarker from `uploads[maxUploads]` -- the first upload NOT
// returned on this page, i.e. the token names the first item of the next
// page -- while seekMultipartMarker's decoder resumes AFTER the item
// matching the marker (`uploads[i+1:]`). Naming the next page's first item
// and then skipping past the matched item drops that exact upload on every
// truncation boundary: Class D, and it needs no delimiter, deletion, or
// tampering to fire -- a plain walk over more uploads than one page holds is
// enough.
func TestListMultipartUploads_BoundaryWalk_NoDelimiter(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mpu-boundary")

	want := make([]string, 0, 23)

	for i := range 23 {
		key := fmt.Sprintf("upload-%02d", i)
		_, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
			Bucket: aws.String("mpu-boundary"),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		want = append(want, key)
	}

	got := walkListMultipartUploads(t, backend, "mpu-boundary", 5, 20)

	assert.Equal(t, want, got,
		"a full walk across pages must reproduce every upload key, nothing dropped or duplicated")
}

// TestListMultipartUploads_DelimiterTruncation_BoundaryWalk is the
// ListMultipartUploads analog of the ListObjects/ListObjectVersions
// delimiter-truncation fix: groupUploadsByDelimiter's CommonPrefixes were
// never truncated or counted toward maxUploads at all, and
// seekMultipartMarker had no delimiter/prefix awareness, so a CommonPrefix
// marker would re-match objects already summarized under it.
func TestListMultipartUploads_DelimiterTruncation_BoundaryWalk(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mpu-delim-trunc")

	keys := []string{
		"a", "b/1", "b/2", "c", "d/1", "d/2", "d/3", "e", "f/1", "g",
	}
	for _, k := range keys {
		_, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
			Bucket: aws.String("mpu-delim-trunc"),
			Key:    aws.String(k),
		})
		require.NoError(t, err)
	}

	want := []string{"a", "c", "e", "g"} // CommonPrefixes are checked separately below.

	var (
		gotKeys []string
		gotCPs  []string
		marker  string
	)

	for range 10 {
		out, err := backend.ListMultipartUploads(t.Context(), &sdk_s3.ListMultipartUploadsInput{
			Bucket:     aws.String("mpu-delim-trunc"),
			Delimiter:  aws.String("/"),
			MaxUploads: aws.Int32(2),
			KeyMarker:  aws.String(marker),
		})
		require.NoError(t, err)

		for _, u := range out.Uploads {
			gotKeys = append(gotKeys, aws.ToString(u.Key))
		}

		for _, cp := range out.CommonPrefixes {
			gotCPs = append(gotCPs, aws.ToString(cp.Prefix))
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}

		marker = aws.ToString(out.NextKeyMarker)
		require.NotEmpty(t, marker)
	}

	assert.ElementsMatch(t, want, gotKeys, "no flat key dropped or duplicated")
	assert.ElementsMatch(t, []string{"b/", "d/", "f/"}, gotCPs, "no CommonPrefix dropped or duplicated")
}
