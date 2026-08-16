package s3_test

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestListObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*testing.T, *s3.InMemoryBackend)
		name      string
		bucket    string
		prefix    string
		wantLen   int
		expectErr bool
	}{
		{
			name:   "list objects with prefix",
			bucket: "bkt",
			prefix: "docs/",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "docs/a.txt", []byte("a"))
				mustPutObject(t, b, "bkt", "docs/b.txt", []byte("b"))
				mustPutObject(t, b, "bkt", "images/c.png", []byte("c"))
			},
			wantLen: 2,
		},
		{
			name:   "list all objects",
			bucket: "bkt",
			prefix: "",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "a", []byte("a"))
				mustPutObject(t, b, "bkt", "b", []byte("b"))
			},
			wantLen: 2,
		},
		{
			name:      "list objects from non-existent bucket",
			bucket:    "no-bucket",
			prefix:    "",
			setup:     func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			out, err := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
				Bucket: aws.String(tt.bucket),
				Prefix: aws.String(tt.prefix),
			})

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Len(t, out.Contents, tt.wantLen)
			}
		})
	}
}

func TestObjectTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*testing.T, *s3.InMemoryBackend)
		wantTags  []types.Tag
		name      string
		bucket    string
		key       string
		tags      []types.Tag
		expectErr bool
	}{
		{
			name:   "put and get tags",
			bucket: "bkt",
			key:    "k",
			tags: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("team"), Value: aws.String("alpha")},
			},
			wantTags: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("team"), Value: aws.String("alpha")},
			},
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k", []byte("data"))
			},
		},
		{
			name:   "put tags on non-existent key",
			bucket: "bkt",
			key:    "no-key",
			tags:   []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
			},
			wantErr:   s3.ErrNoSuchKey,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			inputTags := make([]types.Tag, len(tt.tags))
			copy(inputTags, tt.tags)
			sort.Slice(
				inputTags,
				func(i, j int) bool { return *inputTags[i].Key < *inputTags[j].Key },
			)

			_, putErr := backend.PutObjectTagging(t.Context(), &sdk_s3.PutObjectTaggingInput{
				Bucket:  aws.String(tt.bucket),
				Key:     aws.String(tt.key),
				Tagging: &types.Tagging{TagSet: inputTags},
			})

			if tt.expectErr {
				require.ErrorIs(t, putErr, tt.wantErr)

				return
			}

			require.NoError(t, putErr)

			out, err := backend.GetObjectTagging(t.Context(), &sdk_s3.GetObjectTaggingInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String(tt.key),
			})
			require.NoError(t, err)

			gotTags := out.TagSet
			sort.Slice(gotTags, func(i, j int) bool { return *gotTags[i].Key < *gotTags[j].Key })

			wantSorted := make([]types.Tag, len(tt.wantTags))
			copy(wantSorted, tt.wantTags)
			sort.Slice(
				wantSorted,
				func(i, j int) bool { return *wantSorted[i].Key < *wantSorted[j].Key },
			)

			assert.Empty(
				t,
				cmp.Diff(wantSorted, gotTags, cmpopts.IgnoreUnexported(types.Tag{})),
				"tag set mismatch",
			)
		})
	}
}

func TestDeleteObjectTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*testing.T, *s3.InMemoryBackend)
		name   string
		bucket string
		key    string
	}{
		{
			name:   "delete tags from object",
			bucket: "bkt",
			key:    "k",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k", []byte("data"))
				_, err := b.PutObjectTagging(t.Context(), &sdk_s3.PutObjectTaggingInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("k"),
					Tagging: &types.Tagging{
						TagSet: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
					},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			_, err := backend.DeleteObjectTagging(t.Context(), &sdk_s3.DeleteObjectTaggingInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String(tt.key),
			})
			require.NoError(t, err)

			out, err := backend.GetObjectTagging(t.Context(), &sdk_s3.GetObjectTaggingInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String(tt.key),
			})
			require.NoError(t, err)

			assert.Empty(t,
				cmp.Diff([]types.Tag(nil), out.TagSet, cmpopts.IgnoreUnexported(types.Tag{})),
				"expected empty tag set")
		})
	}
}

func TestMultipartUpload_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "multipart upload full lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			mustCreateBucket(t, b, "bkt")

			out, err := b.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("large-file"),
			})
			require.NoError(t, err)
			uploadID := out.UploadId
			assert.NotEmpty(t, uploadID)

			p1, err := b.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("large-file"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader([]byte("part1")),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, p1.ETag)

			p2, err := b.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("large-file"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(2),
				Body:       bytes.NewReader([]byte("part2")),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, p2.ETag)

			ver, err := b.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket:   aws.String("bkt"),
				Key:      aws.String("large-file"),
				UploadId: uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: p1.ETag},
					{PartNumber: aws.Int32(2), ETag: p2.ETag},
				}},
			})
			require.NoError(t, err)
			assert.NotEmpty(t, ver.ETag)

			obj, err := b.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("large-file"),
			})
			require.NoError(t, err)
			data, _ := io.ReadAll(obj.Body)
			assert.Equal(t, "part1part2", string(data))

			_, abortErr := b.AbortMultipartUpload(t.Context(), &sdk_s3.AbortMultipartUploadInput{
				Bucket:   aws.String("bkt"),
				Key:      aws.String("large-file"),
				UploadId: uploadID,
			})
			assert.Error(t, abortErr)
		})
	}
}

func TestDeleteObjects_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *s3.InMemoryBackend)
		name        string
		bucket      string
		objects     []types.ObjectIdentifier
		wantErrors  int
		wantDeleted int
	}{
		{
			name:   "delete multiple objects",
			bucket: "bkt",
			objects: []types.ObjectIdentifier{
				{Key: aws.String("k1")},
				{Key: aws.String("k2")},
			},
			wantDeleted: 2,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
				mustPutObject(t, b, "bkt", "k2", []byte("d2"))
			},
		},
		{
			name:   "delete some non-existent objects returns success",
			bucket: "bkt",
			objects: []types.ObjectIdentifier{
				{Key: aws.String("k1")},
				{Key: aws.String("no-such-key")},
			},
			wantDeleted: 2, // S3 returns 200 for non-existent objects in bulk delete
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
			},
		},
		{
			name:   "delete versioned objects",
			bucket: "bkt",
			objects: []types.ObjectIdentifier{
				{Key: aws.String("k1"), VersionId: aws.String("v1")},
			},
			wantDeleted: 1,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				// We can't easily force version IDs in backend_memory without internal access or enabling versioning
				// But DeleteObject handles versionId if it exists.
				// In this test, we'll just check it doesn't crash and returns success.
			},
		},
		{
			name:   "non-existent bucket returns error for all objects",
			bucket: "no-bucket",
			objects: []types.ObjectIdentifier{
				{Key: aws.String("k1")},
			},
			wantErrors: 1,
			setup:      func(_ *testing.T, _ *s3.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			out, err := backend.DeleteObjects(t.Context(), &sdk_s3.DeleteObjectsInput{
				Bucket: aws.String(tt.bucket),
				Delete: &types.Delete{
					Objects: tt.objects,
				},
			})

			require.NoError(t, err)
			assert.Len(t, out.Deleted, tt.wantDeleted)
			assert.Len(t, out.Errors, tt.wantErrors)
		})
	}
}

func TestCreateBucket_GlobalUniqueness(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)

	// Create bucket in default region
	mustCreateBucket(t, backend, "unique-bucket")

	// Attempt to create the same bucket name again (even if in a different region via context)
	_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
		Bucket: aws.String("unique-bucket"),
	})
	require.ErrorIs(
		t,
		err,
		s3.ErrBucketAlreadyOwnedByYou,
		"same bucket name should be rejected globally",
	)
}

func TestPutObject_ContentEncodingDisposition(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	mustCreateBucket(t, backend, "enc-bkt")

	_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:             aws.String("enc-bkt"),
		Key:                aws.String("file"),
		Body:               bytes.NewReader([]byte("data")),
		ContentEncoding:    aws.String("gzip"),
		ContentDisposition: aws.String(`attachment; filename="file.txt"`),
	})
	require.NoError(t, err)

	t.Run("GetObject preserves ContentEncoding and ContentDisposition", func(t *testing.T) {
		t.Parallel()

		out, getErr := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: aws.String("enc-bkt"),
			Key:    aws.String("file"),
		})
		require.NoError(t, getErr)
		assert.Equal(t, "gzip", aws.ToString(out.ContentEncoding))
		assert.Equal(t, `attachment; filename="file.txt"`, aws.ToString(out.ContentDisposition))
	})

	t.Run("HeadObject preserves ContentEncoding and ContentDisposition", func(t *testing.T) {
		t.Parallel()

		out, headErr := backend.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
			Bucket: aws.String("enc-bkt"),
			Key:    aws.String("file"),
		})
		require.NoError(t, headErr)
		assert.Equal(t, "gzip", aws.ToString(out.ContentEncoding))
		assert.Equal(t, `attachment; filename="file.txt"`, aws.ToString(out.ContentDisposition))
	})
}

func TestListObjects_Delimiter(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	mustCreateBucket(t, backend, "delim-bkt")

	for _, key := range []string{
		"photos/2023/jan.jpg",
		"photos/2023/feb.jpg",
		"photos/2024/mar.jpg",
		"docs/report.pdf",
		"readme.txt",
	} {
		mustPutObject(t, backend, "delim-bkt", key, []byte("x"))
	}

	out, err := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket:    aws.String("delim-bkt"),
		Prefix:    aws.String(""),
		Delimiter: aws.String("/"),
	})
	require.NoError(t, err)

	require.Len(t, out.Contents, 1)
	assert.Equal(t, "readme.txt", aws.ToString(out.Contents[0].Key))

	prefixes := make([]string, len(out.CommonPrefixes))
	for i, cp := range out.CommonPrefixes {
		prefixes[i] = aws.ToString(cp.Prefix)
	}
	assert.ElementsMatch(t, []string{"photos/", "docs/"}, prefixes)
}

func TestListObjects_DelimiterWithPrefix(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	mustCreateBucket(t, backend, "delim2-bkt")

	for _, key := range []string{
		"photos/2023/jan.jpg",
		"photos/2023/feb.jpg",
		"photos/2024/mar.jpg",
	} {
		mustPutObject(t, backend, "delim2-bkt", key, []byte("x"))
	}

	out, err := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket:    aws.String("delim2-bkt"),
		Prefix:    aws.String("photos/"),
		Delimiter: aws.String("/"),
	})
	require.NoError(t, err)

	assert.Empty(t, out.Contents)

	prefixes := make([]string, len(out.CommonPrefixes))
	for i, cp := range out.CommonPrefixes {
		prefixes[i] = aws.ToString(cp.Prefix)
	}
	assert.ElementsMatch(t, []string{"photos/2023/", "photos/2024/"}, prefixes)
}

// TestListObjectsV2_PaginationConsistency_NoDelimiter walks a bucket page by
// page with an odd MaxKeys (no delimiter, the fast path added for
// gopherstack-3dqa's optimization pass) and reconstructs the full key set
// from the pages, proving the deferred-conversion truncation still returns
// every key exactly once, in sorted order, with a correctly populated
// per-object response (not just "doesn't crash").
func TestListObjectsV2_PaginationConsistency_NoDelimiter(t *testing.T) {
	t.Parallel()

	const objectCount = 253
	const pageSize = 37

	backend := newTestBackend(t)
	mustCreateBucket(t, backend, "page-bkt")

	want := make([]string, objectCount)
	for i := range objectCount {
		key := fmt.Sprintf("obj-%04d", i)
		want[i] = key
		mustPutObject(t, backend, "page-bkt", key, []byte("x"))
	}
	sort.Strings(want)

	var got []string
	continuationToken := ""
	for pages := 0; ; pages++ {
		require.Lessf(t, pages, objectCount, "pagination did not terminate")

		out, err := backend.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
			Bucket:            aws.String("page-bkt"),
			MaxKeys:           aws.Int32(pageSize),
			ContinuationToken: aws.String(continuationToken),
		})
		require.NoError(t, err)

		for _, obj := range out.Contents {
			got = append(got, aws.ToString(obj.Key))
			assert.Equal(t, "gopherstack", aws.ToString(obj.Owner.ID))
			assert.Equal(t, types.ObjectStorageClassStandard, obj.StorageClass)
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = aws.ToString(out.NextContinuationToken)
		require.NotEmpty(t, continuationToken)
	}

	assert.Equal(t, want, got)
}

func TestCreateBucket_NonDefaultRegion_PutObjectSucceeds(t *testing.T) {
	t.Parallel()

	// Reproduces the bug: bucket created with LocationConstraint != default region;
	// subsequent PutObject must succeed (not 404).
	backend := newTestBackend(t)

	_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
		Bucket: aws.String("west-bucket"),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint("us-west-2"),
		},
	})
	require.NoError(t, err)

	_, err = backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("west-bucket"),
		Key:    aws.String("hello.txt"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	require.NoError(
		t,
		err,
		"PutObject must succeed even when bucket was created with a non-default LocationConstraint",
	)

	out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("west-bucket"),
		Key:    aws.String("hello.txt"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	body, readErr := io.ReadAll(out.Body)
	require.NoError(t, readErr)
	assert.Equal(t, "hello", string(body))
}

func TestSetDefaultRegion(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)

	// Set a non-empty region.
	backend.SetDefaultRegion("eu-central-1")

	// Create a bucket (should use "eu-central-1" as default region).
	_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
		Bucket: aws.String("test-default-region-bucket"),
	})
	require.NoError(t, err)

	// Empty string should reset to the internal default.
	backend.SetDefaultRegion("")
}
