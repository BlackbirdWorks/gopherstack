package s3_test

import (
	"bytes"
	"io"
	"sort"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend(t *testing.T) *s3.InMemoryBackend {
	t.Helper()

	return s3.NewInMemoryBackend(&s3.GzipCompressor{})
}

func TestCreateBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*testing.T, *s3.InMemoryBackend)
		name      string
		bucket    string
		expectErr bool
	}{
		{
			name:   "create new bucket",
			bucket: "my-bucket",
			setup:  func(_ *testing.T, _ *s3.InMemoryBackend) {},
		},
		{
			name:   "create duplicate bucket",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
			},
			wantErr:   s3.ErrBucketAlreadyOwnedByYou,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			_, err := backend.CreateBucket(
				t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: aws.String(tt.bucket)},
			)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*testing.T, *s3.InMemoryBackend)
		name      string
		bucket    string
		expectErr bool
	}{
		{
			name:   "delete existing empty bucket",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
			},
		},
		{
			name:      "delete non-existent bucket",
			bucket:    "no-such-bucket",
			setup:     func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
		{
			name:   "delete non-empty bucket",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
				mustPutObject(t, b, "my-bucket", "key", []byte("data"))
			},
			// Async deletion: non-empty buckets are now accepted and queued for
			// background deletion by the Janitor.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			_, err := backend.DeleteBucket(
				t.Context(),
				&sdk_s3.DeleteBucketInput{Bucket: aws.String(tt.bucket)},
			)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHeadBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*testing.T, *s3.InMemoryBackend)
		name      string
		bucket    string
		expectErr bool
	}{
		{
			name:   "get existing bucket",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
			},
		},
		{
			name:      "get non-existent bucket",
			bucket:    "no-such-bucket",
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

			out, err := backend.HeadBucket(
				t.Context(),
				&sdk_s3.HeadBucketInput{Bucket: aws.String(tt.bucket)},
			)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, out)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestListBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *s3.InMemoryBackend)
		name        string
		wantBuckets []string
	}{
		{
			name:        "no buckets",
			setup:       func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantBuckets: nil,
		},
		{
			name: "one bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "alpha")
			},
			wantBuckets: []string{"alpha"},
		},
		{
			name: "multiple buckets sorted",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "charlie")
				mustCreateBucket(t, b, "alpha")
				mustCreateBucket(t, b, "bravo")
			},
			wantBuckets: []string{"alpha", "bravo", "charlie"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			out, err := backend.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
			require.NoError(t, err)

			gotNames := make([]string, len(out.Buckets))
			for i, b := range out.Buckets {
				gotNames[i] = aws.ToString(b.Name)
			}

			assert.Empty(t, cmp.Diff(tt.wantBuckets, gotNames, cmpopts.EquateEmpty()), "bucket names mismatch")
		})
	}
}

func TestPutObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*testing.T, *s3.InMemoryBackend)
		name      string
		bucket    string
		key       string
		data      []byte
		expectErr bool
	}{
		{
			name:   "put object in existing bucket",
			bucket: "my-bucket",
			key:    "my-key",
			data:   []byte("hello"),
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
			},
		},
		{
			name:      "put object in non-existent bucket",
			bucket:    "no-such-bucket",
			key:       "my-key",
			data:      []byte("hello"),
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

			ver, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket:   aws.String(tt.bucket),
				Key:      aws.String(tt.key),
				Body:     bytes.NewReader(tt.data),
				Metadata: map[string]string{},
			})

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, ver)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, *ver.VersionId)
			}
		})
	}
}

func TestPutObject_ChecksumAutoCalculation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		algorithm string
		wantCRC32 string
	}{
		{name: "CRC32 with value", algorithm: "CRC32", wantCRC32: "val"},
		{name: "CRC32C", algorithm: "CRC32C"},
		{name: "SHA1", algorithm: "SHA1"},
		{name: "SHA256", algorithm: "SHA256"},
		{name: "Invalid algorithm", algorithm: "INVALID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")

			input := &sdk_s3.PutObjectInput{
				Bucket:   aws.String("bkt"),
				Key:      aws.String("key"),
				Body:     bytes.NewReader([]byte("test data")),
				Metadata: map[string]string{},
			}

			if tt.algorithm != "INVALID" {
				input.ChecksumAlgorithm = types.ChecksumAlgorithm(tt.algorithm)
				if tt.algorithm == "CRC32" {
					input.ChecksumCRC32 = aws.String("val")
				}
			}

			ver, err := backend.PutObject(t.Context(), input)
			require.NoError(t, err)

			if tt.wantCRC32 != "" {
				assert.Equal(t, tt.wantCRC32, aws.ToString(ver.ChecksumCRC32))
			}
		})
	}
}

func TestGetObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*testing.T, *s3.InMemoryBackend)
		name      string
		bucket    string
		key       string
		versionID string
		wantData  string
		expectErr bool
	}{
		{
			name:   "get existing object",
			bucket: "my-bucket",
			key:    "my-key",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
				mustPutObject(t, b, "my-bucket", "my-key", []byte("data"))
			},
			wantData: "data",
		},
		{
			name:      "get from non-existent bucket",
			bucket:    "no-such-bucket",
			key:       "my-key",
			setup:     func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
		{
			name:   "get non-existent key",
			bucket: "my-bucket",
			key:    "no-such-key",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
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

			var vid *string
			if tt.versionID != "" {
				vid = aws.String(tt.versionID)
			}

			out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket:    aws.String(tt.bucket),
				Key:       aws.String(tt.key),
				VersionId: vid,
			})

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				data, _ := io.ReadAll(out.Body)
				assert.Equal(t, tt.wantData, string(data))
			}
		})
	}
}

func TestVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		expectErr bool
	}{
		{name: "versioned put creates unique version IDs"},
		{name: "get specific null version"},
		{name: "get non-existent version returns error", wantErr: s3.ErrNoSuchKey, expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "versioned put creates unique version IDs":
				_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
					Bucket: aws.String("bkt"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				v1, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(
						"bkt",
					), Key: aws.String("k"), Body: bytes.NewReader([]byte("v1")),
				})
				require.NoError(t, err)
				assert.NotEqual(t, s3.NullVersion, *v1.VersionId)

				v2, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(
						"bkt",
					), Key: aws.String("k"), Body: bytes.NewReader([]byte("v2")),
				})
				require.NoError(t, err)
				assert.NotEqual(t, *v1.VersionId, *v2.VersionId)

				got, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
					Bucket: aws.String("bkt"), Key: aws.String("k"),
				})
				require.NoError(t, err)
				data, _ := io.ReadAll(got.Body)
				assert.Equal(t, "v2", string(data))

			case "get specific null version":
				mustPutObject(t, backend, "bkt", "k", []byte("data"))

				got, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
					Bucket: aws.String(
						"bkt",
					), Key: aws.String("k"), VersionId: aws.String(s3.NullVersion),
				})
				require.NoError(t, err)
				data, _ := io.ReadAll(got.Body)
				assert.Equal(t, "data", string(data))

			case "get non-existent version returns error":
				mustPutObject(t, backend, "bkt", "k", []byte("data"))

				_, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
					Bucket: aws.String(
						"bkt",
					), Key: aws.String("k"), VersionId: aws.String("non-existent-version"),
				})
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestDeleteObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		key        string
		versionID  string
		expectErr  bool
		wantMarker bool
	}{
		{
			name:       "simple delete creates delete marker when versioning enabled",
			bucket:     "bkt",
			key:        "k",
			wantMarker: true,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				_, err := b.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
					Bucket: aws.String("bkt"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)
				mustPutObject(t, b, "bkt", "k", []byte("data"))
			},
		},
		{
			name:      "delete from non-existent bucket",
			bucket:    "no-bucket",
			key:       "k",
			setup:     func(_ *testing.T, _ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
		{
			name:      "delete specific version not found is a no-op",
			bucket:    "bkt",
			key:       "k",
			versionID: "bad-version",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k", []byte("data"))
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(t, backend)

			var vid *string
			if tt.versionID != "" {
				vid = aws.String(tt.versionID)
			}

			out, err := backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
				Bucket:    aws.String(tt.bucket),
				Key:       aws.String(tt.key),
				VersionId: vid,
			})

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				if tt.wantMarker {
					assert.True(t, *out.DeleteMarker)
					assert.NotEmpty(t, *out.VersionId)
				}
			}
		})
	}
}

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

			assert.Empty(t, cmp.Diff(wantSorted, gotTags, cmpopts.IgnoreUnexported(types.Tag{})), "tag set mismatch")
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
	require.ErrorIs(t, err, s3.ErrBucketAlreadyOwnedByYou, "same bucket name should be rejected globally")
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
	require.NoError(t, err, "PutObject must succeed even when bucket was created with a non-default LocationConstraint")

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

func TestPutBucketACL_GetBucketACL(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)

	_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
		Bucket: aws.String("acl-test-bucket"),
	})
	require.NoError(t, err)

	// Default ACL is "private".
	acl, err := backend.GetBucketACL(t.Context(), "acl-test-bucket")
	require.NoError(t, err)
	assert.Equal(t, "private", acl)

	// Set a new ACL.
	err = backend.PutBucketACL(t.Context(), "acl-test-bucket", "public-read")
	require.NoError(t, err)

	acl, err = backend.GetBucketACL(t.Context(), "acl-test-bucket")
	require.NoError(t, err)
	assert.Equal(t, "public-read", acl)
}

func TestPutBucketACL_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.PutBucketACL(t.Context(), "nonexistent-bucket", "private")
	assert.Error(t, err)
}

func TestGetBucketACL_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	_, err := backend.GetBucketACL(t.Context(), "nonexistent-bucket")
	assert.Error(t, err)
}

func TestBucketWebsiteConfiguration(t *testing.T) {
	t.Parallel()

	const websiteXML = `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<IndexDocument><Suffix>index.html</Suffix></IndexDocument>` +
		`<ErrorDocument><Key>error.html</Key></ErrorDocument>` +
		`</WebsiteConfiguration>`

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, backend *s3.InMemoryBackend, bucket string)
		name    string
		bucket  string
		want    string
	}{
		{
			name:   "get returns NoSuchWebsiteConfiguration when not set",
			bucket: "website-test-bucket",
			setup: func(_ *testing.T, _ *s3.InMemoryBackend, _ string) {
				// No website config stored.
			},
			wantErr: s3.ErrNoWebsiteConfig,
		},
		{
			name:   "put then get returns stored config",
			bucket: "website-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketWebsite(t.Context(), bucket, websiteXML)
				require.NoError(t, err)
			},
			want: websiteXML,
		},
		{
			name:   "delete clears the config",
			bucket: "website-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketWebsite(t.Context(), bucket, websiteXML)
				require.NoError(t, err)
				err = backend.DeleteBucketWebsite(t.Context(), bucket)
				require.NoError(t, err)
			},
			wantErr: s3.ErrNoWebsiteConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
				Bucket: aws.String(tt.bucket),
			})
			require.NoError(t, err)

			tt.setup(t, backend, tt.bucket)

			got, err := backend.GetBucketWebsite(t.Context(), tt.bucket)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPutBucketWebsite_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.PutBucketWebsite(t.Context(), "nonexistent-bucket", "<WebsiteConfiguration/>")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestGetBucketWebsite_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	_, err := backend.GetBucketWebsite(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestDeleteBucketWebsite_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.DeleteBucketWebsite(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestBucketEncryptionConfiguration(t *testing.T) {
	t.Parallel()

	const encryptionXML = `<ServerSideEncryptionConfiguration>` +
		`<Rule><ApplyServerSideEncryptionByDefault>` +
		`<SSEAlgorithm>AES256</SSEAlgorithm>` +
		`</ApplyServerSideEncryptionByDefault></Rule>` +
		`</ServerSideEncryptionConfiguration>`

	const kmsEncryptionXML = `<ServerSideEncryptionConfiguration>` +
		`<Rule><ApplyServerSideEncryptionByDefault>` +
		`<SSEAlgorithm>aws:kms</SSEAlgorithm>` +
		`<KMSMasterKeyID>arn:aws:kms:us-east-1:000000000000:key/test-key</KMSMasterKeyID>` +
		`</ApplyServerSideEncryptionByDefault></Rule>` +
		`</ServerSideEncryptionConfiguration>`

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, backend *s3.InMemoryBackend, bucket string)
		name    string
		bucket  string
		want    string
	}{
		{
			name:   "get returns ServerSideEncryptionConfigurationNotFoundError when not set",
			bucket: "encryption-test-bucket",
			setup: func(_ *testing.T, _ *s3.InMemoryBackend, _ string) {
				// No encryption config stored.
			},
			wantErr: s3.ErrNoEncryptionConfig,
		},
		{
			name:   "put then get returns stored AES256 config",
			bucket: "encryption-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketEncryption(t.Context(), bucket, encryptionXML)
				require.NoError(t, err)
			},
			want: encryptionXML,
		},
		{
			name:   "put then get returns stored aws:kms config",
			bucket: "encryption-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketEncryption(t.Context(), bucket, kmsEncryptionXML)
				require.NoError(t, err)
			},
			want: kmsEncryptionXML,
		},
		{
			name:   "delete clears the config",
			bucket: "encryption-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketEncryption(t.Context(), bucket, encryptionXML)
				require.NoError(t, err)
				err = backend.DeleteBucketEncryption(t.Context(), bucket)
				require.NoError(t, err)
			},
			wantErr: s3.ErrNoEncryptionConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
				Bucket: aws.String(tt.bucket),
			})
			require.NoError(t, err)

			tt.setup(t, backend, tt.bucket)

			got, err := backend.GetBucketEncryption(t.Context(), tt.bucket)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPutBucketEncryption_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.PutBucketEncryption(t.Context(), "nonexistent-bucket", "<ServerSideEncryptionConfiguration/>")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestGetBucketEncryption_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	_, err := backend.GetBucketEncryption(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestDeleteBucketEncryption_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.DeleteBucketEncryption(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestCompressionMinBytes_PutObject(t *testing.T) {
	t.Parallel()

	smallData := bytes.Repeat([]byte("a"), 512)
	largeData := bytes.Repeat([]byte("a"), 2048)

	tests := []struct {
		name                string
		data                []byte
		compressionMinBytes int
		wantCompressed      bool
	}{
		{
			name:                "small object below threshold is not compressed",
			data:                smallData,
			compressionMinBytes: 1024,
			wantCompressed:      false,
		},
		{
			name:                "large object at or above threshold is compressed",
			data:                largeData,
			compressionMinBytes: 1024,
			wantCompressed:      true,
		},
		{
			name:                "zero threshold compresses all objects",
			data:                smallData,
			compressionMinBytes: 0,
			wantCompressed:      true,
		},
		{
			name:                "object exactly at threshold is compressed",
			data:                bytes.Repeat([]byte("b"), 1024),
			compressionMinBytes: 1024,
			wantCompressed:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := &recordingCompressor{delegate: &s3.GzipCompressor{}}
			backend := s3.NewInMemoryBackend(rc).
				WithCompressionMinBytes(tt.compressionMinBytes)
			mustCreateBucket(t, backend, "bkt")

			_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket:   aws.String("bkt"),
				Key:      aws.String("key"),
				Body:     bytes.NewReader(tt.data),
				Metadata: map[string]string{},
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantCompressed, rc.compressCalled,
				"unexpected compression decision for object of size %d with threshold %d",
				len(tt.data), tt.compressionMinBytes,
			)

			// Verify the round-trip: GetObject must return the original data.
			out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.data, body)
		})
	}
}

// recordingCompressor wraps a Compressor and records whether Compress was called.
type recordingCompressor struct {
	delegate       s3.Compressor
	compressCalled bool
}

func (r *recordingCompressor) Compress(data []byte) ([]byte, error) {
	r.compressCalled = true

	return r.delegate.Compress(data)
}

func (r *recordingCompressor) Decompress(data []byte) ([]byte, error) {
	return r.delegate.Decompress(data)
}

func TestCompressionMinBytes_CompleteMultipartUpload(t *testing.T) {
	t.Parallel()

	// Each part is 512 bytes; two parts assemble to 1024 bytes total.
	partData := bytes.Repeat([]byte("x"), 512)

	tests := []struct {
		name                string
		compressionMinBytes int
		wantCompressed      bool
	}{
		{
			name:                "assembled size below threshold is not compressed",
			compressionMinBytes: 2048,
			wantCompressed:      false,
		},
		{
			name:                "assembled size at or above threshold is compressed",
			compressionMinBytes: 1024,
			wantCompressed:      true,
		},
		{
			name:                "zero threshold compresses all objects",
			compressionMinBytes: 0,
			wantCompressed:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := &recordingCompressor{delegate: &s3.GzipCompressor{}}
			backend := s3.NewInMemoryBackend(rc).
				WithCompressionMinBytes(tt.compressionMinBytes)
			mustCreateBucket(t, backend, "bkt")

			// Start multipart upload
			createOut, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)
			uploadID := createOut.UploadId

			// Upload two parts
			p1, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("key"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(partData),
			})
			require.NoError(t, err)

			p2, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("key"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(2),
				Body:       bytes.NewReader(partData),
			})
			require.NoError(t, err)

			// Complete
			_, err = backend.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket:   aws.String("bkt"),
				Key:      aws.String("key"),
				UploadId: uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: p1.ETag},
					{PartNumber: aws.Int32(2), ETag: p2.ETag},
				}},
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantCompressed, rc.compressCalled,
				"unexpected compression decision for assembled size %d with threshold %d",
				len(partData)*2, tt.compressionMinBytes,
			)

			// Verify round-trip.
			out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, append(partData, partData...), body)
		})
	}
}
