package s3_test

import (
	"bytes"
	"io"
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

	return s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithSkipMultipartSizeCheck()
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
		{
			// A bucket that has been deleted is pending janitor cleanup; it must
			// still block re-creation until the Janitor has fully removed it.
			name:   "create bucket with same name as pending-delete bucket is rejected",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
				_, err := b.DeleteBucket(
					t.Context(),
					&sdk_s3.DeleteBucketInput{Bucket: aws.String("my-bucket")},
				)
				require.NoError(t, err)
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
			// Real S3 refuses DeleteBucket with 409 BucketNotEmpty until the
			// caller removes every object/version/delete-marker first.
			name:   "delete non-empty bucket",
			bucket: "my-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "my-bucket")
				mustPutObject(t, b, "my-bucket", "key", []byte("data"))
			},
			wantErr:   s3.ErrBucketNotEmpty,
			expectErr: true,
		},
		{
			// A well-known real-S3 gotcha: incomplete multipart uploads block
			// deletion even though they never appear in ListObjects, so a
			// bucket that "looks empty" can still return BucketNotEmpty.
			name:   "delete bucket with incomplete multipart upload",
			bucket: "mpu-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "mpu-bucket")
				_, err := b.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
					Bucket: aws.String("mpu-bucket"),
					Key:    aws.String("mpu-key"),
				})
				require.NoError(t, err)
			},
			wantErr:   s3.ErrBucketNotEmpty,
			expectErr: true,
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

			assert.Empty(
				t,
				cmp.Diff(tt.wantBuckets, gotNames, cmpopts.EquateEmpty()),
				"bucket names mismatch",
			)
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
		{name: "CRC32 with value", algorithm: "CRC32", wantCRC32: "0wiusg=="},
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
					input.ChecksumCRC32 = aws.String("0wiusg==")
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

// TestVersionID_RandomFormat verifies that object version IDs use random hex
// strings rather than sequential Unix-nanosecond timestamps.
func TestVersionID_RandomFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put_object_with_versioning"},
		{name: "two_successive_puts_differ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")

			_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
				Bucket: aws.String("bkt"),
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			require.NoError(t, err)

			out1, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
				Body:   bytes.NewReader([]byte("data")),
			})
			require.NoError(t, err)

			vid := aws.ToString(out1.VersionId)
			assert.NotEmpty(t, vid)

			// Must NOT be purely numeric (the old UnixNano format was all digits).
			isNumeric := true
			for _, c := range vid {
				if c < '0' || c > '9' {
					isNumeric = false

					break
				}
			}
			assert.False(
				t,
				isNumeric,
				"version ID should not be a purely numeric Unix timestamp: %s",
				vid,
			)

			// Must be 32 hex chars (16 random bytes encoded as hex).
			assert.Len(t, vid, 32)

			out2, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
				Body:   bytes.NewReader([]byte("data2")),
			})
			require.NoError(t, err)

			assert.NotEqual(t, vid, aws.ToString(out2.VersionId),
				"successive version IDs must differ")
		})
	}
}
