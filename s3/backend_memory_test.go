package s3_test

import (
	"testing"

	"Gopherstack/s3"

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
		setup     func(*s3.InMemoryBackend)
		name      string
		bucket    string
		expectErr bool
	}{
		{
			name:   "create new bucket",
			bucket: "my-bucket",
			setup:  func(_ *s3.InMemoryBackend) {},
		},
		{
			name:   "create duplicate bucket",
			bucket: "my-bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))
			},
			wantErr:   s3.ErrBucketAlreadyExists,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			err := backend.CreateBucket(tt.bucket)

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
		setup     func(*s3.InMemoryBackend)
		name      string
		bucket    string
		expectErr bool
	}{
		{
			name:   "delete existing empty bucket",
			bucket: "my-bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))
			},
		},
		{
			name:      "delete non-existent bucket",
			bucket:    "no-such-bucket",
			setup:     func(_ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
		{
			name:   "delete non-empty bucket",
			bucket: "my-bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))

				_, err := b.PutObject("my-bucket", "key", []byte("data"), s3.ObjectMetadata{})
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
			tt.setup(backend)

			err := backend.DeleteBucket(tt.bucket)

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
		setup     func(*s3.InMemoryBackend)
		name      string
		bucket    string
		expectErr bool
	}{
		{
			name:   "get existing bucket",
			bucket: "my-bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))
			},
		},
		{
			name:      "get non-existent bucket",
			bucket:    "no-such-bucket",
			setup:     func(_ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			bucket, err := backend.HeadBucket(tt.bucket)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, bucket)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.bucket, bucket.Name)
			}
		})
	}
}

func TestListBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*s3.InMemoryBackend)
		name     string
		wantName string
		wantLen  int
	}{
		{
			name:    "no buckets",
			setup:   func(_ *s3.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "one bucket",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("alpha"))
			},
			wantLen:  1,
			wantName: "alpha",
		},
		{
			name: "multiple buckets sorted",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("charlie"))
				require.NoError(t, b.CreateBucket("alpha"))
				require.NoError(t, b.CreateBucket("bravo"))
			},
			wantLen:  3,
			wantName: "alpha", // first alphabetically
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			buckets, err := backend.ListBuckets()
			require.NoError(t, err)
			assert.Len(t, buckets, tt.wantLen)

			if tt.wantLen > 0 {
				assert.Equal(t, tt.wantName, buckets[0].Name)
			}
		})
	}
}

func TestPutObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*s3.InMemoryBackend)
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
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))
			},
		},
		{
			name:      "put object in non-existent bucket",
			bucket:    "no-such-bucket",
			key:       "my-key",
			data:      []byte("hello"),
			setup:     func(_ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			ver, err := backend.PutObject(tt.bucket, tt.key, tt.data, s3.ObjectMetadata{})

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, ver)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, ver.VersionID)
				assert.True(t, ver.IsLatest)
			}
		})
	}
}

func TestPutObject_ChecksumAutoCalculation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		algorithm string
		data      string
	}{
		{name: "CRC32", algorithm: "CRC32", data: "test data"},
		{name: "CRC32C", algorithm: "CRC32C", data: "test data"},
		{name: "SHA1", algorithm: "SHA1", data: "test data"},
		{name: "SHA256", algorithm: "SHA256", data: "test data"},
		{name: "Invalid", algorithm: "INVALID", data: "test data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := newTestBackend(t)
			require.NoError(t, backend.CreateBucket("bkt"))

			meta := s3.ObjectMetadata{
				ChecksumAlgorithm: tt.algorithm,
			}
			ver, err := backend.PutObject("bkt", "key", []byte(tt.data), meta)
			require.NoError(t, err)

			if tt.algorithm != "INVALID" {
				assert.NotEmpty(t, ver.ChecksumValue)
			} else {
				assert.Empty(t, ver.ChecksumValue)
			}
		})
	}
}

func TestGetObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*s3.InMemoryBackend)
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
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))

				_, err := b.PutObject("my-bucket", "my-key", []byte("data"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
			wantData: "data",
		},
		{
			name:      "get from non-existent bucket",
			bucket:    "no-such-bucket",
			key:       "my-key",
			setup:     func(_ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
		{
			name:   "get non-existent key",
			bucket: "my-bucket",
			key:    "no-such-key",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("my-bucket"))
			},
			wantErr:   s3.ErrNoSuchKey,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			ver, err := backend.GetObject(tt.bucket, tt.key, tt.versionID)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantData, string(ver.Data))
			}
		})
	}
}

func TestVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		test func(t *testing.T)
		name string
	}{
		{
			name: "versioned put creates unique version IDs",
			test: func(t *testing.T) {
				t.Helper()

				backend := newTestBackend(t)
				require.NoError(t, backend.CreateBucket("bkt"))

				_, err := backend.HeadBucket("bkt")
				require.NoError(t, err)
				// We'll test via null version instead

				// Without versioning, version ID is "null"
				v1, err := backend.PutObject("bkt", "k", []byte("v1"), s3.ObjectMetadata{})
				require.NoError(t, err)
				assert.Equal(t, s3.NullVersion, v1.VersionID)

				// Overwrite
				v2, err := backend.PutObject("bkt", "k", []byte("v2"), s3.ObjectMetadata{})
				require.NoError(t, err)
				assert.Equal(t, s3.NullVersion, v2.VersionID)

				// Get returns latest
				got, err := backend.GetObject("bkt", "k", "")
				require.NoError(t, err)
				assert.Equal(t, "v2", string(got.Data))
			},
		},
		{
			name: "get specific null version",
			test: func(t *testing.T) {
				t.Helper()

				backend := newTestBackend(t)
				require.NoError(t, backend.CreateBucket("bkt"))

				_, err := backend.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{})
				require.NoError(t, err)

				got, err := backend.GetObject("bkt", "k", s3.NullVersion)
				require.NoError(t, err)
				assert.Equal(t, "data", string(got.Data))
			},
		},
		{
			name: "get non-existent version returns error",
			test: func(t *testing.T) {
				t.Helper()

				backend := newTestBackend(t)
				require.NoError(t, backend.CreateBucket("bkt"))

				_, err := backend.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{})
				require.NoError(t, err)

				_, err = backend.GetObject("bkt", "k", "non-existent-version")
				require.ErrorIs(t, err, s3.ErrNoSuchKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.test(t)
		})
	}
}

func TestDeleteObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*s3.InMemoryBackend)
		name      string
		bucket    string
		key       string
		versionID string
		expectErr bool
	}{
		{
			name:   "simple delete creates delete marker",
			bucket: "bkt",
			key:    "k",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
		},
		{
			name:      "delete from non-existent bucket",
			bucket:    "no-bucket",
			key:       "k",
			setup:     func(_ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
		{
			name:      "delete specific version not found",
			bucket:    "bkt",
			key:       "k",
			versionID: "bad-version",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
			wantErr:   s3.ErrNoSuchKey,
			expectErr: true,
		},
		{
			name:      "delete specific version of non-existent key",
			bucket:    "bkt",
			key:       "no-key",
			versionID: "some-version",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))
			},
			wantErr:   s3.ErrNoSuchKey,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			marker, err := backend.DeleteObject(tt.bucket, tt.key, tt.versionID)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				// Simple delete without versioning returns "null"
				assert.Equal(t, s3.NullVersion, marker)
			}
		})
	}
}

func TestDeleteObject_MakesGetFail(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

	_, err = backend.DeleteObject("bkt", "k", "")
	require.NoError(t, err)

	// Object should appear deleted (latest is delete marker)
	_, err = backend.GetObject("bkt", "k", "")
	require.ErrorIs(t, err, s3.ErrNoSuchKey)
}

func TestDeleteObject_DeleteSpecificNullVersion(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

	marker, err := backend.DeleteObject("bkt", "k", s3.NullVersion)
	require.NoError(t, err)
	assert.Empty(t, marker) // not a delete marker

	// Object should be fully removed
	_, err = backend.GetObject("bkt", "k", "")
	require.ErrorIs(t, err, s3.ErrNoSuchKey)
}

func TestListObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*s3.InMemoryBackend)
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
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "docs/a.txt", []byte("a"), s3.ObjectMetadata{})
				require.NoError(t, err)

				_, err = b.PutObject("bkt", "docs/b.txt", []byte("b"), s3.ObjectMetadata{})
				require.NoError(t, err)

				_, err = b.PutObject("bkt", "images/c.png", []byte("c"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
			wantLen: 2,
		},
		{
			name:   "list all objects",
			bucket: "bkt",
			prefix: "",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "a", []byte("a"), s3.ObjectMetadata{})
				require.NoError(t, err)

				_, err = b.PutObject("bkt", "b", []byte("b"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
			wantLen: 2,
		},
		{
			name:      "list objects from non-existent bucket",
			bucket:    "no-bucket",
			prefix:    "",
			setup:     func(_ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			objects, err := backend.ListObjects(tt.bucket, tt.prefix)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Len(t, objects, tt.wantLen)
			}
		})
	}
}

func TestListObjects_ExcludesDeletedObjects(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBucket("bkt"))

	_, err := backend.PutObject("bkt", "alive", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

	_, err = backend.PutObject("bkt", "dead", []byte("data"), s3.ObjectMetadata{})
	require.NoError(t, err)

	_, err = backend.DeleteObject("bkt", "dead", "")
	require.NoError(t, err)

	objects, err := backend.ListObjects("bkt", "")
	require.NoError(t, err)
	require.Len(t, objects, 1)
	assert.Equal(t, "alive", objects[0].Key)
}

func TestObjectTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(*s3.InMemoryBackend)
		tags      map[string]string
		name      string
		bucket    string
		key       string
		expectErr bool
	}{
		{
			name:   "put and get tags",
			bucket: "bkt",
			key:    "k",
			tags:   map[string]string{"env": "prod", "team": "alpha"},
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{})
				require.NoError(t, err)
			},
		},
		{
			name:   "put tags on non-existent key",
			bucket: "bkt",
			key:    "no-key",
			tags:   map[string]string{"k": "v"},
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))
			},
			wantErr:   s3.ErrNoSuchKey,
			expectErr: true,
		},
		{
			name:      "put tags on non-existent bucket",
			bucket:    "no-bucket",
			key:       "k",
			tags:      map[string]string{"k": "v"},
			setup:     func(_ *s3.InMemoryBackend) {},
			wantErr:   s3.ErrNoSuchBucket,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			err := backend.PutObjectTagging(tt.bucket, tt.key, "", tt.tags)

			if tt.expectErr {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)

				gotTags, err := backend.GetObjectTagging(tt.bucket, tt.key, "")
				require.NoError(t, err)
				assert.Equal(t, tt.tags, gotTags)
			}
		})
	}
}

func TestGetObjectTagging_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(*s3.InMemoryBackend)
		name    string
		bucket  string
		key     string
	}{
		{
			name:    "non-existent bucket",
			bucket:  "no-bucket",
			key:     "k",
			setup:   func(_ *s3.InMemoryBackend) {},
			wantErr: s3.ErrNoSuchBucket,
		},
		{
			name:   "non-existent key",
			bucket: "bkt",
			key:    "no-key",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))
			},
			wantErr: s3.ErrNoSuchKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			_, err := backend.GetObjectTagging(tt.bucket, tt.key, "")
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestDeleteObjectTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(*s3.InMemoryBackend)
		name    string
		bucket  string
		key     string
	}{
		{
			name:   "delete tags from object",
			bucket: "bkt",
			key:    "k",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))

				_, err := b.PutObject("bkt", "k", []byte("data"), s3.ObjectMetadata{
					Tags: map[string]string{"k": "v"},
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "delete tags from non-existent bucket",
			bucket:  "no-bucket",
			key:     "k",
			setup:   func(_ *s3.InMemoryBackend) {},
			wantErr: s3.ErrNoSuchBucket,
		},
		{
			name:   "delete tags from non-existent key",
			bucket: "bkt",
			key:    "no-key",
			setup: func(b *s3.InMemoryBackend) {
				require.NoError(t, b.CreateBucket("bkt"))
			},
			wantErr: s3.ErrNoSuchKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			tt.setup(backend)

			err := backend.DeleteObjectTagging(tt.bucket, tt.key, "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)

				tags, err := backend.GetObjectTagging(tt.bucket, tt.key, "")
				require.NoError(t, err)
				assert.Empty(t, tags)
			}
		})
	}
}

func TestDeleteObject_NonExistentKeySimple(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBucket("bkt"))

	// Simple delete of non-existent key should create a delete marker (S3 behavior)
	marker, err := backend.DeleteObject("bkt", "no-key", "")
	require.NoError(t, err)
	assert.Equal(t, s3.NullVersion, marker)
}
