package s3_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// newFastJanitor creates a Janitor with a short interval for deterministic tests.
func newFastJanitor(b *s3.InMemoryBackend) *s3.Janitor {
	return s3.NewJanitor(b, s3.Settings{JanitorInterval: 5 * time.Millisecond})
}

func TestS3Janitor_BucketDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *s3.InMemoryBackend)
		act    func(t *testing.T, b *s3.InMemoryBackend)
		verify func(t *testing.T, b *s3.InMemoryBackend)
		name   string
	}{
		{
			name: "empty bucket removed by janitor",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bucket-a")
			},
			act: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("bucket-a")})
				require.NoError(t, err)

				_, err = b.HeadBucket(t.Context(), &sdk_s3.HeadBucketInput{Bucket: aws.String("bucket-a")})
				require.ErrorIs(t, err, s3.ErrNoSuchBucket)
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				go newFastJanitor(b).Run(ctx)

				require.Eventually(t, func() bool {
					listed, listErr := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})

					return listErr == nil && len(listed.Buckets) == 0
				}, 500*time.Millisecond, 10*time.Millisecond)
			},
		},
		{
			name: "bucket with objects removed after drain",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "full-bucket")
				for i := range 5 {
					mustPutObject(t, b, "full-bucket", fmt.Sprintf("key-%d", i), []byte("data"))
				}
			},
			act: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("full-bucket")})
				require.NoError(t, err)

				_, err = b.GetObject(t.Context(), &sdk_s3.GetObjectInput{
					Bucket: aws.String("full-bucket"),
					Key:    aws.String("key-a"),
				})
				require.ErrorIs(t, err, s3.ErrNoSuchBucket)
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				go newFastJanitor(b).Run(ctx)

				require.Eventually(t, func() bool {
					listed, listErr := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})

					return listErr == nil && len(listed.Buckets) == 0
				}, 500*time.Millisecond, 10*time.Millisecond)
			},
		},
		{
			name: "delete is idempotent",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "idem-bucket")
			},
			act: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("idem-bucket")})
				require.NoError(t, err)

				_, err = b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("idem-bucket")})
				require.NoError(t, err)
			},
			verify: func(_ *testing.T, _ *s3.InMemoryBackend) {},
		},
		{
			name: "orphaned uploads and tags cleaned up when bucket is fully drained",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "cleanup-bucket")

				// Put an object and tag it so there is a tag entry in b.tags.
				mustPutObject(t, b, "cleanup-bucket", "tagged-key", []byte("data"))
				_, err := b.PutObjectTagging(t.Context(), &sdk_s3.PutObjectTaggingInput{
					Bucket: aws.String("cleanup-bucket"),
					Key:    aws.String("tagged-key"),
					Tagging: &sdk_s3types.Tagging{
						TagSet: []sdk_s3types.Tag{{
							Key:   aws.String("env"),
							Value: aws.String("test"),
						}},
					},
				})
				require.NoError(t, err)

				// Start (but do not complete) a multipart upload.
				_, err = b.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
					Bucket: aws.String("cleanup-bucket"),
					Key:    aws.String("mpu-key"),
				})
				require.NoError(t, err)

				// Verify preconditions.
				assert.Equal(t, 1, b.UploadsForBucket("cleanup-bucket"))
				assert.Equal(t, 1, b.TagsForBucket("cleanup-bucket"))
			},
			act: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("cleanup-bucket")})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				go newFastJanitor(b).Run(ctx)

				// Wait until the janitor has fully removed the bucket.
				require.Eventually(t, func() bool {
					return b.UploadsForBucket("cleanup-bucket") == 0 &&
						b.TagsForBucket("cleanup-bucket") == 0
				}, 500*time.Millisecond, 10*time.Millisecond, "orphaned uploads/tags must be cleaned up")
			},
		},
		{
			name: "list buckets excludes pending-delete bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "keep")
				mustCreateBucket(t, b, "gone")
			},
			act: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("gone")})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				listed, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
				require.NoError(t, err)
				assert.Len(t, listed.Buckets, 1)
				assert.Equal(t, "keep", aws.ToString(listed.Buckets[0].Name))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3.NewInMemoryBackend(nil)
			tt.setup(t, b)
			tt.act(t, b)
			tt.verify(t, b)
		})
	}
}

func TestS3Janitor_LifecycleExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *s3.InMemoryBackend)
		verify       func(t *testing.T, b *s3.InMemoryBackend)
		name         string
		bucket       string
		lifecycleXML string
	}{
		{
			name:   "zero days expiry removes all matching objects",
			bucket: "lc-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "lc-bucket")
				mustPutObject(t, b, "lc-bucket", "logs/old.txt", []byte("data"))
				mustPutObject(t, b, "lc-bucket", "logs/old2.txt", []byte("data"))
			},
			lifecycleXML: `<LifecycleConfiguration>
<Rule>
  <ID>expire-all</ID>
  <Status>Enabled</Status>
  <Filter><Prefix>logs/</Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				require.Eventually(t, func() bool {
					out, listErr := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
						Bucket: aws.String("lc-bucket"),
					})

					return listErr == nil && len(out.Contents) == 0
				}, 500*time.Millisecond, 10*time.Millisecond)
			},
		},
		{
			name:   "prefix filter removes only matching objects",
			bucket: "prefix-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "prefix-bucket")
				mustPutObject(t, b, "prefix-bucket", "logs/old.txt", []byte("data"))
				mustPutObject(t, b, "prefix-bucket", "data/keep.txt", []byte("keep"))
			},
			lifecycleXML: `<LifecycleConfiguration>
<Rule>
  <ID>expire-logs</ID>
  <Status>Enabled</Status>
  <Filter><Prefix>logs/</Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				require.Eventually(t, func() bool {
					out, listErr := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
						Bucket: aws.String("prefix-bucket"),
					})
					if listErr != nil {
						return false
					}
					for _, obj := range out.Contents {
						if aws.ToString(obj.Key) == "logs/old.txt" {
							return false
						}
					}

					return true
				}, 500*time.Millisecond, 10*time.Millisecond)

				out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
					Bucket: aws.String("prefix-bucket"),
				})
				require.NoError(t, err)
				assert.Len(t, out.Contents, 1)
				assert.Equal(t, "data/keep.txt", aws.ToString(out.Contents[0].Key))
			},
		},
		{
			name:   "disabled rule does not expire objects",
			bucket: "disabled-bucket",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "disabled-bucket")
				mustPutObject(t, b, "disabled-bucket", "key.txt", []byte("data"))
			},
			lifecycleXML: `<LifecycleConfiguration>
<Rule>
  <ID>disabled-rule</ID>
  <Status>Disabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				// Wait a few janitor ticks then confirm the object is NOT deleted.
				time.Sleep(50 * time.Millisecond)

				out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
					Bucket: aws.String("disabled-bucket"),
				})
				require.NoError(t, err)
				assert.Len(t, out.Contents, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3.NewInMemoryBackend(nil)
			tt.setup(t, b)

			err := b.PutBucketLifecycleConfiguration(t.Context(), tt.bucket, tt.lifecycleXML)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go newFastJanitor(b).Run(ctx)

			tt.verify(t, b)
		})
	}
}

// TestS3Janitor_NoncurrentVersionExpiration verifies that noncurrent object versions
// are deleted when the NoncurrentVersionExpiration rule fires.
func TestS3Janitor_NoncurrentVersionExpiration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		lcXML    string
		wantGone bool
	}{
		{
			name:   "zero noncurrent days removes old noncurrent versions",
			bucket: "nv-expire-bucket",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>nv-expire</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <NoncurrentVersionExpiration><NoncurrentDays>0</NoncurrentDays></NoncurrentVersionExpiration>
</Rule>
</LifecycleConfiguration>`,
			wantGone: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(nil)
			mustCreateBucket(t, backend, tt.bucket)

			// Enable versioning.
			_, err := backend.PutBucketVersioning(
				t.Context(),
				&sdk_s3.PutBucketVersioningInput{
					Bucket: aws.String(tt.bucket),
					VersioningConfiguration: &sdk_s3types.VersioningConfiguration{
						Status: sdk_s3types.BucketVersioningStatusEnabled,
					},
				},
			)
			require.NoError(t, err)

			// Write two versions of the object to create one noncurrent version.
			mustPutObject(t, backend, tt.bucket, "obj.txt", []byte("v1"))
			mustPutObject(t, backend, tt.bucket, "obj.txt", []byte("v2"))

			err = backend.PutBucketLifecycleConfiguration(t.Context(), tt.bucket, tt.lcXML)
			require.NoError(t, err)

			j := newFastJanitor(backend)
			go j.Run(t.Context())

			if tt.wantGone {
				require.Eventually(t, func() bool {
					out, listErr := backend.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
						Bucket: aws.String(tt.bucket),
					})

					return listErr == nil && len(out.Versions) <= 1
				}, 500*time.Millisecond, 10*time.Millisecond)
			}
		})
	}
}

// TestS3Backend_Reset verifies that Reset() clears all stored state from the backend.
func TestS3Backend_Reset(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(nil)
	mustCreateBucket(t, backend, "reset-test")
	mustPutObject(t, backend, "reset-test", "key1", []byte("data"))

	// Confirm state exists before reset.
	out, err := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("reset-test"),
	})
	require.NoError(t, err)
	require.Len(t, out.Contents, 1)

	// Reset clears all buckets and objects.
	backend.Reset()

	// Confirm bucket is gone after reset.
	_, err = backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("reset-test"),
	})
	require.Error(t, err)
}

// TestLifecycle_TagFilter verifies that lifecycle rules with tag filters only
// expire objects whose latest version has the required tags.
func TestLifecycle_TagFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lcXML     string
		taggedKey string   // key to tag with "env=prod"
		wantGone  []string // keys that must be expired
		wantKept  []string // keys that must survive
	}{
		{
			name: "tag_filter_evicts_only_tagged_objects",
			lcXML: `<LifecycleConfiguration>
  <Rule>
    <ID>expire-tagged</ID>
    <Status>Enabled</Status>
    <Filter>
      <Tag>
        <Key>env</Key>
        <Value>prod</Value>
      </Tag>
    </Filter>
    <Expiration><Days>0</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`,
			taggedKey: "tagged-key",
			wantGone:  []string{"tagged-key"},
			wantKept:  []string{"untagged-key"},
		},
		{
			name: "no_tag_filter_evicts_all",
			lcXML: `<LifecycleConfiguration>
  <Rule>
    <ID>expire-all</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Days>0</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`,
			taggedKey: "tagged-key",
			wantGone:  []string{"tagged-key", "untagged-key"},
			wantKept:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(nil)
			bucket := "tag-lc-" + tt.name
			mustCreateBucket(t, backend, bucket)

			// Create two objects: one tagged "env=prod", one without tags.
			mustPutObject(t, backend, bucket, tt.taggedKey, []byte("data"))
			mustPutObject(t, backend, bucket, "untagged-key", []byte("data"))

			// Apply tags to the tagged key.
			_, err := backend.PutObjectTagging(t.Context(), &sdk_s3.PutObjectTaggingInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tt.taggedKey),
				Tagging: &sdk_s3types.Tagging{
					TagSet: []sdk_s3types.Tag{
						{Key: aws.String("env"), Value: aws.String("prod")},
					},
				},
			})
			require.NoError(t, err)

			// Install lifecycle config.
			err = backend.PutBucketLifecycleConfiguration(t.Context(), bucket, tt.lcXML)
			require.NoError(t, err)

			j := newFastJanitor(backend)
			go j.Run(t.Context())

			// Wait for wantGone keys to disappear.
			for _, goneKey := range tt.wantGone {
				require.Eventually(t, func() bool {
					out, listErr := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
						Bucket: aws.String(bucket),
					})
					if listErr != nil {
						return false
					}
					for _, obj := range out.Contents {
						if aws.ToString(obj.Key) == goneKey {
							return false
						}
					}

					return true
				}, 500*time.Millisecond, 10*time.Millisecond,
					"expected key %q to be evicted", goneKey)
			}

			// Verify wantKept keys are still present.
			for _, keptKey := range tt.wantKept {
				out, listErr := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, listErr)

				found := false
				for _, obj := range out.Contents {
					if aws.ToString(obj.Key) == keptKey {
						found = true

						break
					}
				}
				assert.True(t, found, "expected key %q to still exist", keptKey)
			}
		})
	}
}
