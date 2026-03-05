package s3_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/s3"
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
