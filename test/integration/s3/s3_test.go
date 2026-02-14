package s3_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gophers3 "Gopherstack/s3"
)

const (
	largeObjectSize = 1 << 20 // 1 MiB
)

// createS3Client sets up a local httptest server and returns an S3 client.
func createS3Client(t *testing.T) *s3.Client {
	t.Helper()

	backend := gophers3.NewInMemoryBackend(&gophers3.GzipCompressor{})
	handler := gophers3.NewHandler(backend)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(server.URL)
	})

	return client
}

func TestS3BucketLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "create and list buckets",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("alpha"),
				})
				require.NoError(t, err)

				_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bravo"),
				})
				require.NoError(t, err)

				out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
				require.NoError(t, err)
				require.Len(t, out.Buckets, 2)
				assert.Equal(t, "alpha", *out.Buckets[0].Name)
				assert.Equal(t, "bravo", *out.Buckets[1].Name)
			},
		},
		{
			name: "head bucket returns 200 for existing bucket",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("exists"),
				})
				require.NoError(t, err)

				_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
					Bucket: aws.String("exists"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "delete empty bucket succeeds",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("ephemeral"),
				})
				require.NoError(t, err)

				_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
					Bucket: aws.String("ephemeral"),
				})
				require.NoError(t, err)

				// Verify gone
				out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
				require.NoError(t, err)
				assert.Empty(t, out.Buckets)
			},
		},
		{
			name: "delete non-empty bucket fails",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("full"),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("full"),
					Key:    aws.String("blocker"),
					Body:   strings.NewReader("data"),
				})
				require.NoError(t, err)

				_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
					Bucket: aws.String("full"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "duplicate bucket creation fails",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("original"),
				})
				require.NoError(t, err)

				_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("original"),
				})
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createS3Client(t)
			tt.verify(t, client)
		})
	}
}

func TestS3ObjectCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "put and get object round-trip",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)

				content := "Hello S3!"
				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("greeting.txt"),
					Body:   strings.NewReader(content),
				})
				require.NoError(t, err)

				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("greeting.txt"),
				})
				require.NoError(t, err)

				body, err := io.ReadAll(got.Body)
				require.NoError(t, err)
				assert.Equal(t, content, string(body))
			},
		},
		{
			name: "delete object then get returns 404",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("to-delete"),
					Body:   strings.NewReader("bye"),
				})
				require.NoError(t, err)

				_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("to-delete"),
				})
				require.NoError(t, err)

				_, err = client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("to-delete"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "overwrite without versioning returns latest",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v1"),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v2"),
				})
				require.NoError(t, err)

				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
				})
				require.NoError(t, err)

				body, err := io.ReadAll(got.Body)
				require.NoError(t, err)
				assert.Equal(t, "v2", string(body))
			},
		},
		{
			name: "head object returns metadata",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("obj"),
					Body:   strings.NewReader("hello"),
				})
				require.NoError(t, err)

				head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("obj"),
				})
				require.NoError(t, err)
				assert.NotNil(t, head.ContentLength)
			},
		},
		{
			name: "large object integrity",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("large"),
				})
				require.NoError(t, err)

				// Create 1MiB payload
				payload := bytes.Repeat([]byte("A"), largeObjectSize)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("large"),
					Key:    aws.String("big.bin"),
					Body:   bytes.NewReader(payload),
				})
				require.NoError(t, err)

				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String("large"),
					Key:    aws.String("big.bin"),
				})
				require.NoError(t, err)

				body, err := io.ReadAll(got.Body)
				require.NoError(t, err)
				assert.Len(t, body, largeObjectSize)
				assert.Equal(t, payload, body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createS3Client(t)
			tt.verify(t, client)
		})
	}
}

func TestS3PrefixListing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		keys   []string
		want   int
	}{
		{
			name:   "filter by docs/ prefix",
			keys:   []string{"docs/a.md", "docs/b.md", "images/c.png", "readme.txt"},
			prefix: "docs/",
			want:   2,
		},
		{
			name:   "filter by images/ prefix",
			keys:   []string{"docs/a.md", "images/c.png", "images/d.jpg"},
			prefix: "images/",
			want:   2,
		},
		{
			name:   "no prefix returns all",
			keys:   []string{"a", "b", "c"},
			prefix: "",
			want:   3,
		},
		{
			name:   "prefix matches none",
			keys:   []string{"foo/bar", "baz/qux"},
			prefix: "xyz/",
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createS3Client(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String("listing"),
			})
			require.NoError(t, err)

			for _, key := range tt.keys {
				_, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("listing"),
					Key:    aws.String(key),
					Body:   strings.NewReader("data"),
				})
				require.NoError(t, err)
			}

			out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String("listing"),
				Prefix: aws.String(tt.prefix),
			})
			require.NoError(t, err)
			assert.Len(t, out.Contents, tt.want)
		})
	}
}

func TestS3BucketIsolation(t *testing.T) {
	t.Parallel()

	client := createS3Client(t)
	ctx := t.Context()

	// Create two buckets
	for _, name := range []string{"bucket-a", "bucket-b"} {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(name),
		})
		require.NoError(t, err)
	}

	// Put objects in each
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("bucket-a"),
		Key:    aws.String("only-in-a"),
		Body:   strings.NewReader("a-data"),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("bucket-b"),
		Key:    aws.String("only-in-b"),
		Body:   strings.NewReader("b-data"),
	})
	require.NoError(t, err)

	// Verify bucket-a only has its object
	outA, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("bucket-a"),
	})
	require.NoError(t, err)
	require.Len(t, outA.Contents, 1)
	assert.Equal(t, "only-in-a", *outA.Contents[0].Key)

	// Verify bucket-b only has its object
	outB, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("bucket-b"),
	})
	require.NoError(t, err)
	require.Len(t, outB.Contents, 1)
	assert.Equal(t, "only-in-b", *outB.Contents[0].Key)

	// Cross-bucket get should fail
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("bucket-a"),
		Key:    aws.String("only-in-b"),
	})
	require.Error(t, err)
}

func TestS3VersioningLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "enable versioning and verify status",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)

				_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket: aws.String("bkt"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				ver, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.BucketVersioningStatusEnabled, ver.Status)
			},
		},
		{
			name: "versioned puts create unique IDs and both retrievable",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)

				// Put unversioned (null version)
				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v0-unversioned"),
				})
				require.NoError(t, err)

				// Enable versioning
				_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket: aws.String("bkt"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				// Put V1
				putV1, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v1-data"),
				})
				require.NoError(t, err)
				require.NotNil(t, putV1.VersionId)

				// Put V2
				putV2, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v2-data"),
				})
				require.NoError(t, err)
				require.NotNil(t, putV2.VersionId)
				assert.NotEqual(t, *putV1.VersionId, *putV2.VersionId)

				// Get latest = V2
				latest, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
				})
				require.NoError(t, err)

				body, err := io.ReadAll(latest.Body)
				require.NoError(t, err)
				assert.Equal(t, "v2-data", string(body))

				// Get V1 by version ID
				v1, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket:    aws.String("bkt"),
					Key:       aws.String("key"),
					VersionId: putV1.VersionId,
				})
				require.NoError(t, err)

				bodyV1, err := io.ReadAll(v1.Body)
				require.NoError(t, err)
				assert.Equal(t, "v1-data", string(bodyV1))

				// Get null (pre-versioning) version
				v0, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket:    aws.String("bkt"),
					Key:       aws.String("key"),
					VersionId: aws.String("null"),
				})
				require.NoError(t, err)

				bodyV0, err := io.ReadAll(v0.Body)
				require.NoError(t, err)
				assert.Equal(t, "v0-unversioned", string(bodyV0))
			},
		},
		{
			name: "delete with versioning creates delete marker and old versions remain",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String("bkt"),
				})
				require.NoError(t, err)

				_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket: aws.String("bkt"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				putOut, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
					Body:   strings.NewReader("data"),
				})
				require.NoError(t, err)
				require.NotNil(t, putOut.VersionId)

				// Delete creates a delete marker
				delOut, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
				})
				require.NoError(t, err)

				if delOut.DeleteMarker != nil {
					assert.True(t, *delOut.DeleteMarker)
				}

				// Get latest should fail (deleted)
				_, err = client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
				})
				require.Error(t, err)

				// But specific version still accessible
				old, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket:    aws.String("bkt"),
					Key:       aws.String("key"),
					VersionId: putOut.VersionId,
				})
				require.NoError(t, err)

				body, err := io.ReadAll(old.Body)
				require.NoError(t, err)
				assert.Equal(t, "data", string(body))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createS3Client(t)
			tt.verify(t, client)
		})
	}
}

func TestS3TaggingRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, client *s3.Client, bucket, key string)
		name   string
		tags   []types.Tag
	}{
		{
			name: "put and get single tag",
			tags: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
			},
			verify: func(t *testing.T, client *s3.Client, bucket, key string) {
				t.Helper()

				out, err := client.GetObjectTagging(t.Context(), &s3.GetObjectTaggingInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				require.Len(t, out.TagSet, 1)
				assert.Equal(t, "env", *out.TagSet[0].Key)
				assert.Equal(t, "prod", *out.TagSet[0].Value)
			},
		},
		{
			name: "put and get multiple tags",
			tags: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("staging")},
				{Key: aws.String("team"), Value: aws.String("backend")},
				{Key: aws.String("cost-center"), Value: aws.String("eng-42")},
			},
			verify: func(t *testing.T, client *s3.Client, bucket, key string) {
				t.Helper()

				out, err := client.GetObjectTagging(t.Context(), &s3.GetObjectTaggingInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				require.Len(t, out.TagSet, 3)

				tagMap := make(map[string]string)
				for _, tag := range out.TagSet {
					tagMap[*tag.Key] = *tag.Value
				}

				assert.Equal(t, "staging", tagMap["env"])
				assert.Equal(t, "backend", tagMap["team"])
				assert.Equal(t, "eng-42", tagMap["cost-center"])
			},
		},
		{
			name: "delete tags then get returns empty",
			tags: []types.Tag{
				{Key: aws.String("temp"), Value: aws.String("val")},
			},
			verify: func(t *testing.T, client *s3.Client, bucket, key string) {
				t.Helper()
				ctx := t.Context()

				_, err := client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)

				out, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				assert.Empty(t, out.TagSet)
			},
		},
		{
			name: "overwrite tags replaces set",
			tags: []types.Tag{
				{Key: aws.String("original"), Value: aws.String("yes")},
			},
			verify: func(t *testing.T, client *s3.Client, bucket, key string) {
				t.Helper()
				ctx := t.Context()

				// Overwrite with different tags
				_, err := client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Tagging: &types.Tagging{
						TagSet: []types.Tag{
							{Key: aws.String("replaced"), Value: aws.String("true")},
						},
					},
				})
				require.NoError(t, err)

				out, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				require.Len(t, out.TagSet, 1)
				assert.Equal(t, "replaced", *out.TagSet[0].Key)
				assert.Equal(t, "true", *out.TagSet[0].Value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createS3Client(t)
			ctx := t.Context()
			bucket := "tagging-test"
			key := "tagged-obj"

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err)

			_, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, err)

			_, err = client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Tagging: &types.Tagging{
					TagSet: tt.tags,
				},
			})
			require.NoError(t, err)

			tt.verify(t, client, bucket, key)
		})
	}
}
func TestS3ChecksumSHA256(t *testing.T) {
	t.Parallel()

	client := createS3Client(t)
	ctx := t.Context()
	bucket := "checksum-test"
	key := "sha256-obj"
	content := "checksum data"

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	// Put with SHA256 auto-calculation
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              strings.NewReader(content),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	// Verify via GetObject
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	require.NoError(t, err)
	assert.NotNil(t, out.ChecksumSHA256)

	// Verify manual calculation matches
	h := sha256.Sum256([]byte(content))
	expectedChecksum := base64.StdEncoding.EncodeToString(h[:])
	assert.Equal(t, expectedChecksum, *out.ChecksumSHA256)
}
