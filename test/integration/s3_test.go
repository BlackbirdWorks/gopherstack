//go:build integration

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	largeObjectSize = 1 << 20 // 1 MiB
)

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
				bkt1 := "alpha-" + uuid.NewString()
				bkt2 := "bravo-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt1),
				})
				require.NoError(t, err)

				_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt2),
				})
				require.NoError(t, err)

				out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
				require.NoError(t, err)

				// Verify our buckets are present
				found := 0
				for _, b := range out.Buckets {
					if *b.Name == bkt1 || *b.Name == bkt2 {
						found++
					}
				}
				assert.Equal(t, 2, found)
			},
		},
		{
			name: "head bucket returns 200 for existing bucket",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "exists-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "delete empty bucket succeeds",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "ephemeral-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				// Verify gone
				out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
				require.NoError(t, err)
				for _, b := range out.Buckets {
					assert.NotEqual(t, bkt, *b.Name)
				}
			},
		},
		{
			name: "delete non-empty bucket fails",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "full-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bkt),
					Key:    aws.String("blocker"),
					Body:   strings.NewReader("data"),
				})
				require.NoError(t, err)

				_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
					Bucket: aws.String(bkt),
				})
				require.Error(t, err)
			},
		},
		{
			name: "duplicate bucket creation fails",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "original-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
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
				bucketName := "bkt-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				content := "Hello S3!"
				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("greeting.txt"),
					Body:   strings.NewReader(content),
				})
				require.NoError(t, err)

				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("greeting.txt"),
				})
				require.NoError(t, err)
				defer got.Body.Close()

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
				bucketName := "bkt-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("to-delete"),
					Body:   strings.NewReader("bye"),
				})
				require.NoError(t, err)

				_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("to-delete"),
				})
				require.NoError(t, err)

				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("to-delete"),
				})
				require.Error(t, err)
				if got != nil && got.Body != nil {
					got.Body.Close()
				}
			},
		},
		{
			name: "overwrite without versioning returns latest",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucketName := "bkt-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v1"),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v2"),
				})
				require.NoError(t, err)

				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
				})
				require.NoError(t, err)
				defer got.Body.Close()

				body, err := io.ReadAll(got.Body)
				require.NoError(t, err)
				assert.Equal(t, "v2", string(body))
			},
		},
		{
			name: "head object returns full metadata and content-type",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucketName := "bkt-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket:      aws.String(bucketName),
					Key:         aws.String("obj"),
					Body:        strings.NewReader("hello"),
					ContentType: aws.String("text/plain"),
					Metadata: map[string]string{
						"Author":   "Antigravity",
						"Priority": "High",
					},
				})
				require.NoError(t, err)

				head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("obj"),
				})
				require.NoError(t, err)

				assert.Equal(t, "text/plain", *head.ContentType)
				assert.Equal(t, "Antigravity", head.Metadata["author"])
				assert.Equal(t, "High", head.Metadata["priority"])
				assert.NotNil(t, head.ContentLength)
				assert.Equal(t, int64(5), *head.ContentLength)
				assert.NotNil(t, head.LastModified)
			},
		},
		{
			name: "large object integrity",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucketName := "bkt-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				// Create 1MiB payload
				payload := bytes.Repeat([]byte("A"), largeObjectSize)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("big.bin"),
					Body:   bytes.NewReader(payload),
				})
				require.NoError(t, err)

				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("big.bin"),
				})
				require.NoError(t, err)
				defer got.Body.Close()

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
			bucket := "listing-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err)

			for _, key := range tt.keys {
				_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   strings.NewReader("data"),
				})
				require.NoError(t, putErr)
			}

			out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(bucket),
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

	bktA := "iso-a-" + uuid.NewString()
	bktB := "iso-b-" + uuid.NewString()

	// Create two buckets
	for _, name := range []string{bktA, bktB} {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(name),
		})
		require.NoError(t, err)
	}

	// Put objects in each
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bktA),
		Key:    aws.String("only-in-a"),
		Body:   strings.NewReader("a-data"),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bktB),
		Key:    aws.String("only-in-b"),
		Body:   strings.NewReader("b-data"),
	})
	require.NoError(t, err)

	// Verify bucket-a only has its object
	outA, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bktA),
	})
	require.NoError(t, err)
	require.Len(t, outA.Contents, 1)
	assert.Equal(t, "only-in-a", *outA.Contents[0].Key)

	// Verify bucket-b only has its object
	outB, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bktB),
	})
	require.NoError(t, err)
	require.Len(t, outB.Contents, 1)
	assert.Equal(t, "only-in-b", *outB.Contents[0].Key)

	// Cross-bucket get should fail
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bktA),
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
				bucketName := "ver-" + uuid.NewString()

				out, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				dumpSDKOutput(out)
				require.NoError(t, err)

				_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket: aws.String(bucketName),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				ver, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
					Bucket: aws.String(bucketName),
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
				bucketName := "ver-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				// Put unversioned (null version)
				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v0-unversioned"),
				})
				require.NoError(t, err)

				// Enable versioning
				_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket: aws.String(bucketName),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				// Put V1
				putV1, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v1-data"),
				})
				require.NoError(t, err)
				require.NotNil(t, putV1.VersionId)

				// Put V2
				putV2, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
					Body:   strings.NewReader("v2-data"),
				})
				require.NoError(t, err)
				require.NotNil(t, putV2.VersionId)
				assert.NotEqual(t, *putV1.VersionId, *putV2.VersionId)

				// Get latest = V2
				latest, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
				})
				require.NoError(t, err)
				defer latest.Body.Close()

				body, err := io.ReadAll(latest.Body)
				require.NoError(t, err)
				assert.Equal(t, "v2-data", string(body))

				// Get V1 by version ID
				v1, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket:    aws.String(bucketName),
					Key:       aws.String("key"),
					VersionId: putV1.VersionId,
				})
				require.NoError(t, err)
				defer v1.Body.Close()

				bodyV1, err := io.ReadAll(v1.Body)
				require.NoError(t, err)
				assert.Equal(t, "v1-data", string(bodyV1))

				// Get null (pre-versioning) version
				v0, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket:    aws.String(bucketName),
					Key:       aws.String("key"),
					VersionId: aws.String("null"),
				})
				require.NoError(t, err)
				defer v0.Body.Close()

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
				bucketName := "ver-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err)

				_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket: aws.String(bucketName),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				putOut, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
					Body:   strings.NewReader("data"),
				})
				require.NoError(t, err)
				require.NotNil(t, putOut.VersionId)

				// Delete creates a delete marker
				delOut, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
				})
				require.NoError(t, err)

				if delOut.DeleteMarker != nil {
					assert.True(t, *delOut.DeleteMarker)
				}

				// Get latest should fail (deleted)
				got, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("key"),
				})
				require.Error(t, err)
				if got != nil && got.Body != nil {
					got.Body.Close()
				}

				// But specific version still accessible
				old, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket:    aws.String(bucketName),
					Key:       aws.String("key"),
					VersionId: putOut.VersionId,
				})
				require.NoError(t, err)
				defer old.Body.Close()

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
			bucket := "tag-" + uuid.NewString()
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
func TestS3ListObjectsV2(t *testing.T) {
	t.Parallel()

	t.Run("basic listing returns all keys", func(t *testing.T) {
		t.Parallel()

		client := createS3Client(t)
		ctx := t.Context()
		bucketName := "v2-basic-" + uuid.NewString()

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
		require.NoError(t, err)

		for _, key := range []string{"a/1", "a/2", "b/1"} {
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, putErr)
		}

		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
		require.NoError(t, err)
		assert.EqualValues(t, 3, *out.KeyCount)
		assert.False(t, *out.IsTruncated)
		assert.Len(t, out.Contents, 3)
	})

	t.Run("pagination with max-keys and continuation token", func(t *testing.T) {
		t.Parallel()

		client := createS3Client(t)
		ctx := t.Context()
		bucketName := "v2-paged-" + uuid.NewString()

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
		require.NoError(t, err)

		keys := []string{"k1", "k2", "k3", "k4", "k5"}
		for _, key := range keys {
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, putErr)
		}

		// First page of 2
		page1, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(2),
		})
		require.NoError(t, err)
		assert.Len(t, page1.Contents, 2)
		assert.True(t, *page1.IsTruncated)
		require.NotNil(t, page1.NextContinuationToken)

		// Second page of 2
		page2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			MaxKeys:           aws.Int32(2),
			ContinuationToken: page1.NextContinuationToken,
		})
		require.NoError(t, err)
		assert.Len(t, page2.Contents, 2)
		assert.True(t, *page2.IsTruncated)

		// Third page: last 1 item
		page3, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			MaxKeys:           aws.Int32(2),
			ContinuationToken: page2.NextContinuationToken,
		})
		require.NoError(t, err)
		assert.Len(t, page3.Contents, 1)
		assert.False(t, *page3.IsTruncated)

		// All pages together cover all keys
		allKeys := make([]string, 0, len(page1.Contents)+len(page2.Contents)+len(page3.Contents))
		for _, c := range page1.Contents {
			allKeys = append(allKeys, *c.Key)
		}
		for _, c := range page2.Contents {
			allKeys = append(allKeys, *c.Key)
		}
		for _, c := range page3.Contents {
			allKeys = append(allKeys, *c.Key)
		}
		assert.ElementsMatch(t, keys, allKeys)
	})

	t.Run("delimiter groups keys into common prefixes", func(t *testing.T) {
		t.Parallel()

		client := createS3Client(t)
		ctx := t.Context()
		bucketName := "v2-delim-" + uuid.NewString()

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
		require.NoError(t, err)

		for _, key := range []string{"docs/a.md", "docs/b.md", "images/c.png", "readme.txt"} {
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, putErr)
		}

		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)
		assert.Len(t, out.Contents, 1) // only readme.txt
		assert.Equal(t, "readme.txt", *out.Contents[0].Key)
		assert.Len(t, out.CommonPrefixes, 2)

		prefixes := []string{*out.CommonPrefixes[0].Prefix, *out.CommonPrefixes[1].Prefix}
		assert.ElementsMatch(t, []string{"docs/", "images/"}, prefixes)
	})

	t.Run("start-after skips keys up to and including the value", func(t *testing.T) {
		t.Parallel()

		client := createS3Client(t)
		ctx := t.Context()
		bucketName := "v2-startafter-" + uuid.NewString()

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
		require.NoError(t, err)

		for _, key := range []string{"apple", "banana", "cherry", "date"} {
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, putErr)
		}

		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:     aws.String(bucketName),
			StartAfter: aws.String("banana"),
		})
		require.NoError(t, err)
		require.Len(t, out.Contents, 2)
		assert.Equal(t, "cherry", *out.Contents[0].Key)
		assert.Equal(t, "date", *out.Contents[1].Key)
	})

	t.Run("empty bucket returns zero keys", func(t *testing.T) {
		t.Parallel()

		client := createS3Client(t)
		ctx := t.Context()
		bucketName := "v2-empty-" + uuid.NewString()

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
		require.NoError(t, err)

		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
		require.NoError(t, err)
		assert.Empty(t, out.Contents)
		assert.EqualValues(t, 0, *out.KeyCount)
		assert.False(t, *out.IsTruncated)
	})

	t.Run("non-existent bucket returns error", func(t *testing.T) {
		t.Parallel()

		client := createS3Client(t)
		ctx := t.Context()

		_, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("no-such-bucket-" + uuid.NewString())})
		require.Error(t, err)
	})

	t.Run("prefix filter with delimiter", func(t *testing.T) {
		t.Parallel()

		client := createS3Client(t)
		ctx := t.Context()
		bucketName := "v2-prefix-delim-" + uuid.NewString()

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
		require.NoError(t, err)

		for _, key := range []string{"a/b/1", "a/b/2", "a/c/1", "b/1"} {
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, putErr)
		}

		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Prefix:    aws.String("a/"),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)
		assert.Empty(t, out.Contents)
		assert.Len(t, out.CommonPrefixes, 2)

		prefixes := []string{*out.CommonPrefixes[0].Prefix, *out.CommonPrefixes[1].Prefix}
		assert.ElementsMatch(t, []string{"a/b/", "a/c/"}, prefixes)
	})
}

func TestS3ChecksumSHA256(t *testing.T) {
	t.Parallel()

	client := createS3Client(t)
	ctx := t.Context()
	bucket := "checksum-" + uuid.NewString()
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
	defer out.Body.Close()
	assert.NotNil(t, out.ChecksumSHA256)

	// Verify manual calculation matches
	h := sha256.Sum256([]byte(content))
	expectedChecksum := base64.StdEncoding.EncodeToString(h[:])
	assert.Equal(t, expectedChecksum, *out.ChecksumSHA256)
}
