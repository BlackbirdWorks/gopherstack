package integration_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_ListMultipartUploads verifies that in-progress multipart uploads are
// returned by ListMultipartUploads and can be filtered by prefix.
func TestIntegration_S3_ListMultipartUploads(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "list returns all in-progress uploads",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "list-mpu-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				// Start two multipart uploads.
				create1, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String("key1"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(create1.UploadId))

				create2, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String("key2"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(create2.UploadId))

				t.Cleanup(func() {
					_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
						Bucket: aws.String(bkt), Key: aws.String("key1"), UploadId: create1.UploadId,
					})
					_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
						Bucket: aws.String(bkt), Key: aws.String("key2"), UploadId: create2.UploadId,
					})
				})

				// ListMultipartUploads should return both.
				listOut, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)
				assert.Len(t, listOut.Uploads, 2)

				// Verify the upload IDs are present.
				gotIDs := make(map[string]bool)
				for _, u := range listOut.Uploads {
					gotIDs[aws.ToString(u.UploadId)] = true
				}
				assert.True(t, gotIDs[aws.ToString(create1.UploadId)], "upload 1 missing from list")
				assert.True(t, gotIDs[aws.ToString(create2.UploadId)], "upload 2 missing from list")
			},
		},
		{
			name: "list returns empty when no uploads in progress",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "list-mpu-empty-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				listOut, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)
				assert.Empty(t, listOut.Uploads)
			},
		},
		{
			name: "list with prefix filters uploads",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "list-mpu-prefix-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				// Start uploads under different prefixes.
				keys := []string{"logs/2024/jan.csv", "logs/2024/feb.csv", "data/raw.csv"}
				uploadIDs := make([]string, len(keys))

				for i, k := range keys {
					c, cerr := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
						Bucket: aws.String(bkt),
						Key:    aws.String(k),
					})
					require.NoError(t, cerr)
					uploadIDs[i] = aws.ToString(c.UploadId)
				}

				t.Cleanup(func() {
					for i, k := range keys {
						_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
							Bucket:   aws.String(bkt),
							Key:      aws.String(k),
							UploadId: aws.String(uploadIDs[i]),
						})
					}
				})

				// List with prefix "logs/" should return only the two log uploads.
				listOut, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bkt),
					Prefix: aws.String("logs/"),
				})
				require.NoError(t, err)
				require.Len(t, listOut.Uploads, 2)

				for _, u := range listOut.Uploads {
					assert.True(t, len(aws.ToString(u.Key)) > len("logs/"), "key too short")
				}
			},
		},
		{
			name: "upload is removed from list after abort",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "list-mpu-abort-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				c, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String("to-abort"),
				})
				require.NoError(t, err)

				// Verify it appears.
				listBefore, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)
				assert.Len(t, listBefore.Uploads, 1)

				// Abort the upload.
				_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bkt),
					Key:      aws.String("to-abort"),
					UploadId: c.UploadId,
				})
				require.NoError(t, err)

				// Should be gone from list.
				listAfter, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)
				assert.Empty(t, listAfter.Uploads)
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

// TestIntegration_S3_ListParts verifies that uploaded parts are returned correctly by ListParts.
func TestIntegration_S3_ListParts(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "list parts returns all uploaded parts in order",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "list-parts-" + uuid.NewString()
				key := "multipart-key"

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				c, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				uploadID := c.UploadId

				t.Cleanup(func() {
					_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
						Bucket: aws.String(bkt), Key: aws.String(key), UploadId: uploadID,
					})
				})

				// Upload three parts (larger than 5MiB for parts 1 and 2; smaller for part 3).
				part1Data := bytes.Repeat([]byte("A"), minPartSize)
				part2Data := bytes.Repeat([]byte("B"), minPartSize)
				part3Data := bytes.Repeat([]byte("C"), lastPartSize)

				up1, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: aws.String(bkt), Key: aws.String(key),
					UploadId: uploadID, PartNumber: aws.Int32(1),
					Body: bytes.NewReader(part1Data),
				})
				require.NoError(t, err)

				up2, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: aws.String(bkt), Key: aws.String(key),
					UploadId: uploadID, PartNumber: aws.Int32(2),
					Body: bytes.NewReader(part2Data),
				})
				require.NoError(t, err)

				up3, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: aws.String(bkt), Key: aws.String(key),
					UploadId: uploadID, PartNumber: aws.Int32(3),
					Body: bytes.NewReader(part3Data),
				})
				require.NoError(t, err)

				// ListParts should return all three parts sorted by part number.
				listOut, err := client.ListParts(ctx, &s3.ListPartsInput{
					Bucket:   aws.String(bkt),
					Key:      aws.String(key),
					UploadId: uploadID,
				})
				require.NoError(t, err)
				require.Len(t, listOut.Parts, 3)

				assert.Equal(t, int32(1), aws.ToInt32(listOut.Parts[0].PartNumber))
				assert.Equal(t, int32(2), aws.ToInt32(listOut.Parts[1].PartNumber))
				assert.Equal(t, int32(3), aws.ToInt32(listOut.Parts[2].PartNumber))

				// Verify ETags match what was returned from UploadPart.
				assert.Equal(t, aws.ToString(up1.ETag), aws.ToString(listOut.Parts[0].ETag))
				assert.Equal(t, aws.ToString(up2.ETag), aws.ToString(listOut.Parts[1].ETag))
				assert.Equal(t, aws.ToString(up3.ETag), aws.ToString(listOut.Parts[2].ETag))

				// Verify sizes.
				assert.Equal(t, int64(minPartSize), aws.ToInt64(listOut.Parts[0].Size))
				assert.Equal(t, int64(minPartSize), aws.ToInt64(listOut.Parts[1].Size))
				assert.Equal(t, int64(lastPartSize), aws.ToInt64(listOut.Parts[2].Size))
			},
		},
		{
			name: "list parts empty when no parts uploaded",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "list-parts-empty-" + uuid.NewString()

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				c, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String("empty-parts"),
				})
				require.NoError(t, err)

				t.Cleanup(func() {
					_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
						Bucket: aws.String(bkt), Key: aws.String("empty-parts"), UploadId: c.UploadId,
					})
				})

				listOut, err := client.ListParts(ctx, &s3.ListPartsInput{
					Bucket:   aws.String(bkt),
					Key:      aws.String("empty-parts"),
					UploadId: c.UploadId,
				})
				require.NoError(t, err)
				assert.Empty(t, listOut.Parts)
			},
		},
		{
			name: "list parts then complete removes from ListMultipartUploads",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "list-parts-complete-" + uuid.NewString()
				key := "to-complete"

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				c, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(key),
				})
				require.NoError(t, err)

				partData := bytes.Repeat([]byte("X"), minPartSize)
				up, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: aws.String(bkt), Key: aws.String(key),
					UploadId: c.UploadId, PartNumber: aws.Int32(1),
					Body: bytes.NewReader(partData),
				})
				require.NoError(t, err)

				// ListParts returns the part.
				listOut, err := client.ListParts(ctx, &s3.ListPartsInput{
					Bucket:   aws.String(bkt),
					Key:      aws.String(key),
					UploadId: c.UploadId,
				})
				require.NoError(t, err)
				require.Len(t, listOut.Parts, 1)

				// Complete the upload.
				_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket:   aws.String(bkt),
					Key:      aws.String(key),
					UploadId: c.UploadId,
					MultipartUpload: &s3types.CompletedMultipartUpload{
						Parts: []s3types.CompletedPart{
							{PartNumber: aws.Int32(1), ETag: up.ETag},
						},
					},
				})
				require.NoError(t, err)

				// Upload should be gone from list.
				listMPU, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)
				assert.Empty(t, listMPU.Uploads)
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
