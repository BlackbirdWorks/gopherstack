package integration_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	minPartSize = 5*1024*1024 + 1 // 5 MiB + 1 byte (minimum multipart part size)
	lastPartSize = 1024            // smaller last part is allowed
)

func TestIntegration_S3_MultipartUpload(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "basic multipart upload and verify content",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "mp-basic-" + uuid.NewString()
				key := "multipart-object"

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				createOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				require.NotNil(t, createOut.UploadId)

				uploadID := createOut.UploadId
				part1Data := bytes.Repeat([]byte("A"), minPartSize)
				part2Data := bytes.Repeat([]byte("B"), minPartSize)
				part3Data := bytes.Repeat([]byte("C"), lastPartSize)

				up1, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bkt),
					Key:        aws.String(key),
					UploadId:   uploadID,
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(part1Data),
				})
				require.NoError(t, err)

				up2, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bkt),
					Key:        aws.String(key),
					UploadId:   uploadID,
					PartNumber: aws.Int32(2),
					Body:       bytes.NewReader(part2Data),
				})
				require.NoError(t, err)

				up3, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bkt),
					Key:        aws.String(key),
					UploadId:   uploadID,
					PartNumber: aws.Int32(3),
					Body:       bytes.NewReader(part3Data),
				})
				require.NoError(t, err)

				_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket:   aws.String(bkt),
					Key:      aws.String(key),
					UploadId: uploadID,
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: []types.CompletedPart{
							{PartNumber: aws.Int32(1), ETag: up1.ETag},
							{PartNumber: aws.Int32(2), ETag: up2.ETag},
							{PartNumber: aws.Int32(3), ETag: up3.ETag},
						},
					},
				})
				require.NoError(t, err)

				getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				defer getOut.Body.Close()

				body, err := io.ReadAll(getOut.Body)
				require.NoError(t, err)

				expected := append(append(part1Data, part2Data...), part3Data...)
				assert.Equal(t, expected, body)
			},
		},
		{
			name: "abort multipart upload",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "mp-abort-" + uuid.NewString()
				key := "aborted-object"

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				createOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(key),
				})
				require.NoError(t, err)

				uploadID := createOut.UploadId
				partData := bytes.Repeat([]byte("X"), minPartSize)

				_, err = client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bkt),
					Key:        aws.String(key),
					UploadId:   uploadID,
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(partData),
				})
				require.NoError(t, err)

				_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bkt),
					Key:      aws.String(key),
					UploadId: uploadID,
				})
				require.NoError(t, err)

				// Object should not exist after abort
				_, err = client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(key),
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

func TestIntegration_S3_CopyObject(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "copy object within same bucket",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "copy-same-" + uuid.NewString()
				srcKey := "source-key"
				dstKey := "dest-key"
				content := []byte("hello copy within bucket")

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(srcKey),
					Body:   bytes.NewReader(content),
				})
				require.NoError(t, err)

				_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
					Bucket:     aws.String(bkt),
					Key:        aws.String(dstKey),
					CopySource: aws.String(bkt + "/" + srcKey),
				})
				require.NoError(t, err)

				srcOut, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(srcKey),
				})
				require.NoError(t, err)
				defer srcOut.Body.Close()
				srcBody, err := io.ReadAll(srcOut.Body)
				require.NoError(t, err)

				dstOut, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(dstKey),
				})
				require.NoError(t, err)
				defer dstOut.Body.Close()
				dstBody, err := io.ReadAll(dstOut.Body)
				require.NoError(t, err)

				assert.Equal(t, srcBody, dstBody)
			},
		},
		{
			name: "copy object across buckets",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bktA := "copy-src-" + uuid.NewString()
				bktB := "copy-dst-" + uuid.NewString()
				srcKey := "original"
				dstKey := "copy"
				content := []byte("cross-bucket copy content")

				for _, b := range []string{bktA, bktB} {
					_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
						Bucket: aws.String(b),
					})
					require.NoError(t, err)
				}

				_, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bktA),
					Key:    aws.String(srcKey),
					Body:   bytes.NewReader(content),
				})
				require.NoError(t, err)

				_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
					Bucket:     aws.String(bktB),
					Key:        aws.String(dstKey),
					CopySource: aws.String(bktA + "/" + srcKey),
				})
				require.NoError(t, err)

				dstOut, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bktB),
					Key:    aws.String(dstKey),
				})
				require.NoError(t, err)
				defer dstOut.Body.Close()
				dstBody, err := io.ReadAll(dstOut.Body)
				require.NoError(t, err)

				assert.Equal(t, content, dstBody)
			},
		},
		{
			name: "copy object preserves metadata",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bkt := "copy-meta-" + uuid.NewString()
				srcKey := "with-content-type"
				dstKey := "copied-with-content-type"
				content := []byte("metadata preservation test")
				contentType := "text/plain; charset=utf-8"

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bkt),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket:      aws.String(bkt),
					Key:         aws.String(srcKey),
					Body:        bytes.NewReader(content),
					ContentType: aws.String(contentType),
				})
				require.NoError(t, err)

				_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
					Bucket:     aws.String(bkt),
					Key:        aws.String(dstKey),
					CopySource: aws.String(bkt + "/" + srcKey),
				})
				require.NoError(t, err)

				dstOut, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bkt),
					Key:    aws.String(dstKey),
				})
				require.NoError(t, err)
				defer dstOut.Body.Close()

				assert.Equal(t, contentType, aws.ToString(dstOut.ContentType))
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
