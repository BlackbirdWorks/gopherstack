package integration_test

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_S3_ConcurrentBucketOps(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name       string
		goroutines int
		iterations int
	}{
		{
			name:       "100_goroutines_10_iterations",
			goroutines: 100,
			iterations: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)
			ctx := t.Context()
			s3Client := createS3Client(t)

			var wg sync.WaitGroup

			for id := range tt.goroutines {
				wg.Go(func() {
					for j := range tt.iterations {
						bucketName := fmt.Sprintf("concurrent-bucket-%d-%d", id, j)
						key := "stress-test-object"
						data := fmt.Appendf(nil, "data-from-worker-%d-iteration-%d", id, j)

						_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
							Bucket: aws.String(bucketName),
						})
						require.NoError(t, err, "Worker %d iteration %d: CreateBucket failed", id, j)

						_, err = s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
							Bucket: aws.String(bucketName),
							VersioningConfiguration: &types.VersioningConfiguration{
								Status: types.BucketVersioningStatusEnabled,
							},
						})
						require.NoError(t, err, "Worker %d iteration %d: PutBucketVersioning failed", id, j)

						listRes, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
						require.NoError(t, err, "Worker %d iteration %d: ListBuckets failed", id, j)

						found := false
						for _, b := range listRes.Buckets {
							if *b.Name == bucketName {
								found = true

								break
							}
						}
						msg := fmt.Sprintf(
							"Worker %d iteration %d: Bucket %s should be present",
							id,
							j,
							bucketName,
						)
						if !assert.True(t, found, msg) {
							return
						}

						_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(key),
							Body:   bytes.NewReader(data),
						})
						require.NoError(t, err, "Worker %d iteration %d: PutObject failed", id, j)

						listObjs, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
							Bucket: aws.String(bucketName),
						})
						require.NoError(t, err, "Worker %d iteration %d: ListObjectsV2 failed", id, j)

						if !assert.Len(
							t,
							listObjs.Contents,
							1,
							"Worker %d iteration %d: ListObjectsV2 contents len",
							id,
							j,
						) {
							return
						}
						assert.Equal(
							t,
							key,
							*listObjs.Contents[0].Key,
							"Worker %d iteration %d: ListObjectsV2 key match",
							id,
							j,
						)

						getObj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(key),
						})
						require.NoError(t, err, "Worker %d iteration %d: GetObject failed", id, j)

						body, err := io.ReadAll(getObj.Body)
						if err != nil {
							getObj.Body.Close()
							require.NoError(t, err, "Worker %d iteration %d: ReadAll Body failed", id, j)
						}
						assert.Equal(t, data, body, "Worker %d iteration %d: Data mismatch", id, j)
						getObj.Body.Close()

						_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(key),
						})
						require.NoError(t, err, "Worker %d iteration %d: DeleteObject failed", id, j)

						versions, err := s3Client.ListObjectVersions(
							ctx,
							&s3.ListObjectVersionsInput{
								Bucket: aws.String(bucketName),
							},
						)
						require.NoError(t, err, "Worker %d iteration %d: ListObjectVersions failed", id, j)

						for _, v := range versions.Versions {
							_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
								Bucket:    aws.String(bucketName),
								Key:       v.Key,
								VersionId: v.VersionId,
							})
							require.NoError(t, err, "Worker %d iteration %d: DeleteObject (version) failed", id, j)
						}

						for _, d := range versions.DeleteMarkers {
							_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
								Bucket:    aws.String(bucketName),
								Key:       d.Key,
								VersionId: d.VersionId,
							})
							require.NoError(t, err, "Worker %d iteration %d: DeleteObject (marker) failed", id, j)
						}

						_, err = s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
							Bucket: aws.String(bucketName),
						})
						require.NoError(t, err, "Worker %d iteration %d: DeleteBucket failed", id, j)
					}
				})
			}

			wg.Wait()
		})
	}
}
