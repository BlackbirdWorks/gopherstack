package integration_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_S3_PresignedURLs(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		verify func(t *testing.T, client *s3.Client)
		name   string
	}{
		{
			name: "presigned GET retrieves object",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucket := "presign-get-" + uuid.NewString()
				key := "hello.txt"
				content := []byte("presigned content")

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   bytes.NewReader(content),
				})
				require.NoError(t, err)

				// Generate presigned GET URL valid for 1 hour.
				presignClient := s3.NewPresignClient(client)
				presigned, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				}, func(o *s3.PresignOptions) {
					o.Expires = time.Hour
				})
				require.NoError(t, err)
				require.NotEmpty(t, presigned.URL)

				// Fetch via the presigned URL.
				resp, err := http.Get(presigned.URL) //nolint:noctx // test helper
				require.NoError(t, err)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusOK, resp.StatusCode)

				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				assert.Equal(t, content, body)
			},
		},
		{
			name: "presigned PUT uploads object",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucket := "presign-put-" + uuid.NewString()
				key := "uploaded.txt"
				content := []byte("uploaded via presigned PUT")

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)

				// Generate presigned PUT URL valid for 1 hour.
				presignClient := s3.NewPresignClient(client)
				presigned, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				}, func(o *s3.PresignOptions) {
					o.Expires = time.Hour
				})
				require.NoError(t, err)
				require.NotEmpty(t, presigned.URL)

				// Upload via the presigned URL.
				req, err := http.NewRequestWithContext(ctx, http.MethodPut, presigned.URL,
					bytes.NewReader(content))
				require.NoError(t, err)

				httpClient := &http.Client{}
				resp, err := httpClient.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusOK, resp.StatusCode)

				// Verify object is retrievable via the normal SDK client.
				out, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				defer out.Body.Close()

				body, err := io.ReadAll(out.Body)
				require.NoError(t, err)
				assert.Equal(t, content, body)
			},
		},
		{
			name: "expired presigned URL returns 403",
			verify: func(t *testing.T, client *s3.Client) {
				t.Helper()
				ctx := t.Context()
				bucket := "presign-expired-" + uuid.NewString()
				key := "file.txt"

				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   bytes.NewReader([]byte("data")),
				})
				require.NoError(t, err)

				// Generate presigned URL with a 1 second expiry, then wait for it to expire.
				presignClient := s3.NewPresignClient(client)
				presigned, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				}, func(o *s3.PresignOptions) {
					o.Expires = time.Second
				})
				require.NoError(t, err)

				// Wait for expiry.
				time.Sleep(2 * time.Second)

				resp, err := http.Get(presigned.URL) //nolint:noctx // test helper
				require.NoError(t, err)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusForbidden, resp.StatusCode)
			},
		},
	}

	s3Client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.verify(t, s3Client)
		})
	}
}
