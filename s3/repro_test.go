package s3_test

import (
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"Gopherstack/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aws_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestPutObject_SDKv2_Repro(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bucket string
		key    string
		body   string
	}{
		{
			name:   "put object via SDK v2",
			bucket: "test-bucket",
			key:    "test-key",
			body:   "content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			handler := s3.NewHandler(backend, slog.Default())
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)

			cfg, err := config.LoadDefaultConfig(
				t.Context(),
				config.WithRegion("us-east-1"),
				config.WithCredentialsProvider(
					credentials.NewStaticCredentialsProvider("AKIATEST", "secret", ""),
				),
			)
			require.NoError(t, err)

			client := aws_s3.NewFromConfig(cfg, func(o *aws_s3.Options) {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String(server.URL)
			})

			_, err = client.CreateBucket(t.Context(), &aws_s3.CreateBucketInput{
				Bucket: aws.String(tt.bucket),
			})
			require.NoError(t, err)

			_, err = client.PutObject(t.Context(), &aws_s3.PutObjectInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String(tt.key),
				Body:   strings.NewReader(tt.body),
			})
			require.NoError(t, err)
		})
	}
}
