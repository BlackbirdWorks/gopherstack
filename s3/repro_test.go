package s3_test

import (
	"context"
	"strings"
	"testing"

	"Gopherstack/s3"

	"net/http/httptest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aws_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestPutObject_SDKv2_Repro(t *testing.T) {
	// Setup server
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	handler := s3.NewHandler(backend)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Setup SDK client
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKIATEST", "secret", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           server.URL,
				SigningRegion: "us-east-1",
			}, nil
		})),
	)
	require.NoError(t, err)

	client := aws_s3.NewFromConfig(cfg, func(o *aws_s3.Options) {
		o.UsePathStyle = true
	})

	// Create bucket
	_, err = client.CreateBucket(context.TODO(), &aws_s3.CreateBucketInput{
		Bucket: aws.String("test-bucket"),
	})
	require.NoError(t, err)

	// Put Object
	_, err = client.PutObject(context.TODO(), &aws_s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
		Body:   strings.NewReader("content"),
	})
	require.NoError(t, err)
}
