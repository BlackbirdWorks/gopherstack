package demo_test

import (
	"context"
	"log/slog"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"Gopherstack/dashboard"
	"Gopherstack/demo"
	ddbbackend "Gopherstack/dynamodb"
	s3backend "Gopherstack/s3"
)

func TestLoadData(t *testing.T) {
	t.Parallel()
	// Setup Backends
	ddbHandler := ddbbackend.NewHandler(slog.Default())
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend, slog.Default())

	// Setup Mux
	apiMux := http.NewServeMux()
	apiMux.Handle("/", ddbHandler)
	apiMux.Handle("/s3", http.StripPrefix("/s3", s3Handler))
	apiMux.Handle("/s3/", http.StripPrefix("/s3", s3Handler))

	// Setup Client
	inMemClient := &dashboard.InMemClient{Handler: apiMux}

	// Setup AWS Config
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		),
		config.WithHTTPClient(inMemClient),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local/s3")
	})

	// Run LoadData
	err = demo.LoadData(context.Background(), slog.Default(), ddbClient, s3Client)
	require.NoError(t, err)

	// Verify DynamoDB
	tableName := "Movies"
	items, err := ddbClient.Scan(context.Background(), &dynamodb.ScanInput{TableName: &tableName})
	require.NoError(t, err)
	assert.Equal(t, int32(2), items.Count)

	// Verify S3
	bucketName := "demo-bucket"
	objects, err := s3Client.ListObjectsV2(
		context.Background(),
		&s3.ListObjectsV2Input{Bucket: &bucketName},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(2), *objects.KeyCount)
}
