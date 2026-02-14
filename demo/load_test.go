package demo

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"

	"Gopherstack/dashboard"
	ddbbackend "Gopherstack/dynamodb"
	s3backend "Gopherstack/s3"
)

func TestLoadData(t *testing.T) {
	// Setup Backends
	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend)

	// Setup Mux
	apiMux := http.NewServeMux()
	apiMux.Handle("/", ddbHandler)
	apiMux.Handle("/s3", http.StripPrefix("/s3", s3Handler))
	apiMux.Handle("/s3/", http.StripPrefix("/s3", s3Handler))

	// Setup Client
	inMemClient := &dashboard.InMemClient{Handler: apiMux}

	// Setup AWS Config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return aws.Endpoint{URL: "http://local/s3", SigningRegion: "us-east-1"}, nil
				}
				return aws.Endpoint{URL: "http://local", SigningRegion: "us-east-1"}, nil
			},
		)),
	)
	assert.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Run LoadData
	err = LoadData(context.Background(), ddbClient, s3Client)
	assert.NoError(t, err)

	// Verify DynamoDB
	tableName := "Movies"
	items, err := ddbClient.Scan(context.Background(), &dynamodb.ScanInput{TableName: &tableName})
	assert.NoError(t, err)
	assert.Equal(t, int32(2), items.Count)

	// Verify S3
	bucketName := "demo-bucket"
	objects, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: &bucketName})
	assert.NoError(t, err)
	assert.Equal(t, int32(2), *objects.KeyCount)
}
