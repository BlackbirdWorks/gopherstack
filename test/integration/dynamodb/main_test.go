//go:build integration

package dynamodb_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	endpoint string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Define the container request
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../../../", // Root of the project
			Dockerfile: "Dockerfile",
			BuildArgs: map[string]*string{
				"BUILDKIT_INLINE_CACHE": stringPtr("0"),
			},
			PrintBuildLog: true, // Show build progress
		},
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForHTTP("/").WithStatusCodeMatcher(func(status int) bool {
			return true
		}).WithStartupTimeout(60 * time.Second),
	}

	// Start the container
	gopherstackC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Printf("Error starting container: %s\n", err)
		os.Exit(1)
	}

	// Get the mapped port
	mappedPort, err := gopherstackC.MappedPort(ctx, "8000")
	if err != nil {
		fmt.Printf("Error getting mapped port: %s\n", err)
		os.Exit(1)
	}

	endpoint = fmt.Sprintf("http://localhost:%s", mappedPort.Port())
	fmt.Printf("Gopherstack running at %s\n", endpoint)

	// Give it a split second to be fully ready to accept connections if needed
	time.Sleep(1 * time.Second)

	// Run tests
	code := m.Run()

	// Terminate container
	if err := gopherstackC.Terminate(ctx); err != nil {
		fmt.Printf("Error terminating container: %s\n", err)
	}

	os.Exit(code)
}

// createDynamoDBClient creates a new DynamoDB client connected to the test container
func createDynamoDBClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           endpoint,
				SigningRegion: "us-east-1",
			}, nil
		})),
	)
	if err != nil {
		t.Fatalf("unable to load SDK config, %v", err)
	}

	return dynamodb.NewFromConfig(cfg)
}

// stringPtr returns a pointer to the string value.
func stringPtr(s string) *string {
	return &s
}
