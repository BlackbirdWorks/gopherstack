package dynamodb_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

const ddbTagsRTRegion = "us-east-1"

// newTestDynamoDBClient stands up the real aws-sdk-go-v2 dynamodb client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestDynamoDBClient(t *testing.T, h *dynamodb.DynamoDBHandler) *dynamodbsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(ddbTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return dynamodbsdk.NewFromConfig(cfg, func(o *dynamodbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateTable_TagsRoundTrip drives CreateTable, whose real Input struct
// accepts Tags (dynamodb@v1.63.1 api_op_CreateTable.go:260), through the real
// SDK client and asserts ListTagsOfResource sees what was supplied at
// creation (gopherstack-2mwl).
func TestCreateTable_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	out, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName: aws.String("tagged-table"),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		Tags:        []dynamodbtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	got, err := client.ListTagsOfResource(t.Context(), &dynamodbsdk.ListTagsOfResourceInput{
		ResourceArn: out.TableDescription.TableArn,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
}
