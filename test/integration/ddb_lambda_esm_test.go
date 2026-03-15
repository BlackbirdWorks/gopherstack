package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdaesdktypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Lambda_DynamoDB_ESM_CRUD verifies that a DynamoDB Stream ARN can be
// used as the EventSourceArn for a Lambda event source mapping via the CRUD API.
//
// This test validates the DDB stream ESM wiring end-to-end at the API layer:
//  1. Create a DynamoDB table with streaming enabled.
//  2. Retrieve the stream ARN from DescribeTable.
//  3. Create a Lambda ESM pointing at the stream ARN.
//  4. Verify the ESM is stored and can be retrieved.
//  5. Verify the ESM can be deleted.
func TestIntegration_Lambda_DynamoDB_ESM_CRUD(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	ddbClient := createDynamoDBClient(t)
	lambdaClient := createLambdaClient(t)

	// --- Step 1: Create a DynamoDB table with streams enabled ---
	tableName := "esm-ddb-crud-test"

	_, err := ddbClient.CreateTable(ctx, &ddbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbsdktypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbsdktypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbsdktypes.KeyTypeHash},
		},
		BillingMode: ddbsdktypes.BillingModePayPerRequest,
		StreamSpecification: &ddbsdktypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbsdktypes.StreamViewTypeNewImage,
		},
	})
	require.NoError(t, err)

	// --- Step 2: Retrieve the stream ARN ---
	descOut, err := ddbClient.DescribeTable(ctx, &ddbsdk.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)

	streamARN := aws.ToString(descOut.Table.LatestStreamArn)
	require.NotEmpty(t, streamARN, "table should have a stream ARN when streaming is enabled")

	// --- Step 3: Create Lambda ESM for the DDB stream ---
	createOut, err := lambdaClient.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String(streamARN),
		FunctionName:     aws.String("stream-consumer"),
		StartingPosition: lambdaesdktypes.EventSourcePositionTrimHorizon,
		BatchSize:        aws.Int32(10),
	})
	require.NoError(t, err)

	esmUUID := aws.ToString(createOut.UUID)
	require.NotEmpty(t, esmUUID)
	assert.Equal(t, streamARN, aws.ToString(createOut.EventSourceArn))

	// --- Step 4: Get and verify the ESM ---
	getOut, err := lambdaClient.GetEventSourceMapping(ctx, &lambdasdk.GetEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err)
	assert.Equal(t, streamARN, aws.ToString(getOut.EventSourceArn))
	assert.Equal(t, "Enabled", aws.ToString(getOut.State))

	// --- Step 5: Delete the ESM ---
	_, err = lambdaClient.DeleteEventSourceMapping(ctx, &lambdasdk.DeleteEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err)

	// Verify it's gone
	_, getErr := lambdaClient.GetEventSourceMapping(ctx, &lambdasdk.GetEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	assert.Error(t, getErr, "ESM should not be found after deletion")
}
