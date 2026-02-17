package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_DeleteAndRecreate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	tableName := "LifecycleTestTable-" + uuid.NewString()
	ctx := t.Context()

	createTableInput := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	}

	// Create a table
	_, err := client.CreateTable(ctx, createTableInput)
	require.NoError(t, err)

	// Delete the table
	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)

	// Wait for table to be deleted using waiter
	waiter := dynamodb.NewTableNotExistsWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 10*time.Second)
	elapsed := time.Since(start)
	
	t.Logf("Waiter completed in %v", elapsed)
	require.NoError(t, err)
	require.Less(t, elapsed, 3*time.Second, "Waiter should complete quickly")

	// Recreate the table with the same name
	_, err = client.CreateTable(ctx, createTableInput)
	require.NoError(t, err, "Should be able to recreate table after deletion")

	// Clean up
	_, _ = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
}
