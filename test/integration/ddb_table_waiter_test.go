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

func TestIntegration_DDB_TableDeletionWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	tableName := "WaiterTestTable-" + uuid.NewString()
	ctx := t.Context()

	// Create a table
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Delete the table
	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)

	// Use the waiter to wait for the table to be deleted
	waiter := dynamodb.NewTableNotExistsWaiter(client)

	// Set a longer max wait time for the test (30 seconds)
	// If the waiter works correctly, it should return immediately after deletion
	// If it doesn't work, it will wait the full 30 seconds or timeout
	start := time.Now()
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 30*time.Second)
	elapsed := time.Since(start)

	t.Logf("Waiter completed in %v", elapsed)
	require.NoError(t, err, "Waiter should complete successfully when table is deleted")
	
	// The waiter should complete quickly (< 2 seconds) since the table is already deleted
	// If it takes close to the full 30 seconds, the waiter isn't recognizing the deletion
	require.Less(t, elapsed, 2*time.Second, "Waiter should complete quickly, but took %v", elapsed)
}
