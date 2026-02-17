package integration_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestIntegration_DDB_CustomWaitForDeletion tests a custom implementation
// of waiting for table deletion, similar to what a user might implement
func TestIntegration_DDB_CustomWaitForDeletion(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	tableName := "CustomWaitTest-" + uuid.NewString()
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

	// Implement custom wait logic (like a user might do)
	start := time.Now()
	maxWait := 30 * time.Second
	pollInterval := 1 * time.Second
	
	for {
		_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		
		if err != nil {
			// Check if it's ResourceNotFoundException
			var rnfe *types.ResourceNotFoundException
			if errors.As(err, &rnfe) {
				t.Logf("Table deleted successfully, detected via ResourceNotFoundException")
				break
			}
			// Other error - fail the test
			require.NoError(t, err, "Unexpected error while waiting for table deletion")
		}
		
		// Table still exists
		if time.Since(start) > maxWait {
			t.Fatalf("Timeout waiting for table deletion after %v", time.Since(start))
		}
		
		time.Sleep(pollInterval)
	}
	
	elapsed := time.Since(start)
	t.Logf("Custom wait completed in %v", elapsed)
	
	// Should complete quickly since deletion is immediate in Gopherstack
	require.Less(t, elapsed, 5*time.Second, "Custom wait should complete quickly")
}
