package integration_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_TTL_Eviction(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	tableName := "TTLTestTable-" + uuid.NewString()
	ctx := t.Context()

	// 1. Create a table
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

	// Clean up table after test
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	// 2. Enable TTL
	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expires"),
			Enabled:       aws.Bool(true),
		},
	})
	require.NoError(t, err)

	// 3. Verify TTL Status
	desc, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TimeToLiveStatusEnabled, desc.TimeToLiveDescription.TimeToLiveStatus)
	assert.Equal(t, "expires", *desc.TimeToLiveDescription.AttributeName)

	// 4. Put an item that is already expired
	now := time.Now().Unix()
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "expired-item"},
			"expires": &types.AttributeValueMemberN{Value: strconv.FormatInt(now-10, 10)},
		},
	})
	require.NoError(t, err)

	// 5. Put an item that is NOT expired
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "active-item"},
			"expires": &types.AttributeValueMemberN{Value: strconv.FormatInt(now+3600, 10)},
		},
	})
	require.NoError(t, err)

	// 6. Wait for Janitor to sweep.
	// The Janitor interval in integration tests is likely the default (500ms).
	// We'll give it a few seconds to be sure.
	assert.Eventually(t, func() bool {
		out, scanErr := client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		})
		if scanErr != nil {
			return false
		}
		// Expect only 1 item (the active one)
		return len(out.Items) == 1
	}, 5*time.Second, 500*time.Millisecond, "Expired item should have been evicted by janitor")

	// 7. Verify the correct item remains
	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	assert.Equal(t, "active-item", out.Items[0]["pk"].(*types.AttributeValueMemberS).Value)
}
