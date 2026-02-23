package dynamodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/dynamodb"
)

// createInput returns a minimal CreateTableInput for testing.
func createInput(name string) *sdk.CreateTableInput {
	return &sdk.CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}
}

func TestTableStatus_ImmediatelyActive(t *testing.T) {
	t.Parallel()

	// Default: createDelay = 0 → table is ACTIVE immediately.
	db := ddb.NewInMemoryDB()

	out, err := db.CreateTable(context.Background(), createInput("status-table"))
	require.NoError(t, err)
	assert.Equal(t, types.TableStatusActive, out.TableDescription.TableStatus)

	// DescribeTable should also return ACTIVE.
	desc, err := db.DescribeTable(context.Background(), &sdk.DescribeTableInput{
		TableName: aws.String("status-table"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TableStatusActive, desc.Table.TableStatus)
}

func TestTableStatus_LifecycleWithDelay(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()
	db.SetCreateDelay(80 * time.Millisecond)

	out, err := db.CreateTable(context.Background(), createInput("lifecycle-table"))
	require.NoError(t, err)

	// Immediately after create → CREATING.
	assert.Equal(t, types.TableStatusCreating, out.TableDescription.TableStatus,
		"expected CREATING immediately after CreateTable when delay>0")

	// DescribeTable should also return CREATING before delay elapses.
	desc, err := db.DescribeTable(context.Background(), &sdk.DescribeTableInput{
		TableName: aws.String("lifecycle-table"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TableStatusCreating, desc.Table.TableStatus)

	// Wait for the goroutine to transition to ACTIVE.
	time.Sleep(200 * time.Millisecond)

	desc2, err := db.DescribeTable(context.Background(), &sdk.DescribeTableInput{
		TableName: aws.String("lifecycle-table"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TableStatusActive, desc2.Table.TableStatus,
		"expected ACTIVE after delay elapsed")
}
