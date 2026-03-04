package dynamodb_test

import (
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

func TestTableStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		tableName       string
		wantInitStatus  types.TableStatus
		wantFinalStatus types.TableStatus
		createDelay     time.Duration
		finalSleep      time.Duration
	}{
		{
			name:           "immediately_active",
			tableName:      "status-table",
			wantInitStatus: types.TableStatusActive,
		},
		{
			name:            "lifecycle_with_delay",
			tableName:       "lifecycle-table",
			createDelay:     80 * time.Millisecond,
			wantInitStatus:  types.TableStatusCreating,
			wantFinalStatus: types.TableStatusActive,
			finalSleep:      200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddb.NewInMemoryDB()
			if tt.createDelay > 0 {
				db.SetCreateDelay(tt.createDelay)
			}

			out, err := db.CreateTable(t.Context(), createInput(tt.tableName))
			require.NoError(t, err)
			assert.Equal(t, tt.wantInitStatus, out.TableDescription.TableStatus)

			desc, err := db.DescribeTable(t.Context(), &sdk.DescribeTableInput{
				TableName: aws.String(tt.tableName),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantInitStatus, desc.Table.TableStatus)

			if tt.finalSleep > 0 {
				time.Sleep(tt.finalSleep)

				desc2, err2 := db.DescribeTable(t.Context(), &sdk.DescribeTableInput{
					TableName: aws.String(tt.tableName),
				})
				require.NoError(t, err2)
				assert.Equal(t, tt.wantFinalStatus, desc2.Table.TableStatus,
					"expected ACTIVE after delay elapsed")
			}
		})
	}
}
