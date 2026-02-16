package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_ErrorSimulation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	// Wait a bit to ensure container readiness
	time.Sleep(100 * time.Millisecond)

	type testCase struct {
		operation func(t *testing.T, ctx context.Context, tableName string) error
		check     func(t *testing.T, err error)
		name      string
	}

	tests := []testCase{
		{
			name: "ResourceNotFoundException",
			operation: func(t *testing.T, ctx context.Context, tableName string) error {
				t.Helper()
				// Don't create the table
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
				})

				return err
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				var resourceNotFound *types.ResourceNotFoundException
				assert.ErrorAs(t, err, &resourceNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// For positive tests we create tables, but here we specifically might NOT want to.
			// The test case itself decides what to do with the tableName.
			// We generate a unique name anyway to ensure NO collision.
			tableName := "NonExistentTable-" + uuid.NewString()
			ctx := t.Context()

			err := tt.operation(t, ctx, tableName)
			tt.check(t, err)
		})
	}
}
