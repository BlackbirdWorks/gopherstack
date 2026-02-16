package integration_test

import (
	"context"
	"strings"
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
	client := createDynamoDBClient(t)

	// Wait a bit to ensure container readiness
	time.Sleep(100 * time.Millisecond)

	type testCase struct {
		name      string
		operation func(t *testing.T, ctx context.Context, tableName string) error
		check     func(t *testing.T, err error)
	}

	tests := []testCase{
		{
			name: "ResourceNotFoundException",
			operation: func(t *testing.T, ctx context.Context, tableName string) error {
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
				require.Error(t, err)

				var resourceNotFound *types.ResourceNotFoundException
				if assert.ErrorAs(t, err, &resourceNotFound) {
					// Success
				} else {
					if !strings.Contains(err.Error(), "ResourceNotFoundException") {
						t.Errorf("Expected ResourceNotFoundException, got %T: %v", err, err)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
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
