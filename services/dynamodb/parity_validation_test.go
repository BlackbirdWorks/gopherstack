package dynamodb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateTable_BillingMode_EnumValidation asserts ValidationException for unknown BillingMode values.
func TestCreateTable_BillingMode_EnumValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		billingMode types.BillingMode
		name        string
		wantContain string
		wantErr     bool
	}{
		{
			name:        "valid_provisioned",
			billingMode: types.BillingModeProvisioned,
			wantErr:     false,
		},
		{
			name:        "valid_pay_per_request",
			billingMode: types.BillingModePayPerRequest,
			wantErr:     false,
		},
		{
			name:        "empty_billing_mode_defaults_to_provisioned",
			billingMode: "",
			wantErr:     false,
		},
		{
			name:        "invalid_billing_mode_rejected",
			billingMode: "INVALID_MODE",
			wantErr:     true,
			wantContain: "billingMode",
		},
		{
			name:        "typo_billing_mode_rejected",
			billingMode: "PAY_PER_USE",
			wantErr:     true,
			wantContain: "billingMode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newAuditTestDB(t)
			ctx := context.Background()

			input := &dynamodb.CreateTableInput{
				TableName: aws.String("billing-test-" + tt.name),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: tt.billingMode,
			}

			if tt.billingMode == types.BillingModeProvisioned || tt.billingMode == "" {
				input.ProvisionedThroughput = &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				}
			}

			_, err := db.CreateTable(ctx, input)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpdateTable_GSI_Ceiling asserts LimitExceededException when adding a 21st GSI.
func TestUpdateTable_GSI_Ceiling(t *testing.T) {
	t.Parallel()

	db := newAuditTestDB(t)
	ctx := context.Background()

	const gsiCount = 20
	gsis := make([]types.GlobalSecondaryIndex, gsiCount)
	attrDefs := make([]types.AttributeDefinition, 0, 1+len(gsis))
	attrDefs = append(attrDefs, types.AttributeDefinition{
		AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS,
	})

	for i := range gsis {
		an := fmt.Sprintf("gk%d", i)
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: aws.String(an), AttributeType: types.ScalarAttributeTypeS,
		})
		gsis[i] = types.GlobalSecondaryIndex{
			IndexName: aws.String(fmt.Sprintf("gsi-%d", i)),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(an), KeyType: types.KeyTypeHash},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		}
	}

	_, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:              aws.String("gsi-ceiling-table"),
		KeySchema:              []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions:   attrDefs,
		GlobalSecondaryIndexes: gsis,
		BillingMode:            types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	_, err = db.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("gsi-ceiling-table"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("extra_gk"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName: aws.String("gsi-21"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("extra_gk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LimitExceededException")
}
