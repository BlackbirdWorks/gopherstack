package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestUpdateAndDescribeContinuousBackups_AgreeOnPITRState proves
// UpdateContinuousBackups and DescribeContinuousBackups report the same
// PointInTimeRecoveryStatus and RecoveryPeriodInDays for a table, using a
// real aws-sdk-go-v2 client so a typed decode proves both values actually
// reach the wire (RecoveryPeriodInDays previously had no wire representation
// at all). ContinuousBackupsStatus itself is checked separately: real
// DynamoDB has no API to disable it, so it stays ENABLED regardless of PITR
// state.
func TestUpdateAndDescribeContinuousBackups_AgreeOnPITRState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		recoveryPeriodInDays *int32
		name                 string
		table                string
		wantStatus           types.PointInTimeRecoveryStatus
		wantRecoveryPeriod   int32
		pitrEnabled          bool
	}{
		{
			name:                 "custom recovery period",
			table:                "cb-agree-custom",
			pitrEnabled:          true,
			recoveryPeriodInDays: aws.Int32(7),
			wantStatus:           types.PointInTimeRecoveryStatusEnabled,
			wantRecoveryPeriod:   7,
		},
		{
			name:               "default recovery period",
			table:              "cb-agree-default",
			pitrEnabled:        true,
			wantStatus:         types.PointInTimeRecoveryStatusEnabled,
			wantRecoveryPeriod: 35,
		},
		{
			name:        "disabled",
			table:       "cb-agree-disabled",
			pitrEnabled: false,
			wantStatus:  types.PointInTimeRecoveryStatusDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()
			tableName := tt.table

			_, err := client.CreateTable(ctx, &sdk.CreateTableInput{
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

			updOut, err := client.UpdateContinuousBackups(ctx, &sdk.UpdateContinuousBackupsInput{
				TableName: aws.String(tableName),
				PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
					PointInTimeRecoveryEnabled: aws.Bool(tt.pitrEnabled),
					RecoveryPeriodInDays:       tt.recoveryPeriodInDays,
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeContinuousBackups(ctx, &sdk.DescribeContinuousBackupsInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)

			updPITR := updOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription
			descPITR := descOut.ContinuousBackupsDescription.PointInTimeRecoveryDescription

			require.NotNil(t, updPITR)
			require.NotNil(t, descPITR)
			assert.Equal(t, descPITR.PointInTimeRecoveryStatus, updPITR.PointInTimeRecoveryStatus,
				"Update and Describe must agree on PointInTimeRecoveryStatus")
			assert.Equal(t, tt.wantStatus, updPITR.PointInTimeRecoveryStatus)

			assert.Equal(t, aws.ToInt32(descPITR.RecoveryPeriodInDays), aws.ToInt32(updPITR.RecoveryPeriodInDays),
				"Update and Describe must agree on RecoveryPeriodInDays")
			if tt.pitrEnabled {
				assert.Equal(t, tt.wantRecoveryPeriod, aws.ToInt32(updPITR.RecoveryPeriodInDays))
			}

			assert.Equal(
				t,
				types.ContinuousBackupsStatusEnabled,
				updOut.ContinuousBackupsDescription.ContinuousBackupsStatus,
			)
			assert.Equal(
				t,
				types.ContinuousBackupsStatusEnabled,
				descOut.ContinuousBackupsDescription.ContinuousBackupsStatus,
			)
		})
	}
}
