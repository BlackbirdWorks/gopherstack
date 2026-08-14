// Package dynamodb_test covers gopherstack-ajej: RestoreTableFromBackup and
// RestoreTableToPointInTime declare GlobalSecondaryIndexOverride,
// OnDemandThroughputOverride and SSESpecificationOverride, but the backend
// used to never read any of them -- a table restored with an index override
// or a different encryption setting silently came back with the source
// table's configuration instead. Each test drives the real aws-sdk-go-v2
// client over HTTP so the wire decode -> backend conversion is exercised end
// to end, not just the backend method's Go-level behaviour.
package dynamodb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestRestoreTableFromBackup_Overrides(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("override-src"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	backupOut, err := client.CreateBackup(t.Context(), &sdk.CreateBackupInput{
		TableName:  aws.String("override-src"),
		BackupName: aws.String("override-src-backup"),
	})
	require.NoError(t, err)
	backupArn := aws.ToString(backupOut.BackupDetails.BackupArn)

	// Baseline: restoring with no overrides keeps the source's GSI and
	// reports no on-demand throughput caps -- the contrast every override
	// case below is checked against.
	baseline, err := client.RestoreTableFromBackup(t.Context(), &sdk.RestoreTableFromBackupInput{
		BackupArn:       aws.String(backupArn),
		TargetTableName: aws.String("override-baseline"),
	})
	require.NoError(t, err)
	require.Len(t, baseline.TableDescription.GlobalSecondaryIndexes, 1)
	assert.Nil(t, baseline.TableDescription.OnDemandThroughput)
	require.NotNil(t, baseline.TableDescription.SSEDescription)
	assert.Equal(t, types.SSETypeAes256, baseline.TableDescription.SSEDescription.SSEType)

	cases := []struct {
		input  *sdk.RestoreTableFromBackupInput
		verify func(t *testing.T, td *types.TableDescription)
		name   string
		target string
	}{
		{
			name:   "gsi override excludes all indexes",
			target: "override-gsi",
			input:  &sdk.RestoreTableFromBackupInput{GlobalSecondaryIndexOverride: []types.GlobalSecondaryIndex{}},
			verify: func(t *testing.T, td *types.TableDescription) {
				t.Helper()
				assert.Empty(t, td.GlobalSecondaryIndexes)
			},
		},
		{
			name:   "sse override switches to KMS",
			target: "override-sse",
			input: &sdk.RestoreTableFromBackupInput{
				SSESpecificationOverride: &types.SSESpecification{
					Enabled:        aws.Bool(true),
					SSEType:        types.SSETypeKms,
					KMSMasterKeyId: aws.String("alias/override-key"),
				},
			},
			verify: func(t *testing.T, td *types.TableDescription) {
				t.Helper()
				require.NotNil(t, td.SSEDescription)
				assert.Equal(t, types.SSETypeKms, td.SSEDescription.SSEType)
				assert.Equal(t, "alias/override-key", aws.ToString(td.SSEDescription.KMSMasterKeyArn))
			},
		},
		{
			name:   "on-demand throughput override sets caps",
			target: "override-ondemand",
			input: &sdk.RestoreTableFromBackupInput{
				OnDemandThroughputOverride: &types.OnDemandThroughput{
					MaxReadRequestUnits:  aws.Int64(500),
					MaxWriteRequestUnits: aws.Int64(500),
				},
			},
			verify: func(t *testing.T, td *types.TableDescription) {
				t.Helper()
				require.NotNil(t, td.OnDemandThroughput)
				assert.Equal(t, int64(500), aws.ToInt64(td.OnDemandThroughput.MaxReadRequestUnits))
				assert.Equal(t, int64(500), aws.ToInt64(td.OnDemandThroughput.MaxWriteRequestUnits))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.input.BackupArn = aws.String(backupArn)
			tc.input.TargetTableName = aws.String(tc.target)

			out, callErr := client.RestoreTableFromBackup(t.Context(), tc.input)
			require.NoError(t, callErr)
			require.NotNil(t, out.TableDescription)
			tc.verify(t, out.TableDescription)
		})
	}
}

func TestRestoreTableToPointInTime_OnDemandThroughputOverride(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("ondemand-pitr-src"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		OnDemandThroughput: &types.OnDemandThroughput{
			MaxReadRequestUnits:  aws.Int64(1000),
			MaxWriteRequestUnits: aws.Int64(1000),
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateContinuousBackups(t.Context(), &sdk.UpdateContinuousBackupsInput{
		TableName: aws.String("ondemand-pitr-src"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String("ondemand-pitr-src"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item1"},
		},
	})
	require.NoError(t, err)

	// Force a synchronous PITR snapshot rather than waiting on the janitor's
	// ~1-minute ticker (see pitr_test.go's identical use of SweepOnce).
	j := dynamodb.NewJanitor(backend, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(t.Context())

	restoreTime := aws.Time(time.Now().Add(time.Second))

	cases := []struct {
		override *types.OnDemandThroughput
		name     string
		target   string
		want     int64
	}{
		{
			name:     "no override inherits source caps",
			target:   "ondemand-pitr-inherited",
			override: nil,
			want:     1000,
		},
		{
			name:   "override replaces source caps",
			target: "ondemand-pitr-overridden",
			override: &types.OnDemandThroughput{
				MaxReadRequestUnits:  aws.Int64(2000),
				MaxWriteRequestUnits: aws.Int64(2000),
			},
			want: 2000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, callErr := client.RestoreTableToPointInTime(t.Context(), &sdk.RestoreTableToPointInTimeInput{
				SourceTableName:            aws.String("ondemand-pitr-src"),
				TargetTableName:            aws.String(tc.target),
				RestoreDateTime:            restoreTime,
				OnDemandThroughputOverride: tc.override,
			})
			require.NoError(t, callErr)
			require.NotNil(t, out.TableDescription.OnDemandThroughput)
			assert.Equal(t, tc.want, aws.ToInt64(out.TableDescription.OnDemandThroughput.MaxReadRequestUnits))
		})
	}
}
