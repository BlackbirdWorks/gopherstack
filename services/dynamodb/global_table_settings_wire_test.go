// Package dynamodb_test covers gopherstack-rrtz item 4: legacy Global
// Tables v1's DescribeGlobalTableSettings was inconsistent with
// UpdateGlobalTableSettings -- Update echoed ReplicaBillingModeSummary and
// ReplicaTableClassSummary, Describe did not, at both the backend
// (global_tables.go) and the wire-handler layer (handler_global_tables.go
// had two separate, differently-narrow conversion structs). This test
// drives the real aws-sdk-go-v2 client over HTTP and checks the two ops
// agree on the same replica's settings.
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

func TestGlobalTableSettings_DescribeAgreesWithUpdate(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	createPPRTableViaClient(t, client, "gt-settings-table")

	_, err := client.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
		GlobalTableName: aws.String("gt-settings-table"),
		ReplicationGroup: []types.Replica{
			{RegionName: aws.String("us-east-1")},
			{RegionName: aws.String("ap-southeast-1")},
		},
	})
	require.NoError(t, err)

	updateOut, err := client.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
		GlobalTableName:                          aws.String("gt-settings-table"),
		GlobalTableBillingMode:                   types.BillingModeProvisioned,
		GlobalTableProvisionedWriteCapacityUnits: aws.Int64(77),
		ReplicaSettingsUpdate: []types.ReplicaSettingsUpdate{
			{
				RegionName:        aws.String("ap-southeast-1"),
				ReplicaTableClass: types.TableClassStandardInfrequentAccess,
			},
		},
	})
	require.NoError(t, err)

	updateReplica := findReplicaSettings(t, updateOut.ReplicaSettings, "ap-southeast-1")
	require.NotNil(t, updateReplica.ReplicaBillingModeSummary)
	assert.Equal(t, types.BillingModeProvisioned, updateReplica.ReplicaBillingModeSummary.BillingMode)
	require.NotNil(t, updateReplica.ReplicaTableClassSummary)
	assert.Equal(t, types.TableClassStandardInfrequentAccess, updateReplica.ReplicaTableClassSummary.TableClass)
	require.NotNil(t, updateReplica.ReplicaProvisionedWriteCapacityUnits)
	assert.Equal(t, int64(77), *updateReplica.ReplicaProvisionedWriteCapacityUnits)

	descOut, err := client.DescribeGlobalTableSettings(t.Context(), &sdk.DescribeGlobalTableSettingsInput{
		GlobalTableName: aws.String("gt-settings-table"),
	})
	require.NoError(t, err)

	descReplica := findReplicaSettings(t, descOut.ReplicaSettings, "ap-southeast-1")

	// The two ops must agree: same billing mode and table class, since both
	// now read the same stored global-table state instead of Describe
	// silently omitting the fields Update reported.
	require.NotNil(t, descReplica.ReplicaBillingModeSummary)
	assert.Equal(
		t,
		updateReplica.ReplicaBillingModeSummary.BillingMode,
		descReplica.ReplicaBillingModeSummary.BillingMode,
	)
	require.NotNil(t, descReplica.ReplicaTableClassSummary)
	assert.Equal(
		t,
		updateReplica.ReplicaTableClassSummary.TableClass,
		descReplica.ReplicaTableClassSummary.TableClass,
	)
}

func findReplicaSettings(
	t *testing.T, settings []types.ReplicaSettingsDescription, region string,
) types.ReplicaSettingsDescription {
	t.Helper()

	for _, rs := range settings {
		if aws.ToString(rs.RegionName) == region {
			return rs
		}
	}

	t.Fatalf("no ReplicaSettingsDescription found for region %q", region)

	return types.ReplicaSettingsDescription{}
}
