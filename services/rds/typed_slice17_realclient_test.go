package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestSlice17_RDS_RealClient covers four of rds's five typed-client-
// uncovered ops (gopherstack-n3zi slice 17): DisableHttpEndpoint,
// ModifyDBClusterEndpoint, ModifyDBRecommendation,
// DeleteDBInstanceAutomatedBackup. The fifth, GetPerformanceInsightsMetrics,
// belongs to AWS's separate Performance Insights ("pi") API -- confirmed by
// its absence from aws-sdk-go-v2/service/rds@v1.124.1 (the pinned rds
// module has no api_op_GetPerformanceInsightsMetrics.go at all), and this
// repo has no "pi" module dependency -- so it is structurally uncoverable
// by a typed client without adding a new SDK dependency; left uncovered,
// not stubbed.
//
// Found and fixed two real bugs while building this test:
//  1. EnableHttpEndpointOutput/DisableHttpEndpointOutput both carry a real
//     ResourceArn member (rds@v1.124.1 deserializers.go's
//     awsAwsquery_deserializeOpDocumentDisableHttpEndpointOutput, case
//     "ResourceArn") that gopherstack's response structs never emitted --
//     always nil for a real client regardless of backend state. Fixed both
//     handlers (handler_data_api.go).
//  2. ModifyDBClusterEndpointOutput (and, by the shared toXMLClusterEndpointFields
//     helper, Create/Delete/DescribeDBClusterEndpoints too) has separate
//     EndpointType and CustomEndpointType members: every endpoint reachable
//     through this API family is a CUSTOM endpoint, so real EndpointType is
//     always the literal "CUSTOM" while CustomEndpointType carries the
//     caller's READER/WRITER/ANY value -- gopherstack emitted only a single
//     EndpointType set to the caller's value, so a real client's
//     CustomEndpointType was always empty and EndpointType never read
//     "CUSTOM". Fixed at the wire-serialization boundary only
//     (handler_cluster_endpoints.go); the internal DBClusterEndpoint model
//     and its own unit tests are unchanged.
func TestSlice17_RDS_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testSlice17DisableHTTPEndpointRealClient, "disable_http_endpoint"},
		{testSlice17ModifyDBClusterEndpointRealClient, "modify_db_cluster_endpoint"},
		{testSlice17ModifyDBRecommendationRealClient, "modify_db_recommendation"},
		{testSlice17DeleteDBInstanceAutomatedBackupRealClient, "delete_db_instance_automated_backup"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testSlice17DisableHTTPEndpointRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8RDSBackendAndClient(t)
	ctx := t.Context()

	cluster, err := backend.CreateDBCluster(
		"slice17-http-cluster", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, cluster.DBClusterArn)

	out, err := client.DisableHttpEndpoint(ctx, &rdssdk.DisableHttpEndpointInput{
		ResourceArn: aws.String(cluster.DBClusterArn),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(out.HttpEndpointEnabled))
	assert.Equal(t, cluster.DBClusterArn, aws.ToString(out.ResourceArn))
}

func testSlice17ModifyDBClusterEndpointRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8RDSBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"slice17-ep-cluster", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	_, err = client.CreateDBClusterEndpoint(ctx, &rdssdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("slice17-endpoint"),
		DBClusterIdentifier:         aws.String("slice17-ep-cluster"),
		EndpointType:                aws.String("READER"),
	})
	require.NoError(t, err)

	modified, err := client.ModifyDBClusterEndpoint(ctx, &rdssdk.ModifyDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("slice17-endpoint"),
		EndpointType:                aws.String("WRITER"),
	})
	require.NoError(t, err)
	assert.Equal(t, "CUSTOM", aws.ToString(modified.EndpointType))
	assert.Equal(t, "WRITER", aws.ToString(modified.CustomEndpointType))
	assert.Equal(t, "slice17-endpoint", aws.ToString(modified.DBClusterEndpointIdentifier))
}

func testSlice17ModifyDBRecommendationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8RDSBackendAndClient(t)
	ctx := t.Context()

	backend.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "slice17-rec",
		Status:           "active",
	})

	modified, err := client.ModifyDBRecommendation(ctx, &rdssdk.ModifyDBRecommendationInput{
		RecommendationId: aws.String("slice17-rec"),
		Status:           aws.String("dismissed"),
	})
	require.NoError(t, err)
	require.NotNil(t, modified.DBRecommendation)
	assert.Equal(t, "slice17-rec", aws.ToString(modified.DBRecommendation.RecommendationId))
	assert.Equal(t, "dismissed", aws.ToString(modified.DBRecommendation.Status))
}

func testSlice17DeleteDBInstanceAutomatedBackupRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8RDSBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"slice17-backup-inst", "mysql", "db.t3.micro", "mydb", "admin", "", 20,
		rds.DBInstanceOptions{BackupRetentionPeriod: 7},
	)
	require.NoError(t, err)

	deleted, err := client.DeleteDBInstanceAutomatedBackup(ctx, &rdssdk.DeleteDBInstanceAutomatedBackupInput{
		DbiResourceId: aws.String("slice17-backup-inst"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.DBInstanceAutomatedBackup)
	assert.Equal(t, "slice17-backup-inst", aws.ToString(deleted.DBInstanceAutomatedBackup.DBInstanceIdentifier))
	assert.Equal(t, "deleted", aws.ToString(deleted.DBInstanceAutomatedBackup.Status))

	remaining := backend.DescribeDBInstanceAutomatedBackups("slice17-backup-inst")
	assert.Empty(t, remaining)
}
