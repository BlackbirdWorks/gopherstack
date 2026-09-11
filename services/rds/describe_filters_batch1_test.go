package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wantInvalidParameterValue(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
}

func setUpClusterEndpointFilterFixture(t *testing.T, client *rdssdk.Client) {
	t.Helper()

	_, err := client.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("flt-endpoint-clu"),
		Engine:              aws.String("aurora-mysql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterEndpoint(t.Context(), &rdssdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("flt-ep-reader"),
		DBClusterIdentifier:         aws.String("flt-endpoint-clu"),
		EndpointType:                aws.String("READER"),
	})
	require.NoError(t, err)
	_, err = client.CreateDBClusterEndpoint(t.Context(), &rdssdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("flt-ep-writer"),
		DBClusterIdentifier:         aws.String("flt-endpoint-clu"),
		EndpointType:                aws.String("WRITER"),
	})
	require.NoError(t, err)
}

func TestDescribeDBClusterEndpoints_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)
	setUpClusterEndpointFilterFixture(t, client)

	t.Run("type filter narrows to matching endpoint", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDBClusterEndpoints(t.Context(), &rdssdk.DescribeDBClusterEndpointsInput{
			DBClusterIdentifier: aws.String("flt-endpoint-clu"),
			Filters: []types.Filter{
				{Name: aws.String("db-cluster-endpoint-type"), Values: []string{"READER"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.DBClusterEndpoints, 1)
		assert.Equal(t, "flt-ep-reader", aws.ToString(out.DBClusterEndpoints[0].DBClusterEndpointIdentifier))
	})

	t.Run("id filter narrows to matching endpoint", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDBClusterEndpoints(t.Context(), &rdssdk.DescribeDBClusterEndpointsInput{
			DBClusterIdentifier: aws.String("flt-endpoint-clu"),
			Filters: []types.Filter{
				{Name: aws.String("db-cluster-endpoint-id"), Values: []string{"flt-ep-writer"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.DBClusterEndpoints, 1)
		assert.Equal(t, "flt-ep-writer", aws.ToString(out.DBClusterEndpoints[0].DBClusterEndpointIdentifier))
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeDBClusterEndpoints(t.Context(), &rdssdk.DescribeDBClusterEndpointsInput{
			Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
		})
		wantInvalidParameterValue(t, err)
	})
}

func setUpClusterParameterFilterFixture(t *testing.T, client *rdssdk.Client) {
	t.Helper()

	_, err := client.CreateDBClusterParameterGroup(t.Context(), &rdssdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("flt-cluster-pg"),
		DBParameterGroupFamily:      aws.String("aurora-mysql8.0"),
		Description:                 aws.String("filter test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBClusterParameterGroup(t.Context(), &rdssdk.ModifyDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("flt-cluster-pg"),
		Parameters: []types.Parameter{
			{ParameterName: aws.String("max_connections"), ApplyMethod: types.ApplyMethodPendingReboot},
			{ParameterName: aws.String("work_mem"), ApplyMethod: types.ApplyMethodPendingReboot},
		},
	})
	require.NoError(t, err)
}

func TestDescribeDBClusterParameters_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)
	setUpClusterParameterFilterFixture(t, client)

	t.Run("parameter-name filter narrows to the matching parameter", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDBClusterParameters(t.Context(), &rdssdk.DescribeDBClusterParametersInput{
			DBClusterParameterGroupName: aws.String("flt-cluster-pg"),
			Filters: []types.Filter{
				{Name: aws.String("parameter-name"), Values: []string{"max_connections"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Parameters, 1)
		assert.Equal(t, "max_connections", aws.ToString(out.Parameters[0].ParameterName))
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeDBClusterParameters(t.Context(), &rdssdk.DescribeDBClusterParametersInput{
			DBClusterParameterGroupName: aws.String("flt-cluster-pg"),
			Filters:                     []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
		})
		wantInvalidParameterValue(t, err)
	})
}

func setUpParameterFilterFixture(t *testing.T, client *rdssdk.Client) {
	t.Helper()

	_, err := client.CreateDBParameterGroup(t.Context(), &rdssdk.CreateDBParameterGroupInput{
		DBParameterGroupName:   aws.String("flt-pg"),
		DBParameterGroupFamily: aws.String("postgres15"),
		Description:            aws.String("filter test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBParameterGroup(t.Context(), &rdssdk.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String("flt-pg"),
		Parameters: []types.Parameter{
			{ParameterName: aws.String("max_connections"), ApplyMethod: types.ApplyMethodPendingReboot},
			{ParameterName: aws.String("work_mem"), ApplyMethod: types.ApplyMethodPendingReboot},
		},
	})
	require.NoError(t, err)
}

func TestDescribeDBParameters_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)
	setUpParameterFilterFixture(t, client)

	t.Run("parameter-name filter narrows to the matching parameter", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDBParameters(t.Context(), &rdssdk.DescribeDBParametersInput{
			DBParameterGroupName: aws.String("flt-pg"),
			Filters: []types.Filter{
				{Name: aws.String("parameter-name"), Values: []string{"work_mem"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Parameters, 1)
		assert.Equal(t, "work_mem", aws.ToString(out.Parameters[0].ParameterName))
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeDBParameters(t.Context(), &rdssdk.DescribeDBParametersInput{
			DBParameterGroupName: aws.String("flt-pg"),
			Filters:              []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
		})
		wantInvalidParameterValue(t, err)
	})
}

func TestDescribeDBEngineVersions_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	t.Run("engine filter narrows out other engines", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDBEngineVersions(t.Context(), &rdssdk.DescribeDBEngineVersionsInput{
			Filters: []types.Filter{{Name: aws.String("engine"), Values: []string{"mysql"}}},
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.DBEngineVersions)
		for _, v := range out.DBEngineVersions {
			assert.Equal(t, "mysql", aws.ToString(v.Engine))
		}
	})

	t.Run("engine-version filter narrows to one row", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDBEngineVersions(t.Context(), &rdssdk.DescribeDBEngineVersionsInput{
			Filters: []types.Filter{{Name: aws.String("engine-version"), Values: []string{"14.10"}}},
		})
		require.NoError(t, err)
		require.Len(t, out.DBEngineVersions, 1)
		assert.Equal(t, "14.10", aws.ToString(out.DBEngineVersions[0].EngineVersion))
	})

	t.Run("unrecognized engine excludes all rows", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeDBEngineVersions(t.Context(), &rdssdk.DescribeDBEngineVersionsInput{
			Filters: []types.Filter{{Name: aws.String("engine"), Values: []string{"no-such-engine"}}},
		})
		require.NoError(t, err)
		assert.Empty(t, out.DBEngineVersions)
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeDBEngineVersions(t.Context(), &rdssdk.DescribeDBEngineVersionsInput{
			Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
		})
		wantInvalidParameterValue(t, err)
	})
}

// TestDescribeGlobalClusters_Filters pins the "region" filter's deliberately
// vacuous treatment (gopherstack-vl4m): it is a real, documented filter
// (unlike the 22 rds ops whose Filters doc says "This parameter isn't
// currently supported"), but no gopherstack API path ever populates
// GlobalCluster.PrimaryRegion or GlobalClusterMembers, so there is no real
// region data to narrow against -- it is accepted, not rejected, but never
// excludes a global cluster.
func TestDescribeGlobalClusters_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.CreateGlobalCluster(t.Context(), &rdssdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("flt-gc1"),
		Engine:                  aws.String("aurora-postgresql"),
	})
	require.NoError(t, err)
	_, err = client.CreateGlobalCluster(t.Context(), &rdssdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("flt-gc2"),
		Engine:                  aws.String("aurora-postgresql"),
	})
	require.NoError(t, err)

	t.Run("region filter is accepted but does not narrow", func(t *testing.T) {
		t.Parallel()

		out, filterErr := client.DescribeGlobalClusters(t.Context(), &rdssdk.DescribeGlobalClustersInput{
			Filters: []types.Filter{{Name: aws.String("region"), Values: []string{"us-west-2"}}},
		})
		require.NoError(t, filterErr)
		ids := make([]string, 0, len(out.GlobalClusters))
		for _, gc := range out.GlobalClusters {
			ids = append(ids, aws.ToString(gc.GlobalClusterIdentifier))
		}
		assert.Contains(t, ids, "flt-gc1")
		assert.Contains(t, ids, "flt-gc2")
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, filterErr := client.DescribeGlobalClusters(t.Context(), &rdssdk.DescribeGlobalClustersInput{
			Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
		})
		wantInvalidParameterValue(t, filterErr)
	})
}

func setUpExportTaskFilterFixture(t *testing.T, client *rdssdk.Client) {
	t.Helper()

	_, err := client.StartExportTask(t.Context(), &rdssdk.StartExportTaskInput{
		ExportTaskIdentifier: aws.String("flt-task-a"),
		SourceArn:            aws.String("arn:aws:rds:us-east-1:123456789012:cluster:flt-src-a"),
		S3BucketName:         aws.String("flt-bucket-a"),
		IamRoleArn:           aws.String("arn:aws:iam::123456789012:role/export"),
		KmsKeyId:             aws.String("flt-key-a"),
	})
	require.NoError(t, err)
	_, err = client.StartExportTask(t.Context(), &rdssdk.StartExportTaskInput{
		ExportTaskIdentifier: aws.String("flt-task-b"),
		SourceArn:            aws.String("arn:aws:rds:us-east-1:123456789012:cluster:flt-src-b"),
		S3BucketName:         aws.String("flt-bucket-b"),
		IamRoleArn:           aws.String("arn:aws:iam::123456789012:role/export"),
		KmsKeyId:             aws.String("flt-key-b"),
	})
	require.NoError(t, err)
}

func TestDescribeExportTasks_Filters(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)
	setUpExportTaskFilterFixture(t, client)

	cases := []struct {
		name       string
		filterName string
		value      string
		wantID     string
	}{
		{"export-task-identifier narrows", "export-task-identifier", "flt-task-a", "flt-task-a"},
		{"s3-bucket narrows", "s3-bucket", "flt-bucket-b", "flt-task-b"},
		{"source-arn narrows", "source-arn", "arn:aws:rds:us-east-1:123456789012:cluster:flt-src-a", "flt-task-a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.DescribeExportTasks(t.Context(), &rdssdk.DescribeExportTasksInput{
				Filters: []types.Filter{{Name: aws.String(tc.filterName), Values: []string{tc.value}}},
			})
			require.NoError(t, err)
			require.Len(t, out.ExportTasks, 1)
			assert.Equal(t, tc.wantID, aws.ToString(out.ExportTasks[0].ExportTaskIdentifier))
		})
	}

	t.Run("status filter excludes tasks with a different status", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeExportTasks(t.Context(), &rdssdk.DescribeExportTasksInput{
			Filters: []types.Filter{{Name: aws.String("status"), Values: []string{"failed"}}},
		})
		require.NoError(t, err)
		assert.Empty(t, out.ExportTasks, "both tasks are status=complete, so a failed filter must exclude them")
	})

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeExportTasks(t.Context(), &rdssdk.DescribeExportTasksInput{
			Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
		})
		wantInvalidParameterValue(t, err)
	})
}

// TestDescribePendingMaintenanceActions_UnknownFilterErrors is the only part
// of this op's Filters contract observable through the real client: this
// backend has no code path that ever populates real PendingMaintenanceAction
// data (maintenance.go's own comment on DescribePendingMaintenanceActions),
// so a narrowing assertion against real data is impossible here. The
// narrowing match logic itself is covered by
// TestMatchesAllPendingMaintenanceActionFilters (in-package, maintenance_filters_whitebox_test.go).
func TestDescribePendingMaintenanceActions_UnknownFilterErrors(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	_, err := client.DescribePendingMaintenanceActions(t.Context(), &rdssdk.DescribePendingMaintenanceActionsInput{
		Filters: []types.Filter{{Name: aws.String("bogus"), Values: []string{"x"}}},
	})
	wantInvalidParameterValue(t, err)
}
