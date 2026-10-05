package docdb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ModifyDBInstanceRename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newID   string
		errCode string
	}{
		{name: "rename", newID: "inst-new"},
		{name: "collision", newID: "inst-other", errCode: "DBInstanceAlreadyExists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("host"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			for _, id := range []string{"inst-old", "inst-other"} {
				_, err = client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String(id), DBInstanceClass: aws.String("db.r5.large"),
					Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("host"),
					Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})
				require.NoError(t, err)
			}

			out, err := client.ModifyDBInstance(ctx, &docdbsdk.ModifyDBInstanceInput{
				DBInstanceIdentifier:    aws.String("inst-old"),
				NewDBInstanceIdentifier: aws.String(tt.newID),
				ApplyImmediately:        aws.Bool(true),
			})
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)
				_, derr := client.DescribeDBInstances(ctx, &docdbsdk.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("inst-old"),
				})
				require.NoError(t, derr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.newID, aws.ToString(out.DBInstance.DBInstanceIdentifier))
			assert.Contains(t, aws.ToString(out.DBInstance.DBInstanceArn), ":db:"+tt.newID)
			assert.Contains(t, aws.ToString(out.DBInstance.Endpoint.Address), tt.newID)

			_, err = client.DescribeDBInstances(ctx, &docdbsdk.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String("inst-old"),
			})
			require.Error(t, err)

			tags, err := client.ListTagsForResource(ctx, &docdbsdk.ListTagsForResourceInput{
				ResourceName: out.DBInstance.DBInstanceArn,
			})
			require.NoError(t, err)
			require.Len(t, tags.TagList, 1)
		})
	}
}

func TestRealClient_ModifyDBClusterRename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newID   string
		errCode string
	}{
		{name: "rename", newID: "cl-new"},
		{name: "collision", newID: "cl-other", errCode: "DBClusterAlreadyExistsFault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			for _, id := range []string{"cl-old", "cl-other"} {
				_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String(id), Engine: aws.String("docdb"),
					Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})
				require.NoError(t, err)
			}
			_, err := client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("cl-member"), DBInstanceClass: aws.String("db.r5.large"),
				Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("cl-old"),
			})
			require.NoError(t, err)

			out, err := client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
				DBClusterIdentifier:    aws.String("cl-old"),
				NewDBClusterIdentifier: aws.String(tt.newID),
				ApplyImmediately:       aws.Bool(true),
			})
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)
				d, derr := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("cl-other"),
				})
				require.NoError(t, derr)
				require.Len(t, d.DBClusters, 1)
				assert.Empty(t, d.DBClusters[0].DBClusterMembers)

				return
			}
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.DBCluster.DBClusterArn), ":cluster:"+tt.newID)
			assert.Contains(t, aws.ToString(out.DBCluster.Endpoint), tt.newID)
			assert.Contains(t, aws.ToString(out.DBCluster.ReaderEndpoint), tt.newID)

			d, err := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String(tt.newID),
			})
			require.NoError(t, err)
			require.Len(t, d.DBClusters, 1)
			require.Len(t, d.DBClusters[0].DBClusterMembers, 1)

			tags, err := client.ListTagsForResource(ctx, &docdbsdk.ListTagsForResourceInput{
				ResourceName: out.DBCluster.DBClusterArn,
			})
			require.NoError(t, err)
			require.Len(t, tags.TagList, 1)
		})
	}
}

func TestRealClient_ModifyDBClusterMajorVersionUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		allow   *bool
		name    string
		version string
		errCode string
	}{
		{name: "minor change", version: "5.0.1"},
		{name: "major without allow", version: "6.0.0", errCode: "InvalidParameterCombination"},
		{name: "major allowed", version: "6.0.0", allow: aws.Bool(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("mvu"), Engine: aws.String("docdb"),
				EngineVersion: aws.String("5.0.0"),
			})
			require.NoError(t, err)

			out, err := client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
				DBClusterIdentifier: aws.String("mvu"), EngineVersion: aws.String(tt.version),
				AllowMajorVersionUpgrade: tt.allow,
			})
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.version, aws.ToString(out.DBCluster.EngineVersion))
		})
	}
}

func TestRealClient_RestoreDBClusterToPointInTimeRestoreType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		latest      *bool
		at          *time.Time
		name        string
		restoreType string
		version     string
		errCode     string
	}{
		{name: "full copy", restoreType: "full-copy", version: "5.0.0", latest: aws.Bool(true)},
		{name: "full copy at time", restoreType: "full-copy", version: "5.0.0", at: aws.Time(time.Now())},
		{name: "copy on write", restoreType: "copy-on-write", version: "5.0.0", latest: aws.Bool(true)},
		{
			name: "copy on write with time", restoreType: "copy-on-write", version: "5.0.0",
			at: aws.Time(time.Now()), errCode: "InvalidParameterCombination",
		},
		{
			name: "unknown type", restoreType: "snapshot", version: "5.0.0",
			latest: aws.Bool(true), errCode: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("src"), Engine: aws.String("docdb"),
				EngineVersion: aws.String(tt.version),
			})
			require.NoError(t, err)

			out, err := client.RestoreDBClusterToPointInTime(ctx, &docdbsdk.RestoreDBClusterToPointInTimeInput{
				DBClusterIdentifier:       aws.String("dst"),
				SourceDBClusterIdentifier: aws.String("src"),
				RestoreType:               aws.String(tt.restoreType),
				UseLatestRestorableTime:   tt.latest,
				RestoreToTime:             tt.at,
			})
			if tt.errCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errCode)
				_, derr := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("dst"),
				})
				require.Error(t, derr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, "dst", aws.ToString(out.DBCluster.DBClusterIdentifier))
		})
	}
}

func TestRealClient_DescribeDBEngineVersionsLogTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
	}{
		{name: "v3.6", version: "3.6.0"},
		{name: "v4.0", version: "4.0.0"},
		{name: "v5.0", version: "5.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			out, err := client.DescribeDBEngineVersions(t.Context(), &docdbsdk.DescribeDBEngineVersionsInput{
				Engine: aws.String("docdb"), EngineVersion: aws.String(tt.version),
			})
			require.NoError(t, err)
			require.Len(t, out.DBEngineVersions, 1)
			assert.ElementsMatch(t, []string{"audit", "profiler"}, out.DBEngineVersions[0].ExportableLogTypes)
			assert.True(t, aws.ToBool(out.DBEngineVersions[0].SupportsLogExportsToCloudwatchLogs))
		})
	}
}

func TestRealClient_FailoverGlobalClusterAllowDataLossExcludesSwitchover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		allow       *bool
		switchover  *bool
		wantErrCode string
	}{
		{name: "failover", allow: aws.Bool(true)},
		{name: "switchover", switchover: aws.Bool(true)},
		{name: "both", allow: aws.Bool(true), switchover: aws.Bool(true), wantErrCode: "InvalidParameterCombination"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()
			_, err := client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gc"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			_, err = client.FailoverGlobalCluster(ctx, &docdbsdk.FailoverGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gc"), TargetDbClusterIdentifier: aws.String("gc-target"),
				AllowDataLoss: tt.allow, Switchover: tt.switchover,
			})
			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrCode)

				return
			}
			require.NoError(t, err)
		})
	}
}
