package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func newMembersClient(t *testing.T) *neptunesdk.Client {
	t.Helper()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)

	return newTestNeptuneClient(t, neptune.NewHandler(backend))
}

func mustCreateCluster(
	t *testing.T, c *neptunesdk.Client, id string, extra func(*neptunesdk.CreateDBClusterInput),
) *types.DBCluster {
	t.Helper()

	in := &neptunesdk.CreateDBClusterInput{DBClusterIdentifier: aws.String(id), Engine: aws.String("neptune")}
	if extra != nil {
		extra(in)
	}

	out, err := c.CreateDBCluster(t.Context(), in)
	require.NoError(t, err)

	return out.DBCluster
}

func TestModifyDBCluster_Rename_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newID   string
		wantErr bool
	}{
		{name: "renamed", newID: "renamed"},
		{name: "taken", newID: "other", wantErr: true},
		{name: "invalid", newID: "bad--id", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			ctx := t.Context()
			old := mustCreateCluster(t, c, "orig", func(in *neptunesdk.CreateDBClusterInput) {
				in.Tags = []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}
			})
			mustCreateCluster(t, c, "other", nil)

			_, err := c.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("inst1"), DBClusterIdentifier: aws.String("orig"),
				DBInstanceClass: aws.String("db.r5.large"), Engine: aws.String("neptune"),
			})
			require.NoError(t, err)

			out, err := c.ModifyDBCluster(ctx, &neptunesdk.ModifyDBClusterInput{
				DBClusterIdentifier:    aws.String("orig"),
				NewDBClusterIdentifier: aws.String(tc.newID),
				ApplyImmediately:       aws.Bool(true),
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.newID, aws.ToString(out.DBCluster.DBClusterIdentifier))
			assert.NotEqual(t, aws.ToString(old.DBClusterArn), aws.ToString(out.DBCluster.DBClusterArn))
			assert.Contains(t, aws.ToString(out.DBCluster.Endpoint), tc.newID)

			_, err = c.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("orig"),
			})
			require.Error(t, err)

			insts, err := c.DescribeDBInstances(ctx, &neptunesdk.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String("inst1"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.newID, aws.ToString(insts.DBInstances[0].DBClusterIdentifier))

			tags, err := c.ListTagsForResource(ctx, &neptunesdk.ListTagsForResourceInput{
				ResourceName: out.DBCluster.DBClusterArn,
			})
			require.NoError(t, err)
			require.Len(t, tags.TagList, 1)
		})
	}
}

func TestModifyDBInstance_Rename_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newID   string
		wantErr bool
	}{
		{name: "renamed", newID: "inst-new"},
		{name: "taken", newID: "inst2", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			ctx := t.Context()
			mustCreateCluster(t, c, "cl", nil)

			for _, id := range []string{"inst1", "inst2"} {
				_, err := c.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String(id), DBClusterIdentifier: aws.String("cl"),
					DBInstanceClass: aws.String("db.r5.large"), Engine: aws.String("neptune"),
				})
				require.NoError(t, err)
			}

			out, err := c.ModifyDBInstance(ctx, &neptunesdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String("inst1"), NewDBInstanceIdentifier: aws.String(tc.newID),
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.newID, aws.ToString(out.DBInstance.DBInstanceIdentifier))

			cl, err := c.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("cl"),
			})
			require.NoError(t, err)

			ids := make([]string, 0, 2)
			for _, m := range cl.DBClusters[0].DBClusterMembers {
				ids = append(ids, aws.ToString(m.DBInstanceIdentifier))
			}

			assert.Contains(t, ids, tc.newID)
			assert.NotContains(t, ids, "inst1")
		})
	}
}

func TestDBCluster_CloudwatchLogsExports_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enable  []string
		disable []string
		want    []string
	}{
		{name: "enable", enable: []string{"slowquery"}, want: []string{"audit", "slowquery"}},
		{name: "disable", disable: []string{"audit"}, want: nil},
		{name: "noop", want: []string{"audit"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			created := mustCreateCluster(t, c, "logs", func(in *neptunesdk.CreateDBClusterInput) {
				in.EnableCloudwatchLogsExports = []string{"audit"}
			})
			assert.Equal(t, []string{"audit"}, created.EnabledCloudwatchLogsExports)

			out, err := c.ModifyDBCluster(t.Context(), &neptunesdk.ModifyDBClusterInput{
				DBClusterIdentifier: aws.String("logs"),
				CloudwatchLogsExportConfiguration: &types.CloudwatchLogsExportConfiguration{
					EnableLogTypes: tc.enable, DisableLogTypes: tc.disable,
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, out.DBCluster.EnabledCloudwatchLogsExports)
		})
	}
}

func TestModifyDBCluster_AllowMajorVersionUpgrade_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		allow   bool
		wantErr bool
	}{
		{name: "minor", version: "1.4.0.0"},
		{name: "major blocked", version: "2.0.0.0", wantErr: true},
		{name: "major allowed", version: "2.0.0.0", allow: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			mustCreateCluster(t, c, "up", nil)

			out, err := c.ModifyDBCluster(t.Context(), &neptunesdk.ModifyDBClusterInput{
				DBClusterIdentifier: aws.String("up"), EngineVersion: aws.String(tc.version),
				AllowMajorVersionUpgrade: aws.Bool(tc.allow),
			})
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterCombination")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.version, aws.ToString(out.DBCluster.EngineVersion))
		})
	}
}

func TestAddRoleToDBCluster_FeatureName_SDK(t *testing.T) {
	t.Parallel()

	c := newMembersClient(t)
	ctx := t.Context()
	mustCreateCluster(t, c, "roles", nil)

	_, err := c.AddRoleToDBCluster(ctx, &neptunesdk.AddRoleToDBClusterInput{
		DBClusterIdentifier: aws.String("roles"), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		FeatureName: aws.String("Lambda"),
	})
	require.NoError(t, err)

	out, err := c.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{DBClusterIdentifier: aws.String("roles")})
	require.NoError(t, err)
	require.Len(t, out.DBClusters[0].AssociatedRoles, 1)
	assert.Equal(t, "Lambda", aws.ToString(out.DBClusters[0].AssociatedRoles[0].FeatureName))
}

func TestCopyOps_Tags_SDK(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}}

	tests := []struct {
		copyFn func(t *testing.T, c *neptunesdk.Client) string
		name   string
	}{
		{
			name: "cluster parameter group",
			copyFn: func(t *testing.T, c *neptunesdk.Client) string {
				t.Helper()
				_, err := c.CreateDBClusterParameterGroup(t.Context(), &neptunesdk.CreateDBClusterParameterGroupInput{
					DBClusterParameterGroupName: aws.String("src"),
					DBParameterGroupFamily:      aws.String("neptune1.3"),
					Description:                 aws.String("d"),
				})
				require.NoError(t, err)
				out, err := c.CopyDBClusterParameterGroup(t.Context(), &neptunesdk.CopyDBClusterParameterGroupInput{
					SourceDBClusterParameterGroupIdentifier:  aws.String("src"),
					TargetDBClusterParameterGroupIdentifier:  aws.String("dst"),
					TargetDBClusterParameterGroupDescription: aws.String("d"), Tags: tags,
				})
				require.NoError(t, err)

				return aws.ToString(out.DBClusterParameterGroup.DBClusterParameterGroupArn)
			},
		},
		{
			name: "parameter group",
			copyFn: func(t *testing.T, c *neptunesdk.Client) string {
				t.Helper()
				_, err := c.CreateDBParameterGroup(t.Context(), &neptunesdk.CreateDBParameterGroupInput{
					DBParameterGroupName:   aws.String("src"),
					DBParameterGroupFamily: aws.String("neptune1.3"),
					Description:            aws.String("d"),
				})
				require.NoError(t, err)
				out, err := c.CopyDBParameterGroup(t.Context(), &neptunesdk.CopyDBParameterGroupInput{
					SourceDBParameterGroupIdentifier:  aws.String("src"),
					TargetDBParameterGroupIdentifier:  aws.String("dst"),
					TargetDBParameterGroupDescription: aws.String("d"), Tags: tags,
				})
				require.NoError(t, err)

				return aws.ToString(out.DBParameterGroup.DBParameterGroupArn)
			},
		},
		{
			name: "cluster snapshot",
			copyFn: func(t *testing.T, c *neptunesdk.Client) string {
				t.Helper()
				mustCreateCluster(t, c, "cl", nil)
				_, err := c.CreateDBClusterSnapshot(t.Context(), &neptunesdk.CreateDBClusterSnapshotInput{
					DBClusterIdentifier: aws.String("cl"), DBClusterSnapshotIdentifier: aws.String("src"),
				})
				require.NoError(t, err)
				out, err := c.CopyDBClusterSnapshot(t.Context(), &neptunesdk.CopyDBClusterSnapshotInput{
					SourceDBClusterSnapshotIdentifier: aws.String("src"),
					TargetDBClusterSnapshotIdentifier: aws.String("dst"),
					Tags:                              tags,
				})
				require.NoError(t, err)

				return aws.ToString(out.DBClusterSnapshot.DBClusterSnapshotArn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			arn := tc.copyFn(t, c)

			got, err := c.ListTagsForResource(t.Context(), &neptunesdk.ListTagsForResourceInput{
				ResourceName: aws.String(arn),
			})
			require.NoError(t, err)
			require.Len(t, got.TagList, 1)
			assert.Equal(t, "env", aws.ToString(got.TagList[0].Key))
		})
	}
}

func TestDescribeCatalogFilters_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		engine       *string
		version      *string
		family       *string
		name         string
		wantVersions int
		wantAny      bool
	}{
		{name: "family", family: aws.String("neptune1.2"), wantVersions: 4},
		{name: "version", version: aws.String("1.4.0.0"), wantVersions: 1},
		{name: "other engine", engine: aws.String("docdb"), wantVersions: 0},
		{name: "unfiltered", wantAny: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			out, err := c.DescribeDBEngineVersions(t.Context(), &neptunesdk.DescribeDBEngineVersionsInput{
				Engine: tc.engine, EngineVersion: tc.version, DBParameterGroupFamily: tc.family,
			})
			require.NoError(t, err)

			if tc.wantAny {
				assert.Greater(t, len(out.DBEngineVersions), 4)

				return
			}

			assert.Len(t, out.DBEngineVersions, tc.wantVersions)
		})
	}
}

func TestDescribeEventCategories_SourceType_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sourceType *string
		want       []string
	}{
		{name: "instance", sourceType: aws.String("db-instance"), want: []string{"db-instance"}},
		{name: "unknown", sourceType: aws.String("db-nothing"), want: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			out, err := c.DescribeEventCategories(t.Context(), &neptunesdk.DescribeEventCategoriesInput{
				SourceType: tc.sourceType,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.EventCategoriesMapList))
			for _, m := range out.EventCategoriesMapList {
				got = append(got, aws.ToString(m.SourceType))
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRestoreDBClusterToPointInTime_RestoreType_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rt      string
		wantErr bool
	}{
		{name: "full copy", rt: "full-copy"},
		{name: "clone", rt: "copy-on-write"},
		{name: "invalid", rt: "snapshot", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)
			mustCreateCluster(t, c, "src", nil)

			_, err := c.RestoreDBClusterToPointInTime(t.Context(), &neptunesdk.RestoreDBClusterToPointInTimeInput{
				DBClusterIdentifier: aws.String("dst"), SourceDBClusterIdentifier: aws.String("src"),
				UseLatestRestorableTime: aws.Bool(true), RestoreType: aws.String(tc.rt),
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestModifyDBInstance_CACertificateIdentifier_SDK(t *testing.T) {
	t.Parallel()

	c := newMembersClient(t)
	ctx := t.Context()
	mustCreateCluster(t, c, "cl", nil)

	_, err := c.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("i"), DBClusterIdentifier: aws.String("cl"),
		DBInstanceClass: aws.String("db.r5.large"), Engine: aws.String("neptune"),
	})
	require.NoError(t, err)

	out, err := c.ModifyDBInstance(ctx, &neptunesdk.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String("i"), CACertificateIdentifier: aws.String("rds-ca-rsa2048-g1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "rds-ca-rsa2048-g1", aws.ToString(out.DBInstance.CACertificateIdentifier))
}

func TestFailoverGlobalCluster_ExclusiveFlags_SDK(t *testing.T) {
	t.Parallel()

	c := newMembersClient(t)

	_, err := c.FailoverGlobalCluster(t.Context(), &neptunesdk.FailoverGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("g"), TargetDbClusterIdentifier: aws.String("t"),
		AllowDataLoss: aws.Bool(true), Switchover: aws.Bool(true),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterCombination")
}
