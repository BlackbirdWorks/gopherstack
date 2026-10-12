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

func newFlowClient(t *testing.T) *neptunesdk.Client {
	t.Helper()

	return newTestNeptuneClient(t, neptune.NewHandler(neptune.NewInMemoryBackend("123456789012", "us-east-1")))
}

func TestSDK_DeleteReportsDeleting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind string
	}{
		{name: "instance", kind: "instance"},
		{name: "cluster", kind: "cluster"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newFlowClient(t)
			ctx := t.Context()

			_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("c"), Engine: aws.String("neptune"),
			})
			require.NoError(t, err)

			for _, id := range []string{"i1", "i2"} {
				_, err = client.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String(id), DBInstanceClass: aws.String("db.r5.large"),
					Engine: aws.String("neptune"), DBClusterIdentifier: aws.String("c"),
				})
				require.NoError(t, err)
			}

			if tt.kind == "instance" {
				out, delErr := client.DeleteDBInstance(ctx, &neptunesdk.DeleteDBInstanceInput{
					DBInstanceIdentifier: aws.String("i2"), SkipFinalSnapshot: aws.Bool(true),
				})
				require.NoError(t, delErr)
				assert.Equal(t, "deleting", aws.ToString(out.DBInstance.DBInstanceStatus))

				return
			}

			out, delErr := client.DeleteDBCluster(ctx, &neptunesdk.DeleteDBClusterInput{
				DBClusterIdentifier: aws.String("c"), SkipFinalSnapshot: aws.Bool(true),
			})
			require.NoError(t, delErr)
			assert.Equal(t, "deleting", aws.ToString(out.DBCluster.Status))
		})
	}
}

func TestSDK_InstanceReportsParameterGroupStatus(t *testing.T) {
	t.Parallel()

	client := newFlowClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("c"), Engine: aws.String("neptune"),
	})
	require.NoError(t, err)
	out, err := client.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("i"), DBInstanceClass: aws.String("db.r5.large"),
		Engine: aws.String("neptune"), DBClusterIdentifier: aws.String("c"),
	})
	require.NoError(t, err)

	require.Len(t, out.DBInstance.DBParameterGroups, 1)
	assert.Equal(t, "default.neptune1.3", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
	assert.Equal(t, "in-sync", aws.ToString(out.DBInstance.DBParameterGroups[0].ParameterApplyStatus))
}

func TestSDK_ClusterParameterPendingReboot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		param      string
		value      string
		wantBefore string
		wantAfter  string
		reboot     bool
	}{
		{
			name:       "static",
			param:      "neptune_enable_audit_log",
			value:      "1",
			wantBefore: "pending-reboot",
			wantAfter:  "pending-reboot",
		},
		{
			name:       "static rebooted",
			param:      "neptune_enable_audit_log",
			value:      "1",
			reboot:     true,
			wantBefore: "pending-reboot",
			wantAfter:  "in-sync",
		},
		{
			name:       "dynamic",
			param:      "neptune_enable_slow_query_log",
			value:      "info",
			wantBefore: "in-sync",
			wantAfter:  "in-sync",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newFlowClient(t)
			ctx := t.Context()

			_, err := client.CreateDBClusterParameterGroup(ctx, &neptunesdk.CreateDBClusterParameterGroupInput{
				DBClusterParameterGroupName: aws.String("pg"), DBParameterGroupFamily: aws.String("neptune1.3"),
				Description: aws.String("d"),
			})
			require.NoError(t, err)
			_, err = client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("c"), Engine: aws.String("neptune"),
				DBClusterParameterGroupName: aws.String("pg"),
			})
			require.NoError(t, err)
			_, err = client.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("i"), DBInstanceClass: aws.String("db.r5.large"),
				Engine: aws.String("neptune"), DBClusterIdentifier: aws.String("c"),
			})
			require.NoError(t, err)

			member := func() (string, string) {
				out, descErr := client.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("c"),
				})
				require.NoError(t, descErr)
				require.Len(t, out.DBClusters[0].DBClusterMembers, 1)
				inst, instErr := client.DescribeDBInstances(ctx, &neptunesdk.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("i"),
				})
				require.NoError(t, instErr)

				return aws.ToString(out.DBClusters[0].DBClusterMembers[0].DBClusterParameterGroupStatus),
					aws.ToString(inst.DBInstances[0].DBParameterGroups[0].ParameterApplyStatus)
			}
			memberStatus, instStatus := member()
			require.Equal(t, "in-sync", memberStatus)
			require.Equal(t, "in-sync", instStatus)

			_, err = client.ModifyDBClusterParameterGroup(ctx, &neptunesdk.ModifyDBClusterParameterGroupInput{
				DBClusterParameterGroupName: aws.String("pg"),
				Parameters: []types.Parameter{{
					ParameterName: aws.String(tt.param), ParameterValue: aws.String(tt.value),
					ApplyMethod: types.ApplyMethodPendingReboot,
				}},
			})
			require.NoError(t, err)

			memberStatus, instStatus = member()
			assert.Equal(t, tt.wantBefore, memberStatus)
			assert.Equal(t, tt.wantBefore, instStatus)

			if tt.reboot {
				_, err = client.RebootDBInstance(
					ctx,
					&neptunesdk.RebootDBInstanceInput{DBInstanceIdentifier: aws.String("i")},
				)
				require.NoError(t, err)
			}

			memberStatus, instStatus = member()
			assert.Equal(t, tt.wantAfter, memberStatus)
			assert.Equal(t, tt.wantAfter, instStatus)
		})
	}
}

func TestSDK_ClusterParameterGroupMustExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "create", op: "create"},
		{name: "modify", op: "modify"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newFlowClient(t)
			ctx := t.Context()

			var err error

			if tt.op == "create" {
				_, err = client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c"), Engine: aws.String("neptune"),
					DBClusterParameterGroupName: aws.String("missing"),
				})
			} else {
				_, err = client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c"), Engine: aws.String("neptune"),
				})
				require.NoError(t, err)

				_, err = client.ModifyDBCluster(ctx, &neptunesdk.ModifyDBClusterInput{
					DBClusterIdentifier: aws.String("c"), DBClusterParameterGroupName: aws.String("missing"),
				})
			}

			var notFound *types.DBClusterParameterGroupNotFoundFault
			require.ErrorAs(t, err, &notFound)
		})
	}
}
