package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ClusterParameterPendingReboot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		param      string
		method     types.ApplyMethod
		value      string
		wantBefore string
		wantAfter  string
		reboot     bool
	}{
		{
			name:       "static",
			method:     types.ApplyMethodPendingReboot,
			param:      "tls",
			value:      "disabled",
			wantBefore: "pending-reboot",
			wantAfter:  "pending-reboot",
		},
		{
			name:       "static rebooted",
			method:     types.ApplyMethodPendingReboot,
			param:      "tls",
			value:      "disabled",
			reboot:     true,
			wantBefore: "pending-reboot",
			wantAfter:  "in-sync",
		},
		{
			name:       "dynamic",
			method:     types.ApplyMethodImmediate,
			param:      "audit_logs",
			value:      "enabled",
			wantBefore: "in-sync",
			wantAfter:  "in-sync",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
				DBClusterParameterGroupName: aws.String("pg"), DBParameterGroupFamily: aws.String("docdb4.0"),
				Description: aws.String("d"),
			})
			require.NoError(t, err)
			_, err = client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("c"), Engine: aws.String("docdb"),
				DBClusterParameterGroupName: aws.String("pg"),
			})
			require.NoError(t, err)
			_, err = client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("i"), DBInstanceClass: aws.String("db.r5.large"),
				Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("c"),
			})
			require.NoError(t, err)

			status := func() string {
				out, descErr := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("c"),
				})
				require.NoError(t, descErr)
				require.Len(t, out.DBClusters[0].DBClusterMembers, 1)

				return aws.ToString(out.DBClusters[0].DBClusterMembers[0].DBClusterParameterGroupStatus)
			}
			require.Equal(t, "in-sync", status())

			_, err = client.ModifyDBClusterParameterGroup(ctx, &docdbsdk.ModifyDBClusterParameterGroupInput{
				DBClusterParameterGroupName: aws.String("pg"),
				Parameters: []types.Parameter{{
					ParameterName: aws.String(tt.param), ParameterValue: aws.String(tt.value),
					ApplyMethod: tt.method,
				}},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantBefore, status())

			if tt.reboot {
				_, err = client.RebootDBInstance(
					ctx,
					&docdbsdk.RebootDBInstanceInput{DBInstanceIdentifier: aws.String("i")},
				)
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantAfter, status())
		})
	}
}

func TestRealClient_ModifyClusterParameterGroupSwitchPendingReboot(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	for _, name := range []string{"pg1", "pg2"} {
		_, err := client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
			DBClusterParameterGroupName: aws.String(name), DBParameterGroupFamily: aws.String("docdb4.0"),
			Description: aws.String("d"),
		})
		require.NoError(t, err)
	}
	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("c"), Engine: aws.String("docdb"),
		DBClusterParameterGroupName: aws.String("pg1"),
	})
	require.NoError(t, err)
	_, err = client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("i"), DBInstanceClass: aws.String("db.r5.large"),
		Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("c"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
		DBClusterIdentifier: aws.String("c"), DBClusterParameterGroupName: aws.String("pg2"),
		ApplyImmediately: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{DBClusterIdentifier: aws.String("c")})
	require.NoError(t, err)
	assert.Equal(t, "pending-reboot", aws.ToString(out.DBClusters[0].DBClusterMembers[0].DBClusterParameterGroupStatus))
}

func TestRealClient_ClusterParameterGroupMustExist(t *testing.T) {
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

			client := newRealClient(t)
			ctx := t.Context()

			var err error

			if tt.op == "create" {
				_, err = client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c"), Engine: aws.String("docdb"),
					DBClusterParameterGroupName: aws.String("missing"),
				})
			} else {
				_, err = client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c"), Engine: aws.String("docdb"),
				})
				require.NoError(t, err)

				_, err = client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
					DBClusterIdentifier: aws.String("c"), DBClusterParameterGroupName: aws.String("missing"),
				})
			}

			var notFound *types.DBClusterParameterGroupNotFoundFault
			require.ErrorAs(t, err, &notFound)

			if tt.op == "create" {
				_, descErr := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
					DBClusterIdentifier: aws.String("c"),
				})
				require.Error(t, descErr, "rejected create must not leave a cluster")
			}
		})
	}
}
