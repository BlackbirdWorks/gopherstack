package timestreaminfluxdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBClusterLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "create get and members", run: func(t *testing.T, f *fixture) {
			t.Helper()

			out, err := f.client.CreateDbCluster(t.Context(), clusterInput("analytics"))
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusCreating, out.DbClusterStatus)

			c, err := f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: out.DbClusterId})
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusCreating, c.Status)
			assert.Empty(t, aws.ToString(c.Endpoint))

			f.settle()

			c, err = f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: out.DbClusterId})
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusAvailable, c.Status)
			assert.Contains(t, aws.ToString(c.Endpoint), aws.ToString(out.DbClusterId))
			assert.Contains(t, aws.ToString(c.ReaderEndpoint), "-ro-")
			assert.Equal(t, types.EngineTypeInfluxdbV2, c.EngineType)
			assert.Equal(t, types.FailoverModeAutomatic, c.FailoverMode)
			assert.Equal(t, int32(8086), aws.ToInt32(c.Port))
			assert.Equal(t, int32(400), aws.ToInt32(c.AllocatedStorage))
			assert.Equal(t, int32(1), aws.ToInt32(c.ClusterConfiguration.IngestQueryInstances))
			assert.Equal(t, int32(2), aws.ToInt32(c.ClusterConfiguration.QueryOnlyInstances))

			members, err := f.client.ListDbInstancesForCluster(
				t.Context(),
				&sdk.ListDbInstancesForClusterInput{DbClusterId: out.DbClusterId},
			)
			require.NoError(t, err)
			require.Len(t, members.Items, 3)
			assert.Equal(t, "analytics-1", aws.ToString(members.Items[0].Name))
			assert.Equal(t, types.InstanceModePrimary, members.Items[0].InstanceMode)
			assert.Equal(t, types.InstanceModeReplica, members.Items[1].InstanceMode)
			assert.Equal(t, types.StatusAvailable, members.Items[0].Status)

			inst, err := f.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: members.Items[0].Id})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(out.DbClusterId), aws.ToString(inst.DbClusterId))
			assert.Equal(t, []types.InstanceMode{types.InstanceModePrimary}, inst.InstanceModes)

			all, err := f.client.ListDbInstances(t.Context(), &sdk.ListDbInstancesInput{})
			require.NoError(t, err)
			assert.Len(t, all.Items, 3)
		}},
		{name: "duplicates and validation", run: func(t *testing.T, f *fixture) {
			t.Helper()

			f.cluster(t, "dupe-cluster")

			_, err := f.client.CreateDbCluster(t.Context(), clusterInput("dupe-cluster"))
			assert.Equal(t, "ConflictException", apiCode(err))

			bad := clusterInput("bad-deploy")
			bad.DeploymentType = types.ClusterDeploymentType("SINGLE_AZ")
			_, err = f.client.CreateDbCluster(t.Context(), bad)
			assert.Equal(t, "ValidationException", apiCode(err))

			bad = clusterInput("bad-failover")
			bad.FailoverMode = "MAYBE"
			_, err = f.client.CreateDbCluster(t.Context(), bad)
			assert.Equal(t, "ValidationException", apiCode(err))

			_, err = f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: aws.String("nope000000")})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "update propagates to members", run: func(t *testing.T, f *fixture) {
			t.Helper()

			id := f.cluster(t, "tuned")

			up, err := f.client.UpdateDbCluster(t.Context(), &sdk.UpdateDbClusterInput{
				DbClusterId: aws.String(id), DbInstanceType: types.DbInstanceTypeDbInflux2xlarge, Port: aws.Int32(9100),
				FailoverMode: types.FailoverModeNoFailover,
			})
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusUpdatingInstanceType, up.DbClusterStatus)

			_, err = f.client.UpdateDbCluster(
				t.Context(),
				&sdk.UpdateDbClusterInput{DbClusterId: aws.String(id), Port: aws.Int32(9101)},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			f.settle()

			c, err := f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: aws.String(id)})
			require.NoError(t, err)
			assert.Equal(t, types.DbInstanceTypeDbInflux2xlarge, c.DbInstanceType)
			assert.Equal(t, int32(9100), aws.ToInt32(c.Port))
			assert.Equal(t, types.FailoverModeNoFailover, c.FailoverMode)

			members, err := f.client.ListDbInstancesForCluster(
				t.Context(),
				&sdk.ListDbInstancesForClusterInput{DbClusterId: aws.String(id)},
			)
			require.NoError(t, err)

			for _, m := range members.Items {
				assert.Equal(t, types.DbInstanceTypeDbInflux2xlarge, m.DbInstanceType)
				assert.Equal(t, int32(9100), aws.ToInt32(m.Port))
			}
		}},
		{name: "reboot subset", run: func(t *testing.T, f *fixture) {
			t.Helper()

			id := f.cluster(t, "reboot-me")
			members, err := f.client.ListDbInstancesForCluster(
				t.Context(),
				&sdk.ListDbInstancesForClusterInput{DbClusterId: aws.String(id)},
			)
			require.NoError(t, err)

			_, err = f.client.RebootDbCluster(
				t.Context(),
				&sdk.RebootDbClusterInput{DbClusterId: aws.String(id), InstanceIds: []string{"stranger"}},
			)
			assert.Equal(t, "ValidationException", apiCode(err))

			out, err := f.client.RebootDbCluster(t.Context(), &sdk.RebootDbClusterInput{
				DbClusterId: aws.String(id), InstanceIds: []string{aws.ToString(members.Items[1].Id)},
			})
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusRebooting, out.DbClusterStatus)

			after, err := f.client.ListDbInstancesForCluster(
				t.Context(),
				&sdk.ListDbInstancesForClusterInput{DbClusterId: aws.String(id)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.StatusAvailable, after.Items[0].Status)
			assert.Equal(t, types.StatusRebooting, after.Items[1].Status)

			f.settle()

			c, err := f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: aws.String(id)})
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusAvailable, c.Status)
		}},
		{name: "delete cascades and members are protected", run: func(t *testing.T, f *fixture) {
			t.Helper()

			id := f.cluster(t, "gone-soon")
			members, err := f.client.ListDbInstancesForCluster(
				t.Context(),
				&sdk.ListDbInstancesForClusterInput{DbClusterId: aws.String(id)},
			)
			require.NoError(t, err)

			_, err = f.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: members.Items[0].Id})
			assert.Equal(t, "ConflictException", apiCode(err))

			_, err = f.client.UpdateDbInstance(
				t.Context(),
				&sdk.UpdateDbInstanceInput{Identifier: members.Items[0].Id, Port: aws.Int32(9000)},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			out, err := f.client.DeleteDbCluster(t.Context(), &sdk.DeleteDbClusterInput{DbClusterId: aws.String(id)})
			require.NoError(t, err)
			assert.Equal(t, types.ClusterStatusDeleting, out.DbClusterStatus)

			f.settle()

			_, err = f.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: aws.String(id)})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = f.client.ListDbInstancesForCluster(
				t.Context(),
				&sdk.ListDbInstancesForClusterInput{DbClusterId: aws.String(id)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			all, err := f.client.ListDbInstances(t.Context(), &sdk.ListDbInstancesInput{})
			require.NoError(t, err)
			assert.Empty(t, all.Items)
		}},
		{name: "list clusters", run: func(t *testing.T, f *fixture) {
			t.Helper()

			f.cluster(t, "bravo")
			f.cluster(t, "alpha")

			out, err := f.client.ListDbClusters(t.Context(), &sdk.ListDbClustersInput{MaxResults: aws.Int32(1)})
			require.NoError(t, err)
			require.Len(t, out.Items, 1)
			assert.Equal(t, "alpha", aws.ToString(out.Items[0].Name))
			assert.NotNil(t, out.NextToken)

			out, err = f.client.ListDbClusters(t.Context(), &sdk.ListDbClustersInput{NextToken: out.NextToken})
			require.NoError(t, err)
			assert.Equal(t, "bravo", aws.ToString(out.Items[0].Name))
			assert.Nil(t, out.NextToken)

			_, err = f.client.ListDbClusters(t.Context(), &sdk.ListDbClustersInput{NextToken: aws.String("!!")})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newFixture(t))
		})
	}
}
