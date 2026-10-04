package ecs_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListContainerInstancesInput.Status doc (ecs@v1.96.0): the default includes
// "all states other than INACTIVE"; a deregistered instance becomes INACTIVE.
func TestDeregisteredContainerInstance_InactiveVisibility_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     ecstypes.ContainerInstanceStatus
		wantListed bool
	}{
		{name: "default excludes inactive", status: "", wantListed: false},
		{name: "active filter excludes", status: ecstypes.ContainerInstanceStatusActive, wantListed: false},
		{name: "inactive filter includes", status: ecstypes.ContainerInstanceStatus("INACTIVE"), wantListed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("ci")})
			require.NoError(t, err)

			reg, err := client.RegisterContainerInstance(ctx, &ecssdk.RegisterContainerInstanceInput{
				Cluster:                  aws.String("ci"),
				InstanceIdentityDocument: aws.String(fakeInstanceIdentityDocument("i-inactive")),
			})
			require.NoError(t, err)

			ciArn := aws.ToString(reg.ContainerInstance.ContainerInstanceArn)

			_, err = client.DeregisterContainerInstance(ctx, &ecssdk.DeregisterContainerInstanceInput{
				Cluster: aws.String("ci"), ContainerInstance: aws.String(ciArn),
			})
			require.NoError(t, err)

			list, err := client.ListContainerInstances(ctx, &ecssdk.ListContainerInstancesInput{
				Cluster: aws.String("ci"), Status: tt.status,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantListed, len(list.ContainerInstanceArns) == 1)

			desc, err := client.DescribeContainerInstances(ctx, &ecssdk.DescribeContainerInstancesInput{
				Cluster: aws.String("ci"), ContainerInstances: []string{ciArn},
			})
			require.NoError(t, err)
			require.Len(t, desc.ContainerInstances, 1)
			assert.Equal(t, "INACTIVE", aws.ToString(desc.ContainerInstances[0].Status))

			_, err = client.DeregisterContainerInstance(ctx, &ecssdk.DeregisterContainerInstanceInput{
				Cluster: aws.String("ci"), ContainerInstance: aws.String(ciArn),
			})
			require.Error(t, err)

			upd, err := client.UpdateContainerInstancesState(ctx, &ecssdk.UpdateContainerInstancesStateInput{
				Cluster:            aws.String("ci"),
				ContainerInstances: []string{ciArn},
				Status:             ecstypes.ContainerInstanceStatusActive,
			})
			require.NoError(t, err)
			assert.Empty(t, upd.ContainerInstances)
			assert.Len(t, upd.Failures, 1)

			cl, err := client.DescribeClusters(ctx, &ecssdk.DescribeClustersInput{Clusters: []string{"ci"}})
			require.NoError(t, err)
			assert.Zero(t, cl.Clusters[0].RegisteredContainerInstancesCount)

			_, err = client.DeleteCluster(ctx, &ecssdk.DeleteClusterInput{Cluster: aws.String("ci")})
			require.NoError(t, err)
		})
	}
}

func TestDeregisteredContainerInstance_EvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, _ := newDrainTestBackend(t)

		ci, err := b.RegisterContainerInstance("", "i-ttl")
		require.NoError(t, err)

		_, err = b.DeregisterContainerInstance("", ci.ContainerInstanceArn, false)
		require.NoError(t, err)

		got, failures, err := b.DescribeContainerInstances("", []string{ci.ContainerInstanceArn})
		require.NoError(t, err)
		require.Empty(t, failures)
		require.Len(t, got, 1)

		time.Sleep(time.Hour + time.Second)

		got, failures, err = b.DescribeContainerInstances("", []string{ci.ContainerInstanceArn})
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Len(t, failures, 1)

		_, err = b.RegisterContainerInstance("", "i-other")
		require.NoError(t, err)

		all, _, err := b.DescribeContainerInstances("", nil)
		require.NoError(t, err)
		assert.Len(t, all, 1)
	})
}
