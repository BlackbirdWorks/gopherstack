package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resourceByName(rs []ecstypes.Resource, name string) *ecstypes.Resource {
	for i := range rs {
		if aws.ToString(rs[i].Name) == name {
			return &rs[i]
		}
	}

	return nil
}

func TestContainerInstanceResources_SDKShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		total   []ecstypes.Resource
		wantCPU int32
	}{
		{
			name: "explicit_total",
			total: []ecstypes.Resource{
				{Name: aws.String("CPU"), Type: aws.String("INTEGER"), IntegerValue: 1024},
				{Name: aws.String("MEMORY"), Type: aws.String("INTEGER"), IntegerValue: 2048},
			},
			wantCPU: 1024,
		},
		{name: "defaults", wantCPU: 4096},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestECSClient(t, h)
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("res")})
			require.NoError(t, err)

			reg, err := client.RegisterContainerInstance(ctx, &ecssdk.RegisterContainerInstanceInput{
				Cluster:                  aws.String("res"),
				InstanceIdentityDocument: aws.String(fakeInstanceIdentityDocument("i-res")),
				TotalResources:           tt.total,
			})
			require.NoError(t, err)

			desc, err := client.DescribeContainerInstances(ctx, &ecssdk.DescribeContainerInstancesInput{
				Cluster:            aws.String("res"),
				ContainerInstances: []string{aws.ToString(reg.ContainerInstance.ContainerInstanceArn)},
			})
			require.NoError(t, err)
			require.Len(t, desc.ContainerInstances, 1)

			ci := desc.ContainerInstances[0]
			cpu := resourceByName(ci.RegisteredResources, "CPU")
			require.NotNil(t, cpu)
			assert.Equal(t, tt.wantCPU, cpu.IntegerValue)

			rem := resourceByName(ci.RemainingResources, "CPU")
			require.NotNil(t, rem)
			assert.Equal(t, tt.wantCPU, rem.IntegerValue)

			ports := resourceByName(ci.RegisteredResources, "PORTS")
			require.NotNil(t, ports)
			assert.Contains(t, ports.StringSetValue, "2376")
			assert.NotNil(t, resourceByName(ci.RegisteredResources, "PORTS_UDP"))
		})
	}
}
