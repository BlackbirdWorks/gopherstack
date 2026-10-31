//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"net/http/httptest"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

func computeEnv(t *testing.T) *env {
	t.Helper()

	cb := gcpcompute.NewInMemoryBackend()
	e := echo.New()
	e.Any("/*", gcpcompute.NewHandler(cb).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	opts := []option.ClientOption{option.WithEndpoint(srv.URL), option.WithoutAuthentication()}

	nets, err := compute.NewNetworksRESTClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nets.Close() })

	_, err = nets.Insert(t.Context(), &computepb.InsertNetworkRequest{
		Project: project, NetworkResource: &computepb.Network{Name: new("vpc"), AutoCreateSubnetworks: new(false)},
	})
	require.NoError(t, err)

	subs, err := compute.NewSubnetworksRESTClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = subs.Close() })

	_, err = subs.Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
			Name: new(
				"sub",
			),
			Network:     new("projects/" + project + "/global/networks/vpc"),
			IpCidrRange: new("10.10.0.0/20"),
			SecondaryIpRanges: []*computepb.SubnetworkSecondaryRange{
				{RangeName: new("pods"), IpCidrRange: new("172.20.0.0/16")},
				{RangeName: new("svcs"), IpCidrRange: new("172.21.0.0/24")},
			},
		},
	})
	require.NoError(t, err)

	b := gcpcontainer.NewInMemoryBackend()
	b.SetCompute(cb)

	return newEnvWith(t, b)
}

func TestClusterNetworkValidationAgainstCompute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cluster *containerpb.Cluster
		check   func(t *testing.T, c *containerpb.Cluster)
		name    string
		want    string
	}{
		{
			name:    "default network needs no compute network",
			cluster: &containerpb.Cluster{Name: "d", InitialNodeCount: 1},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, "default", c.GetNetwork())
			},
		},
		{
			name: "vpc and subnet", cluster: &containerpb.Cluster{
				Name: "v", InitialNodeCount: 1, Network: "vpc", Subnetwork: "sub",
			},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, "projects/"+project+"/global/networks/vpc", c.GetNetworkConfig().GetNetwork())
				assert.Equal(
					t,
					"projects/"+project+"/regions/"+region+"/subnetworks/sub",
					c.GetNetworkConfig().GetSubnetwork(),
				)
			},
		},
		{
			name: "full resource paths", cluster: &containerpb.Cluster{
				Name: "p", InitialNodeCount: 1,
				Network:    "projects/" + project + "/global/networks/vpc",
				Subnetwork: "projects/" + project + "/regions/" + region + "/subnetworks/sub",
			},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, "vpc", c.GetNetwork())
				assert.Equal(t, "sub", c.GetSubnetwork())
			},
		},
		{
			name: "secondary ranges", cluster: &containerpb.Cluster{
				Name: "s", InitialNodeCount: 1, Network: "vpc", Subnetwork: "sub",
				IpAllocationPolicy: &containerpb.IPAllocationPolicy{
					UseIpAliases: true, ClusterSecondaryRangeName: "pods", ServicesSecondaryRangeName: "svcs",
				},
			},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, "172.20.0.0/16", c.GetIpAllocationPolicy().GetClusterIpv4CidrBlock())
				assert.Equal(t, "172.21.0.0/24", c.GetIpAllocationPolicy().GetServicesIpv4CidrBlock())
				assert.Equal(t, "pods", c.GetIpAllocationPolicy().GetClusterSecondaryRangeName())
			},
		},
		{
			name: "missing network", want: "Network \"nope\" does not exist",
			cluster: &containerpb.Cluster{Name: "n", InitialNodeCount: 1, Network: "nope"},
		},
		{
			name: "missing subnet", want: "Subnetwork \"nope\" does not exist",
			cluster: &containerpb.Cluster{Name: "n", InitialNodeCount: 1, Network: "vpc", Subnetwork: "nope"},
		},
		{
			name: "missing secondary range", want: "Secondary range \"ghost\" does not exist",
			cluster: &containerpb.Cluster{
				Name: "n", InitialNodeCount: 1, Network: "vpc", Subnetwork: "sub",
				IpAllocationPolicy: &containerpb.IPAllocationPolicy{ClusterSecondaryRangeName: "ghost"},
			},
		},
		{
			name: "unknown machine type",
			want: "does not exist",
			cluster: &containerpb.Cluster{
				Name:             "n",
				InitialNodeCount: 1,
				NodeConfig:       &containerpb.NodeConfig{MachineType: "z9-huge"},
			},
		},
		{
			name: "custom machine type", cluster: &containerpb.Cluster{
				Name: "c", InitialNodeCount: 1, NodeConfig: &containerpb.NodeConfig{MachineType: "e2-custom-2-4096"},
			},
			check: func(t *testing.T, c *containerpb.Cluster) {
				t.Helper()
				assert.Equal(t, "e2-custom-2-4096", c.GetNodePools()[0].GetConfig().GetMachineType())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := computeEnv(t)

			_, err := e.gc.CreateCluster(
				t.Context(),
				&containerpb.CreateClusterRequest{Parent: zoneLoc, Cluster: tt.cluster},
			)
			if tt.want != "" {
				assertCode(t, codes.InvalidArgument, err, tt.want)

				return
			}

			require.NoError(t, err)
			tt.check(t, e.getCluster(t, zone, tt.cluster.GetName()))
		})
	}
}

func TestNodePoolMachineTypeAgainstCompute(t *testing.T) {
	t.Parallel()

	e := computeEnv(t)
	e.ready(t, zone, stdCluster("mt"))

	_, err := e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: clusterPath(zone, "mt"), NodePool: &containerpb.NodePool{
			Name: "big", InitialNodeCount: 1, Config: &containerpb.NodeConfig{MachineType: "n2-standard-64"},
		},
	})
	require.NoError(t, err)

	_, err = e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: clusterPath(zone, "mt"), NodePool: &containerpb.NodePool{
			Name: "bad", InitialNodeCount: 1, Config: &containerpb.NodeConfig{MachineType: "n2-standard-3"},
		},
	})
	assertCode(t, codes.InvalidArgument, err, "does not exist")
}
