//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/netip"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
)

func TestClusterLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	op := e.create(t, zone, &containerpb.Cluster{
		Name: "dev", InitialNodeCount: 2, Description: "d", ResourceLabels: map[string]string{"env": "dev"},
		NodeConfig: &containerpb.NodeConfig{MachineType: "n2-standard-4", DiskSizeGb: 50},
	})

	assert.True(t, strings.HasPrefix(op.GetName(), "operation-"))
	assert.Equal(t, containerpb.Operation_CREATE_CLUSTER, op.GetOperationType())
	assert.Equal(t, containerpb.Operation_RUNNING, op.GetStatus())
	assert.Equal(t, zone, op.GetZone())
	assert.Equal(t, zone, op.GetLocation())
	assert.Equal(t, "https://container.googleapis.com/v1/"+zoneLoc+"/operations/"+op.GetName(), op.GetSelfLink())
	assert.Equal(t, "https://container.googleapis.com/v1/"+clusterPath(zone, "dev"), op.GetTargetLink())
	assert.NotEmpty(t, op.GetStartTime())

	c := e.getCluster(t, zone, "dev")
	assert.Equal(t, containerpb.Cluster_PROVISIONING, c.GetStatus())
	assert.Equal(t, "dev", c.GetName())
	assert.Equal(t, zone, c.GetLocation())
	assert.Equal(t, zone, c.GetZone())
	assert.Equal(t, op.GetTargetLink(), c.GetSelfLink())
	assert.Equal(t, "default", c.GetNetwork())
	assert.Equal(t, "default", c.GetSubnetwork())
	assert.Equal(t, "projects/"+project+"/global/networks/default", c.GetNetworkConfig().GetNetwork())
	assert.Equal(t, "2026-10-26T12:00:00+00:00", c.GetCreateTime())
	assert.NotEmpty(t, c.GetId())
	assert.NotEmpty(t, c.GetEndpoint())
	assert.Equal(t, []string{zone}, c.GetLocations())
	assert.Equal(t, int32(2), c.GetCurrentNodeCount())
	assert.Equal(t, "logging.googleapis.com/kubernetes", c.GetLoggingService())
	assert.Equal(t, "monitoring.googleapis.com/kubernetes", c.GetMonitoringService())
	assert.Equal(t, c.GetCurrentMasterVersion(), c.GetInitialClusterVersion())
	assert.Contains(t, c.GetCurrentMasterVersion(), "-gke.")
	assert.Equal(t, map[string]string{"env": "dev"}, c.GetResourceLabels())
	assert.NotEmpty(t, c.GetLabelFingerprint())
	assert.NotEmpty(t, c.GetEtag())
	assert.True(t, c.GetShieldedNodes().GetEnabled())
	assert.NotEmpty(t, c.GetMaintenancePolicy().GetResourceVersion())

	block, _ := pem.Decode(mustB64(t, c.GetMasterAuth().GetClusterCaCertificate()))
	require.NotNil(t, block)
	_, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	ip := c.GetIpAllocationPolicy()
	assert.True(t, ip.GetUseIpAliases())

	pod := netip.MustParsePrefix(ip.GetClusterIpv4CidrBlock())
	svc := netip.MustParsePrefix(ip.GetServicesIpv4CidrBlock())
	assert.Equal(t, 14, pod.Bits())
	assert.Equal(t, 20, svc.Bits())
	assert.False(t, pod.Overlaps(svc))
	assert.Equal(t, ip.GetClusterIpv4CidrBlock(), c.GetClusterIpv4Cidr())

	require.Len(t, c.GetNodePools(), 1)

	np := c.GetNodePools()[0]
	assert.Equal(t, "default-pool", np.GetName())
	assert.Equal(t, containerpb.NodePool_PROVISIONING, np.GetStatus())
	assert.Equal(t, "n2-standard-4", np.GetConfig().GetMachineType())
	assert.Equal(t, int32(50), np.GetConfig().GetDiskSizeGb())
	assert.Equal(t, "COS_CONTAINERD", np.GetConfig().GetImageType())
	assert.Equal(t, c.GetCurrentMasterVersion(), np.GetVersion())
	assert.True(t, np.GetManagement().GetAutoRepair())
	assert.Len(t, np.GetInstanceGroupUrls(), 1)
	assert.Equal(t, int64(110), np.GetMaxPodsConstraint().GetMaxPodsPerNode())
	assert.Equal(t, int32(24), np.GetPodIpv4CidrSize())
	assert.Equal(t, np.GetInstanceGroupUrls(), c.GetInstanceGroupUrls())

	assert.Equal(t, containerpb.Operation_RUNNING, e.getOp(t, op).GetStatus())

	e.clock.Advance(599 * time.Millisecond)
	assert.Equal(t, containerpb.Cluster_PROVISIONING, e.getCluster(t, zone, "dev").GetStatus())

	e.clock.Advance(time.Millisecond)

	done := e.getOp(t, op)
	assert.Equal(t, containerpb.Operation_DONE, done.GetStatus())
	assert.Equal(t, "2026-10-26T12:00:00.6Z", done.GetEndTime())
	assert.Nil(t, done.GetError())

	c = e.getCluster(t, zone, "dev")
	assert.Equal(t, containerpb.Cluster_RUNNING, c.GetStatus())
	assert.Equal(t, containerpb.NodePool_RUNNING, c.GetNodePools()[0].GetStatus())

	list, err := e.gc.ListClusters(
		t.Context(),
		&containerpb.ListClustersRequest{Parent: "projects/" + project + "/locations/-"},
	)
	require.NoError(t, err)
	require.Len(t, list.GetClusters(), 1)

	delOp, err := e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "dev")})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_DELETE_CLUSTER, delOp.GetOperationType())
	assert.Equal(t, containerpb.Cluster_STOPPING, e.getCluster(t, zone, "dev").GetStatus())

	_, err = e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "dev")})
	assertCode(t, codes.FailedPrecondition, err, "incompatible operation "+delOp.GetName())

	e.settle()

	_, err = e.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: clusterPath(zone, "dev")})
	assertCode(t, codes.NotFound, err, "Not found: "+clusterPath(zone, "dev"))

	list, err = e.gc.ListClusters(t.Context(), &containerpb.ListClustersRequest{Parent: zoneLoc})
	require.NoError(t, err)
	assert.Empty(t, list.GetClusters())

	ops, err := e.gc.ListOperations(t.Context(), &containerpb.ListOperationsRequest{Parent: zoneLoc})
	require.NoError(t, err)
	require.Len(t, ops.GetOperations(), 2)
	assert.Equal(t, containerpb.Operation_DONE, ops.GetOperations()[1].GetStatus())
}

func mustB64(t *testing.T, s string) []byte {
	t.Helper()

	raw, err := base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)

	return raw
}

func TestCreateClusterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cluster *containerpb.Cluster
		name    string
		loc     string
		want    string
	}{
		{&containerpb.Cluster{Name: "Bad_Name", InitialNodeCount: 1}, "bad name", zone, "Bad_Name"},
		{&containerpb.Cluster{InitialNodeCount: 1}, "no name", zone, "must be specified"},
		{&containerpb.Cluster{Name: "a"}, "no pools", zone, "node_pools or initial_node_count"},
		{
			&containerpb.Cluster{Name: "a", InitialNodeCount: 1},
			"bad location",
			"mars-central1-a",
			"not a valid zone or region",
		},
		{
			&containerpb.Cluster{Name: "a", InitialNodeCount: 1, InitialClusterVersion: "9.9"},
			"bad version",
			zone,
			"unsupported",
		},
		{
			&containerpb.Cluster{
				Name: "a", InitialNodeCount: 1, InitialClusterVersion: "1.35.1",
				ReleaseChannel: &containerpb.ReleaseChannel{Channel: containerpb.ReleaseChannel_STABLE},
			}, "version outside channel", zone, "STABLE",
		},
		{
			&containerpb.Cluster{Name: "a", InitialNodeCount: 1, ResourceLabels: map[string]string{"Bad": "x"}},
			"bad label", zone, "Invalid label key",
		},
		{
			&containerpb.Cluster{Name: "a", InitialNodeCount: 1, IpAllocationPolicy: &containerpb.IPAllocationPolicy{
				ClusterIpv4CidrBlock: "10.0.0.0/14", ServicesIpv4CidrBlock: "10.0.0.0/20",
			}}, "overlapping ranges", zone, "overlaps",
		},
		{
			&containerpb.Cluster{Name: "a", InitialNodeCount: 1, IpAllocationPolicy: &containerpb.IPAllocationPolicy{
				ClusterIpv4CidrBlock: "10.0.0.1/14",
			}}, "host bits", zone, "Invalid CIDR",
		},
		{
			&containerpb.Cluster{Name: "a", InitialNodeCount: 1, IpAllocationPolicy: &containerpb.IPAllocationPolicy{
				ClusterIpv4CidrBlock: "/30",
			}}, "pod range too small", zone, "between /8 and /21",
		},
		{
			&containerpb.Cluster{Name: "a", InitialNodeCount: 1, NodeConfig: &containerpb.NodeConfig{DiskSizeGb: 1}},
			"disk too small", zone, "Disk size",
		},
		{
			&containerpb.Cluster{
				Name:             "a",
				InitialNodeCount: 1,
				NodeConfig:       &containerpb.NodeConfig{Spot: true, Preemptible: true},
			},
			"spot and preemptible",
			zone,
			"cannot both be set",
		},
		{
			&containerpb.Cluster{
				Name:             "a",
				InitialNodeCount: 1,
				NodeConfig:       &containerpb.NodeConfig{MachineType: "Bad Type"},
			},
			"bad machine type",
			zone,
			"Invalid machine type",
		},
		{
			&containerpb.Cluster{Name: "a", NodePools: []*containerpb.NodePool{
				{Name: "p", InitialNodeCount: 1}, {Name: "p", InitialNodeCount: 1},
			}}, "duplicate pool", zone, "Duplicate node pool",
		},
		{
			&containerpb.Cluster{Name: "a", Locations: []string{"europe-west1-b"}, InitialNodeCount: 1},
			"foreign zone", zone, "not a zone of",
		},
		{
			&containerpb.Cluster{
				Name:                 "a",
				InitialNodeCount:     1,
				PrivateClusterConfig: &containerpb.PrivateClusterConfig{EnablePrivateNodes: true},
			},
			"private without master range",
			zone,
			"master_ipv4_cidr_block",
		},
		{
			&containerpb.Cluster{Name: "a", Autopilot: &containerpb.Autopilot{Enabled: true}},
			"zonal autopilot", zone, "must be regional",
		},
		{
			&containerpb.Cluster{Name: "a", Autopilot: &containerpb.Autopilot{Enabled: true}, InitialNodeCount: 3},
			"autopilot with nodes", region, "manage their own nodes",
		},
		{
			&containerpb.Cluster{Name: "a", NodePools: []*containerpb.NodePool{{Name: "p"}}},
			"pool without nodes", zone, "greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.gc.CreateCluster(t.Context(), &containerpb.CreateClusterRequest{
				Parent: "projects/" + project + "/locations/" + tt.loc, Cluster: tt.cluster,
			})
			assertCode(t, codes.InvalidArgument, err, tt.want)
			assert.True(t, hasDetail[*errdetails.BadRequest](err), "want BadRequest detail")
		})
	}
}

func TestClusterNotFoundAndExists(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.ready(t, zone, stdCluster("dup"))

	_, err := e.gc.CreateCluster(
		t.Context(),
		&containerpb.CreateClusterRequest{Parent: zoneLoc, Cluster: stdCluster("dup")},
	)
	assertCode(t, codes.AlreadyExists, err, "Already exists: "+clusterPath(zone, "dup"))
	assert.True(t, hasDetail[*errdetails.ResourceInfo](err))

	e.ready(t, "us-east1-b", stdCluster("dup"))

	missing := clusterPath(zone, "missing")
	_, err = e.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: missing})
	assertCode(t, codes.NotFound, err, "Not found: "+missing)
	assert.True(t, hasDetail[*errdetails.ResourceInfo](err))

	_, err = e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: missing})
	assertCode(t, codes.NotFound, err, "")

	_, err = e.gc.UpdateCluster(t.Context(), &containerpb.UpdateClusterRequest{
		Name: missing, Update: &containerpb.ClusterUpdate{DesiredMonitoringService: "none"},
	})
	assertCode(t, codes.NotFound, err, "")

	_, err = e.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: "garbage"})
	assertCode(t, codes.InvalidArgument, err, "Invalid cluster name")

	_, err = e.gc.GetOperation(t.Context(), &containerpb.GetOperationRequest{Name: zoneLoc + "/operations/nope"})
	assertCode(t, codes.NotFound, err, "")

	_, err = e.gc.ListClusters(
		t.Context(),
		&containerpb.ListClustersRequest{Parent: "projects/" + project + "/locations/mars-1"},
	)
	assertCode(t, codes.NotFound, err, "")
}

func TestLegacyRequestFields(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	_, err := e.gc.CreateCluster(t.Context(), &containerpb.CreateClusterRequest{
		ProjectId: project, Zone: zone, Cluster: stdCluster("legacy"),
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.gc.GetCluster(
		t.Context(),
		&containerpb.GetClusterRequest{ProjectId: project, Zone: zone, ClusterId: "legacy"},
	)
	require.NoError(t, err)
	assert.Equal(t, containerpb.Cluster_RUNNING, got.GetStatus())

	list, err := e.gc.ListClusters(t.Context(), &containerpb.ListClustersRequest{ProjectId: project, Zone: zone})
	require.NoError(t, err)
	assert.Len(t, list.GetClusters(), 1)

	_, err = e.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{ProjectId: project, Zone: zone})
	assertCode(t, codes.InvalidArgument, err, "Cluster ID")

	_, err = e.gc.DeleteCluster(
		t.Context(),
		&containerpb.DeleteClusterRequest{ProjectId: project, Zone: zone, ClusterId: "legacy"},
	)
	require.NoError(t, err)
}

func TestRegionalCluster(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.ready(t, region, &containerpb.Cluster{
		Name: "reg", NodePools: []*containerpb.NodePool{{Name: "p", InitialNodeCount: 2}},
	})

	assert.Equal(t, []string{"us-central1-a", "us-central1-b", "us-central1-c"}, c.GetLocations())
	assert.Equal(t, region, c.GetLocation())
	assert.Equal(t, int32(6), c.GetCurrentNodeCount())
	assert.Len(t, c.GetInstanceGroupUrls(), 3)
	assert.Equal(t, c.GetLocations(), c.GetNodePools()[0].GetLocations())

	sub := e.ready(t, region, &containerpb.Cluster{
		Name: "two-zone", Locations: []string{"us-central1-b", "us-central1-a"},
		NodePools: []*containerpb.NodePool{{Name: "p", InitialNodeCount: 3}},
	})
	assert.Equal(t, []string{"us-central1-a", "us-central1-b"}, sub.GetLocations())
	assert.Equal(t, int32(6), sub.GetCurrentNodeCount())
}

func TestAutopilotCluster(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.ready(t, region, &containerpb.Cluster{Name: "ap", Autopilot: &containerpb.Autopilot{Enabled: true}})

	assert.True(t, c.GetAutopilot().GetEnabled())
	assert.Equal(t, containerpb.ReleaseChannel_REGULAR, c.GetReleaseChannel().GetChannel())
	assert.Equal(t, containerpb.DatapathProvider_ADVANCED_DATAPATH, c.GetNetworkConfig().GetDatapathProvider())
	assert.Equal(t, project+".svc.id.goog", c.GetWorkloadIdentityConfig().GetWorkloadPool())
	require.Len(t, c.GetNodePools(), 1)

	_, err := e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: clusterPath(region, "ap"), NodePool: &containerpb.NodePool{Name: "x", InitialNodeCount: 1},
	})
	assertCode(t, codes.FailedPrecondition, err, "Autopilot")

	_, err = e.gc.DeleteNodePool(
		t.Context(),
		&containerpb.DeleteNodePoolRequest{Name: poolPath(region, "ap", "default-pool")},
	)
	assertCode(t, codes.FailedPrecondition, err, "managed by Google")

	_, err = e.gc.SetLegacyAbac(
		t.Context(),
		&containerpb.SetLegacyAbacRequest{Name: clusterPath(region, "ap"), Enabled: true},
	)
	assertCode(t, codes.InvalidArgument, err, "Autopilot")
}

func TestReleaseChannelVersions(t *testing.T) {
	t.Parallel()

	cfg, err := newEnv(t).gc.GetServerConfig(t.Context(), &containerpb.GetServerConfigRequest{Name: zoneLoc})
	require.NoError(t, err)

	for _, ch := range cfg.GetChannels() {
		t.Run(ch.GetChannel().String(), func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			def := e.ready(t, zone, &containerpb.Cluster{
				Name: "def", InitialNodeCount: 1, ReleaseChannel: &containerpb.ReleaseChannel{Channel: ch.GetChannel()},
			})
			assert.Equal(t, ch.GetDefaultVersion(), def.GetCurrentMasterVersion())
			assert.Equal(t, ch.GetChannel(), def.GetReleaseChannel().GetChannel())
			assert.True(t, def.GetNodePools()[0].GetManagement().GetAutoUpgrade())

			latest := e.ready(t, zone, &containerpb.Cluster{
				Name: "latest", InitialNodeCount: 1, InitialClusterVersion: "latest",
				ReleaseChannel: &containerpb.ReleaseChannel{Channel: ch.GetChannel()},
			})
			assert.Equal(t, ch.GetValidVersions()[0], latest.GetCurrentMasterVersion())

			last := ch.GetValidVersions()[len(ch.GetValidVersions())-1]
			pinned := e.ready(t, zone, &containerpb.Cluster{
				Name: "pinned", InitialNodeCount: 1, InitialClusterVersion: last,
				ReleaseChannel: &containerpb.ReleaseChannel{Channel: ch.GetChannel()},
			})
			assert.Equal(t, last, pinned.GetCurrentMasterVersion())
		})
	}
}

func TestVersionPrefixResolution(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.ready(t, zone, &containerpb.Cluster{Name: "minor", InitialNodeCount: 1, InitialClusterVersion: "1.33"})
	assert.True(t, strings.HasPrefix(c.GetCurrentMasterVersion(), "1.33."), c.GetCurrentMasterVersion())
}

func TestIncompatibleOperations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	create := e.create(t, zone, stdCluster("busy"))

	fp := e.getCluster(t, zone, "busy").GetLabelFingerprint()
	_, err := e.gc.SetLabels(t.Context(), &containerpb.SetLabelsRequest{
		Name: clusterPath(zone, "busy"), ResourceLabels: map[string]string{"a": "b"}, LabelFingerprint: fp,
	})
	assertCode(t, codes.FailedPrecondition, err, "incompatible operation "+create.GetName())
	assert.True(t, hasDetail[*errdetails.PreconditionFailure](err))

	_, err = e.gc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: clusterPath(zone, "busy"), NodePool: &containerpb.NodePool{Name: "x", InitialNodeCount: 1},
	})
	assertCode(t, codes.FailedPrecondition, err, "incompatible operation")

	e.settle()

	_, err = e.gc.SetLabels(t.Context(), &containerpb.SetLabelsRequest{
		Name: clusterPath(zone, "busy"), ResourceLabels: map[string]string{"a": "b"}, LabelFingerprint: fp,
	})
	require.NoError(t, err)
}

func TestDeleteWhileProvisioningAbortsCreate(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	create := e.create(t, zone, stdCluster("abort"))

	_, err := e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "abort")})
	require.NoError(t, err)

	assert.Equal(t, containerpb.Operation_DONE, e.getOp(t, create).GetStatus())
	assert.Equal(t, containerpb.Cluster_STOPPING, e.getCluster(t, zone, "abort").GetStatus())

	e.settle()

	_, err = e.gc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: clusterPath(zone, "abort")})
	assertCode(t, codes.NotFound, err, "")
}
