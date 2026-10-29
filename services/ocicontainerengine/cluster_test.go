package ocicontainerengine_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	ctx := t.Context()

	created, err := e.ce.CreateCluster(ctx, containerengine.CreateClusterRequest{
		CreateClusterDetails: func() containerengine.CreateClusterDetails {
			d := clusterDetails("life")
			d.FreeformTags = map[string]string{"env": "test"}
			d.Type = containerengine.ClusterTypeEnhancedCluster

			return d
		}(),
	})
	require.NoError(t, err)

	wr, err := e.ce.GetWorkRequest(ctx, containerengine.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, containerengine.WorkRequestStatusInProgress, wr.Status)
	assert.Equal(t, containerengine.WorkRequestOperationTypeClusterCreate, wr.OperationType)

	list, err := e.ce.ListClusters(ctx, containerengine.ListClustersRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, containerengine.ClusterLifecycleStateCreating, list.Items[0].LifecycleState)

	e.settle()

	got, err := e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: list.Items[0].Id})
	require.NoError(t, err)
	c := got.Cluster
	assert.Equal(t, containerengine.ClusterLifecycleStateActive, c.LifecycleState)
	assert.Equal(t, containerengine.ClusterTypeEnhancedCluster, c.Type)
	assert.Equal(t, "test", c.FreeformTags["env"])
	assert.Equal(t, testVersion, *c.KubernetesVersion)
	assert.Equal(t, vcnA, *c.VcnId)
	assert.Equal(t, []string{"v1.34.10", "v1.35.2"}, c.AvailableKubernetesUpgrades)
	assert.Equal(t, "10.244.0.0/16", *c.Options.KubernetesNetworkConfig.PodsCidr)
	assert.Equal(t, "10.96.0.0/16", *c.Options.KubernetesNetworkConfig.ServicesCidr)
	assert.False(t, *c.Options.AddOns.IsTillerEnabled)
	require.Len(t, c.ClusterPodNetworkOptions, 1)
	assert.Equal(t, "10.0.3.5:6443", *c.Endpoints.PrivateEndpoint)
	assert.Regexp(t, `^203\.0\.113\.\d+:6443$`, *c.Endpoints.PublicEndpoint)
	assert.Nil(t, c.Endpoints.Kubernetes)
	assert.Equal(t, created.OpcWorkRequestId, c.Metadata.CreatedByWorkRequestId)
	require.NotNil(t, got.Etag)

	wr, err = e.ce.GetWorkRequest(ctx, containerengine.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, containerengine.WorkRequestStatusSucceeded, wr.Status)
	assert.Equal(t, containerengine.WorkRequestResourceActionTypeCreated, wr.Resources[0].ActionType)
	assert.Equal(t, *c.Id, *wr.Resources[0].Identifier)

	_, err = e.ce.UpdateCluster(ctx, containerengine.UpdateClusterRequest{
		ClusterId: c.Id, IfMatch: new("stale"),
		UpdateClusterDetails: containerengine.UpdateClusterDetails{Name: new("renamed")},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.ce.UpdateCluster(ctx, containerengine.UpdateClusterRequest{
		ClusterId: c.Id, IfMatch: got.Etag,
		UpdateClusterDetails: containerengine.UpdateClusterDetails{
			Name: new("renamed"), KubernetesVersion: new("v1.35.2"),
		},
	})
	require.NoError(t, err)

	mid, err := e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterLifecycleStateUpdating, mid.LifecycleState)

	_, err = e.ce.DeleteCluster(ctx, containerengine.DeleteClusterRequest{ClusterId: c.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "UPDATING clusters cannot be deleted")

	e.settle()

	after, err := e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterLifecycleStateActive, after.LifecycleState)
	assert.Equal(t, "renamed", *after.Name)
	assert.Equal(t, "v1.35.2", *after.KubernetesVersion)
	assert.Equal(t, upd.OpcWorkRequestId, after.Metadata.UpdatedByWorkRequestId)

	del, err := e.ce.DeleteCluster(ctx, containerengine.DeleteClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)

	mid, err = e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterLifecycleStateDeleting, mid.LifecycleState)

	e.settle()

	end, err := e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterLifecycleStateDeleted, end.LifecycleState)
	assert.NotNil(t, end.Metadata.TimeDeleted)
	assert.Equal(t, del.OpcWorkRequestId, end.Metadata.DeletedByWorkRequestId)

	gone, err := e.ce.ListClusters(ctx, containerengine.ListClustersRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, gone.Items, "deleted clusters are hidden unless asked for")

	withDeleted, err := e.ce.ListClusters(ctx, containerengine.ListClustersRequest{
		CompartmentId:  new(testCompartment),
		LifecycleState: []containerengine.ClusterLifecycleStateEnum{containerengine.ClusterLifecycleStateDeleted},
	})
	require.NoError(t, err)
	assert.Len(t, withDeleted.Items, 1)

	ops, err := e.ce.ListWorkRequests(ctx, containerengine.ListWorkRequestsRequest{
		CompartmentId: new(testCompartment), ResourceId: c.Id, SortBy: containerengine.ListWorkRequestsSortByTimeAccepted,
		SortOrder: containerengine.ListWorkRequestsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, ops.Items, 3)
	assert.Equal(t, containerengine.WorkRequestOperationTypeClusterCreate, ops.Items[0].OperationType)
	assert.Equal(t, containerengine.WorkRequestOperationTypeClusterUpdate, ops.Items[1].OperationType)
	assert.Equal(t, containerengine.WorkRequestOperationTypeClusterDelete, ops.Items[2].OperationType)
}

func TestCreateClusterValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		mod    func(*containerengine.CreateClusterDetails)
		code   string
		status int
	}{
		{"missing compartment", func(d *containerengine.CreateClusterDetails) { d.CompartmentId = nil }, "MissingParameter", 400},
		{"missing name", func(d *containerengine.CreateClusterDetails) { d.Name = nil }, "MissingParameter", 400},
		{"missing version", func(d *containerengine.CreateClusterDetails) { d.KubernetesVersion = nil }, "MissingParameter", 400},
		{"bad version", func(d *containerengine.CreateClusterDetails) { d.KubernetesVersion = new("v1.12.0") }, "InvalidParameter", 400},
		{"unsupported patch", func(d *containerengine.CreateClusterDetails) { d.KubernetesVersion = new("v1.34.9") }, "InvalidParameter", 400},
		{"unknown subnet", func(d *containerengine.CreateClusterDetails) {
			d.EndpointConfig.SubnetId = new("ocid1.subnet.oc1.iad.nope")
		}, "InvalidParameter", 400},
		{"malformed subnet", func(d *containerengine.CreateClusterDetails) { d.EndpointConfig.SubnetId = new("subnet") }, "InvalidParameter", 400},
		{"subnet in other vcn", func(d *containerengine.CreateClusterDetails) { d.EndpointConfig.SubnetId = new(subOther) }, "InvalidParameter", 400},
		{"empty subnet", func(d *containerengine.CreateClusterDetails) { d.EndpointConfig.SubnetId = nil }, "MissingParameter", 400},
		{"unknown vcn", func(d *containerengine.CreateClusterDetails) { d.VcnId = new("ocid1.vcn.oc1.iad.nope") }, "InvalidParameter", 400},
		{"lb subnet in other vcn", func(d *containerengine.CreateClusterDetails) {
			d.Options = &containerengine.ClusterCreateOptions{ServiceLbSubnetIds: []string{subOther}}
		}, "InvalidParameter", 400},
		{"overlapping cidrs", func(d *containerengine.CreateClusterDetails) {
			d.Options = &containerengine.ClusterCreateOptions{KubernetesNetworkConfig: &containerengine.KubernetesNetworkConfig{
				PodsCidr: new("10.0.0.0/16"), ServicesCidr: new("10.0.1.0/24"),
			}}
		}, "InvalidParameter", 400},
		{"bad cidr", func(d *containerengine.CreateClusterDetails) {
			d.Options = &containerengine.ClusterCreateOptions{KubernetesNetworkConfig: &containerengine.KubernetesNetworkConfig{
				PodsCidr: new("nope"),
			}}
		}, "InvalidParameter", 400},
		{"missing key", func(d *containerengine.CreateClusterDetails) { d.KmsKeyId = new("ocid1.key.oc1.iad.nope") }, "InvalidParameter", 400},
		{"disabled key", func(d *containerengine.CreateClusterDetails) { d.KmsKeyId = new(keyOff) }, "InvalidParameter", 400},
		{"bad cni", func(d *containerengine.CreateClusterDetails) {
			d.ClusterPodNetworkOptions = []containerengine.ClusterPodNetworkOptionDetails{
				containerengine.FlannelOverlayClusterPodNetworkOptionDetails{},
				containerengine.OciVcnIpNativeClusterPodNetworkOptionDetails{},
			}
		}, "InvalidParameter", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := clusterDetails("v-" + tt.name)
			tt.mod(&d)

			_, err := e.ce.CreateCluster(t.Context(), containerengine.CreateClusterRequest{CreateClusterDetails: d})
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestCreateClusterAccepts(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name  string
		mod   func(*containerengine.CreateClusterDetails)
		check func(*testing.T, containerengine.Cluster)
	}{
		{"legacy endpoint", func(d *containerengine.CreateClusterDetails) { d.EndpointConfig = nil; d.VcnId = nil },
			func(t *testing.T, c containerengine.Cluster) {
				t.Helper()
				assert.Regexp(t, `^203\.0\.113\.\d+:6443$`, *c.Endpoints.Kubernetes)
				assert.Nil(t, c.Endpoints.PrivateEndpoint)
			}},
		{"vcn from subnet", func(d *containerengine.CreateClusterDetails) { d.VcnId = nil },
			func(t *testing.T, c containerengine.Cluster) {
				t.Helper()
				assert.Equal(t, vcnA, *c.VcnId)
			}},
		{"custom cidrs and lb", func(d *containerengine.CreateClusterDetails) {
			d.Options = &containerengine.ClusterCreateOptions{
				ServiceLbSubnetIds:      []string{subLB},
				KubernetesNetworkConfig: &containerengine.KubernetesNetworkConfig{PodsCidr: new("10.200.0.0/16")},
			}
		}, func(t *testing.T, c containerengine.Cluster) {
			t.Helper()
			assert.Equal(t, []string{subLB}, c.Options.ServiceLbSubnetIds)
			assert.Equal(t, "10.200.0.0/16", *c.Options.KubernetesNetworkConfig.PodsCidr)
			assert.Equal(t, "10.96.0.0/16", *c.Options.KubernetesNetworkConfig.ServicesCidr)
		}},
		{"native cni and key", func(d *containerengine.CreateClusterDetails) {
			d.KmsKeyId = new(keyOK)
			d.ClusterPodNetworkOptions = []containerengine.ClusterPodNetworkOptionDetails{
				containerengine.OciVcnIpNativeClusterPodNetworkOptionDetails{},
			}
		}, func(t *testing.T, c containerengine.Cluster) {
			t.Helper()
			assert.Equal(t, keyOK, *c.KmsKeyId)
			_, ok := c.ClusterPodNetworkOptions[0].(containerengine.OciVcnIpNativeClusterPodNetworkOptionDetails)
			assert.True(t, ok)
		}},
		{"private endpoint only", func(d *containerengine.CreateClusterDetails) { d.EndpointConfig.IsPublicIpEnabled = new(false) },
			func(t *testing.T, c containerengine.Cluster) {
				t.Helper()
				assert.Nil(t, c.Endpoints.PublicEndpoint)
				assert.NotNil(t, c.Endpoints.PrivateEndpoint)
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := e.mkCluster(t, "a-"+tt.name, tt.mod)
			tt.check(t, c)
		})
	}
}

func TestClusterStateGuards(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()

	_, err := e.ce.CreateCluster(ctx, containerengine.CreateClusterRequest{CreateClusterDetails: clusterDetails("guard")})
	require.NoError(t, err)

	list, err := e.ce.ListClusters(ctx, containerengine.ListClustersRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)

	id := list.Items[0].Id

	tests := []struct {
		name string
		call func() error
	}{
		{"update while creating", func() error {
			_, err := e.ce.UpdateCluster(ctx, containerengine.UpdateClusterRequest{ClusterId: id,
				UpdateClusterDetails: containerengine.UpdateClusterDetails{Name: new("x")}})
			return err
		}},
		{"delete while creating", func() error {
			_, err := e.ce.DeleteCluster(ctx, containerengine.DeleteClusterRequest{ClusterId: id})
			return err
		}},
		{"kubeconfig while creating", func() error {
			_, err := e.ce.CreateKubeconfig(ctx, containerengine.CreateKubeconfigRequest{ClusterId: id})
			return err
		}},
		{"node pool while creating", func() error {
			_, err := e.ce.CreateNodePool(ctx, containerengine.CreateNodePoolRequest{CreateNodePoolDetails: poolDetails(id, "p")})
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
			assert.Equal(t, "IncorrectState", se.GetCode())
		})
	}

	missing := []struct {
		name string
		call func() error
	}{
		{"get", func() error {
			_, err := e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: new("ocid1.cluster.oc1.iad.nope")})
			return err
		}},
		{"delete", func() error {
			_, err := e.ce.DeleteCluster(ctx, containerengine.DeleteClusterRequest{ClusterId: new("ocid1.cluster.oc1.iad.nope")})
			return err
		}},
		{"migrate status", func() error {
			_, err := e.ce.GetClusterMigrateToNativeVcnStatus(ctx, containerengine.GetClusterMigrateToNativeVcnStatusRequest{
				ClusterId: new("ocid1.cluster.oc1.iad.nope")})
			return err
		}},
	}

	for _, tt := range missing {
		t.Run("missing "+tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Contains(t, []int{400, 404}, se.GetHTTPStatusCode())
		})
	}
}

func TestUpdateClusterValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "upd", nil)

	tests := []struct {
		name string
		in   containerengine.UpdateClusterDetails
	}{
		{"downgrade", containerengine.UpdateClusterDetails{KubernetesVersion: new("v1.34.1")}},
		{"skip minor", containerengine.UpdateClusterDetails{KubernetesVersion: new("v1.36.4")}},
		{"unknown version", containerengine.UpdateClusterDetails{KubernetesVersion: new("v1.99.0")}},
		{"empty name", containerengine.UpdateClusterDetails{Name: new(" ")}},
		{"basic to enhanced reverse", containerengine.UpdateClusterDetails{Type: containerengine.ClusterTypeBasicCluster}},
	}

	// A BASIC cluster may become ENHANCED but not the reverse; the reverse case needs an ENHANCED cluster first.
	_, err := e.ce.UpdateCluster(t.Context(), containerengine.UpdateClusterRequest{
		ClusterId: c.Id, UpdateClusterDetails: containerengine.UpdateClusterDetails{Type: containerengine.ClusterTypeEnhancedCluster},
	})
	require.NoError(t, err)
	e.settle()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.ce.UpdateCluster(t.Context(), containerengine.UpdateClusterRequest{
				ClusterId: c.Id, UpdateClusterDetails: tt.in,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestClusterListFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkCluster(t, "alpha", nil)
	e.clock.Advance(1)
	b := e.mkCluster(t, "bravo", nil)

	tests := []struct {
		name string
		req  containerengine.ListClustersRequest
		want []string
	}{
		{"all newest first", containerengine.ListClustersRequest{}, []string{*b.Name, *a.Name}},
		{"by name asc", containerengine.ListClustersRequest{SortBy: containerengine.ListClustersSortByName, SortOrder: containerengine.ListClustersSortOrderAsc}, []string{"alpha", "bravo"}},
		{"name filter", containerengine.ListClustersRequest{Name: new("bravo")}, []string{"bravo"}},
		{"state filter", containerengine.ListClustersRequest{LifecycleState: []containerengine.ClusterLifecycleStateEnum{containerengine.ClusterLifecycleStateCreating}}, nil},
		{"limit", containerengine.ListClustersRequest{Limit: new(1), SortBy: containerengine.ListClustersSortByName, SortOrder: containerengine.ListClustersSortOrderAsc}, []string{"alpha"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.req.CompartmentId = new(testCompartment)

			resp, err := e.ce.ListClusters(t.Context(), tt.req)
			require.NoError(t, err)

			var names []string
			for _, c := range resp.Items {
				names = append(names, *c.Name)
			}

			assert.Equal(t, tt.want, names)

			if tt.req.Limit != nil {
				require.NotNil(t, resp.OpcNextPage)
			}
		})
	}
}

func TestMigrateToNativeVcn(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	legacy := e.mkCluster(t, "legacy", func(d *containerengine.CreateClusterDetails) { d.EndpointConfig = nil; d.VcnId = new(vcnA) })
	native := e.mkCluster(t, "native", nil)

	st, err := e.ce.GetClusterMigrateToNativeVcnStatus(ctx, containerengine.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterMigrateToNativeVcnStatusStateNotStarted, st.State)

	tests := []struct {
		name string
		id   *string
		in   containerengine.ClusterMigrateToNativeVcnDetails
	}{
		{"already native", native.Id, containerengine.ClusterMigrateToNativeVcnDetails{EndpointConfig: &containerengine.ClusterEndpointConfig{SubnetId: new(subRegional)}}},
		{"no endpoint config", legacy.Id, containerengine.ClusterMigrateToNativeVcnDetails{}},
		{"subnet other vcn", legacy.Id, containerengine.ClusterMigrateToNativeVcnDetails{EndpointConfig: &containerengine.ClusterEndpointConfig{SubnetId: new(subOther)}}},
		{"bad duration", legacy.Id, containerengine.ClusterMigrateToNativeVcnDetails{
			EndpointConfig: &containerengine.ClusterEndpointConfig{SubnetId: new(subRegional)}, DecommissionDelayDuration: new("forever")}},
		{"duration too long", legacy.Id, containerengine.ClusterMigrateToNativeVcnDetails{
			EndpointConfig: &containerengine.ClusterEndpointConfig{SubnetId: new(subRegional)}, DecommissionDelayDuration: new("P31D")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.ce.ClusterMigrateToNativeVcn(ctx, containerengine.ClusterMigrateToNativeVcnRequest{
				ClusterId: tt.id, ClusterMigrateToNativeVcnDetails: tt.in,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	_, err = e.ce.ClusterMigrateToNativeVcn(ctx, containerengine.ClusterMigrateToNativeVcnRequest{
		ClusterId: legacy.Id, ClusterMigrateToNativeVcnDetails: containerengine.ClusterMigrateToNativeVcnDetails{
			EndpointConfig: &containerengine.ClusterEndpointConfig{SubnetId: new(subRegional)}, DecommissionDelayDuration: new("P2D"),
		},
	})
	require.NoError(t, err)

	st, err = e.ce.GetClusterMigrateToNativeVcnStatus(ctx, containerengine.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterMigrateToNativeVcnStatusStateInProgress, st.State)

	e.settle()

	st, err = e.ce.GetClusterMigrateToNativeVcnStatus(ctx, containerengine.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterMigrateToNativeVcnStatusStatePendingDecommission, st.State)
	require.NotNil(t, st.TimeDecommissionScheduled)

	got, err := e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.NotNil(t, got.Endpoints.PrivateEndpoint)
	assert.NotNil(t, got.Endpoints.Kubernetes, "the legacy endpoint survives until decommission")
	assert.Equal(t, subRegional, *got.EndpointConfig.SubnetId)

	e.clock.Advance(3 * 24 * time.Hour)

	st, err = e.ce.GetClusterMigrateToNativeVcnStatus(ctx, containerengine.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.Equal(t, containerengine.ClusterMigrateToNativeVcnStatusStateCompleted, st.State)

	got, err = e.ce.GetCluster(ctx, containerengine.GetClusterRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.Nil(t, got.Endpoints.Kubernetes)
}

func TestStandaloneWithoutCore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{noNet: true})

	tests := []struct {
		name   string
		mod    func(*containerengine.CreateClusterDetails)
		status int
	}{
		{"unknown subnet accepted", func(d *containerengine.CreateClusterDetails) {
			d.EndpointConfig.SubnetId = new("ocid1.subnet.oc1.iad.any")
		}, 0},
		{"malformed subnet rejected", func(d *containerengine.CreateClusterDetails) { d.EndpointConfig.SubnetId = new("x") }, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := clusterDetails("s-" + tt.name)
			tt.mod(&d)

			_, err := e.ce.CreateCluster(t.Context(), containerengine.CreateClusterRequest{CreateClusterDetails: d})
			if tt.status == 0 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
