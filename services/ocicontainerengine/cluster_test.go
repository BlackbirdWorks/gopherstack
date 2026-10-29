package ocicontainerengine_test

import (
	"net/http"
	"testing"
	"time"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	ctx := t.Context()

	created, err := e.ce.CreateCluster(ctx, ke.CreateClusterRequest{
		CreateClusterDetails: func() ke.CreateClusterDetails {
			d := clusterDetails("life")
			d.FreeformTags = map[string]string{"env": "test"}
			d.Type = ke.ClusterTypeEnhancedCluster

			return d
		}(),
	})
	require.NoError(t, err)

	wr, err := e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestStatusInProgress, wr.Status)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterCreate, wr.OperationType)

	list, err := e.ce.ListClusters(ctx, ke.ListClustersRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, ke.ClusterLifecycleStateCreating, list.Items[0].LifecycleState)

	e.settle()

	got, err := e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: list.Items[0].Id})
	require.NoError(t, err)
	c := got.Cluster
	assert.Equal(t, ke.ClusterLifecycleStateActive, c.LifecycleState)
	assert.Equal(t, ke.ClusterTypeEnhancedCluster, c.Type)
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

	wr, err = e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestStatusSucceeded, wr.Status)
	assert.Equal(t, ke.WorkRequestResourceActionTypeCreated, wr.Resources[0].ActionType)
	assert.Equal(t, *c.Id, *wr.Resources[0].Identifier)

	_, err = e.ce.UpdateCluster(ctx, ke.UpdateClusterRequest{
		ClusterId: c.Id, IfMatch: new("stale"),
		Name: new("renamed"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.ce.UpdateCluster(ctx, ke.UpdateClusterRequest{
		ClusterId: c.Id, IfMatch: got.Etag,
		Name: new("renamed"), KubernetesVersion: new("v1.35.2"),
	})
	require.NoError(t, err)

	mid, err := e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterLifecycleStateUpdating, mid.LifecycleState)

	_, err = e.ce.DeleteCluster(ctx, ke.DeleteClusterRequest{ClusterId: c.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "UPDATING clusters cannot be deleted")

	e.settle()

	after, err := e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterLifecycleStateActive, after.LifecycleState)
	assert.Equal(t, "renamed", *after.Name)
	assert.Equal(t, "v1.35.2", *after.KubernetesVersion)
	assert.Equal(t, upd.OpcWorkRequestId, after.Metadata.UpdatedByWorkRequestId)

	del, err := e.ce.DeleteCluster(ctx, ke.DeleteClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)

	mid, err = e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterLifecycleStateDeleting, mid.LifecycleState)

	e.settle()

	end, err := e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterLifecycleStateDeleted, end.LifecycleState)
	assert.NotNil(t, end.Metadata.TimeDeleted)
	assert.Equal(t, del.OpcWorkRequestId, end.Metadata.DeletedByWorkRequestId)

	gone, err := e.ce.ListClusters(ctx, ke.ListClustersRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, gone.Items, "deleted clusters are hidden unless asked for")

	withDeleted, err := e.ce.ListClusters(ctx, ke.ListClustersRequest{
		CompartmentId:  new(testCompartment),
		LifecycleState: []ke.ClusterLifecycleStateEnum{ke.ClusterLifecycleStateDeleted},
	})
	require.NoError(t, err)
	assert.Len(t, withDeleted.Items, 1)

	ops, err := e.ce.ListWorkRequests(ctx, ke.ListWorkRequestsRequest{
		CompartmentId: new(
			testCompartment,
		),
		ResourceId: c.Id,
		SortBy:     ke.ListWorkRequestsSortByTimeAccepted,
		SortOrder:  ke.ListWorkRequestsSortOrderAsc,
	})
	require.NoError(t, err)
	require.Len(t, ops.Items, 3)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterCreate, ops.Items[0].OperationType)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterUpdate, ops.Items[1].OperationType)
	assert.Equal(t, ke.WorkRequestOperationTypeClusterDelete, ops.Items[2].OperationType)
}

func TestCreateClusterValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		mod    func(*ke.CreateClusterDetails)
		code   string
		status int
	}{
		{
			"missing compartment",
			func(d *ke.CreateClusterDetails) { d.CompartmentId = nil },
			"MissingParameter",
			400,
		},
		{"missing name", func(d *ke.CreateClusterDetails) { d.Name = nil }, "MissingParameter", 400},
		{
			"missing version",
			func(d *ke.CreateClusterDetails) { d.KubernetesVersion = nil },
			"MissingParameter",
			400,
		},
		{
			"bad version",
			func(d *ke.CreateClusterDetails) { d.KubernetesVersion = new("v1.12.0") },
			"InvalidParameter",
			400,
		},
		{
			"unsupported patch",
			func(d *ke.CreateClusterDetails) { d.KubernetesVersion = new("v1.34.9") },
			"InvalidParameter",
			400,
		},
		{"unknown subnet", func(d *ke.CreateClusterDetails) {
			d.EndpointConfig.SubnetId = new("ocid1.subnet.oc1.iad.nope")
		}, "InvalidParameter", 400},
		{
			"malformed subnet",
			func(d *ke.CreateClusterDetails) { d.EndpointConfig.SubnetId = new("subnet") },
			"InvalidParameter",
			400,
		},
		{
			"subnet in other vcn",
			func(d *ke.CreateClusterDetails) { d.EndpointConfig.SubnetId = new(subOther) },
			"InvalidParameter",
			400,
		},
		{
			"empty subnet",
			func(d *ke.CreateClusterDetails) { d.EndpointConfig.SubnetId = nil },
			"MissingParameter",
			400,
		},
		{
			"unknown vcn",
			func(d *ke.CreateClusterDetails) { d.VcnId = new("ocid1.vcn.oc1.iad.nope") },
			"InvalidParameter",
			400,
		},
		{"lb subnet in other vcn", func(d *ke.CreateClusterDetails) {
			d.Options = &ke.ClusterCreateOptions{ServiceLbSubnetIds: []string{subOther}}
		}, "InvalidParameter", 400},
		{"overlapping cidrs", func(d *ke.CreateClusterDetails) {
			d.Options = &ke.ClusterCreateOptions{
				KubernetesNetworkConfig: &ke.KubernetesNetworkConfig{
					PodsCidr: new("10.0.0.0/16"), ServicesCidr: new("10.0.1.0/24"),
				},
			}
		}, "InvalidParameter", 400},
		{"bad cidr", func(d *ke.CreateClusterDetails) {
			d.Options = &ke.ClusterCreateOptions{
				KubernetesNetworkConfig: &ke.KubernetesNetworkConfig{
					PodsCidr: new("nope"),
				},
			}
		}, "InvalidParameter", 400},
		{
			"missing key",
			func(d *ke.CreateClusterDetails) { d.KmsKeyId = new("ocid1.key.oc1.iad.nope") },
			"InvalidParameter",
			400,
		},
		{
			"disabled key",
			func(d *ke.CreateClusterDetails) { d.KmsKeyId = new(keyOff) },
			"InvalidParameter",
			400,
		},
		{"bad cni", func(d *ke.CreateClusterDetails) {
			d.ClusterPodNetworkOptions = []ke.ClusterPodNetworkOptionDetails{
				ke.FlannelOverlayClusterPodNetworkOptionDetails{},
				ke.OciVcnIpNativeClusterPodNetworkOptionDetails{},
			}
		}, "InvalidParameter", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := clusterDetails("v-" + tt.name)
			tt.mod(&d)

			_, err := e.ce.CreateCluster(t.Context(), ke.CreateClusterRequest{CreateClusterDetails: d})
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
		mod   func(*ke.CreateClusterDetails)
		check func(*testing.T, ke.Cluster)
		name  string
	}{
		{
			func(d *ke.CreateClusterDetails) { d.EndpointConfig = nil; d.VcnId = nil },
			func(t *testing.T, c ke.Cluster) {
				t.Helper()
				assert.Regexp(t, `^203\.0\.113\.\d+:6443$`, *c.Endpoints.Kubernetes)
				assert.Nil(t, c.Endpoints.PrivateEndpoint)
			},
			"legacy endpoint",
		},
		{
			func(d *ke.CreateClusterDetails) { d.VcnId = nil },
			func(t *testing.T, c ke.Cluster) {
				t.Helper()
				assert.Equal(t, vcnA, *c.VcnId)
			},
			"vcn from subnet",
		},
		{func(d *ke.CreateClusterDetails) {
			d.Options = &ke.ClusterCreateOptions{
				ServiceLbSubnetIds:      []string{subLB},
				KubernetesNetworkConfig: &ke.KubernetesNetworkConfig{PodsCidr: new("10.200.0.0/16")},
			}
		}, func(t *testing.T, c ke.Cluster) {
			t.Helper()
			assert.Equal(t, []string{subLB}, c.Options.ServiceLbSubnetIds)
			assert.Equal(t, "10.200.0.0/16", *c.Options.KubernetesNetworkConfig.PodsCidr)
			assert.Equal(t, "10.96.0.0/16", *c.Options.KubernetesNetworkConfig.ServicesCidr)
		}, "custom cidrs and lb"},
		{func(d *ke.CreateClusterDetails) {
			d.KmsKeyId = new(keyOK)
			d.ClusterPodNetworkOptions = []ke.ClusterPodNetworkOptionDetails{
				ke.OciVcnIpNativeClusterPodNetworkOptionDetails{},
			}
		}, func(t *testing.T, c ke.Cluster) {
			t.Helper()
			assert.Equal(t, keyOK, *c.KmsKeyId)
			_, ok := c.ClusterPodNetworkOptions[0].(ke.OciVcnIpNativeClusterPodNetworkOptionDetails)
			assert.True(t, ok)
		}, "native cni and key"},
		{
			func(d *ke.CreateClusterDetails) { d.EndpointConfig.IsPublicIpEnabled = new(false) },
			func(t *testing.T, c ke.Cluster) {
				t.Helper()
				assert.Nil(t, c.Endpoints.PublicEndpoint)
				assert.NotNil(t, c.Endpoints.PrivateEndpoint)
			},
			"private endpoint only",
		},
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

	_, err := e.ce.CreateCluster(
		ctx,
		ke.CreateClusterRequest{CreateClusterDetails: clusterDetails("guard")},
	)
	require.NoError(t, err)

	list, err := e.ce.ListClusters(ctx, ke.ListClustersRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)

	id := list.Items[0].Id

	tests := []struct {
		call func() error
		name string
	}{
		{func() error {
			_, rerr := e.ce.UpdateCluster(ctx, ke.UpdateClusterRequest{ClusterId: id,
				Name: new("x")})

			return rerr
		}, "update while creating"},
		{func() error {
			_, rerr := e.ce.DeleteCluster(ctx, ke.DeleteClusterRequest{ClusterId: id})

			return rerr
		}, "delete while creating"},
		{func() error {
			_, rerr := e.ce.CreateKubeconfig(ctx, ke.CreateKubeconfigRequest{ClusterId: id})

			return rerr
		}, "kubeconfig while creating"},
		{func() error {
			_, rerr := e.ce.CreateNodePool(
				ctx,
				ke.CreateNodePoolRequest{CreateNodePoolDetails: poolDetails(id, "p")},
			)

			return rerr
		}, "node pool while creating"},
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
		call func() error
		name string
	}{
		{func() error {
			_, rerr := e.ce.GetCluster(
				ctx,
				ke.GetClusterRequest{ClusterId: new("ocid1.cluster.oc1.iad.nope")},
			)

			return rerr
		}, "get"},
		{func() error {
			_, rerr := e.ce.DeleteCluster(
				ctx,
				ke.DeleteClusterRequest{ClusterId: new("ocid1.cluster.oc1.iad.nope")},
			)

			return rerr
		}, "delete"},
		{func() error {
			_, rerr := e.ce.GetClusterMigrateToNativeVcnStatus(
				ctx,
				ke.GetClusterMigrateToNativeVcnStatusRequest{
					ClusterId: new("ocid1.cluster.oc1.iad.nope")},
			)

			return rerr
		}, "migrate status"},
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
		in   ke.UpdateClusterDetails
	}{
		{"downgrade", ke.UpdateClusterDetails{KubernetesVersion: new("v1.34.1")}},
		{"skip minor", ke.UpdateClusterDetails{KubernetesVersion: new("v1.36.4")}},
		{"unknown version", ke.UpdateClusterDetails{KubernetesVersion: new("v1.99.0")}},
		{"empty name", ke.UpdateClusterDetails{Name: new(" ")}},
		{
			"basic to enhanced reverse",
			ke.UpdateClusterDetails{Type: ke.ClusterTypeBasicCluster},
		},
	}

	// A BASIC cluster may become ENHANCED but not the reverse; the reverse case needs an ENHANCED cluster first.
	_, err := e.ce.UpdateCluster(t.Context(), ke.UpdateClusterRequest{
		ClusterId: c.Id, Type: ke.ClusterTypeEnhancedCluster,
	})
	require.NoError(t, err)
	e.settle()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, rerr := e.ce.UpdateCluster(t.Context(), ke.UpdateClusterRequest{
				ClusterId: c.Id, UpdateClusterDetails: tt.in,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, rerr).GetHTTPStatusCode())
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
		req  ke.ListClustersRequest
		want []string
	}{
		{"all newest first", ke.ListClustersRequest{}, []string{*b.Name, *a.Name}},
		{
			"by name asc",
			ke.ListClustersRequest{
				SortBy:    ke.ListClustersSortByName,
				SortOrder: ke.ListClustersSortOrderAsc,
			},
			[]string{"alpha", "bravo"},
		},
		{"name filter", ke.ListClustersRequest{Name: new("bravo")}, []string{"bravo"}},
		{
			"state filter",
			ke.ListClustersRequest{
				LifecycleState: []ke.ClusterLifecycleStateEnum{
					ke.ClusterLifecycleStateCreating,
				},
			},
			nil,
		},
		{
			"limit",
			ke.ListClustersRequest{
				Limit:     new(1),
				SortBy:    ke.ListClustersSortByName,
				SortOrder: ke.ListClustersSortOrderAsc,
			},
			[]string{"alpha"},
		},
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
	legacy := e.mkCluster(
		t,
		"legacy",
		func(d *ke.CreateClusterDetails) { d.EndpointConfig = nil; d.VcnId = new(vcnA) },
	)
	native := e.mkCluster(t, "native", nil)

	st, err := e.ce.GetClusterMigrateToNativeVcnStatus(
		ctx,
		ke.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterMigrateToNativeVcnStatusStateNotStarted, st.State)

	tests := []struct {
		in   ke.ClusterMigrateToNativeVcnDetails
		id   *string
		name string
	}{
		{
			ke.ClusterMigrateToNativeVcnDetails{
				EndpointConfig: &ke.ClusterEndpointConfig{SubnetId: new(subRegional)},
			},
			native.Id,
			"already native",
		},
		{ke.ClusterMigrateToNativeVcnDetails{}, legacy.Id, "no endpoint config"},
		{
			ke.ClusterMigrateToNativeVcnDetails{
				EndpointConfig: &ke.ClusterEndpointConfig{SubnetId: new(subOther)},
			},
			legacy.Id,
			"subnet other vcn",
		},
		{
			ke.ClusterMigrateToNativeVcnDetails{
				EndpointConfig: &ke.ClusterEndpointConfig{
					SubnetId: new(subRegional),
				},
				DecommissionDelayDuration: new("forever"),
			},
			legacy.Id,
			"bad duration",
		},
		{
			ke.ClusterMigrateToNativeVcnDetails{
				EndpointConfig: &ke.ClusterEndpointConfig{
					SubnetId: new(subRegional),
				},
				DecommissionDelayDuration: new("P31D"),
			},
			legacy.Id,
			"duration too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, rerr := e.ce.ClusterMigrateToNativeVcn(ctx, ke.ClusterMigrateToNativeVcnRequest{
				ClusterId: tt.id, ClusterMigrateToNativeVcnDetails: tt.in,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, rerr).GetHTTPStatusCode())
		})
	}

	_, err = e.ce.ClusterMigrateToNativeVcn(ctx, ke.ClusterMigrateToNativeVcnRequest{
		ClusterId: legacy.Id,
		EndpointConfig: &ke.ClusterEndpointConfig{
			SubnetId: new(subRegional),
		},
		DecommissionDelayDuration: new("P2D"),
	})
	require.NoError(t, err)

	st, err = e.ce.GetClusterMigrateToNativeVcnStatus(
		ctx,
		ke.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterMigrateToNativeVcnStatusStateInProgress, st.State)

	e.settle()

	st, err = e.ce.GetClusterMigrateToNativeVcnStatus(
		ctx,
		ke.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterMigrateToNativeVcnStatusStatePendingDecommission, st.State)
	require.NotNil(t, st.TimeDecommissionScheduled)

	got, err := e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.NotNil(t, got.Endpoints.PrivateEndpoint)
	assert.NotNil(t, got.Endpoints.Kubernetes, "the legacy endpoint survives until decommission")
	assert.Equal(t, subRegional, *got.EndpointConfig.SubnetId)

	e.clock.Advance(3 * 24 * time.Hour)

	st, err = e.ce.GetClusterMigrateToNativeVcnStatus(
		ctx,
		ke.GetClusterMigrateToNativeVcnStatusRequest{ClusterId: legacy.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, ke.ClusterMigrateToNativeVcnStatusStateCompleted, st.State)

	got, err = e.ce.GetCluster(ctx, ke.GetClusterRequest{ClusterId: legacy.Id})
	require.NoError(t, err)
	assert.Nil(t, got.Endpoints.Kubernetes)
}

func TestStandaloneWithoutCore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{noNet: true})

	tests := []struct {
		mod    func(*ke.CreateClusterDetails)
		name   string
		status int
	}{
		{func(d *ke.CreateClusterDetails) {
			d.EndpointConfig.SubnetId = new("ocid1.subnet.oc1.iad.any")
		}, "unknown subnet accepted", 0},
		{
			func(d *ke.CreateClusterDetails) { d.EndpointConfig.SubnetId = new("x") },
			"malformed subnet rejected",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := clusterDetails("s-" + tt.name)
			tt.mod(&d)

			_, err := e.ce.CreateCluster(t.Context(), ke.CreateClusterRequest{CreateClusterDetails: d})
			if tt.status == 0 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
