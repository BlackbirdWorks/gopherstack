package ocicontainerengine_test

import (
	"net/http"
	"testing"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodePoolLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	c := e.mkCluster(t, "pools", nil)

	created, err := e.ce.CreateNodePool(ctx, ke.CreateNodePoolRequest{
		CreateNodePoolDetails: poolDetails(c.Id, "workers"),
	})
	require.NoError(t, err)

	wr, err := e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeNodepoolCreate, wr.OperationType)
	assert.Equal(t, ke.WorkRequestStatusInProgress, wr.Status)

	list, err := e.ce.ListNodePools(ctx, ke.ListNodePoolsRequest{
		CompartmentId: new(testCompartment), ClusterId: c.Id,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, ke.NodePoolLifecycleStateCreating, list.Items[0].LifecycleState)

	mid, err := e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: list.Items[0].Id})
	require.NoError(t, err)
	require.Len(t, mid.Nodes, 3)

	for _, n := range mid.Nodes {
		assert.Equal(t, ke.NodeLifecycleStateCreating, n.LifecycleState)
	}

	e.settle()

	got, err := e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: list.Items[0].Id})
	require.NoError(t, err)
	np := got.NodePool
	assert.Equal(t, ke.NodePoolLifecycleStateActive, np.LifecycleState)
	assert.Equal(t, testVersion, *np.KubernetesVersion, "defaults to the cluster version")
	assert.Equal(t, imgOL8, *np.NodeImageId)
	assert.Equal(t, "Oracle-Linux-8.10-2024.09.30-0", *np.NodeImageName)
	assert.Equal(t, 3, *np.NodeConfigDetails.Size)
	assert.ElementsMatch(t, []string{subAD1, subAD2}, np.SubnetIds)
	require.Len(t, np.Nodes, 3)

	perAD := map[string]int{}
	ips := map[string]bool{}

	for _, n := range np.Nodes {
		assert.Equal(t, ke.NodeLifecycleStateActive, n.LifecycleState)
		assert.Equal(t, testVersion, *n.KubernetesVersion)
		assert.Equal(t, *np.Id, *n.NodePoolId)
		assert.Contains(t, *n.Name, "oke-c")
		assert.Regexp(t, `^10\.0\.[12]\.\d+$`, *n.PrivateIp)
		ips[*n.PrivateIp] = true
		perAD[*n.AvailabilityDomain]++
	}

	assert.Len(t, ips, 3, "node IPs are unique")
	assert.Equal(t, map[string]int{ad1: 2, ad2: 1}, perAD, "nodes spread over placements")

	wr, err = e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestStatusSucceeded, wr.Status)

	// scale up to 5, then rename
	_, err = e.ce.UpdateNodePool(ctx, ke.UpdateNodePoolRequest{
		NodePoolId: np.Id,
		Name:       new("renamed"), NodeConfigDetails: &ke.UpdateNodePoolNodeConfigDetails{Size: new(5)},
		InitialNodeLabels: []ke.KeyValue{{Key: new("tier"), Value: new("web")}},
	})
	require.NoError(t, err)

	got, err = e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.NodePoolLifecycleStateUpdating, got.LifecycleState)
	assert.Len(t, got.Nodes, 5)

	e.settle()

	got, err = e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *got.Name)
	assert.Equal(t, "web", *got.InitialNodeLabels[0].Value)
	assert.Len(t, got.Nodes, 5)

	// scale down to 2
	_, err = e.ce.UpdateNodePool(ctx, ke.UpdateNodePoolRequest{
		NodePoolId:        np.Id,
		NodeConfigDetails: &ke.UpdateNodePoolNodeConfigDetails{Size: new(2)},
	})
	require.NoError(t, err)
	e.settle()

	got, err = e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	require.Len(t, got.Nodes, 2)

	// delete one node with decrement, then one with replacement
	victim := got.Nodes[0]
	_, err = e.ce.DeleteNode(ctx, ke.DeleteNodeRequest{
		NodePoolId: np.Id, NodeId: victim.Id, IsDecrementSize: new(true),
	})
	require.NoError(t, err)
	e.settle()

	got, err = e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	require.Len(t, got.Nodes, 1)
	assert.Equal(t, 1, *got.NodeConfigDetails.Size)
	assert.NotEqual(t, *victim.Id, *got.Nodes[0].Id)

	old := got.Nodes[0]
	_, err = e.ce.DeleteNode(ctx, ke.DeleteNodeRequest{NodePoolId: np.Id, NodeId: old.Id})
	require.NoError(t, err)
	e.settle()

	got, err = e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	require.Len(t, got.Nodes, 1, "the deleted node is replaced")
	assert.NotEqual(t, *old.Id, *got.Nodes[0].Id)
	assert.Equal(t, *old.AvailabilityDomain, *got.Nodes[0].AvailabilityDomain)

	_, err = e.ce.DeleteNode(
		ctx,
		ke.DeleteNodeRequest{NodePoolId: np.Id, NodeId: new("ocid1.instance.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	del, err := e.ce.DeleteNodePool(ctx, ke.DeleteNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)

	got, err = e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.NodePoolLifecycleStateDeleting, got.LifecycleState)
	e.settle()

	got, err = e.ce.GetNodePool(ctx, ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.NodePoolLifecycleStateDeleted, got.LifecycleState)
	assert.Empty(t, got.Nodes)

	wr, err = e.ce.GetWorkRequest(ctx, ke.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestOperationTypeNodepoolDelete, wr.OperationType)
	assert.Equal(t, ke.WorkRequestStatusSucceeded, wr.Status)

	ops, err := e.ce.ListWorkRequests(ctx, ke.ListWorkRequestsRequest{
		CompartmentId: new(
			testCompartment,
		),
		ClusterId:    c.Id,
		ResourceType: ke.ListWorkRequestsResourceTypeNodepool,
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(ops.Items), 6)
}

func TestClusterDeleteCascadesToNodePools(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "cascade", nil)
	np := e.mkPool(t, c.Id, "p", nil)

	_, err := e.ce.DeleteCluster(t.Context(), ke.DeleteClusterRequest{ClusterId: c.Id})
	require.NoError(t, err)

	got, err := e.ce.GetNodePool(t.Context(), ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.NodePoolLifecycleStateDeleting, got.LifecycleState)

	e.settle()

	got, err = e.ce.GetNodePool(t.Context(), ke.GetNodePoolRequest{NodePoolId: np.Id})
	require.NoError(t, err)
	assert.Equal(t, ke.NodePoolLifecycleStateDeleted, got.LifecycleState)
}

func TestCreateNodePoolValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "val", nil)

	tests := []struct {
		name string
		mod  func(*ke.CreateNodePoolDetails)
		code string
		http int
	}{
		{"missing name", func(d *ke.CreateNodePoolDetails) { d.Name = nil }, "MissingParameter", 400},
		{
			"missing shape",
			func(d *ke.CreateNodePoolDetails) { d.NodeShape = nil },
			"MissingParameter",
			400,
		},
		{
			"unknown shape",
			func(d *ke.CreateNodePoolDetails) { d.NodeShape = new("VM.Nope") },
			"InvalidParameter",
			400,
		},
		{
			"flex without config",
			func(d *ke.CreateNodePoolDetails) { d.NodeShape = new(shapeFlex) },
			"MissingParameter",
			400,
		},
		{"flex ocpus too big", func(d *ke.CreateNodePoolDetails) {
			d.NodeShape = new(shapeFlex)
			d.NodeShapeConfig = &ke.CreateNodeShapeConfigDetails{Ocpus: new(float32(65))}
		}, "InvalidParameter", 400},
		{"flex memory too big", func(d *ke.CreateNodePoolDetails) {
			d.NodeShape = new(shapeFlex)
			d.NodeShapeConfig = &ke.CreateNodeShapeConfigDetails{
				Ocpus:       new(float32(1)),
				MemoryInGBs: new(float32(900)),
			}
		}, "InvalidParameter", 400},
		{"fixed with config", func(d *ke.CreateNodePoolDetails) {
			d.NodeShapeConfig = &ke.CreateNodeShapeConfigDetails{Ocpus: new(float32(2))}
		}, "InvalidParameter", 400},
		{
			"no image",
			func(d *ke.CreateNodePoolDetails) { d.NodeSourceDetails = nil },
			"MissingParameter",
			400,
		},
		{"unknown image", func(d *ke.CreateNodePoolDetails) {
			d.NodeSourceDetails = ke.NodeSourceViaImageDetails{ImageId: new("ocid1.image.oc1.iad.nope")}
		}, "InvalidParameter", 400},
		{"arch mismatch", func(d *ke.CreateNodePoolDetails) {
			d.NodeSourceDetails = ke.NodeSourceViaImageDetails{ImageId: new(imgOL8Arm)}
		}, "InvalidParameter", 400},
		{"unknown image name", func(d *ke.CreateNodePoolDetails) {
			d.NodeSourceDetails = nil
			d.NodeImageName = new("nope")
		}, "InvalidParameter", 400},
		{"no placements", func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.PlacementConfigs = []ke.NodePoolPlacementConfigDetails{}
		}, "MissingParameter", 400},
		{"bad ad", func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.PlacementConfigs[0].AvailabilityDomain = new("AD-1")
		}, "InvalidParameter", 400},
		{"unknown subnet", func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.PlacementConfigs[0].SubnetId = new("ocid1.subnet.oc1.iad.nope")
		}, "InvalidParameter", 400},
		{"subnet other vcn", func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.PlacementConfigs[0].SubnetId = new(subOther)
		}, "InvalidParameter", 400},
		{"subnet wrong ad", func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.PlacementConfigs[0].AvailabilityDomain = new(ad2)
		}, "InvalidParameter", 400},
		{
			"negative size",
			func(d *ke.CreateNodePoolDetails) { d.NodeConfigDetails.Size = new(-1) },
			"InvalidParameter",
			400,
		},
		{
			"huge size",
			func(d *ke.CreateNodePoolDetails) { d.NodeConfigDetails.Size = new(100000) },
			"InvalidParameter",
			400,
		},
		{
			"newer than cluster",
			func(d *ke.CreateNodePoolDetails) { d.KubernetesVersion = new("v1.35.2") },
			"InvalidParameter",
			400,
		},
		{
			"malformed version",
			func(d *ke.CreateNodePoolDetails) { d.KubernetesVersion = new("1.34") },
			"InvalidParameter",
			400,
		},
		{"bad kms key", func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.KmsKeyId = new("ocid1.key.oc1.iad.nope")
		}, "InvalidParameter", 400},
		{"cni mismatch", func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.NodePoolPodNetworkOptionDetails = ke.OciVcnIpNativeNodePoolPodNetworkOptionDetails{
				PodSubnetIds: []string{subLB},
			}
		}, "InvalidParameter", 400},
		{
			"no config or subnets",
			func(d *ke.CreateNodePoolDetails) { d.NodeConfigDetails = nil },
			"MissingParameter",
			400,
		},
		{
			"both config and subnets",
			func(d *ke.CreateNodePoolDetails) { d.SubnetIds = []string{subAD1} },
			"InvalidParameter",
			400,
		},
		{
			"unknown cluster",
			func(d *ke.CreateNodePoolDetails) { d.ClusterId = new("ocid1.cluster.oc1.iad.nope") },
			"NotAuthorizedOrNotFound",
			404,
		},
		{
			"missing cluster",
			func(d *ke.CreateNodePoolDetails) { d.ClusterId = nil },
			"MissingParameter",
			400,
		},
		{"empty label key", func(d *ke.CreateNodePoolDetails) {
			d.InitialNodeLabels = []ke.KeyValue{{Key: new("")}}
		}, "InvalidParameter", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := poolDetails(c.Id, "v-"+tt.name)
			tt.mod(&d)

			_, err := e.ce.CreateNodePool(t.Context(), ke.CreateNodePoolRequest{CreateNodePoolDetails: d})
			se := svcErr(t, err)
			assert.Equal(t, tt.http, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestCreateNodePoolAccepts(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "ok", nil)
	native := e.mkCluster(t, "ok-native", func(d *ke.CreateClusterDetails) {
		d.ClusterPodNetworkOptions = []ke.ClusterPodNetworkOptionDetails{
			ke.OciVcnIpNativeClusterPodNetworkOptionDetails{},
		}
	})

	tests := []struct {
		cluster *string
		mod     func(*ke.CreateNodePoolDetails)
		check   func(*testing.T, ke.NodePool)
		name    string
	}{
		{c.Id, func(d *ke.CreateNodePoolDetails) {
			d.NodeShape = new(shapeFlex)
			d.NodeShapeConfig = &ke.CreateNodeShapeConfigDetails{Ocpus: new(float32(2))}
		}, func(t *testing.T, np ke.NodePool) {
			t.Helper()
			assert.InDelta(t, 2, *np.NodeShapeConfig.Ocpus, 0)
			assert.InDelta(t, 32, *np.NodeShapeConfig.MemoryInGBs, 0, "16 GB per OCPU by default")
		}, "flex shape"},
		{c.Id, func(d *ke.CreateNodePoolDetails) {
			d.NodeShape = new(shapeArm)
			d.NodeShapeConfig = &ke.CreateNodeShapeConfigDetails{Ocpus: new(float32(1))}
			d.NodeSourceDetails = ke.NodeSourceViaImageDetails{ImageId: new(imgOL8Arm)}
		}, func(t *testing.T, np ke.NodePool) {
			t.Helper()
			assert.InDelta(t, 6, *np.NodeShapeConfig.MemoryInGBs, 0)
		}, "arm shape and image"},
		{c.Id, func(d *ke.CreateNodePoolDetails) {
			d.NodeSourceDetails = nil
			d.NodeImageName = new("Oracle-Linux-8.10-2024.09.30-0")
		}, func(t *testing.T, np ke.NodePool) {
			t.Helper()
			assert.Equal(t, imgOL8, *np.NodeImageId)
		}, "image by name"},
		{
			c.Id,
			func(d *ke.CreateNodePoolDetails) { d.KubernetesVersion = new("v1.34.1") },
			func(t *testing.T, np ke.NodePool) {
				t.Helper()
				assert.Equal(t, "v1.34.1", *np.KubernetesVersion)
				assert.Equal(t, "v1.34.1", *np.Nodes[0].KubernetesVersion)
			},
			"older version",
		},
		{c.Id, func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails = nil
			d.SubnetIds = []string{subAD1, subRegional}
			d.QuantityPerSubnet = new(2)
		}, func(t *testing.T, np ke.NodePool) {
			t.Helper()
			assert.Len(t, np.Nodes, 4)
			assert.Equal(t, 2, *np.QuantityPerSubnet)
		}, "legacy subnets"},
		{
			c.Id,
			func(d *ke.CreateNodePoolDetails) { d.NodeConfigDetails.Size = new(0) },
			func(t *testing.T, np ke.NodePool) {
				t.Helper()
				assert.Empty(t, np.Nodes)
			},
			"zero size",
		},
		{native.Id, func(d *ke.CreateNodePoolDetails) {
			d.NodeConfigDetails.NodePoolPodNetworkOptionDetails = ke.OciVcnIpNativeNodePoolPodNetworkOptionDetails{
				PodSubnetIds:   []string{subLB},
				MaxPodsPerNode: new(31),
			}
		}, func(t *testing.T, np ke.NodePool) {
			t.Helper()
			opt, ok := np.NodeConfigDetails.NodePoolPodNetworkOptionDetails.(ke.OciVcnIpNativeNodePoolPodNetworkOptionDetails)
			require.True(t, ok)
			assert.Equal(t, []string{subLB}, opt.PodSubnetIds)
		}, "native cni"},
		{c.Id, func(d *ke.CreateNodePoolDetails) {
			d.FreeformTags = map[string]string{"k": "v"}
			d.NodeMetadata = map[string]string{"m": "1"}
			d.SshPublicKey = new("ssh-rsa AAAA")
		}, func(t *testing.T, np ke.NodePool) {
			t.Helper()
			assert.Equal(t, "v", np.FreeformTags["k"])
			assert.Equal(t, "1", np.NodeMetadata["m"])
			assert.Equal(t, "ssh-rsa AAAA", *np.SshPublicKey)
		}, "tags and metadata"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			np := e.mkPool(t, tt.cluster, "a-"+tt.name, tt.mod)
			tt.check(t, np)
		})
	}
}

func TestUpdateNodePoolValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "updval", nil)
	np := e.mkPool(t, c.Id, "p", nil)

	tests := []struct {
		in   ke.UpdateNodePoolDetails
		name string
	}{
		{ke.UpdateNodePoolDetails{KubernetesVersion: new("v1.36.4")}, "newer version"},
		{ke.UpdateNodePoolDetails{Name: new("")}, "empty name"},
		{ke.UpdateNodePoolDetails{NodeShape: new("VM.Nope")}, "unknown shape"},
		{
			ke.UpdateNodePoolDetails{
				NodeShape: new(
					shapeArm,
				),
				NodeShapeConfig: &ke.UpdateNodeShapeConfigDetails{Ocpus: new(float32(1))},
			},
			"arm shape on x86 image",
		},
		{
			ke.UpdateNodePoolDetails{
				NodeConfigDetails: &ke.UpdateNodePoolNodeConfigDetails{Size: new(-2)},
			},
			"negative size",
		},
		{ke.UpdateNodePoolDetails{NodeConfigDetails: &ke.UpdateNodePoolNodeConfigDetails{
			PlacementConfigs: []ke.NodePoolPlacementConfigDetails{
				{AvailabilityDomain: new(ad1), SubnetId: new(subOther)},
			},
		}}, "bad placement"},
		{ke.UpdateNodePoolDetails{SubnetIds: []string{subAD1}}, "legacy subnets on config pool"},
		{
			ke.UpdateNodePoolDetails{
				NodeSourceDetails: ke.NodeSourceViaImageDetails{
					ImageId: new("ocid1.image.oc1.iad.nope"),
				},
			},
			"unknown image",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.ce.UpdateNodePool(t.Context(), ke.UpdateNodePoolRequest{
				NodePoolId: np.Id, UpdateNodePoolDetails: tt.in,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	_, err := e.ce.UpdateNodePool(t.Context(), ke.UpdateNodePoolRequest{
		NodePoolId: np.Id, IfMatch: new("stale"), Name: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())
}

func TestNodePoolListFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c1 := e.mkCluster(t, "one", nil)
	c2 := e.mkCluster(t, "two", nil)
	e.mkPool(t, c1.Id, "a", nil)
	e.mkPool(t, c2.Id, "b", nil)

	tests := []struct {
		name string
		req  ke.ListNodePoolsRequest
		want []string
	}{
		{
			"all",
			ke.ListNodePoolsRequest{
				SortBy:    ke.ListNodePoolsSortByName,
				SortOrder: ke.ListNodePoolsSortOrderAsc,
			},
			[]string{"a", "b"},
		},
		{"by cluster", ke.ListNodePoolsRequest{ClusterId: c2.Id}, []string{"b"}},
		{"by name", ke.ListNodePoolsRequest{Name: new("a")}, []string{"a"}},
		{
			"creating",
			ke.ListNodePoolsRequest{
				LifecycleState: []ke.NodePoolLifecycleStateEnum{
					ke.NodePoolLifecycleStateCreating,
				},
			},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.req.CompartmentId = new(testCompartment)

			resp, err := e.ce.ListNodePools(t.Context(), tt.req)
			require.NoError(t, err)

			var names []string
			for _, np := range resp.Items {
				names = append(names, *np.Name)
			}

			assert.Equal(t, tt.want, names)
		})
	}
}
