package eks_test

// batch2_accuracy_test.go — AWS-accuracy audit batch-2 for EKS (go-rra3).
//
// Covers: EKS Auto Mode (computeConfig/storageConfig/networkingConfig), accessConfig,
// nodegroup updateConfig, labels/taints via UpdateNodegroupConfig, access entry
// kubernetesGroups, fargate profile subnets, PodIdentity UUID assocId, insight
// insightStatus object, control plane subnet updates, connectorConfig nested parsing,
// ownerArn in pod identity response.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// ---------------------------------------------------------------------------
// Gap 1: EKS Auto Mode — computeConfig round-trip
// ---------------------------------------------------------------------------

func TestBatch2_ComputeConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantValue any
		body      map[string]any
		name      string
		wantField string
	}{
		{
			name: "compute_config_enabled",
			body: map[string]any{
				"name": "auto-cluster",
				"computeConfig": map[string]any{
					"enabled":     true,
					"nodeRoleArn": "arn:aws:iam::123456789012:role/eks-node",
					"nodePools":   []string{"general-purpose", "system"},
				},
			},
			wantField: "enabled",
			wantValue: true,
		},
		{
			name: "compute_config_disabled",
			body: map[string]any{
				"name": "manual-cluster",
				"computeConfig": map[string]any{
					"enabled": false,
				},
			},
			wantField: "enabled",
			wantValue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			rec := doREST(t, h, http.MethodPost, "/clusters", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			cluster, ok := resp["cluster"].(map[string]any)
			require.True(t, ok)

			cc, ok := cluster["computeConfig"].(map[string]any)
			require.True(t, ok, "computeConfig must be present in response")
			assert.Equal(t, tt.wantValue, cc[tt.wantField])
		})
	}
}

func TestBatch2_ComputeConfig_NodeRoleArn_And_NodePools(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "auto-full",
		"computeConfig": map[string]any{
			"enabled":     true,
			"nodeRoleArn": "arn:aws:iam::123456789012:role/eks-node",
			"nodePools":   []string{"general-purpose"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doREST(t, h, http.MethodGet, "/clusters/auto-full", nil)
	require.Equal(t, http.StatusOK, desc.Code)
	cluster := parseResp(t, desc)["cluster"].(map[string]any)

	cc := cluster["computeConfig"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::123456789012:role/eks-node", cc["nodeRoleArn"])

	pools, _ := cc["nodePools"].([]any)
	assert.Equal(t, "general-purpose", pools[0])
}

// ---------------------------------------------------------------------------
// Gap 2: EKS Auto Mode — storageConfig and networkingConfig round-trip
// ---------------------------------------------------------------------------

func TestBatch2_StorageConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantEnabled bool
	}{
		{
			name: "storage_enabled",
			body: map[string]any{
				"name": "storage-on",
				"storageConfig": map[string]any{
					"blockStorage": map[string]any{"enabled": true},
				},
			},
			wantEnabled: true,
		},
		{
			name: "storage_disabled",
			body: map[string]any{
				"name": "storage-off",
				"storageConfig": map[string]any{
					"blockStorage": map[string]any{"enabled": false},
				},
			},
			wantEnabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			rec := doREST(t, h, http.MethodPost, "/clusters", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			desc := doREST(t, h, http.MethodGet, "/clusters/"+tt.body["name"].(string), nil)
			cluster := parseResp(t, desc)["cluster"].(map[string]any)

			sc, ok := cluster["storageConfig"].(map[string]any)
			require.True(t, ok, "storageConfig must be present")
			bs := sc["blockStorage"].(map[string]any)
			assert.Equal(t, tt.wantEnabled, bs["enabled"])
		})
	}
}

func TestBatch2_NetworkingConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "net-cluster",
		"networkingConfig": map[string]any{
			"elasticLoadBalancing": map[string]any{"enabled": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doREST(t, h, http.MethodGet, "/clusters/net-cluster", nil)
	cluster := parseResp(t, desc)["cluster"].(map[string]any)

	nc, ok := cluster["networkingConfig"].(map[string]any)
	require.True(t, ok, "networkingConfig must be present")
	elb := nc["elasticLoadBalancing"].(map[string]any)
	assert.Equal(t, true, elb["enabled"])
}

// ---------------------------------------------------------------------------
// Gap 3: accessConfig round-trip
// ---------------------------------------------------------------------------

func TestBatch2_AccessConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantAuthMode  string
		wantBootstrap bool
	}{
		{
			name: "access_config_api_mode",
			body: map[string]any{
				"name": "api-cluster",
				"accessConfig": map[string]any{
					"authenticationMode":                      "API",
					"bootstrapClusterCreatorAdminPermissions": true,
				},
			},
			wantAuthMode:  "API",
			wantBootstrap: true,
		},
		{
			name: "access_config_config_map",
			body: map[string]any{
				"name": "cm-cluster",
				"accessConfig": map[string]any{
					"authenticationMode":                      "CONFIG_MAP",
					"bootstrapClusterCreatorAdminPermissions": false,
				},
			},
			wantAuthMode:  "CONFIG_MAP",
			wantBootstrap: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			rec := doREST(t, h, http.MethodPost, "/clusters", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			desc := doREST(t, h, http.MethodGet, "/clusters/"+tt.body["name"].(string), nil)
			cluster := parseResp(t, desc)["cluster"].(map[string]any)

			ac, ok := cluster["accessConfig"].(map[string]any)
			require.True(t, ok, "accessConfig must be present in response")
			assert.Equal(t, tt.wantAuthMode, ac["authenticationMode"])
			assert.Equal(t, tt.wantBootstrap, ac["bootstrapClusterCreatorAdminPermissions"])
		})
	}
}

func TestBatch2_AccessConfig_Absent_When_Not_Provided(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "no-ac"})

	desc := doREST(t, h, http.MethodGet, "/clusters/no-ac", nil)
	cluster := parseResp(t, desc)["cluster"].(map[string]any)
	_, hasAC := cluster["accessConfig"]
	assert.False(t, hasAC, "accessConfig must be absent when not provided")
}

// ---------------------------------------------------------------------------
// Gap 4: UpdateClusterConfig updates accessConfig, computeConfig, subnetIds
// ---------------------------------------------------------------------------

func TestBatch2_UpdateClusterConfig_AccessConfig(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("upd-ac", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateClusterConfig("upd-ac", eks.ClusterConfigUpdate{
		AccessConfig: &eks.AccessConfig{AuthenticationMode: "API_AND_CONFIG_MAP"},
	})
	require.NoError(t, err)

	c, err := b.DescribeCluster("upd-ac")
	require.NoError(t, err)
	require.NotNil(t, c.AccessConfig)
	assert.Equal(t, "API_AND_CONFIG_MAP", c.AccessConfig.AuthenticationMode)
}

func TestBatch2_UpdateClusterConfig_SubnetIDs(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("upd-subnets", "1.32", "", &eks.VpcConfig{
		SubnetIDs: []string{"subnet-old"},
	}, nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateClusterConfig("upd-subnets", eks.ClusterConfigUpdate{
		SubnetIDs: []string{"subnet-new-1", "subnet-new-2"},
	})
	require.NoError(t, err)

	c, err := b.DescribeCluster("upd-subnets")
	require.NoError(t, err)
	require.NotNil(t, c.VpcConfig)
	assert.Equal(t, []string{"subnet-new-1", "subnet-new-2"}, c.VpcConfig.SubnetIDs)
}

func TestBatch2_UpdateClusterConfig_SubnetIDs_Via_Handler(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "upd-sub-cluster",
		"resourcesVpcConfig": map[string]any{
			"subnetIds": []string{"subnet-orig"},
		},
	})

	rec := doREST(t, h, http.MethodPut, "/clusters/upd-sub-cluster", map[string]any{
		"resourcesVpcConfig": map[string]any{
			"subnetIds": []string{"subnet-a", "subnet-b"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doREST(t, h, http.MethodGet, "/clusters/upd-sub-cluster", nil)
	cluster := parseResp(t, desc)["cluster"].(map[string]any)
	vpc := cluster["resourcesVpcConfig"].(map[string]any)
	subs, _ := vpc["subnetIds"].([]any)
	require.Len(t, subs, 2)
	assert.Equal(t, "subnet-a", subs[0])
}

func TestBatch2_UpdateClusterConfig_ComputeConfig(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("upd-compute", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateClusterConfig("upd-compute", eks.ClusterConfigUpdate{
		ComputeConfig: &eks.ComputeConfig{Enabled: true, NodeRoleARN: "arn:aws:iam::123:role/node"},
	})
	require.NoError(t, err)

	c, err := b.DescribeCluster("upd-compute")
	require.NoError(t, err)
	require.NotNil(t, c.ComputeConfig)
	assert.True(t, c.ComputeConfig.Enabled)
	assert.Equal(t, "arn:aws:iam::123:role/node", c.ComputeConfig.NodeRoleARN)
}

// ---------------------------------------------------------------------------
// Gap 5: AccessEntry — kubernetesGroups in create and update
// ---------------------------------------------------------------------------

func TestBatch2_AccessEntry_KubernetesGroups_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		groups []string
	}{
		{name: "with_groups", groups: []string{"system:masters", "ops-team"}},
		{name: "no_groups", groups: nil},
		{name: "empty_groups", groups: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ae-cluster-" + tt.name})

			body := map[string]any{
				"principalArn": "arn:aws:iam::123456789012:role/my-role",
				"type":         "STANDARD",
			}
			if tt.groups != nil {
				body["kubernetesGroups"] = tt.groups
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/ae-cluster-"+tt.name+"/access-entries", body)
			require.Equal(t, http.StatusOK, rec.Code)

			entry := parseResp(t, rec)["accessEntry"].(map[string]any)
			groups, _ := entry["kubernetesGroups"].([]any)

			if len(tt.groups) > 0 {
				require.Len(t, groups, len(tt.groups))
				assert.Equal(t, tt.groups[0], groups[0])
			} else {
				assert.Empty(t, groups, "kubernetesGroups should be empty array when not set")
			}
		})
	}
}

func TestBatch2_AccessEntry_KubernetesGroups_Update(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ae-upd-cluster"})
	doREST(t, h, http.MethodPost, "/clusters/ae-upd-cluster/access-entries", map[string]any{
		"principalArn": "arn:aws:iam::123456789012:role/my-role",
	})

	rec := doREST(
		t, h, http.MethodPut,
		"/clusters/ae-upd-cluster/access-entries/arn:aws:iam::123456789012:role/my-role",
		map[string]any{
			"kubernetesGroups": []string{"system:masters"},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	entry := parseResp(t, rec)["accessEntry"].(map[string]any)
	groups, _ := entry["kubernetesGroups"].([]any)
	require.Len(t, groups, 1)
	assert.Equal(t, "system:masters", groups[0])
}

func TestBatch2_AccessEntry_KubernetesGroups_Backend_DirectCreate(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	entry, err := b.CreateAccessEntry(
		"c1",
		"arn:aws:iam::123456789012:role/r1",
		"STANDARD", "",
		[]string{"system:masters", "devs"},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"system:masters", "devs"}, entry.KubernetesGroups)
}

func TestBatch2_AccessEntry_KubernetesGroups_Backend_Update(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAccessEntry("c1", "arn:aws:iam::123:role/r1", "STANDARD", "", nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdateAccessEntry("c1", "arn:aws:iam::123:role/r1", eks.AccessEntryUpdate{
		KubernetesGroups: []string{"ops-team"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"ops-team"}, updated.KubernetesGroups)
}

// ---------------------------------------------------------------------------
// Gap 6: Nodegroup — updateConfig round-trip
// ---------------------------------------------------------------------------

func TestBatch2_Nodegroup_UpdateConfig_Create(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ng-uc-cluster"})

	maxUnavail := int32(2)
	rec := doREST(t, h, http.MethodPost, "/clusters/ng-uc-cluster/node-groups", map[string]any{
		"nodegroupName": "ng1",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"scalingConfig": map[string]any{"desiredSize": 3, "minSize": 1, "maxSize": 5},
		"subnets":       []string{"subnet-aaa"},
		"updateConfig":  map[string]any{"maxUnavailable": maxUnavail},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	ng := parseResp(t, rec)["nodegroup"].(map[string]any)
	uc, ok := ng["updateConfig"].(map[string]any)
	require.True(t, ok, "updateConfig must be present")
	assert.InDelta(t, float64(2), uc["maxUnavailable"], 0.001)
}

func TestBatch2_Nodegroup_UpdateConfig_Via_UpdateNodegroupConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		wantMaxU   *float64
		wantMaxPct *float64
		name       string
	}{
		{
			name: "set_max_unavailable",
			body: map[string]any{
				"updateConfig": map[string]any{"maxUnavailable": 2},
			},
			wantMaxU: &[]float64{2}[0],
		},
		{
			name: "set_max_unavailable_percentage",
			body: map[string]any{
				"updateConfig": map[string]any{"maxUnavailablePercentage": 25},
			},
			wantMaxPct: &[]float64{25}[0],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ng-uc-upd-" + tt.name})
			doREST(t, h, http.MethodPost, "/clusters/ng-uc-upd-"+tt.name+"/node-groups", map[string]any{
				"nodegroupName": "ng1",
				"nodeRole":      "arn:aws:iam::123456789012:role/ng",
				"subnets":       []string{"subnet-x"},
			})

			rec := doREST(t, h, http.MethodPost, "/clusters/ng-uc-upd-"+tt.name+"/node-groups/ng1", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			desc := doREST(t, h, http.MethodGet, "/clusters/ng-uc-upd-"+tt.name+"/node-groups/ng1", nil)
			ng := parseResp(t, desc)["nodegroup"].(map[string]any)
			uc, ok := ng["updateConfig"].(map[string]any)
			require.True(t, ok, "updateConfig must be present after update")

			if tt.wantMaxU != nil {
				assert.InDelta(t, *tt.wantMaxU, uc["maxUnavailable"], 0.001)
			}

			if tt.wantMaxPct != nil {
				assert.InDelta(t, *tt.wantMaxPct, uc["maxUnavailablePercentage"], 0.001)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 7: UpdateNodegroupConfig — labels and taints
// ---------------------------------------------------------------------------

func TestBatch2_UpdateNodegroupConfig_Labels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       map[string]any
		update      map[string]any
		name        string
		wantLabel   string
		wantValue   string
		wantMissing string
	}{
		{
			name: "add_label",
			setup: map[string]any{
				"nodegroupName": "ng1",
				"nodeRole":      "arn:aws:iam::123456789012:role/ng",
				"subnets":       []string{"subnet-x"},
				"labels":        map[string]any{"existing": "value"},
			},
			update: map[string]any{
				"labels": map[string]any{
					"addOrUpdateLabels": map[string]any{"env": "prod"},
				},
			},
			wantLabel: "env",
			wantValue: "prod",
		},
		{
			name: "remove_label",
			setup: map[string]any{
				"nodegroupName": "ng1",
				"nodeRole":      "arn:aws:iam::123456789012:role/ng",
				"subnets":       []string{"subnet-x"},
				"labels":        map[string]any{"to-remove": "bye"},
			},
			update: map[string]any{
				"labels": map[string]any{
					"removeLabels": []string{"to-remove"},
				},
			},
			wantMissing: "to-remove",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clusterName := "ng-lbl-" + tt.name
			h := newTestEKSHandler()
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": clusterName})
			doREST(t, h, http.MethodPost, "/clusters/"+clusterName+"/node-groups", tt.setup)

			rec := doREST(t, h, http.MethodPost, "/clusters/"+clusterName+"/node-groups/ng1", tt.update)
			require.Equal(t, http.StatusOK, rec.Code)

			desc := doREST(t, h, http.MethodGet, "/clusters/"+clusterName+"/node-groups/ng1", nil)
			ng := parseResp(t, desc)["nodegroup"].(map[string]any)
			labels, _ := ng["labels"].(map[string]any)

			if tt.wantLabel != "" {
				assert.Equal(t, tt.wantValue, labels[tt.wantLabel])
			}

			if tt.wantMissing != "" {
				_, hasKey := labels[tt.wantMissing]
				assert.False(t, hasKey, "label %q should have been removed", tt.wantMissing)
			}
		})
	}
}

func TestBatch2_UpdateNodegroupConfig_Taints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      map[string]any
		update     map[string]any
		name       string
		wantTaints int
	}{
		{
			name: "add_taint",
			setup: map[string]any{
				"nodegroupName": "ng1",
				"nodeRole":      "arn:aws:iam::123456789012:role/ng",
				"subnets":       []string{"subnet-x"},
			},
			update: map[string]any{
				"taints": map[string]any{
					"addOrUpdateTaints": []any{
						map[string]any{"key": "dedicated", "value": "gpu", "effect": "NO_SCHEDULE"},
					},
				},
			},
			wantTaints: 1,
		},
		{
			name: "remove_taint",
			setup: map[string]any{
				"nodegroupName": "ng1",
				"nodeRole":      "arn:aws:iam::123456789012:role/ng",
				"subnets":       []string{"subnet-x"},
				"taints": []any{
					map[string]any{"key": "spot", "effect": "NO_SCHEDULE"},
				},
			},
			update: map[string]any{
				"taints": map[string]any{
					"removeTaints": []any{
						map[string]any{"key": "spot", "effect": "NO_SCHEDULE"},
					},
				},
			},
			wantTaints: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clusterName := "ng-tnt-" + tt.name
			h := newTestEKSHandler()
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": clusterName})
			doREST(t, h, http.MethodPost, "/clusters/"+clusterName+"/node-groups", tt.setup)

			rec := doREST(t, h, http.MethodPost, "/clusters/"+clusterName+"/node-groups/ng1", tt.update)
			require.Equal(t, http.StatusOK, rec.Code)

			desc := doREST(t, h, http.MethodGet, "/clusters/"+clusterName+"/node-groups/ng1", nil)
			ng := parseResp(t, desc)["nodegroup"].(map[string]any)
			taints, _ := ng["taints"].([]any)
			assert.Len(t, taints, tt.wantTaints)
		})
	}
}

func TestBatch2_UpdateNodegroupConfig_Backend_Taints(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 3,
		eks.NodegroupInput{
			Taints: []eks.NodegroupTaint{
				{Key: "env", Value: "prod", Effect: "NO_SCHEDULE"},
			},
		},
		nil)
	require.NoError(t, err)

	upd := eks.NodegroupConfigUpdate{
		AddOrUpdateTaints: []eks.NodegroupTaint{
			{Key: "env", Value: "staging", Effect: "NO_SCHEDULE"},
			{Key: "spot", Effect: "PREFER_NO_SCHEDULE"},
		},
	}
	ng, err := b.UpdateNodegroupConfig("c1", "ng1", upd)
	require.NoError(t, err)
	assert.Len(t, ng.Taints, 2)

	var envTaint eks.NodegroupTaint
	for _, taint := range ng.Taints {
		if taint.Key == "env" {
			envTaint = taint
		}
	}
	assert.Equal(t, "staging", envTaint.Value, "taint value should be updated")

	// Remove by key+effect
	ng2, err := b.UpdateNodegroupConfig("c1", "ng1", eks.NodegroupConfigUpdate{
		RemoveTaints: []eks.NodegroupTaint{{Key: "spot", Effect: "PREFER_NO_SCHEDULE"}},
	})
	require.NoError(t, err)
	assert.Len(t, ng2.Taints, 1)
}

// ---------------------------------------------------------------------------
// Gap 8: FargateProfile — subnets round-trip
// ---------------------------------------------------------------------------

func TestBatch2_FargateProfile_Subnets_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subnets []string
	}{
		{name: "with_subnets", subnets: []string{"subnet-fp-1", "subnet-fp-2"}},
		{name: "no_subnets", subnets: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "fp-sub-" + tt.name})

			body := map[string]any{
				"fargateProfileName":  "fp1",
				"podExecutionRoleArn": "arn:aws:iam::123:role/fp",
			}
			if tt.subnets != nil {
				body["subnets"] = tt.subnets
			}

			rec := doREST(t, h, http.MethodPost,
				"/clusters/fp-sub-"+tt.name+"/fargate-profiles", body)
			require.Equal(t, http.StatusOK, rec.Code)

			profile := parseResp(t, rec)["fargateProfile"].(map[string]any)
			subs, _ := profile["subnets"].([]any)

			if len(tt.subnets) > 0 {
				require.Len(t, subs, len(tt.subnets))
				assert.Equal(t, tt.subnets[0], subs[0])
			} else {
				assert.Empty(t, subs)
			}
		})
	}
}

func TestBatch2_FargateProfile_Subnets_Describe(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	fp, err := b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp",
		nil, []string{"subnet-a", "subnet-b"}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"subnet-a", "subnet-b"}, fp.Subnets)

	described, err := b.DescribeFargateProfile("c1", "fp1")
	require.NoError(t, err)
	assert.Equal(t, []string{"subnet-a", "subnet-b"}, described.Subnets)
}

func TestBatch2_FargateProfile_Subnets_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	subs := []string{"subnet-a"}
	fp, err := b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp", nil, subs, nil)
	require.NoError(t, err)

	fp.Subnets[0] = "mutated"

	described, err := b.DescribeFargateProfile("c1", "fp1")
	require.NoError(t, err)
	assert.Equal(t, "subnet-a", described.Subnets[0], "subnets must be deep-copied")
}

// ---------------------------------------------------------------------------
// Gap 9: PodIdentityAssociation — UUID-based associationId
// ---------------------------------------------------------------------------

func TestBatch2_PodIdentity_AssocID_IsUUID(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "pi-uuid-cluster"})

	rec := doREST(t, h, http.MethodPost, "/clusters/pi-uuid-cluster/pod-identity-associations", map[string]any{
		"namespace":      "default",
		"serviceAccount": "my-sa",
		"roleArn":        "arn:aws:iam::123456789012:role/my-pod-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assoc := parseResp(t, rec)["association"].(map[string]any)
	assocID, _ := assoc["associationId"].(string)
	require.NotEmpty(t, assocID)
	assert.Len(t, assocID, 36, "associationId should be a UUID (36 chars with hyphens)")
}

func TestBatch2_PodIdentity_DifferentAssocIDs_SameNamespaceSA(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	a1, err := b.CreatePodIdentityAssociation("c1", "default", "my-sa", "arn:aws:iam::123:role/r1", nil)
	require.NoError(t, err)

	a2, err := b.CreatePodIdentityAssociation("c1", "default", "my-sa", "arn:aws:iam::123:role/r2", nil)
	require.NoError(t, err)

	assert.NotEqual(t, a1.AssociationID, a2.AssociationID,
		"each call must produce a unique associationId")
}

// ---------------------------------------------------------------------------
// Gap 10: Insight — insightStatus as object
// ---------------------------------------------------------------------------

func TestBatch2_Insights_InsightStatus_Object(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ins-cluster"})

	rec := doREST(t, h, http.MethodGet, "/clusters/ins-cluster/insights", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	insights, _ := resp["insights"].([]any)
	require.NotEmpty(t, insights)

	for _, ins := range insights {
		insight := ins.(map[string]any)
		insightStatus, ok := insight["insightStatus"].(map[string]any)
		require.True(t, ok, "insightStatus must be an object, not a flat field")
		_, hasStatus := insightStatus["status"]
		assert.True(t, hasStatus, "insightStatus must have a 'status' field")
	}
}

func TestBatch2_DescribeInsight_InsightStatus_Object(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "di-cluster"})

	rec := doREST(t, h, http.MethodGet, "/clusters/di-cluster/insights/some-insight-id", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	insight := parseResp(t, rec)["insight"].(map[string]any)
	insightStatus, ok := insight["insightStatus"].(map[string]any)
	require.True(t, ok, "insightStatus must be an object")
	assert.NotEmpty(t, insightStatus["status"])
}

// ---------------------------------------------------------------------------
// Gap 11: RegisterCluster — connectorConfig nested object parsing
// ---------------------------------------------------------------------------

func TestBatch2_RegisterCluster_ConnectorConfig_Nested(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "with_connector_config",
			body: map[string]any{
				"name": "reg-cluster-1",
				"connectorConfig": map[string]any{
					"provider": "EKS_ANYWHERE",
					"roleArn":  "arn:aws:iam::123456789012:role/connector",
				},
			},
		},
		{
			name: "without_connector_config",
			body: map[string]any{
				"name": "reg-cluster-2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			// RegisterCluster path is POST /clusters/{placeholder}/register
			// The cluster name comes from the request body, not the path.
			rec := doREST(t, h, http.MethodPost, "/clusters/placeholder/register", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			cluster := parseResp(t, rec)["cluster"].(map[string]any)
			assert.Equal(t, tt.body["name"], cluster["name"])
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 12: FargateProfile — subnets preserved in Delete
// ---------------------------------------------------------------------------

func TestBatch2_FargateProfile_Subnets_Preserved_On_Delete(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp",
		nil, []string{"subnet-del"}, nil)
	require.NoError(t, err)

	deleted, err := b.DeleteFargateProfile("c1", "fp1")
	require.NoError(t, err)
	assert.Equal(t, []string{"subnet-del"}, deleted.Subnets)
	assert.Equal(t, "DELETING", deleted.Status)
}

// ---------------------------------------------------------------------------
// Gap 13: AccessEntry — kubernetesGroups field always present in response
// ---------------------------------------------------------------------------

func TestBatch2_AccessEntry_KubernetesGroups_AlwaysPresent(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ae-always-cluster"})
	doREST(t, h, http.MethodPost, "/clusters/ae-always-cluster/access-entries", map[string]any{
		"principalArn": "arn:aws:iam::123456789012:role/r1",
	})

	rec := doREST(t, h, http.MethodGet,
		"/clusters/ae-always-cluster/access-entries/arn:aws:iam::123456789012:role/r1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	entry := parseResp(t, rec)["accessEntry"].(map[string]any)
	_, ok := entry["kubernetesGroups"]
	assert.True(t, ok, "kubernetesGroups must always be present in access entry response")
}

// ---------------------------------------------------------------------------
// Gap 14: Nodegroup — updateConfig present after creation via createNodegroup
// ---------------------------------------------------------------------------

func TestBatch2_Nodegroup_UpdateConfig_AbsentWhenNotSet(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ng-no-uc-cluster"})
	doREST(t, h, http.MethodPost, "/clusters/ng-no-uc-cluster/node-groups", map[string]any{
		"nodegroupName": "ng1",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-x"},
	})

	desc := doREST(t, h, http.MethodGet, "/clusters/ng-no-uc-cluster/node-groups/ng1", nil)
	ng := parseResp(t, desc)["nodegroup"].(map[string]any)
	_, hasUC := ng["updateConfig"]
	assert.False(t, hasUC, "updateConfig should be absent when not set at creation")
}

// ---------------------------------------------------------------------------
// Gap 15: UpdateClusterConfig via handler — accessConfig and computeConfig
// ---------------------------------------------------------------------------

func TestBatch2_UpdateClusterConfig_Handler_AccessConfig(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "upd-ac-handler"})

	rec := doREST(t, h, http.MethodPut, "/clusters/upd-ac-handler", map[string]any{
		"accessConfig": map[string]any{
			"authenticationMode": "API",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doREST(t, h, http.MethodGet, "/clusters/upd-ac-handler", nil)
	cluster := parseResp(t, desc)["cluster"].(map[string]any)
	ac, ok := cluster["accessConfig"].(map[string]any)
	require.True(t, ok, "accessConfig must be present after update")
	assert.Equal(t, "API", ac["authenticationMode"])
}

func TestBatch2_UpdateClusterConfig_Handler_ComputeConfig(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "upd-cc-handler"})

	rec := doREST(t, h, http.MethodPut, "/clusters/upd-cc-handler", map[string]any{
		"computeConfig": map[string]any{
			"enabled":     true,
			"nodeRoleArn": "arn:aws:iam::123:role/node",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doREST(t, h, http.MethodGet, "/clusters/upd-cc-handler", nil)
	cluster := parseResp(t, desc)["cluster"].(map[string]any)
	cc, ok := cluster["computeConfig"].(map[string]any)
	require.True(t, ok, "computeConfig must be present after update")
	assert.Equal(t, true, cc["enabled"])
}
