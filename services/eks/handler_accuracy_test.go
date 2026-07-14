package eks_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// ── Issue 1: Cluster resourcesVpcConfig ─────────────────────────────────────

func TestAccuracy_ClusterVpcConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name":    "vpc-cluster",
		"version": "1.32",
		"roleArn": "arn:aws:iam::123456789012:role/eks",
		"resourcesVpcConfig": map[string]any{
			"subnetIds":             []string{"subnet-aaa", "subnet-bbb"},
			"securityGroupIds":      []string{"sg-111"},
			"endpointPrivateAccess": true,
			"endpointPublicAccess":  false,
			"publicAccessCidrs":     []string{"10.0.0.0/8"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe cluster returns vpcConfig.
	rec2 := doREST(t, h, http.MethodGet, "/clusters/vpc-cluster", nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	resp := parseResp(t, rec2)
	cluster := resp["cluster"].(map[string]any)

	vpc, ok := cluster["resourcesVpcConfig"].(map[string]any)
	require.True(t, ok, "resourcesVpcConfig should be present")

	subs, _ := vpc["subnetIds"].([]any)
	require.Len(t, subs, 2)
	assert.Equal(t, "subnet-aaa", subs[0])

	sgs, _ := vpc["securityGroupIds"].([]any)
	require.Len(t, sgs, 1)
	assert.Equal(t, "sg-111", sgs[0])

	assert.Equal(t, true, vpc["endpointPrivateAccess"])
	assert.Equal(t, false, vpc["endpointPublicAccess"])

	cidrs, _ := vpc["publicAccessCidrs"].([]any)
	require.Len(t, cidrs, 1)
	assert.Equal(t, "10.0.0.0/8", cidrs[0])
}

func TestAccuracy_ClusterVpcConfig_Absent_When_Not_Provided(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "no-vpc"})

	rec := doREST(t, h, http.MethodGet, "/clusters/no-vpc", nil)
	resp := parseResp(t, rec)
	cluster := resp["cluster"].(map[string]any)

	_, hasVpc := cluster["resourcesVpcConfig"]
	assert.False(t, hasVpc, "resourcesVpcConfig should be absent when not provided")
}

func TestAccuracy_ClusterVpcConfig_Backend_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	vpcCfg := &eks.VpcConfig{
		SubnetIDs:            []string{"subnet-orig"},
		EndpointPublicAccess: true,
	}
	_, err := b.CreateCluster("c1", "1.32", "", vpcCfg, nil, nil)
	require.NoError(t, err)

	// Mutating the original slice must not affect the stored copy.
	vpcCfg.SubnetIDs[0] = "subnet-mutated"

	c, err := b.DescribeCluster("c1")
	require.NoError(t, err)
	require.NotNil(t, c.VpcConfig)
	assert.Equal(t, "subnet-orig", c.VpcConfig.SubnetIDs[0], "deep copy must isolate stored VpcConfig")
}

// ── Issue 2: kubernetesNetworkConfig ────────────────────────────────────────

func TestAccuracy_KubernetesNetworkConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "net-cluster",
		"kubernetesNetworkConfig": map[string]any{
			"ipFamily":        "ipv4",
			"serviceIpv4Cidr": "10.96.0.0/12",
		},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/net-cluster", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	cluster := resp["cluster"].(map[string]any)

	net, ok := cluster["kubernetesNetworkConfig"].(map[string]any)
	require.True(t, ok, "kubernetesNetworkConfig should be present")
	assert.Equal(t, "ipv4", net["ipFamily"])
	assert.Equal(t, "10.96.0.0/12", net["serviceIpv4Cidr"])
}

func TestAccuracy_KubernetesNetworkConfig_IPv6(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	netCfg := &eks.KubernetesNetworkConfig{
		IPFamily:        "ipv6",
		ServiceIPv6CIDR: "fd00::/112",
	}
	c, err := b.CreateCluster("ipv6-cluster", "1.32", "", nil, netCfg, nil)
	require.NoError(t, err)
	require.NotNil(t, c.KubernetesNetworkConfig)
	assert.Equal(t, "ipv6", c.KubernetesNetworkConfig.IPFamily)
	assert.Equal(t, "fd00::/112", c.KubernetesNetworkConfig.ServiceIPv6CIDR)
}

func TestAccuracy_KubernetesNetworkConfig_Absent_When_Not_Provided(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "plain"})

	rec := doREST(t, h, http.MethodGet, "/clusters/plain", nil)
	resp := parseResp(t, rec)
	cluster := resp["cluster"].(map[string]any)

	_, hasNet := cluster["kubernetesNetworkConfig"]
	assert.False(t, hasNet)
}

// ── Issue 3: KMS encryptionConfig ───────────────────────────────────────────

func TestAccuracy_EncryptionConfig_ValidKMSArn(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "enc-cluster"})

	rec := doREST(t, h, http.MethodPost, "/clusters/enc-cluster/encryption-config/associate",
		map[string]any{
			"encryptionConfig": []map[string]any{
				{
					"provider":  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123456789012:key/mrk-abc"},
					"resources": []string{"secrets"},
				},
			},
		})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAccuracy_EncryptionConfig_InvalidKMSArn_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "bad-enc"})

	rec := doREST(t, h, http.MethodPost, "/clusters/bad-enc/encryption-config/associate",
		map[string]any{
			"encryptionConfig": []map[string]any{
				{
					"provider":  map[string]string{"keyArn": "not-a-kms-arn"},
					"resources": []string{"secrets"},
				},
			},
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_EncryptionConfig_Replace_Not_Append(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	first := []eks.EncryptionConfig{
		{Provider: map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/k1"}, Resources: []string{"secrets"}},
	}
	result, err := b.AssociateEncryptionConfig("c1", first)
	require.NoError(t, err)
	require.Len(t, result, 1)

	second := []eks.EncryptionConfig{
		{Provider: map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/k2"}, Resources: []string{"secrets"}},
	}
	result2, err := b.AssociateEncryptionConfig("c1", second)
	require.NoError(t, err)
	// Must replace, not append — exactly 1 entry.
	require.Len(t, result2, 1, "AssociateEncryptionConfig must replace the stored config, not append")
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/k2", result2[0].Provider["keyArn"])
}

// ── Issue 4: Nodegroup subnets ───────────────────────────────────────────────

func TestAccuracy_NodegroupSubnets_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng1",
		"subnets":       []string{"subnet-aaa", "subnet-bbb"},
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng1", nil)
	resp := parseResp(t, rec2)
	ng := resp["nodegroup"].(map[string]any)

	subs, _ := ng["subnets"].([]any)
	require.Len(t, subs, 2)
	assert.Equal(t, "subnet-aaa", subs[0])
	assert.Equal(t, "subnet-bbb", subs[1])
}

func TestAccuracy_NodegroupSubnets_Backend_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	input := eks.NodegroupInput{Subnets: []string{"subnet-x"}}
	ng, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, input, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"subnet-x"}, ng.Subnets)

	// Mutate returned copy — must not affect stored copy.
	ng.Subnets[0] = "mutated"
	ng2, err := b.DescribeNodegroup("c1", "ng1")
	require.NoError(t, err)
	assert.Equal(t, "subnet-x", ng2.Subnets[0])
}

// ── Issue 5: Nodegroup labels and taints ────────────────────────────────────

func TestAccuracy_NodegroupLabels_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-labeled",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"labels":        map[string]string{"app": "backend", "tier": "compute"},
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-labeled", nil)
	ng := parseResp(t, rec2)["nodegroup"].(map[string]any)
	labels, _ := ng["labels"].(map[string]any)
	assert.Equal(t, "backend", labels["app"])
	assert.Equal(t, "compute", labels["tier"])
}

func TestAccuracy_NodegroupTaints_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-tainted",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"taints": []map[string]any{
			{"key": "dedicated", "value": "gpu", "effect": "NoSchedule"},
		},
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-tainted", nil)
	ng := parseResp(t, rec2)["nodegroup"].(map[string]any)
	taints, _ := ng["taints"].([]any)
	require.Len(t, taints, 1)
	taint := taints[0].(map[string]any)
	assert.Equal(t, "dedicated", taint["key"])
	assert.Equal(t, "gpu", taint["value"])
	assert.Equal(t, "NoSchedule", taint["effect"])
}

func TestAccuracy_NodegroupTaints_Backend_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	input := eks.NodegroupInput{
		Taints: []eks.NodegroupTaint{{Key: "k", Value: "v", Effect: "NoSchedule"}},
	}
	ng, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, input, nil)
	require.NoError(t, err)

	// Mutate returned taints — must not affect stored copy.
	ng.Taints[0].Key = "mutated"
	ng2, err := b.DescribeNodegroup("c1", "ng1")
	require.NoError(t, err)
	assert.Equal(t, "k", ng2.Taints[0].Key)
}

// ── Issue 6: Nodegroup remoteAccess ─────────────────────────────────────────

func TestAccuracy_NodegroupRemoteAccess_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-ssh",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"remoteAccess": map[string]any{
			"ec2SshKey":            "my-keypair",
			"sourceSecurityGroups": []string{"sg-bastion"},
		},
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-ssh", nil)
	ng := parseResp(t, rec2)["nodegroup"].(map[string]any)
	ra, ok := ng["remoteAccess"].(map[string]any)
	require.True(t, ok, "remoteAccess should be present")
	assert.Equal(t, "my-keypair", ra["ec2SshKey"])
	sgs, _ := ra["sourceSecurityGroups"].([]any)
	require.Len(t, sgs, 1)
	assert.Equal(t, "sg-bastion", sgs[0])
}

func TestAccuracy_NodegroupRemoteAccess_Backend_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	ra := &eks.RemoteAccess{EC2SSHKey: "key1", SourceSecurityGroups: []string{"sg-x"}}
	input := eks.NodegroupInput{RemoteAccess: ra}
	ng, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, input, nil)
	require.NoError(t, err)
	require.NotNil(t, ng.RemoteAccess)

	// Mutate original.
	ra.SourceSecurityGroups[0] = "sg-mutated"
	ng2, err := b.DescribeNodegroup("c1", "ng1")
	require.NoError(t, err)
	assert.Equal(t, "sg-x", ng2.RemoteAccess.SourceSecurityGroups[0])
}

// ── Issue 7: Nodegroup launchTemplate ───────────────────────────────────────

func TestAccuracy_NodegroupLaunchTemplate_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-lt",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"launchTemplate": map[string]any{
			"id":      "lt-0123456789abcdef0",
			"name":    "my-lt",
			"version": "$Default",
		},
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-lt", nil)
	ng := parseResp(t, rec2)["nodegroup"].(map[string]any)
	lt, ok := ng["launchTemplate"].(map[string]any)
	require.True(t, ok, "launchTemplate should be present")
	assert.Equal(t, "lt-0123456789abcdef0", lt["id"])
	assert.Equal(t, "my-lt", lt["name"])
	assert.Equal(t, "$Default", lt["version"])
}

func TestAccuracy_NodegroupLaunchTemplate_AbsentWhenNotSet(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-nolt",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-nolt", nil)
	ng := parseResp(t, rec)["nodegroup"].(map[string]any)
	_, hasLT := ng["launchTemplate"]
	assert.False(t, hasLT, "launchTemplate must be absent when not provided")
}

// ── Issue 8: Nodegroup diskSize ──────────────────────────────────────────────

func TestAccuracy_NodegroupDiskSize_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-disk",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"diskSize":      100,
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-disk", nil)
	ng := parseResp(t, rec2)["nodegroup"].(map[string]any)
	diskSize, _ := ng["diskSize"].(float64)
	assert.InDelta(t, 100.0, diskSize, 0)
}

func TestAccuracy_NodegroupDiskSize_TooSmall_Rejected(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2,
		eks.NodegroupInput{DiskSize: 5}, nil)
	require.ErrorIs(t, err, eks.ErrValidation, "diskSize < 20 must return ErrValidation")
}

func TestAccuracy_NodegroupDiskSize_TooLarge_Rejected(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2,
		eks.NodegroupInput{DiskSize: 20000}, nil)
	require.ErrorIs(t, err, eks.ErrValidation, "diskSize > 16384 must return ErrValidation")
}

func TestAccuracy_NodegroupDiskSize_Boundary_Min_OK(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	ng, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2,
		eks.NodegroupInput{DiskSize: 20}, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(20), ng.DiskSize)
}

func TestAccuracy_NodegroupDiskSize_Boundary_Max_OK(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	ng, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2,
		eks.NodegroupInput{DiskSize: 16384}, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(16384), ng.DiskSize)
}

func TestAccuracy_NodegroupDiskSize_Zero_Omitted(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-nodisk",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-nodisk", nil)
	ng := parseResp(t, rec)["nodegroup"].(map[string]any)
	_, hasDisk := ng["diskSize"]
	assert.False(t, hasDisk, "diskSize must be absent when zero")
}

// ── Issue 9: Addon configuration and resolveConflicts ───────────────────────

func TestAccuracy_Addon_ConfigurationValues_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/addons", map[string]any{
		"addonName":           "vpc-cni",
		"configurationValues": `{"env":{"ENABLE_PREFIX_DELEGATION":"true"}}`,
		"resolveConflicts":    "OVERWRITE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/addons/vpc-cni", nil)
	addon := parseResp(t, rec2)["addon"].(map[string]any)
	assert.JSONEq(t, `{"env":{"ENABLE_PREFIX_DELEGATION":"true"}}`, addon["configurationValues"].(string))
	assert.Equal(t, "OVERWRITE", addon["resolveConflicts"])
}

func TestAccuracy_Addon_ResolveConflicts_InvalidValue_Rejected(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAddon("c1", "vpc-cni", "", "", "", "INVALID", nil)
	require.ErrorIs(t, err, eks.ErrValidation)
}

func TestAccuracy_Addon_ResolveConflicts_ValidValues_Accepted(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	for _, rc := range []string{"OVERWRITE", "NONE", "PRESERVE"} {
		addonName := "addon-" + rc
		_, err = b.CreateAddon("c1", addonName, "", "", "", rc, nil)
		assert.NoError(t, err, "resolveConflicts=%s should be accepted", rc)
	}
}

func TestAccuracy_Addon_EmptyResolveConflicts_Accepted(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAddon("c1", "vpc-cni", "", "", "", "", nil)
	require.NoError(t, err, "empty resolveConflicts must be accepted")
}

func TestAccuracy_Addon_UpdateAddon_Configuration_And_ResolveConflicts(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/addons", map[string]any{"addonName": "coredns"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/addons/coredns/update", map[string]any{
		"configurationValues": `{"replicaCount":3}`,
		"resolveConflicts":    "PRESERVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/addons/coredns", nil)
	addon := parseResp(t, rec2)["addon"].(map[string]any)
	assert.Equal(t, `{"replicaCount":3}`, addon["configurationValues"])
	assert.Equal(t, "PRESERVE", addon["resolveConflicts"])
}

func TestAccuracy_Addon_UpdateAddon_InvalidResolveConflicts_Rejected(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateAddon("c1", "vpc-cni", "", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.UpdateAddon("c1", "vpc-cni", "", "", "", "BAD")
	require.ErrorIs(t, err, eks.ErrValidation)
}

// ── Issue 10: Structured cluster logging ─────────────────────────────────────

func TestAccuracy_Logging_EnableAndDisable_PerType(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	// Enable api and audit.
	_, err = b.UpdateClusterConfig("c1", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"api", "audit"}, Enabled: true},
	}})
	require.NoError(t, err)

	c, err := b.DescribeCluster("c1")
	require.NoError(t, err)
	require.Len(t, c.ClusterLogging, 1)
	assert.ElementsMatch(t, []string{"api", "audit"}, c.ClusterLogging[0].Types)
	assert.True(t, c.ClusterLogging[0].Enabled)

	// Disable api.
	_, err = b.UpdateClusterConfig("c1", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"api"}, Enabled: false},
	}})
	require.NoError(t, err)

	c2, err := b.DescribeCluster("c1")
	require.NoError(t, err)
	require.Len(t, c2.ClusterLogging, 1)
	assert.Equal(t, []string{"audit"}, c2.ClusterLogging[0].Types)
}

func TestAccuracy_Logging_DisableAll_ClearsLogging(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateClusterConfig("c1", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"api"}, Enabled: true},
	}})
	require.NoError(t, err)

	_, err = b.UpdateClusterConfig("c1", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"api"}, Enabled: false},
	}})
	require.NoError(t, err)

	c, err := b.DescribeCluster("c1")
	require.NoError(t, err)
	assert.Empty(t, c.ClusterLogging, "all log types disabled should produce empty ClusterLogging")
}

func TestAccuracy_Logging_NilEntries_NoChange(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.UpdateClusterConfig("c1", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"api"}, Enabled: true},
	}})
	require.NoError(t, err)

	// Nil entries must not change existing logging.
	_, err = b.UpdateClusterConfig("c1", eks.ClusterConfigUpdate{})
	require.NoError(t, err)

	c, err := b.DescribeCluster("c1")
	require.NoError(t, err)
	require.Len(t, c.ClusterLogging, 1, "nil log entries must preserve existing logging")
}

func TestAccuracy_Logging_HTTP_StructuredResponse(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/update-config", map[string]any{
		"logging": map[string]any{
			"clusterLogging": []map[string]any{
				{"types": []string{"api", "audit"}, "enabled": true},
			},
		},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/c1", nil)
	cluster := parseResp(t, rec)["cluster"].(map[string]any)
	logging, ok := cluster["logging"].(map[string]any)
	require.True(t, ok, "logging should appear in describe response after update")
	entries, _ := logging["clusterLogging"].([]any)
	require.NotEmpty(t, entries)
	entry := entries[0].(map[string]any)
	assert.Equal(t, true, entry["enabled"])
}

// ── Issue 11: Nodegroup ASG resources ───────────────────────────────────────

func TestAccuracy_NodegroupASG_PresentOnDescribe(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-asg",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"scalingConfig": map[string]any{"desiredSize": 2, "minSize": 1, "maxSize": 5},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-asg", nil)
	ng := parseResp(t, rec)["nodegroup"].(map[string]any)
	resources, ok := ng["resources"].(map[string]any)
	require.True(t, ok, "resources should be present on describe")
	asgs, _ := resources["autoScalingGroups"].([]any)
	require.Len(t, asgs, 1, "exactly one ASG expected")
	asg := asgs[0].(map[string]any)
	assert.NotEmpty(t, asg["name"], "ASG name must be non-empty")
}

func TestAccuracy_NodegroupASG_Name_StableAcrossDescribes(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	ng, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, eks.NodegroupInput{}, nil)
	require.NoError(t, err)
	require.NotNil(t, ng.Resources)
	require.Len(t, ng.Resources.AutoScalingGroups, 1)
	asgName := ng.Resources.AutoScalingGroups[0].Name

	ng2, err := b.DescribeNodegroup("c1", "ng1")
	require.NoError(t, err)
	assert.Equal(t, asgName, ng2.Resources.AutoScalingGroups[0].Name, "ASG name must be stable")
}

func TestAccuracy_NodegroupASG_Name_DifferentPerNodegroup(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	ng1, err := b.CreateNodegroup("c1", "ng-a", "", "", "", "", "", nil, 1, 1, 2, eks.NodegroupInput{}, nil)
	require.NoError(t, err)
	ng2, err := b.CreateNodegroup("c1", "ng-b", "", "", "", "", "", nil, 1, 1, 2, eks.NodegroupInput{}, nil)
	require.NoError(t, err)

	assert.NotEqual(t, ng1.Resources.AutoScalingGroups[0].Name, ng2.Resources.AutoScalingGroups[0].Name,
		"each nodegroup must get a unique ASG name")
}

// ── Issue 12 (bug fixes) ─────────────────────────────────────────────────────

func TestAccuracy_AssociateAccessPolicy_Dedup(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateAccessEntry("c1", "arn:aws:iam::123456789012:role/r1", "", "", nil, nil)
	require.NoError(t, err)

	policyARN := "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
	scope1 := map[string]any{"type": "cluster"}
	scope2 := map[string]any{"type": "namespace", "namespaces": []string{"default"}}

	_, err = b.AssociateAccessPolicy("c1", "arn:aws:iam::123456789012:role/r1", policyARN, scope1)
	require.NoError(t, err)
	_, err = b.AssociateAccessPolicy("c1", "arn:aws:iam::123456789012:role/r1", policyARN, scope2)
	require.NoError(t, err)

	// List should show exactly 1 association for the policy (replaced, not appended).
	policies, err := b.ListAssociatedAccessPolicies("c1", "arn:aws:iam::123456789012:role/r1")
	require.NoError(t, err)
	count := 0
	for _, p := range policies {
		if p.PolicyARN == policyARN {
			count++
		}
	}
	assert.Equal(t, 1, count, "re-associating same policyARN must replace, not duplicate")
}

func TestAccuracy_EncryptionConfig_NoKeyArn_Accepted(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	// Provider without keyArn should not trigger KMS validation.
	cfg := []eks.EncryptionConfig{
		{Provider: map[string]string{"keyArn": ""}, Resources: []string{"secrets"}},
	}
	_, err = b.AssociateEncryptionConfig("c1", cfg)
	require.NoError(t, err)
}

func TestAccuracy_DeleteCluster_ClearsNodegroups(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, eks.NodegroupInput{}, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, b.NodegroupCount())

	_, err = b.DeleteCluster("c1")
	require.NoError(t, err)

	assert.Equal(t, 0, b.NodegroupCount(), "DeleteCluster must clear all nodegroups")
}

// ── Additional accuracy tests ────────────────────────────────────────────────

func TestAccuracy_Cluster_CreateAndDescribe_AllNewFields(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name":    "full-cluster",
		"version": "1.32",
		"roleArn": "arn:aws:iam::123456789012:role/eks",
		"resourcesVpcConfig": map[string]any{
			"subnetIds":             []string{"subnet-1", "subnet-2"},
			"securityGroupIds":      []string{"sg-1"},
			"endpointPrivateAccess": true,
			"endpointPublicAccess":  true,
			"publicAccessCidrs":     []string{"0.0.0.0/0"},
		},
		"kubernetesNetworkConfig": map[string]any{
			"ipFamily":        "ipv4",
			"serviceIpv4Cidr": "172.20.0.0/16",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/full-cluster", nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	cluster := parseResp(t, rec2)["cluster"].(map[string]any)

	assert.Equal(t, "full-cluster", cluster["name"])
	assert.NotNil(t, cluster["resourcesVpcConfig"])
	assert.NotNil(t, cluster["kubernetesNetworkConfig"])
}

func TestAccuracy_Nodegroup_AllOptionalFields_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-full",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-1"},
		"labels":        map[string]string{"env": "prod"},
		"taints": []map[string]any{
			{"key": "dedicated", "value": "ml", "effect": "NoSchedule"},
		},
		"diskSize": 200,
		"remoteAccess": map[string]any{
			"ec2SshKey": "prod-key",
		},
		"launchTemplate": map[string]any{
			"id": "lt-abc123",
		},
		"scalingConfig": map[string]any{"desiredSize": 2, "minSize": 1, "maxSize": 10},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups/ng-full", nil)
	ng := parseResp(t, rec2)["nodegroup"].(map[string]any)

	assert.NotNil(t, ng["subnets"])
	assert.NotNil(t, ng["labels"])
	assert.NotNil(t, ng["taints"])
	assert.NotNil(t, ng["diskSize"])
	assert.NotNil(t, ng["remoteAccess"])
	assert.NotNil(t, ng["launchTemplate"])
	assert.NotNil(t, ng["resources"])
}

func TestAccuracy_Addon_ConfigurationValues_AbsentWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/addons", map[string]any{"addonName": "coredns"})

	rec := doREST(t, h, http.MethodGet, "/clusters/c1/addons/coredns", nil)
	addon := parseResp(t, rec)["addon"].(map[string]any)
	_, hasCfg := addon["configurationValues"]
	assert.False(t, hasCfg, "configurationValues must be absent when empty")
}

func TestAccuracy_Logging_HTTP_Disabled_Types_Preserved(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	doREST(t, h, http.MethodPost, "/clusters/c1/update-config", map[string]any{
		"logging": map[string]any{
			"clusterLogging": []map[string]any{
				{"types": []string{"api", "audit", "authenticator"}, "enabled": true},
			},
		},
	})

	doREST(t, h, http.MethodPost, "/clusters/c1/update-config", map[string]any{
		"logging": map[string]any{
			"clusterLogging": []map[string]any{
				{"types": []string{"audit"}, "enabled": false},
			},
		},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/c1", nil)
	cluster := parseResp(t, rec)["cluster"].(map[string]any)
	logging := cluster["logging"].(map[string]any)
	entries := logging["clusterLogging"].([]any)
	entry := entries[0].(map[string]any)
	types := entry["types"].([]any)

	typeStrs := make([]string, len(types))
	for i, v := range types {
		typeStrs[i] = v.(string)
	}
	assert.ElementsMatch(t, []string{"api", "authenticator"}, typeStrs)
}

func TestAccuracy_NodegroupASG_Resources_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	ng, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, eks.NodegroupInput{}, nil)
	require.NoError(t, err)

	// Mutate returned resources — must not affect stored copy.
	ng.Resources.AutoScalingGroups[0].Name = "mutated"
	ng2, err := b.DescribeNodegroup("c1", "ng1")
	require.NoError(t, err)
	assert.NotEqual(t, "mutated", ng2.Resources.AutoScalingGroups[0].Name)
}

func TestAccuracy_VpcConfig_HandlerJSON_Fields(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "c1",
		"resourcesVpcConfig": map[string]any{
			"subnetIds": []string{"subnet-x"},
		},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/c1", nil)
	body := rec.Body.Bytes()
	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))

	cluster := raw["cluster"].(map[string]any)
	vpc := cluster["resourcesVpcConfig"].(map[string]any)

	// All 5 VPC fields should be present.
	_, hasSubnets := vpc["subnetIds"]
	_, hasSGs := vpc["securityGroupIds"]
	_, hasEPA := vpc["endpointPrivateAccess"]
	_, hasEPU := vpc["endpointPublicAccess"]
	_, hasCIDRs := vpc["publicAccessCidrs"]
	assert.True(t, hasSubnets)
	assert.True(t, hasSGs)
	assert.True(t, hasEPA)
	assert.True(t, hasEPU)
	assert.True(t, hasCIDRs)
}
