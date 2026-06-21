package eks_test

// batch1_accuracy_test.go — AWS-accuracy audit batch-1 for EKS (go-jy8o / #1690).
//
// Covers: cluster tags, encryptionConfig in DescribeCluster, VPC clusterSecurityGroupId+vpcId,
// nodegroup/addon/fargate/podidentity/accessentry tags, Update params+errors arrays,
// UpdateClusterConfig VPC endpoint access, lifecycle status (CREATING→ACTIVE→DELETING),
// ARN formats, logging types, KMS encryption round-trip, OIDC identity provider,
// AccessEntry+AccessPolicy RBAC, PodIdentity associations, scaling config round-trip.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newB1Backend(t *testing.T) *eks.InMemoryBackend {
	t.Helper()

	return eks.NewInMemoryBackend("123456789012", config.DefaultRegion)
}

func newB1Handler(t *testing.T) (*eks.Handler, *eks.InMemoryBackend) {
	t.Helper()
	b := newB1Backend(t)

	return eks.NewHandler(b), b
}

func mustCreateCluster(t *testing.T, b *eks.InMemoryBackend, name string) *eks.Cluster {
	t.Helper()
	c, err := b.CreateCluster(name, "1.32", "arn:aws:iam::123456789012:role/eks", &eks.VpcConfig{
		SubnetIDs:            []string{"subnet-aaa", "subnet-bbb"},
		SecurityGroupIDs:     []string{"sg-111"},
		EndpointPublicAccess: true,
	}, nil, nil)
	require.NoError(t, err)

	return c
}

func mustCreateClusterNoVpc(t *testing.T, b *eks.InMemoryBackend, name string) *eks.Cluster {
	t.Helper()
	c, err := b.CreateCluster(name, "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	return c
}

func mustCreateNodegroup(t *testing.T, b *eks.InMemoryBackend, cluster string) *eks.Nodegroup {
	t.Helper()
	ng, err := b.CreateNodegroup(cluster, "ng1", "arn:aws:iam::123456789012:role/ng",
		"", "", "", "", []string{"subnet-aaa"}, 1, 1, 3, eks.NodegroupInput{}, nil)
	require.NoError(t, err)

	return ng
}

func b1ParseCluster(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	m := parseResp(t, rec)
	cluster, ok := m["cluster"].(map[string]any)
	require.True(t, ok, "response must have 'cluster' key")

	return cluster
}

func b1ParseNodegroup(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	m := parseResp(t, rec)
	ng, ok := m["nodegroup"].(map[string]any)
	require.True(t, ok, "response must have 'nodegroup' key")

	return ng
}

// ---------------------------------------------------------------------------
// Gap 1: clusterToJSON emits tags
// ---------------------------------------------------------------------------

func TestBatch1_Cluster_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name":    "tagged-cluster",
		"version": "1.32",
		"tags":    map[string]string{"env": "prod", "team": "infra"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	tags, ok := cluster["tags"].(map[string]any)
	require.True(t, ok, "cluster response must include 'tags' field")
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])
}

func TestBatch1_Cluster_Tags_EmptyMap_When_No_Tags(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "notags-cluster",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	tags, ok := cluster["tags"]
	require.True(t, ok, "cluster response must always include 'tags' field")
	// Should be an empty map, not nil/absent.
	asMap, isMap := tags.(map[string]any)
	require.True(t, isMap, "tags must be a map even when empty")
	assert.Empty(t, asMap, "tags map must be empty when no tags set")
}

func TestBatch1_Cluster_Tags_Persist_On_Describe(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "persist-tags",
		"tags": map[string]string{"cost-center": "123"},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/persist-tags", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	tags := cluster["tags"].(map[string]any)
	assert.Equal(t, "123", tags["cost-center"])
}

func TestBatch1_Cluster_TagResource_Reflected_In_Describe(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	c := mustCreateClusterNoVpc(t, b, "tag-reflect")

	// Add tag via TagResource.
	rec := doREST(t, h, http.MethodPost, "/tags/"+c.ARN, map[string]any{
		"tags": map[string]string{"added": "yes"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Tags from TagResource appear in DescribeCluster.
	rec2 := doREST(t, h, http.MethodGet, "/clusters/tag-reflect", nil)
	cluster := b1ParseCluster(t, rec2)
	tags := cluster["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["added"])
}

// ---------------------------------------------------------------------------
// Gap 2: clusterToJSON emits encryptionConfig after AssociateEncryptionConfig
// ---------------------------------------------------------------------------

func TestBatch1_EncryptionConfig_Returned_By_DescribeCluster(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "enc-describe")

	// Associate encryption config.
	rec := doREST(t, h, http.MethodPost, "/clusters/enc-describe/encryption-config/associate",
		map[string]any{
			"encryptionConfig": []map[string]any{
				{
					"provider":  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123"},
					"resources": []string{"secrets"},
				},
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeCluster must now include encryptionConfig.
	rec2 := doREST(t, h, http.MethodGet, "/clusters/enc-describe", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	cluster := b1ParseCluster(t, rec2)
	encCfg, ok := cluster["encryptionConfig"]
	require.True(t, ok, "DescribeCluster must return encryptionConfig after AssociateEncryptionConfig")
	encList, isList := encCfg.([]any)
	require.True(t, isList, "encryptionConfig must be an array")
	require.Len(t, encList, 1)

	entry := encList[0].(map[string]any)
	provider := entry["provider"].(map[string]any)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123", provider["keyArn"])
}

func TestBatch1_EncryptionConfig_Absent_Before_Associate(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "enc-absent")

	rec := doREST(t, h, http.MethodGet, "/clusters/enc-absent", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	// Before AssociateEncryptionConfig, encryptionConfig should be absent or empty.
	if encCfg, ok := cluster["encryptionConfig"]; ok {
		encList, _ := encCfg.([]any)
		assert.Empty(t, encList, "encryptionConfig must be empty before association")
	}
}

func TestBatch1_EncryptionConfig_Replace_Reflected_In_Describe(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "enc-replace")

	doREST(t, h, http.MethodPost, "/clusters/enc-replace/encryption-config/associate",
		map[string]any{
			"encryptionConfig": []map[string]any{
				{
					"provider":  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/k1"},
					"resources": []string{"secrets"},
				},
			},
		})

	doREST(t, h, http.MethodPost, "/clusters/enc-replace/encryption-config/associate",
		map[string]any{
			"encryptionConfig": []map[string]any{
				{
					"provider":  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/k2"},
					"resources": []string{"secrets"},
				},
			},
		})

	rec := doREST(t, h, http.MethodGet, "/clusters/enc-replace", nil)
	cluster := b1ParseCluster(t, rec)
	encList := cluster["encryptionConfig"].([]any)
	require.Len(t, encList, 1, "replace must produce exactly one config")

	entry := encList[0].(map[string]any)
	provider := entry["provider"].(map[string]any)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/k2", provider["keyArn"], "second config must overwrite first")
}

// ---------------------------------------------------------------------------
// Gap 3: VpcConfig clusterSecurityGroupId + vpcId
// ---------------------------------------------------------------------------

func TestBatch1_VpcConfig_ClusterSecurityGroupID_Present(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "vpc-sg-cluster",
		"resourcesVpcConfig": map[string]any{
			"subnetIds":        []string{"subnet-aaa"},
			"securityGroupIds": []string{"sg-user"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	vpc := cluster["resourcesVpcConfig"].(map[string]any)
	clusterSG, ok := vpc["clusterSecurityGroupId"].(string)
	require.True(t, ok, "resourcesVpcConfig must include clusterSecurityGroupId")
	assert.True(t, strings.HasPrefix(clusterSG, "sg-"),
		"clusterSecurityGroupId must start with 'sg-', got: %s", clusterSG)
}

func TestBatch1_VpcConfig_VpcId_Present(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "vpc-id-cluster",
		"resourcesVpcConfig": map[string]any{
			"subnetIds": []string{"subnet-xyz"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	vpc := cluster["resourcesVpcConfig"].(map[string]any)
	vpcID, ok := vpc["vpcId"].(string)
	require.True(t, ok, "resourcesVpcConfig must include vpcId")
	assert.True(t, strings.HasPrefix(vpcID, "vpc-"),
		"vpcId must start with 'vpc-', got: %s", vpcID)
}

func TestBatch1_VpcConfig_ClusterSecurityGroupID_Stable_Per_Cluster(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	vpcCfg := map[string]any{"subnetIds": []string{"subnet-aaa"}}

	rec1 := doREST(t, h, http.MethodPost, "/clusters",
		map[string]any{"name": "stable-sg-c1", "resourcesVpcConfig": vpcCfg})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/stable-sg-c1", nil)
	cluster1a := b1ParseCluster(t, rec1)
	cluster1b := b1ParseCluster(t, rec2)

	sg1a := cluster1a["resourcesVpcConfig"].(map[string]any)["clusterSecurityGroupId"]
	sg1b := cluster1b["resourcesVpcConfig"].(map[string]any)["clusterSecurityGroupId"]
	assert.Equal(t, sg1a, sg1b, "clusterSecurityGroupId must be stable across Describe calls")
}

func TestBatch1_VpcConfig_DifferentClusters_DifferentSG(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	vpcCfg := map[string]any{"subnetIds": []string{"subnet-aaa"}}

	rec1 := doREST(t, h, http.MethodPost, "/clusters",
		map[string]any{"name": "diff-sg-c1", "resourcesVpcConfig": vpcCfg})
	rec2 := doREST(t, h, http.MethodPost, "/clusters",
		map[string]any{"name": "diff-sg-c2", "resourcesVpcConfig": vpcCfg})

	sg1 := b1ParseCluster(t, rec1)["resourcesVpcConfig"].(map[string]any)["clusterSecurityGroupId"]
	sg2 := b1ParseCluster(t, rec2)["resourcesVpcConfig"].(map[string]any)["clusterSecurityGroupId"]
	assert.NotEqual(t, sg1, sg2, "different clusters must get different clusterSecurityGroupIds")
}

// ---------------------------------------------------------------------------
// Gap 4: Nodegroup tags in JSON response
// ---------------------------------------------------------------------------

func TestBatch1_Nodegroup_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ng-tags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/ng-tags-cluster/node-groups", map[string]any{
		"nodegroupName": "ng-tagged",
		"nodeRole":      "arn:aws:iam::123:role/ng",
		"subnets":       []string{"subnet-aaa"},
		"tags":          map[string]string{"env": "staging", "app": "web"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	ng := b1ParseNodegroup(t, rec)
	tags, ok := ng["tags"].(map[string]any)
	require.True(t, ok, "nodegroup response must include 'tags' field")
	assert.Equal(t, "staging", tags["env"])
	assert.Equal(t, "web", tags["app"])
}

func TestBatch1_Nodegroup_Tags_EmptyMap_When_No_Tags(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ng-notags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/ng-notags-cluster/node-groups", map[string]any{
		"nodegroupName": "ng-no-tags",
		"nodeRole":      "arn:aws:iam::123:role/ng",
		"subnets":       []string{"subnet-aaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	ng := b1ParseNodegroup(t, rec)
	tagsRaw, ok := ng["tags"]
	require.True(t, ok, "nodegroup response must always include 'tags' field")
	asMap, isMap := tagsRaw.(map[string]any)
	require.True(t, isMap)
	assert.Empty(t, asMap)
}

func TestBatch1_Nodegroup_Tags_Persist_On_Describe(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ng-persist-cluster")

	doREST(t, h, http.MethodPost, "/clusters/ng-persist-cluster/node-groups", map[string]any{
		"nodegroupName": "ng-persist",
		"nodeRole":      "arn:aws:iam::123:role/ng",
		"subnets":       []string{"subnet-aaa"},
		"tags":          map[string]string{"key1": "val1"},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/ng-persist-cluster/node-groups/ng-persist", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	ng := b1ParseNodegroup(t, rec)
	tags := ng["tags"].(map[string]any)
	assert.Equal(t, "val1", tags["key1"])
}

// ---------------------------------------------------------------------------
// Gap 5: Addon tags in JSON response
// ---------------------------------------------------------------------------

func TestBatch1_Addon_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "addon-tags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/addon-tags-cluster/addons", map[string]any{
		"addonName": "vpc-cni",
		"tags":      map[string]string{"managed": "true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	addon, ok := m["addon"].(map[string]any)
	require.True(t, ok)
	tags, ok := addon["tags"].(map[string]any)
	require.True(t, ok, "addon response must include 'tags' field")
	assert.Equal(t, "true", tags["managed"])
}

func TestBatch1_Addon_Tags_EmptyMap_When_No_Tags(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "addon-notags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/addon-notags-cluster/addons", map[string]any{
		"addonName": "coredns",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	addon := m["addon"].(map[string]any)
	tagsRaw, ok := addon["tags"]
	require.True(t, ok, "addon must always emit 'tags' field")
	asMap, isMap := tagsRaw.(map[string]any)
	require.True(t, isMap)
	assert.Empty(t, asMap)
}

func TestBatch1_Addon_Tags_Persist_On_Describe(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "addon-desc-cluster")

	doREST(t, h, http.MethodPost, "/clusters/addon-desc-cluster/addons", map[string]any{
		"addonName": "aws-ebs-csi-driver",
		"tags":      map[string]string{"billing": "infra"},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/addon-desc-cluster/addons/aws-ebs-csi-driver", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	addon := m["addon"].(map[string]any)
	tags := addon["tags"].(map[string]any)
	assert.Equal(t, "infra", tags["billing"])
}

// ---------------------------------------------------------------------------
// Gap 6: FargateProfile tags in JSON response
// ---------------------------------------------------------------------------

func TestBatch1_FargateProfile_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "fargate-tags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/fargate-tags-cluster/fargate-profiles", map[string]any{
		"fargateProfileName":  "fp-tagged",
		"podExecutionRoleArn": "arn:aws:iam::123:role/fargate",
		"selectors":           []map[string]any{{"namespace": "default"}},
		"tags":                map[string]string{"team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	fp, ok := m["fargateProfile"].(map[string]any)
	require.True(t, ok)
	tags, ok := fp["tags"].(map[string]any)
	require.True(t, ok, "fargateProfile response must include 'tags' field")
	assert.Equal(t, "platform", tags["team"])
}

func TestBatch1_FargateProfile_Tags_EmptyMap_When_No_Tags(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "fargate-notags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/fargate-notags-cluster/fargate-profiles", map[string]any{
		"fargateProfileName":  "fp-notags",
		"podExecutionRoleArn": "arn:aws:iam::123:role/fargate",
		"selectors":           []map[string]any{{"namespace": "default"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	fp := m["fargateProfile"].(map[string]any)
	tagsRaw, ok := fp["tags"]
	require.True(t, ok, "fargateProfile must always emit 'tags' field")
	asMap, isMap := tagsRaw.(map[string]any)
	require.True(t, isMap)
	assert.Empty(t, asMap)
}

// ---------------------------------------------------------------------------
// Gap 7: PodIdentityAssociation tags in JSON response
// ---------------------------------------------------------------------------

func TestBatch1_PodIdentity_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "pi-tags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/pi-tags-cluster/pod-identity-associations", map[string]any{
		"namespace":      "kube-system",
		"serviceAccount": "aws-node",
		"roleArn":        "arn:aws:iam::123:role/pi",
		"tags":           map[string]string{"owner": "ops"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	assoc, ok := m["association"].(map[string]any)
	require.True(t, ok)
	tags, ok := assoc["tags"].(map[string]any)
	require.True(t, ok, "association response must include 'tags' field")
	assert.Equal(t, "ops", tags["owner"])
}

func TestBatch1_PodIdentity_Tags_EmptyMap_When_No_Tags(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "pi-notags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/pi-notags-cluster/pod-identity-associations", map[string]any{
		"namespace":      "default",
		"serviceAccount": "default",
		"roleArn":        "arn:aws:iam::123:role/pi",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	assoc := m["association"].(map[string]any)
	tagsRaw, ok := assoc["tags"]
	require.True(t, ok, "association must always emit 'tags' field")
	asMap, isMap := tagsRaw.(map[string]any)
	require.True(t, isMap)
	assert.Empty(t, asMap)
}

// ---------------------------------------------------------------------------
// Gap 8: AccessEntry tags in JSON response
// ---------------------------------------------------------------------------

func TestBatch1_AccessEntry_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ae-tags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/ae-tags-cluster/access-entries", map[string]any{
		"principalArn": "arn:aws:iam::123456789012:role/DevRole",
		"type":         "STANDARD",
		"tags":         map[string]string{"role": "dev"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	ae, ok := m["accessEntry"].(map[string]any)
	require.True(t, ok)
	tags, ok := ae["tags"].(map[string]any)
	require.True(t, ok, "accessEntry response must include 'tags' field")
	assert.Equal(t, "dev", tags["role"])
}

func TestBatch1_AccessEntry_Tags_EmptyMap_When_No_Tags(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ae-notags-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/ae-notags-cluster/access-entries", map[string]any{
		"principalArn": "arn:aws:iam::123456789012:role/OpRole",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	ae := m["accessEntry"].(map[string]any)
	tagsRaw, ok := ae["tags"]
	require.True(t, ok, "accessEntry must always emit 'tags' field")
	asMap, isMap := tagsRaw.(map[string]any)
	require.True(t, isMap)
	assert.Empty(t, asMap)
}

func TestBatch1_AccessEntry_Tags_Persist_On_Describe(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ae-persist-cluster")

	doREST(t, h, http.MethodPost, "/clusters/ae-persist-cluster/access-entries", map[string]any{
		"principalArn": "arn:aws:iam::123456789012:role/PersistRole",
		"tags":         map[string]string{"persisted": "yes"},
	})

	rec := doREST(t, h, http.MethodGet,
		"/clusters/ae-persist-cluster/access-entries/arn:aws:iam::123456789012:role/PersistRole", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	ae := m["accessEntry"].(map[string]any)
	tags := ae["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["persisted"])
}

// ---------------------------------------------------------------------------
// Gap 9: Update struct includes params and errors arrays
// ---------------------------------------------------------------------------

func TestBatch1_Update_Params_Array_Present_On_Version_Update(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "upd-params-cluster")

	rec := doREST(t, h, http.MethodPut, "/clusters/upd-params-cluster", map[string]any{
		"version": "1.32",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	upd, ok := m["update"].(map[string]any)
	require.True(t, ok, "response must include 'update' object")
	_, hasParams := upd["params"]
	assert.True(t, hasParams, "update must include 'params' field")
	_, hasErrors := upd["errors"]
	assert.True(t, hasErrors, "update must include 'errors' field")
}

func TestBatch1_Update_Params_Array_Present_On_ClusterVersion_Update(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "upd-cv-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/upd-cv-cluster/upgrade-policy", map[string]any{
		"version": "1.33",
	})
	// The endpoint that updates cluster version.
	_ = rec

	// Use backend directly for Update struct params verification.
	upd, err := b.UpdateClusterVersion("upd-cv-cluster", "1.33")
	require.NoError(t, err)
	require.NotEmpty(t, upd.Params, "UpdateClusterVersion must populate Params")
	assert.Equal(t, "Version", upd.Params[0].Type)
	assert.Equal(t, "1.33", upd.Params[0].Value)
}

func TestBatch1_Update_Params_NodegroupVersion(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateCluster(t, b, "ng-upd-cluster")
	mustCreateNodegroup(t, b, "ng-upd-cluster")

	upd, err := b.UpdateNodegroupVersion("ng-upd-cluster", "ng1", "1.33")
	require.NoError(t, err)
	require.NotEmpty(t, upd.Params, "UpdateNodegroupVersion must populate Params")
	assert.Equal(t, "Version", upd.Params[0].Type)
	assert.Equal(t, "1.33", upd.Params[0].Value)
}

func TestBatch1_Update_Errors_Empty_Array_On_Success(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "upd-err-cluster")

	rec := doREST(t, h, http.MethodPut, "/clusters/upd-err-cluster", map[string]any{
		"logging": map[string]any{
			"clusterLogging": []map[string]any{
				{"types": []string{"api"}, "enabled": true},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	upd := m["update"].(map[string]any)
	errorsRaw := upd["errors"]
	errsList, isList := errorsRaw.([]any)
	require.True(t, isList, "errors must be a JSON array")
	assert.Empty(t, errsList, "successful update must return empty errors array")
}

// ---------------------------------------------------------------------------
// Gap 10: UpdateClusterConfig handles VPC endpoint access changes
// ---------------------------------------------------------------------------

func TestBatch1_UpdateClusterConfig_VpcEndpoint_Public_To_Private(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateCluster(t, b, "vpc-upd-cluster")

	rec := doREST(t, h, http.MethodPut, "/clusters/vpc-upd-cluster", map[string]any{
		"resourcesVpcConfig": map[string]any{
			"endpointPublicAccess":  false,
			"endpointPrivateAccess": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify DescribeCluster reflects the change.
	rec2 := doREST(t, h, http.MethodGet, "/clusters/vpc-upd-cluster", nil)
	cluster := b1ParseCluster(t, rec2)
	vpc := cluster["resourcesVpcConfig"].(map[string]any)
	assert.Equal(t, false, vpc["endpointPublicAccess"])
	assert.Equal(t, true, vpc["endpointPrivateAccess"])
}

func TestBatch1_UpdateClusterConfig_VpcEndpoint_PublicAccessCidrs(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateCluster(t, b, "vpc-cidr-cluster")

	rec := doREST(t, h, http.MethodPut, "/clusters/vpc-cidr-cluster", map[string]any{
		"resourcesVpcConfig": map[string]any{
			"publicAccessCidrs": []string{"10.0.0.0/8", "192.168.1.0/24"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/vpc-cidr-cluster", nil)
	cluster := b1ParseCluster(t, rec2)
	vpc := cluster["resourcesVpcConfig"].(map[string]any)
	cidrs, _ := vpc["publicAccessCidrs"].([]any)
	assert.Len(t, cidrs, 2)
}

func TestBatch1_UpdateClusterConfig_VpcEndpoint_Params_Populated(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateCluster(t, b, "vpc-params-cluster")

	pub := true
	priv := false
	upd, err := b.UpdateClusterVpcEndpoint("vpc-params-cluster", eks.VpcEndpointUpdate{
		EndpointPublicAccess:  &pub,
		EndpointPrivateAccess: &priv,
	})
	require.NoError(t, err)
	require.Len(t, upd.Params, 2, "VPC endpoint update must populate both params")

	types := make([]string, len(upd.Params))
	for i, p := range upd.Params {
		types[i] = p.Type
	}
	assert.Contains(t, types, "EndpointPublicAccess")
	assert.Contains(t, types, "EndpointPrivateAccess")
}

// ---------------------------------------------------------------------------
// Gap 11: Cluster status lifecycle (CREATING → ACTIVE → DELETING)
// ---------------------------------------------------------------------------

func TestBatch1_Cluster_Status_CREATING_On_Create(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	c, err := b.CreateCluster("lifecycle-c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATING", c.Status, "CreateCluster must return CREATING status (async transition to ACTIVE)")
}

func TestBatch1_Cluster_Status_DELETING_On_Delete(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "del-status-c1")

	deleted, err := b.DeleteCluster("del-status-c1")
	require.NoError(t, err)
	assert.Equal(t, "DELETING", deleted.Status, "DeleteCluster must return DELETING status")
}

func TestBatch1_Cluster_Status_DELETING_Via_Handler(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "del-handler-c1")

	rec := doREST(t, h, http.MethodDelete, "/clusters/del-handler-c1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	assert.Equal(t, "DELETING", cluster["status"])
}

func TestBatch1_Nodegroup_Status_CREATING_On_Create(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "ng-status-cluster")
	ng := mustCreateNodegroup(t, b, "ng-status-cluster")
	assert.Equal(t, "CREATING", ng.Status, "CreateNodegroup must return CREATING status (async transition to ACTIVE)")
}

func TestBatch1_Nodegroup_Status_DELETING_On_Delete(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "ng-del-cluster")
	mustCreateNodegroup(t, b, "ng-del-cluster")

	deleted, err := b.DeleteNodegroup("ng-del-cluster", "ng1")
	require.NoError(t, err)
	assert.Equal(t, "DELETING", deleted.Status, "DeleteNodegroup must return DELETING status")
}

func TestBatch1_FargateProfile_Status_ACTIVE_On_Create(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "fp-status-cluster")

	fp, err := b.CreateFargateProfile("fp-status-cluster", "fp1",
		"arn:aws:iam::123:role/fargate",
		[]eks.FargateProfileSelector{{Namespace: "default"}}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", fp.Status)
}

func TestBatch1_FargateProfile_Status_DELETING_On_Delete(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "fp-del-cluster")

	_, _ = b.CreateFargateProfile("fp-del-cluster", "fp1",
		"arn:aws:iam::123:role/fargate",
		[]eks.FargateProfileSelector{{Namespace: "default"}}, nil, nil)

	deleted, err := b.DeleteFargateProfile("fp-del-cluster", "fp1")
	require.NoError(t, err)
	assert.Equal(t, "DELETING", deleted.Status)
}

func TestBatch1_Addon_Status_ACTIVE_On_Create(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "addon-status-cluster")

	addon, err := b.CreateAddon("addon-status-cluster", "vpc-cni", "", "", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", addon.Status)
}

func TestBatch1_Addon_Status_DELETING_On_Delete(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "addon-del-cluster")
	_, _ = b.CreateAddon("addon-del-cluster", "coredns", "", "", "", "", nil)

	deleted, err := b.DeleteAddon("addon-del-cluster", "coredns")
	require.NoError(t, err)
	assert.Equal(t, "DELETING", deleted.Status)
}

// ---------------------------------------------------------------------------
// Gap 12: ARN format verification
// ---------------------------------------------------------------------------

func TestBatch1_Cluster_ARN_Format(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	c, err := b.CreateCluster("arn-test-cluster", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	expected := fmt.Sprintf("arn:aws:eks:%s:123456789012:cluster/arn-test-cluster", config.DefaultRegion)
	assert.Equal(t, expected, c.ARN)
}

func TestBatch1_Nodegroup_ARN_Format(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "arn-ng-cluster")
	ng := mustCreateNodegroup(t, b, "arn-ng-cluster")

	assert.Contains(t, ng.ARN, "arn:aws:eks:")
	assert.Contains(t, ng.ARN, ":nodegroup/")
	assert.Contains(t, ng.ARN, "arn-ng-cluster")
	assert.Contains(t, ng.ARN, "ng1")
}

func TestBatch1_FargateProfile_ARN_Format(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "arn-fp-cluster")

	fp, err := b.CreateFargateProfile("arn-fp-cluster", "my-fargate",
		"arn:aws:iam::123:role/fargate",
		[]eks.FargateProfileSelector{{Namespace: "default"}}, nil, nil)
	require.NoError(t, err)

	assert.Contains(t, fp.ARN, "arn:aws:eks:")
	assert.Contains(t, fp.ARN, ":fargateprofile/")
	assert.Contains(t, fp.ARN, "arn-fp-cluster")
}

func TestBatch1_Addon_ARN_Format(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "arn-addon-cluster")

	addon, err := b.CreateAddon("arn-addon-cluster", "vpc-cni", "", "", "", "", nil)
	require.NoError(t, err)

	assert.Contains(t, addon.ARN, "arn:aws:eks:")
	assert.Contains(t, addon.ARN, ":addon/")
	assert.Contains(t, addon.ARN, "arn-addon-cluster")
	assert.Contains(t, addon.ARN, "vpc-cni")
}

// ---------------------------------------------------------------------------
// Gap 13: Logging — all five control-plane log types accepted
// ---------------------------------------------------------------------------

func TestBatch1_Logging_AllFiveTypes_Accepted(t *testing.T) {
	t.Parallel()

	logTypes := []string{"api", "audit", "authenticator", "controllerManager", "scheduler"}
	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "logging-5types")

	entries := make([]eks.ClusterLogEntry, 0, len(logTypes))
	for _, lt := range logTypes {
		entries = append(entries, eks.ClusterLogEntry{Types: []string{lt}, Enabled: true})
	}

	_, err := b.UpdateClusterConfig("logging-5types", eks.ClusterConfigUpdate{LogEntries: entries})
	require.NoError(t, err)

	c, err := b.DescribeCluster("logging-5types")
	require.NoError(t, err)

	enabledTypes := make(map[string]bool)
	for _, e := range c.ClusterLogging {
		if e.Enabled {
			for _, t := range e.Types {
				enabledTypes[t] = true
			}
		}
	}

	for _, lt := range logTypes {
		assert.True(t, enabledTypes[lt], "log type %q must be enabled", lt)
	}
}

func TestBatch1_Logging_Disable_Single_Type(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "logging-disable")

	// Enable all.
	_, err := b.UpdateClusterConfig("logging-disable", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"api", "audit", "authenticator"}, Enabled: true},
	}})
	require.NoError(t, err)

	// Disable just "audit".
	_, err = b.UpdateClusterConfig("logging-disable", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"audit"}, Enabled: false},
	}})
	require.NoError(t, err)

	c, err := b.DescribeCluster("logging-disable")
	require.NoError(t, err)

	enabled := make(map[string]bool)
	for _, e := range c.ClusterLogging {
		if e.Enabled {
			for _, t := range e.Types {
				enabled[t] = true
			}
		}
	}
	assert.True(t, enabled["api"])
	assert.True(t, enabled["authenticator"])
	assert.False(t, enabled["audit"], "audit must be disabled")
}

func TestBatch1_Logging_HTTP_AllTypes_In_Response(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "logging-http-all")

	rec := doREST(t, h, http.MethodPut, "/clusters/logging-http-all", map[string]any{
		"logging": map[string]any{
			"clusterLogging": []map[string]any{
				{"types": []string{"api", "audit", "authenticator", "controllerManager", "scheduler"}, "enabled": true},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/logging-http-all", nil)
	cluster := b1ParseCluster(t, rec2)
	raw, _ := json.Marshal(cluster)
	body := string(raw)

	for _, lt := range []string{"api", "audit", "authenticator", "controllerManager", "scheduler"} {
		assert.Contains(t, body, lt, "log type %q must appear in cluster response", lt)
	}
}

// ---------------------------------------------------------------------------
// Gap 14: KMS encryption config resources field round-trip
// ---------------------------------------------------------------------------

func TestBatch1_KMS_Resources_Field_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "kms-resources")

	cfg := []eks.EncryptionConfig{
		{
			Provider:  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123456789012:key/mrk-xyz"},
			Resources: []string{"secrets"},
		},
	}
	result, err := b.AssociateEncryptionConfig("kms-resources", cfg)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, []string{"secrets"}, result[0].Resources)
}

func TestBatch1_KMS_MultiResource_Accepted(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "kms-multi")

	cfg := []eks.EncryptionConfig{
		{
			Provider:  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/k1"},
			Resources: []string{"secrets", "configmaps"},
		},
	}
	result, err := b.AssociateEncryptionConfig("kms-multi", cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"secrets", "configmaps"}, result[0].Resources)
}

// ---------------------------------------------------------------------------
// Gap 15: VpcConfig endpointPublicAccess defaults + round-trip
// ---------------------------------------------------------------------------

func TestBatch1_VpcConfig_EndpointPublicAccess_ExplicitTrue_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	c, err := b.CreateCluster("vpc-explicit", "1.32", "", &eks.VpcConfig{
		SubnetIDs:            []string{"subnet-aaa"},
		EndpointPublicAccess: true,
	}, nil, nil)
	require.NoError(t, err)
	assert.True(t, c.VpcConfig.EndpointPublicAccess)
}

func TestBatch1_VpcConfig_EndpointPrivateAccess_RoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newB1Handler(t)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "vpc-private",
		"resourcesVpcConfig": map[string]any{
			"subnetIds":             []string{"subnet-aaa"},
			"endpointPrivateAccess": true,
			"endpointPublicAccess":  false,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	vpc := cluster["resourcesVpcConfig"].(map[string]any)
	assert.Equal(t, true, vpc["endpointPrivateAccess"])
	assert.Equal(t, false, vpc["endpointPublicAccess"])
}

// ---------------------------------------------------------------------------
// Gap 16: OIDC identity provider config lifecycle
// ---------------------------------------------------------------------------

func TestBatch1_OIDC_AssociateAndDescribe(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "oidc-cluster")

	params := map[string]string{
		"issuerUrl":     "https://oidc.example.com",
		"clientId":      "my-client-id",
		"usernameClaim": "email",
	}
	cfg, err := b.AssociateIdentityProviderConfig("oidc-cluster", "oidc", "my-client-id", params, nil)
	require.NoError(t, err)
	assert.Equal(t, "oidc", cfg.Type)
	assert.Equal(t, "my-client-id", cfg.Name)

	described, err := b.DescribeIdentityProviderConfig("oidc-cluster", "my-client-id")
	require.NoError(t, err)
	assert.Equal(t, cfg.Name, described.Name)
}

func TestBatch1_OIDC_List(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "oidc-list-cluster")

	_, _ = b.AssociateIdentityProviderConfig("oidc-list-cluster", "oidc", "client-a",
		map[string]string{"clientId": "client-a"}, nil)

	configs, err := b.ListIdentityProviderConfigs("oidc-list-cluster")
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "client-a", configs[0]["name"])
}

func TestBatch1_OIDC_Disassociate(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "oidc-del-cluster")

	_, _ = b.AssociateIdentityProviderConfig("oidc-del-cluster", "oidc", "client-b",
		map[string]string{"clientId": "client-b"}, nil)

	err := b.DisassociateIdentityProviderConfig("oidc-del-cluster", "client-b")
	require.NoError(t, err)

	configs, err := b.ListIdentityProviderConfigs("oidc-del-cluster")
	require.NoError(t, err)
	assert.Empty(t, configs)
}

// ---------------------------------------------------------------------------
// Gap 17: AccessEntry + AccessPolicy RBAC lifecycle
// ---------------------------------------------------------------------------

func TestBatch1_RBAC_CreateDescribeList(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "rbac-cluster")

	entry, err := b.CreateAccessEntry("rbac-cluster",
		"arn:aws:iam::123:role/DevRole", "STANDARD", "dev-user", nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, entry.ARN)

	described, err := b.DescribeAccessEntry("rbac-cluster", "arn:aws:iam::123:role/DevRole")
	require.NoError(t, err)
	assert.Equal(t, "dev-user", described.Username)

	arns, err := b.ListAccessEntries("rbac-cluster")
	require.NoError(t, err)
	assert.Contains(t, arns, "arn:aws:iam::123:role/DevRole")
}

func TestBatch1_RBAC_AssociatePolicy(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "rbac-policy-cluster")

	_, _ = b.CreateAccessEntry("rbac-policy-cluster", "arn:aws:iam::123:role/R1", "STANDARD", "", nil, nil)

	scope := map[string]any{"type": "cluster"}
	assoc, err := b.AssociateAccessPolicy(
		"rbac-policy-cluster",
		"arn:aws:iam::123:role/R1",
		"arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy",
		scope,
	)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123:role/R1", assoc.PrincipalARN)
}

func TestBatch1_RBAC_ListPolicies(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	policies := b.ListAccessPolicies()
	assert.NotEmpty(t, policies, "ListAccessPolicies must return built-in policies")

	for _, p := range policies {
		assert.NotEmpty(t, p["policyArn"], "each policy must have a policyArn")
		assert.NotEmpty(t, p["name"], "each policy must have a name")
	}
}

func TestBatch1_RBAC_DisassociatePolicy(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "rbac-dis-cluster")

	_, _ = b.CreateAccessEntry("rbac-dis-cluster", "arn:aws:iam::123:role/R2", "STANDARD", "", nil, nil)
	policyARN := "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
	_, _ = b.AssociateAccessPolicy("rbac-dis-cluster", "arn:aws:iam::123:role/R2", policyARN,
		map[string]any{"type": "cluster"})

	err := b.DisassociateAccessPolicy("rbac-dis-cluster", "arn:aws:iam::123:role/R2", policyARN)
	require.NoError(t, err)

	policies, err := b.ListAssociatedAccessPolicies("rbac-dis-cluster", "arn:aws:iam::123:role/R2")
	require.NoError(t, err)
	assert.Empty(t, policies)
}

// ---------------------------------------------------------------------------
// Gap 18: PodIdentity association full lifecycle
// ---------------------------------------------------------------------------

func TestBatch1_PodIdentity_CreateDescribeUpdate_Delete(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "pi-lifecycle-cluster")

	assoc, err := b.CreatePodIdentityAssociation(
		"pi-lifecycle-cluster", "kube-system", "aws-node",
		"arn:aws:iam::123:role/pi-role", nil,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)
	assert.Equal(t, "kube-system", assoc.Namespace)
	assert.Equal(t, "aws-node", assoc.ServiceAccount)

	described, err := b.DescribePodIdentityAssociation("pi-lifecycle-cluster", assoc.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, assoc.AssociationID, described.AssociationID)

	updated, err := b.UpdatePodIdentityAssociation(
		"pi-lifecycle-cluster", assoc.AssociationID, "arn:aws:iam::123:role/pi-role-v2",
	)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123:role/pi-role-v2", updated.RoleARN)

	deleted, err := b.DeletePodIdentityAssociation("pi-lifecycle-cluster", assoc.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, assoc.AssociationID, deleted.AssociationID)

	_, err = b.DescribePodIdentityAssociation("pi-lifecycle-cluster", assoc.AssociationID)
	assert.Error(t, err, "describing deleted association must return error")
}

func TestBatch1_PodIdentity_List(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "pi-list-cluster")

	_, _ = b.CreatePodIdentityAssociation("pi-list-cluster", "ns1", "sa1", "arn:aws:iam::123:role/r1", nil)
	_, _ = b.CreatePodIdentityAssociation("pi-list-cluster", "ns2", "sa2", "arn:aws:iam::123:role/r2", nil)

	assocs, err := b.ListPodIdentityAssociations("pi-list-cluster")
	require.NoError(t, err)
	assert.Len(t, assocs, 2)
}

// ---------------------------------------------------------------------------
// Gap 19: Nodegroup scaling config fields round-trip
// ---------------------------------------------------------------------------

func TestBatch1_Nodegroup_ScalingConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ng-scaling-cluster")

	rec := doREST(t, h, http.MethodPost, "/clusters/ng-scaling-cluster/node-groups", map[string]any{
		"nodegroupName": "ng-scaled",
		"nodeRole":      "arn:aws:iam::123:role/ng",
		"subnets":       []string{"subnet-aaa"},
		"scalingConfig": map[string]any{
			"desiredSize": 3,
			"minSize":     1,
			"maxSize":     10,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	ng := b1ParseNodegroup(t, rec)
	scaling := ng["scalingConfig"].(map[string]any)
	assert.InDelta(t, float64(3), scaling["desiredSize"], 0)
	assert.InDelta(t, float64(1), scaling["minSize"], 0)
	assert.InDelta(t, float64(10), scaling["maxSize"], 0)
}

func TestBatch1_Nodegroup_UpdateScalingConfig(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ng-upd-scale-cluster")

	doREST(t, h, http.MethodPost, "/clusters/ng-upd-scale-cluster/node-groups", map[string]any{
		"nodegroupName": "ng-to-scale",
		"nodeRole":      "arn:aws:iam::123:role/ng",
		"subnets":       []string{"subnet-aaa"},
		"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 5},
	})

	rec := doREST(t, h, http.MethodPost, "/clusters/ng-upd-scale-cluster/node-groups/ng-to-scale",
		map[string]any{
			"scalingConfig": map[string]any{"desiredSize": 4, "minSize": 2, "maxSize": 8},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/clusters/ng-upd-scale-cluster/node-groups/ng-to-scale", nil)
	ng := b1ParseNodegroup(t, rec2)
	scaling := ng["scalingConfig"].(map[string]any)
	assert.InDelta(t, float64(4), scaling["desiredSize"], 0)
	assert.InDelta(t, float64(2), scaling["minSize"], 0)
	assert.InDelta(t, float64(8), scaling["maxSize"], 0)
}

// ---------------------------------------------------------------------------
// Gap 20: Cluster and nodegroup ARN in describe response
// ---------------------------------------------------------------------------

func TestBatch1_DescribeCluster_ARN_Present(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "arn-present-cluster")

	rec := doREST(t, h, http.MethodGet, "/clusters/arn-present-cluster", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	arn, ok := cluster["arn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:eks:")
	assert.Contains(t, arn, "arn-present-cluster")
}

func TestBatch1_DescribeNodegroup_ARN_Present(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "ng-arn-cluster")

	doREST(t, h, http.MethodPost, "/clusters/ng-arn-cluster/node-groups", map[string]any{
		"nodegroupName": "ng-arn",
		"nodeRole":      "arn:aws:iam::123:role/ng",
		"subnets":       []string{"subnet-aaa"},
	})

	rec := doREST(t, h, http.MethodGet, "/clusters/ng-arn-cluster/node-groups/ng-arn", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	ng := b1ParseNodegroup(t, rec)
	arn, ok := ng["nodegroupArn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:eks:")
	assert.Contains(t, arn, "ng-arn-cluster")
}

// ---------------------------------------------------------------------------
// Gap 21: DeleteCluster cascades cleanup of related resources
// ---------------------------------------------------------------------------

func TestBatch1_DeleteCluster_Returns_DELETING_Status(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "del-status-cascade-cluster")

	deleted, err := b.DeleteCluster("del-status-cascade-cluster")
	require.NoError(t, err)
	assert.Equal(t, "DELETING", deleted.Status, "DeleteCluster must return DELETING status in response")
}

func TestBatch1_DeleteCluster_Cascades_Nodegroup_Cleanup(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "del-cascade-cluster2")
	mustCreateNodegroup(t, b, "del-cascade-cluster2")

	_, err := b.DeleteCluster("del-cascade-cluster2")
	require.NoError(t, err)
	assert.Equal(t, 0, b.NodegroupCount(), "DeleteCluster must cascade nodegroup cleanup")
}

// ---------------------------------------------------------------------------
// Gap 22: PlatformVersion returned on DescribeCluster
// ---------------------------------------------------------------------------

func TestBatch1_Cluster_PlatformVersion_Present(t *testing.T) {
	t.Parallel()

	h, b := newB1Handler(t)
	mustCreateClusterNoVpc(t, b, "pv-cluster")

	rec := doREST(t, h, http.MethodGet, "/clusters/pv-cluster", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	cluster := b1ParseCluster(t, rec)
	pv, ok := cluster["platformVersion"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, pv, "platformVersion must be non-empty")
}

// ---------------------------------------------------------------------------
// Gap 23: ListClusters returns sorted names
// ---------------------------------------------------------------------------

func TestBatch1_ListClusters_Sorted(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	for _, name := range []string{"cluster-z", "cluster-a", "cluster-m"} {
		mustCreateClusterNoVpc(t, b, name)
	}

	names := b.ListClusters()
	require.Len(t, names, 3)
	assert.Equal(t, "cluster-a", names[0])
	assert.Equal(t, "cluster-m", names[1])
	assert.Equal(t, "cluster-z", names[2])
}

// ---------------------------------------------------------------------------
// Gap 24: UpdateClusterVpcEndpoint backend method
// ---------------------------------------------------------------------------

func TestBatch1_UpdateClusterVpcEndpoint_NoVpc_Creates_Default(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	// Cluster with no VPC config.
	mustCreateClusterNoVpc(t, b, "novpc-endpoint-cluster")

	pub := true
	upd, err := b.UpdateClusterVpcEndpoint("novpc-endpoint-cluster", eks.VpcEndpointUpdate{
		EndpointPublicAccess: &pub,
	})
	require.NoError(t, err)
	assert.Equal(t, "Successful", upd.Status)

	c, err := b.DescribeCluster("novpc-endpoint-cluster")
	require.NoError(t, err)
	require.NotNil(t, c.VpcConfig, "VpcConfig must be created if it was nil")
	assert.True(t, c.VpcConfig.EndpointPublicAccess)
}

func TestBatch1_UpdateClusterVpcEndpoint_NonExistent_Cluster(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	priv := true
	_, err := b.UpdateClusterVpcEndpoint("nonexistent", eks.VpcEndpointUpdate{
		EndpointPrivateAccess: &priv,
	})
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Gap 25: Update lifecycle async — status values
// ---------------------------------------------------------------------------

func TestBatch1_UpdateClusterConfig_Status_InProgress(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "upd-inprog-cluster")

	upd, err := b.UpdateClusterConfig("upd-inprog-cluster", eks.ClusterConfigUpdate{LogEntries: []eks.ClusterLogEntry{
		{Types: []string{"api"}, Enabled: true},
	}})
	require.NoError(t, err)
	assert.Equal(t, "InProgress", upd.Status)
}

func TestBatch1_UpdateNodegroupVersion_Status_InProgress(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "ng-inprog-cluster")
	mustCreateNodegroup(t, b, "ng-inprog-cluster")

	upd, err := b.UpdateNodegroupVersion("ng-inprog-cluster", "ng1", "1.33")
	require.NoError(t, err)
	assert.Equal(t, "InProgress", upd.Status)
}

func TestBatch1_DescribeUpdate_Status_Successful(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "desc-upd-cluster")

	created, err := b.UpdateClusterVersion("desc-upd-cluster", "1.30")
	require.NoError(t, err)

	upd, err := b.DescribeUpdate("desc-upd-cluster", created.ID)
	require.NoError(t, err)
	assert.Equal(t, "Successful", upd.Status)
}

func TestBatch1_DescribeUpdate_NotFound(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "desc-upd-404")

	_, err := b.DescribeUpdate("desc-upd-404", "nonexistent-update-id")
	require.Error(t, err)
	require.ErrorIs(t, err, eks.ErrNotFound)
}

func TestBatch1_ListUpdates_ReturnsStoredIDs(t *testing.T) {
	t.Parallel()

	b := newB1Backend(t)
	mustCreateClusterNoVpc(t, b, "list-upd-cluster")
	mustCreateNodegroup(t, b, "list-upd-cluster")

	ids, err := b.ListUpdates("list-upd-cluster")
	require.NoError(t, err)
	assert.Empty(t, ids, "no updates yet")

	u1, err := b.UpdateClusterVersion("list-upd-cluster", "1.30")
	require.NoError(t, err)

	u2, err := b.UpdateNodegroupVersion("list-upd-cluster", "ng1", "1.30")
	require.NoError(t, err)

	ids, err = b.ListUpdates("list-upd-cluster")
	require.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, u1.ID)
	assert.Contains(t, ids, u2.ID)
}
