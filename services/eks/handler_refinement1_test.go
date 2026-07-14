package eks_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestRefinement1_Reset verifies Backend.Reset() clears all state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, b.ClusterCount())

	b.Reset()

	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.NodegroupCount())
	assert.Equal(t, 0, b.AccessEntryCount())
	assert.Equal(t, 0, b.AddonCount())
	assert.Equal(t, 0, b.FargateProfileCount())
	assert.Equal(t, 0, b.CapabilityCount())
	assert.Equal(t, 0, b.SubscriptionCount())
}

// TestRefinement1_MultipleResetCycle verifies multiple Reset() calls are idempotent.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	for range 3 {
		_, err := b.CreateCluster("cluster", "1.32", "", nil, nil, nil)
		require.NoError(t, err)

		b.Reset()

		assert.Equal(t, 0, b.ClusterCount())
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset() delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	assert.Equal(t, 1, h.Backend.ClusterCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ClusterCount())
}

// TestRefinement1_ProviderInit_NilCtx verifies that Init returns ErrNilAppContext when ctx is nil.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &eks.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, eks.ErrNilAppContext)
}

// TestRefinement1_ProviderInit_OK verifies that Init succeeds with a valid context.
func TestRefinement1_ProviderInit_OK(t *testing.T) {
	t.Parallel()

	p := &eks.Provider{}
	ctx := &service.AppContext{JanitorCtx: t.Context()}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

// TestRefinement1_ErrValidation verifies ErrValidation is a distinct sentinel.
func TestRefinement1_ErrValidation(t *testing.T) {
	t.Parallel()

	require.Error(t, eks.ErrValidation)
	assert.NotEqual(t, eks.ErrNotFound, eks.ErrValidation)
	assert.NotEqual(t, eks.ErrAlreadyExists, eks.ErrValidation)
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation maps to HTTP 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	// CreateCapability with no capabilityName → ErrValidation → 400
	rec := doREST(t, h, http.MethodPost, "/clusters/c1/capabilities", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_SortedListClusters verifies ListClusters returns sorted names.
func TestRefinement1_SortedListClusters(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	for _, name := range []string{"zzz", "aaa", "mmm", "bbb"} {
		doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": name})
	}

	rec := doREST(t, h, http.MethodGet, "/clusters", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	raw, err := json.Marshal(resp["clusters"])
	require.NoError(t, err)

	var names []string
	require.NoError(t, json.Unmarshal(raw, &names))

	require.Equal(t, []string{"aaa", "bbb", "mmm", "zzz"}, names)
}

// TestRefinement1_SortedListNodegroups verifies ListNodegroups returns sorted names.
func TestRefinement1_SortedListNodegroups(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	for _, ng := range []string{"zzz", "aaa", "mmm"} {
		doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
			"nodegroupName": ng,
			"nodeRole":      "arn:aws:iam::123456789012:role/ng",
			"subnets":       []string{"subnet-abc"},
		})
	}

	rec := doREST(t, h, http.MethodGet, "/clusters/c1/node-groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	raw, err := json.Marshal(resp["nodegroups"])
	require.NoError(t, err)

	var names []string
	require.NoError(t, json.Unmarshal(raw, &names))

	require.Equal(t, []string{"aaa", "mmm", "zzz"}, names)
}

// TestRefinement1_CreateClusterInitsAllMaps verifies that CreateCluster initialises
// all per-cluster inner maps so that subsequent operations on a new cluster don't panic.
func TestRefinement1_CreateClusterInitsAllMaps(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	// These should not panic even before any resources have been added.
	_, err = b.CreateAccessEntry("c1", "arn:aws:iam::123:role/r", "STANDARD", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAddon("c1", "coredns", "v1.0", "", "", "", nil)
	require.NoError(t, err)
}

// TestRefinement1_SeedHelpers verifies AddClusterInternal and AddNodegroupInternal.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	b.AddClusterInternal(&eks.Cluster{Name: "seeded", Version: "1.30", Status: "ACTIVE"})
	assert.Equal(t, 1, b.ClusterCount())

	b.AddNodegroupInternal(&eks.Nodegroup{NodegroupName: "ng1", ClusterName: "seeded"})
	assert.Equal(t, 1, b.NodegroupCount())
}

// TestRefinement1_ExportCountHelpers verifies all export_test.go count helpers.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.NodegroupCount())
	assert.Equal(t, 0, b.AccessEntryCount())
	assert.Equal(t, 0, b.AddonCount())
	assert.Equal(t, 0, b.FargateProfileCount())
	assert.Equal(t, 0, b.PodIdentityAssociationCount())
	assert.Equal(t, 0, b.CapabilityCount())
	assert.Equal(t, 0, b.SubscriptionCount())
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot and Restore preserve state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, map[string]string{"env": "test"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.ClusterCount())

	tags, err := b2.ListTagsForResource("arn:aws:eks:" + config.DefaultRegion + ":123456789012:cluster/c1")
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

// TestRefinement1_DeleteClusterCascade verifies DeleteCluster cleans up child resources.
func TestRefinement1_DeleteClusterCascade(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, eks.NodegroupInput{}, nil)
	require.NoError(t, err)

	_, err = b.CreateAccessEntry("c1", "arn:aws:iam::123:role/r", "STANDARD", "", nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, b.NodegroupCount())
	assert.Equal(t, 1, b.AccessEntryCount())

	_, err = b.DeleteCluster("c1")
	require.NoError(t, err)

	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.NodegroupCount())
	assert.Equal(t, 0, b.AccessEntryCount())
}

// TestRefinement1_TagResource verifies TagResource and UntagResource round-trip.
func TestRefinement1_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	require.Equal(t, http.StatusOK, rec.Code)

	clusterARN := "arn:aws:eks:" + config.DefaultRegion + ":123456789012:cluster/c1"

	rec = doREST(t, h, http.MethodPost, "/tags/"+clusterARN, map[string]any{
		"tags": map[string]any{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tags, err := h.Backend.ListTagsForResource(clusterARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])

	doREST(t, h, http.MethodDelete, "/tags/"+clusterARN+"?tagKeys=env", nil)

	tags2, err := h.Backend.ListTagsForResource(clusterARN)
	require.NoError(t, err)
	assert.Empty(t, tags2["env"])
}

// TestRefinement1_TagResourceNotFound verifies TagResource returns 404 for unknown ARN.
func TestRefinement1_TagResourceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	rec := doREST(t, h, http.MethodPost, "/tags/arn:aws:eks:us-east-1:123:cluster/nonexistent",
		map[string]any{"tags": map[string]any{"k": "v"}})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_AccessEntryAddAddon verifies AddAccessEntryInternal and AddAddonInternal.
func TestRefinement1_AccessEntryAddAddon(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	b.AddClusterInternal(&eks.Cluster{Name: "c1", Status: "ACTIVE"})

	b.AddAccessEntryInternal(&eks.AccessEntry{
		ClusterName:  "c1",
		PrincipalARN: "arn:aws:iam::123:role/r",
		Type:         "STANDARD",
	})

	b.AddAddonInternal(&eks.Addon{ClusterName: "c1", AddonName: "coredns", Status: "ACTIVE"})

	assert.Equal(t, 1, b.AccessEntryCount())
	assert.Equal(t, 1, b.AddonCount())
}

// TestRefinement1_AddFargateProfile verifies AddFargateProfileInternal seed helper.
func TestRefinement1_AddFargateProfile(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	b.AddFargateProfileInternal(&eks.FargateProfile{
		ClusterName:        "c1",
		FargateProfileName: "fp1",
		Status:             "ACTIVE",
	})

	assert.Equal(t, 1, b.FargateProfileCount())
}

// TestRefinement1_AddCapabilityAndSubscription verifies AddCapabilityInternal and AddSubscriptionInternal.
func TestRefinement1_AddCapabilityAndSubscription(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	b.AddCapabilityInternal(&eks.Capability{ClusterName: "c1", CapabilityName: "cap1", Status: "ACTIVE"})
	b.AddSubscriptionInternal(&eks.AnywhereSubscription{ID: "sub1", Name: "s1", Status: "ACTIVE"})

	assert.Equal(t, 1, b.CapabilityCount())
	assert.Equal(t, 1, b.SubscriptionCount())
}

// TestRefinement1_ErrNilAppContext verifies ErrNilAppContext sentinel.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	require.Error(t, eks.ErrNilAppContext)
	assert.NotEmpty(t, eks.ErrNilAppContext.Error())
}

// TestRefinement1_ResetAfterNewOps verifies Reset clears new ops state.
func TestRefinement1_ResetAfterNewOps(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/addons", map[string]any{"addonName": "addon1"})
	doREST(t, h, http.MethodPost, "/clusters/c1/capabilities", map[string]any{
		"capabilityName":          "cap1",
		"type":                    "ARGOCD",
		"roleArn":                 "arn:aws:iam::123456789012:role/capability-role",
		"deletePropagationPolicy": "RETAIN",
	})
	doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions", map[string]any{"name": "sub1"})

	assert.Equal(t, 1, h.Backend.ClusterCount())
	assert.Equal(t, 1, h.Backend.AddonCount())
	assert.Equal(t, 1, h.Backend.CapabilityCount())
	assert.Equal(t, 1, h.Backend.SubscriptionCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ClusterCount())
	assert.Equal(t, 0, h.Backend.AddonCount())
	assert.Equal(t, 0, h.Backend.CapabilityCount())
	assert.Equal(t, 0, h.Backend.SubscriptionCount())
}

// TestRefinement1_AnywhereSubscriptionTypeNotStutter verifies the type rename.
func TestRefinement1_AnywhereSubscriptionTypeNotStutter(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	sub := &eks.AnywhereSubscription{
		ID:     "sub1",
		Name:   "test",
		Status: "ACTIVE",
	}

	b.AddSubscriptionInternal(sub)
	assert.Equal(t, 1, b.SubscriptionCount())
}

// TestRefinement1_TagResourceOnAddon verifies TagResource works on add-on ARNs.
func TestRefinement1_TagResourceOnAddon(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	addon, err := b.CreateAddon("c1", "coredns", "v1.0", "", "", "", nil)
	require.NoError(t, err)

	err = b.TagResource(addon.ARN, map[string]string{"k": "v"})
	require.NoError(t, err)

	t2, err := b.ListTagsForResource(addon.ARN)
	require.NoError(t, err)
	assert.Equal(t, "v", t2["k"])
}

// TestRefinement1_TagResourceOnFargateProfile verifies TagResource works on fargate profile ARNs.
func TestRefinement1_TagResourceOnFargateProfile(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	fp, err := b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/r", nil, nil, nil)
	require.NoError(t, err)

	err = b.TagResource(fp.ARN, map[string]string{"env": "staging"})
	require.NoError(t, err)

	t2, err := b.ListTagsForResource(fp.ARN)
	require.NoError(t, err)
	assert.Equal(t, "staging", t2["env"])
}

// TestRefinement1_TagResourceOnPodIdentity verifies TagResource on pod identity ARNs.
func TestRefinement1_TagResourceOnPodIdentity(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.CreatePodIdentityAssociation("c1", "default", "my-sa", "arn:aws:iam::123:role/r", nil)
	require.NoError(t, err)

	err = b.TagResource(assoc.ARN, map[string]string{"a": "b"})
	require.NoError(t, err)

	t2, err := b.ListTagsForResource(assoc.ARN)
	require.NoError(t, err)
	assert.Equal(t, "b", t2["a"])
}

// TestRefinement1_ParseAddonAndFargatePaths verifies HTTP routing for addons and fargate profiles.
func TestRefinement1_ParseAddonAndFargatePaths(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	// Create addon
	rec := doREST(t, h, http.MethodPost, "/clusters/c1/addons", map[string]any{"addonName": "coredns"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Create fargate profile
	rec = doREST(t, h, http.MethodPost, "/clusters/c1/fargate-profiles",
		map[string]any{"fargateProfileName": "fp1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 1, h.Backend.AddonCount())
	assert.Equal(t, 1, h.Backend.FargateProfileCount())
}

// TestRefinement1_ParseAssocPaths verifies HTTP routing for encrypt/IDP associate.
func TestRefinement1_ParseAssocPaths(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	// AssociateEncryptionConfig
	rec := doREST(t, h, http.MethodPost, "/clusters/c1/encryption-config/associate",
		map[string]any{"encryptionConfig": []any{}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// AssociateIdentityProviderConfig
	rec = doREST(t, h, http.MethodPost, "/clusters/c1/identity-provider-configs/associate",
		map[string]any{
			"oidc": map[string]any{
				"issuerUrl":  "https://example.com",
				"clientId":   "my-client",
				"configName": "my-idp",
			},
		})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_ResetWithTaggedResources verifies Reset closes all tags correctly.
func TestRefinement1_ResetWithTaggedResources(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, map[string]string{"a": "1"})
	require.NoError(t, err)

	_, err = b.CreateNodegroup(
		"c1", "ng1", "", "", "", "", "", nil, 1, 1, 2, eks.NodegroupInput{}, map[string]string{"b": "2"},
	)
	require.NoError(t, err)

	_, err = b.CreateAccessEntry("c1", "arn:aws:iam::123:role/r", "STANDARD", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAddon("c1", "coredns", "", "", "", "", map[string]string{"c": "3"})
	require.NoError(t, err)

	_, err = b.CreateFargateProfile("c1", "fp1", "", nil, nil, map[string]string{"d": "4"})
	require.NoError(t, err)

	// Reset should not panic even with tagged resources.
	b.Reset()

	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.FargateProfileCount())
}
