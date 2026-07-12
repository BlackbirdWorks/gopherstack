package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// ---- EKS Anywhere Subscription tests ----

func TestEKS_Subscription_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	// Create subscription
	rec := doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions", map[string]any{
		"name":            "my-sub",
		"licenseType":     "Cluster",
		"licenseQuantity": 5,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	subID, _ := resp["subscription"].(map[string]any)["id"].(string)
	require.NotEmpty(t, subID)

	// List subscriptions
	rec = doREST(t, h, http.MethodGet, "/eks-anywhere-subscriptions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe subscription
	rec = doREST(t, h, http.MethodGet, "/eks-anywhere-subscriptions/"+subID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe nonexistent subscription
	rec = doREST(t, h, http.MethodGet, "/eks-anywhere-subscriptions/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Update subscription (POST to the same leaf path, not PUT)
	qty := int32(10)
	rec = doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions/"+subID, map[string]any{
		"licenseType":     "Cluster",
		"licenseQuantity": qty,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update nonexistent
	rec = doREST(t, h, http.MethodPost, "/eks-anywhere-subscriptions/nonexistent", map[string]any{
		"licenseType": "Cluster",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Delete subscription
	rec = doREST(t, h, http.MethodDelete, "/eks-anywhere-subscriptions/"+subID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete nonexistent
	rec = doREST(t, h, http.MethodDelete, "/eks-anywhere-subscriptions/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Fargate Profile tests ----

func TestEKS_FargateProfile_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "fg-cluster"})

	// List fargate profiles (empty)
	rec := doREST(t, h, http.MethodGet, "/clusters/fg-cluster/fargate-profiles", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List for nonexistent cluster
	rec = doREST(t, h, http.MethodGet, "/clusters/nonexistent/fargate-profiles", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Describe nonexistent fargate profile
	rec = doREST(t, h, http.MethodGet, "/clusters/fg-cluster/fargate-profiles/my-profile", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Delete nonexistent fargate profile
	rec = doREST(t, h, http.MethodDelete, "/clusters/fg-cluster/fargate-profiles/my-profile", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Create fargate profile via backend, then test describe/delete via handler
	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	_, err := b.CreateCluster("fg-cluster2", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	fp, err := b.CreateFargateProfile(
		"fg-cluster2",
		"my-profile",
		"arn:aws:iam::123:role/fp",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "my-profile", fp.FargateProfileName)
	assert.NotEmpty(t, fp.ARN)

	// Describe and delete via backend directly
	described, err := b.DescribeFargateProfile("fg-cluster2", "my-profile")
	require.NoError(t, err)
	assert.Equal(t, "my-profile", described.FargateProfileName)

	names, err := b.ListFargateProfiles("fg-cluster2")
	require.NoError(t, err)
	assert.Contains(t, names, "my-profile")

	deleted, err := b.DeleteFargateProfile("fg-cluster2", "my-profile")
	require.NoError(t, err)
	assert.Equal(t, "my-profile", deleted.FargateProfileName)

	// List after delete
	names, err = b.ListFargateProfiles("fg-cluster2")
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestEKS_FargateProfile_Handler(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	// Create cluster first
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "fg-h-cluster"})

	// Create fargate profile directly in backend for handler to use
	h.Backend.Reset()
	_, err := h.Backend.CreateCluster("fg-h-cluster", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateFargateProfile(
		"fg-h-cluster",
		"test-fp",
		"arn:aws:iam::123:role/fp",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Describe via handler
	rec := doREST(t, h, http.MethodGet, "/clusters/fg-h-cluster/fargate-profiles/test-fp", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete via handler
	rec = doREST(t, h, http.MethodDelete, "/clusters/fg-h-cluster/fargate-profiles/test-fp", nil)
	require.Equal(t, http.StatusOK, rec.Code)
}

// ---- Pod Identity Association tests ----

func TestEKS_PodIdentityAssociation_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "pid-cluster"})

	// List pod identity associations (empty)
	rec := doREST(t, h, http.MethodGet, "/clusters/pid-cluster/pod-identity-associations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List for nonexistent cluster
	rec = doREST(t, h, http.MethodGet, "/clusters/nonexistent/pod-identity-associations", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Create association via backend
	assoc, err := h.Backend.CreatePodIdentityAssociation(
		"pid-cluster", "default", "my-sa", "arn:aws:iam::123:role/sa-role", nil,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)

	assocID := assoc.AssociationID

	// Describe via handler
	rec = doREST(
		t,
		h,
		http.MethodGet,
		"/clusters/pid-cluster/pod-identity-associations/"+assocID,
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe nonexistent
	rec = doREST(
		t,
		h,
		http.MethodGet,
		"/clusters/pid-cluster/pod-identity-associations/nonexistent",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Update via handler (POST, not PUT -- verified against the SDK serializer)
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/pid-cluster/pod-identity-associations/"+assocID,
		map[string]any{
			"roleArn": "arn:aws:iam::123:role/new-role",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Update nonexistent
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/pid-cluster/pod-identity-associations/nonexistent",
		map[string]any{
			"roleArn": "arn:aws:iam::123:role/new-role",
		},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Delete via handler
	rec = doREST(
		t,
		h,
		http.MethodDelete,
		"/clusters/pid-cluster/pod-identity-associations/"+assocID,
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete nonexistent
	rec = doREST(
		t,
		h,
		http.MethodDelete,
		"/clusters/pid-cluster/pod-identity-associations/nonexistent",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Access Entry tests (describe/list/update/disassociate) ----

func TestEKS_AccessEntry_DescribeListUpdate(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ac-cluster"})

	principalARN := "arn:aws:iam::123456789012:role/my-role"

	// Create access entry
	rec := doREST(t, h, http.MethodPost, "/clusters/ac-cluster/access-entries", map[string]any{
		"principalArn": principalARN,
		"type":         "STANDARD",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe access entry
	rec = doREST(t, h, http.MethodGet, "/clusters/ac-cluster/access-entries/"+principalARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe nonexistent
	rec = doREST(t, h, http.MethodGet, "/clusters/ac-cluster/access-entries/nonexistent-arn", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// List access entries
	rec = doREST(t, h, http.MethodGet, "/clusters/ac-cluster/access-entries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Update access entry (POST, not PUT -- verified against the SDK serializer)
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/ac-cluster/access-entries/"+principalARN,
		map[string]any{
			"username": "my-user",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// List access policies
	rec = doREST(t, h, http.MethodGet, "/access-policies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Associate access policy
	policyARN := "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/ac-cluster/access-entries/"+principalARN+"/access-policies",
		map[string]any{
			"policyArn": policyARN,
			"accessScope": map[string]any{
				"type": "cluster",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// List associated access policies
	rec = doREST(
		t,
		h,
		http.MethodGet,
		"/clusters/ac-cluster/access-entries/"+principalARN+"/access-policies",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Disassociate access policy
	rec = doREST(
		t,
		h,
		http.MethodDelete,
		"/clusters/ac-cluster/access-entries/"+principalARN+"/access-policies/"+policyARN,
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)
}

// ---- Identity Provider Config tests ----

func TestEKS_IdentityProviderConfig_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "idp-cluster"})

	// Associate IDP config via backend
	_, err := h.Backend.AssociateIdentityProviderConfig(
		"idp-cluster",
		"oidc",
		"my-oidc",
		map[string]string{"issuerUrl": "https://oidc.example.com"},
		nil,
	)
	require.NoError(t, err)

	// List IDP configs
	rec := doREST(t, h, http.MethodGet, "/clusters/idp-cluster/identity-provider-configs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List for nonexistent cluster
	rec = doREST(t, h, http.MethodGet, "/clusters/nonexistent/identity-provider-configs", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Describe IDP config
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/idp-cluster/identity-provider-configs/describe",
		map[string]any{
			"identityProviderConfig": map[string]any{
				"name": "my-oidc",
				"type": "oidc",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe nonexistent
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/idp-cluster/identity-provider-configs/describe",
		map[string]any{
			"identityProviderConfig": map[string]any{
				"name": "nonexistent",
				"type": "oidc",
			},
		},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Disassociate IDP config
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/idp-cluster/identity-provider-configs/disassociate",
		map[string]any{
			"identityProviderConfig": map[string]any{
				"name": "my-oidc",
				"type": "oidc",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)
}

// ---- Insights tests ----

func TestEKS_Insights_Lifecycle(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	_, err := b.CreateCluster("insight-cluster", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	// List insights
	insights, err := b.ListInsights("insight-cluster")
	require.NoError(t, err)
	assert.NotNil(t, insights)

	// List for nonexistent cluster
	_, err = b.ListInsights("nonexistent")
	require.Error(t, err)

	// Describe insight (use handler path)
	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ih-cluster"})

	// List via handler. ListInsights is POST (it carries an optional filter
	// body), not GET -- verified against the SDK serializer.
	rec := doREST(t, h, http.MethodPost, "/clusters/ih-cluster/insights", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List nonexistent cluster
	rec = doREST(t, h, http.MethodPost, "/clusters/nonexistent/insights", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Describe insight (synthetic - always succeeds for valid cluster)
	rec = doREST(t, h, http.MethodGet, "/clusters/ih-cluster/insights/some-id", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Start insights refresh. This is a cluster-level singleton at
	// /clusters/{name}/insights-refresh -- there is no per-refresh id in the
	// real API.
	rec = doREST(t, h, http.MethodPost, "/clusters/ih-cluster/insights-refresh", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	_, hasStatus := resp["status"]
	assert.True(t, hasStatus)

	// Describe insights refresh
	rec = doREST(t, h, http.MethodGet, "/clusters/ih-cluster/insights-refresh", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Cluster Update tests ----

func TestEKS_ClusterUpdates(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "update-cluster"})

	// Update cluster config (logging). Real path is POST
	// /clusters/{name}/update-config, not a bare-path PUT.
	rec := doREST(t, h, http.MethodPost, "/clusters/update-cluster/update-config", map[string]any{
		"logging": map[string]any{
			"clusterLogging": []map[string]any{
				{"types": []string{"api", "audit"}, "enabled": true},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update nonexistent cluster config
	rec = doREST(t, h, http.MethodPost, "/clusters/nonexistent/update-config", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Update cluster version. Real path is POST /clusters/{name}/updates
	// (shared with ListUpdates' GET), not "/update-version".
	rec = doREST(t, h, http.MethodPost, "/clusters/update-cluster/updates", map[string]any{
		"version": "1.33",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update cluster version for nonexistent cluster
	rec = doREST(t, h, http.MethodPost, "/clusters/nonexistent/updates", map[string]any{
		"version": "1.33",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// List updates
	rec = doREST(t, h, http.MethodGet, "/clusters/update-cluster/updates", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List updates for nonexistent cluster
	rec = doREST(t, h, http.MethodGet, "/clusters/nonexistent/updates", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Describe cluster versions
	rec = doREST(t, h, http.MethodGet, "/cluster-versions", nil)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestEKS_NodegroupVersionUpdate(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ng-upd-cluster"})
	doREST(t, h, http.MethodPost, "/clusters/ng-upd-cluster/node-groups", map[string]any{
		"nodegroupName": "my-ng",
		"nodeRole":      "arn:aws:iam::123:role/ng",
		"subnets":       []string{"subnet-1"},
	})

	// Update nodegroup version
	rec := doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/ng-upd-cluster/node-groups/my-ng/update-version",
		map[string]any{
			"version": "1.33",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	updateID, _ := resp["update"].(map[string]any)["id"].(string)
	require.NotEmpty(t, updateID)

	// Describe the update
	rec = doREST(t, h, http.MethodGet, "/clusters/ng-upd-cluster/updates/"+updateID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeUpdate returns 404 for an unknown update ID.
	rec = doREST(t, h, http.MethodGet, "/clusters/ng-upd-cluster/updates/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Register/Deregister cluster tests ----

func TestEKS_RegisterDeregisterCluster(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	// Register cluster. Real path is global POST /cluster-registrations (no
	// cluster name in the URI -- Name comes from the body).
	rec := doREST(t, h, http.MethodPost, "/cluster-registrations", map[string]any{
		"name":                    "my-ext-cluster",
		"connectorConfigProvider": "EKS_ANYWHERE",
		"connectorConfigRoleArn":  "arn:aws:iam::123:role/connector",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Register cluster missing name
	rec = doREST(t, h, http.MethodPost, "/cluster-registrations", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Deregister cluster. Real path is DELETE /cluster-registrations/{name}.
	rec = doREST(t, h, http.MethodDelete, "/cluster-registrations/my-ext-cluster", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Deregister nonexistent cluster
	rec = doREST(t, h, http.MethodDelete, "/cluster-registrations/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Persistence coverage ----

func TestEKS_Persistence_SubscriptionAndFargate(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateEksAnywhereSubscription("my-sub", 3, "Cluster", nil)
	require.NoError(t, err)

	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePodIdentityAssociation("c1", "default", "sa1", "arn:aws:iam::123:role/sa", nil)
	require.NoError(t, err)

	// Snapshot and restore
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	subs := b2.ListEksAnywhereSubscriptions()
	assert.Len(t, subs, 1)

	names, err := b2.ListFargateProfiles("c1")
	require.NoError(t, err)
	assert.Len(t, names, 1)

	podAssocs, err := b2.ListPodIdentityAssociations("c1")
	require.NoError(t, err)
	assert.Len(t, podAssocs, 1)
}
