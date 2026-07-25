package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

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

func TestAssociateAccessPolicy_Dedup(t *testing.T) {
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

func TestEKS_CreateAccessEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_access_entry_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"principalArn": "arn:aws:iam::123456789012:role/my-role",
				"type":         "STANDARD",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create_access_entry_missing_principal_arn",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"type": "STANDARD"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_access_entry_cluster_not_found",
			body:       map[string]any{"principalArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "create_access_entry_duplicate",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/access-entries",
					map[string]any{"principalArn": "arn:aws:iam::123456789012:role/r"})
			},
			body:       map[string]any{"principalArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/access-entries", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				entry, ok := resp["accessEntry"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", entry["clusterName"])
				assert.NotEmpty(t, entry["accessEntryArn"])
				assert.Equal(t, "STANDARD", entry["type"])
			}
		})
	}
}

func TestEKS_DeleteAccessEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		name       string
		principal  string
		wantStatus int
	}{
		{
			name: "delete_access_entry_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/access-entries",
					map[string]any{"principalArn": "arn:aws:iam::123456789012:role/my-role"})
			},
			principal:  "arn:aws:iam::123456789012:role/my-role",
			wantStatus: http.StatusOK,
		},
		{
			name: "delete_access_entry_not_found",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			principal:  "arn:aws:iam::123456789012:role/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_access_entry_cluster_not_found",
			principal:  "arn:aws:iam::123456789012:role/r",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodDelete, "/clusters/my-cluster/access-entries/"+tt.principal, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestEKS_AssociateAccessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		principal  string
		wantStatus int
	}{
		{
			name: "associate_access_policy_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/access-entries",
					map[string]any{"principalArn": "arn:aws:iam::123456789012:role/my-role"})
			},
			principal: "arn:aws:iam::123456789012:role/my-role",
			body: map[string]any{
				"policyArn":   "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy",
				"accessScope": map[string]any{"type": "cluster"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "associate_access_policy_missing_policy_arn",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/access-entries",
					map[string]any{"principalArn": "arn:aws:iam::123456789012:role/my-role"})
			},
			principal:  "arn:aws:iam::123456789012:role/my-role",
			body:       map[string]any{"accessScope": map[string]any{"type": "cluster"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "associate_access_policy_entry_not_found",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			principal: "arn:aws:iam::123456789012:role/nonexistent",
			body: map[string]any{
				"policyArn": "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			path := "/clusters/my-cluster/access-entries/" + tt.principal + "/access-policies"
			rec := doREST(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				pol, ok := resp["associatedAccessPolicy"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", pol["clusterName"])
				assert.NotEmpty(t, pol["policyArn"])
			}
		})
	}
}

func TestRBAC_CreateDescribeList(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
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

func TestRBAC_AssociatePolicy(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
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

func TestRBAC_ListPolicies(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	policies := b.ListAccessPolicies()
	assert.NotEmpty(t, policies, "ListAccessPolicies must return built-in policies")

	for _, p := range policies {
		// Real wire key is "arn", not "policyArn" -- verified against
		// aws-sdk-go-v2/service/eks/types.AccessPolicy (distinct from
		// AssociatedAccessPolicy's "policyArn" key used elsewhere).
		assert.NotEmpty(t, p["arn"], "each policy must have an arn")
		assert.NotEmpty(t, p["name"], "each policy must have a name")
	}
}

func TestRBAC_DisassociatePolicy(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
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

func TestAccessEntry_KubernetesGroups_Create(t *testing.T) {
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

			h := newTestEKSHandler(t)
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

func TestAccessEntry_KubernetesGroups_Update(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ae-upd-cluster"})
	doREST(t, h, http.MethodPost, "/clusters/ae-upd-cluster/access-entries", map[string]any{
		"principalArn": "arn:aws:iam::123456789012:role/my-role",
	})

	rec := doREST(
		t, h, http.MethodPost,
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

func TestAccessEntry_KubernetesGroups_Backend_DirectCreate(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
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

func TestAccessEntry_KubernetesGroups_Backend_Update(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
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

func TestAccessEntry_KubernetesGroups_AlwaysPresent(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
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

// TestAccessEntry_ModifiedAt verifies the "modifiedAt" field -- present on
// the real aws-sdk-go-v2/service/eks/types.AccessEntry but previously absent
// from gopherstack's model -- is populated on both create and update.
func TestAccessEntry_ModifiedAt(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ae-modified-cluster"})

	principalARN := "arn:aws:iam::123456789012:role/r1"
	createRec := doREST(t, h, http.MethodPost, "/clusters/ae-modified-cluster/access-entries", map[string]any{
		"principalArn": principalARN,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	created := parseResp(t, createRec)["accessEntry"].(map[string]any)
	createdModifiedAt, ok := created["modifiedAt"].(float64)
	require.True(t, ok, "modifiedAt must be present after create")
	assert.Positive(t, createdModifiedAt)

	updateRec := doREST(t, h, http.MethodPost, "/clusters/ae-modified-cluster/access-entries/"+principalARN,
		map[string]any{"username": "new-username"})
	require.Equal(t, http.StatusOK, updateRec.Code)

	updated := parseResp(t, updateRec)["accessEntry"].(map[string]any)
	updatedModifiedAt, ok := updated["modifiedAt"].(float64)
	require.True(t, ok, "modifiedAt must be present after update")
	assert.GreaterOrEqual(t, updatedModifiedAt, createdModifiedAt)
}
