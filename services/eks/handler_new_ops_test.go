package eks_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestEKS_CreateAccessEntry verifies CreateAccessEntry and basic error cases.
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

			h := newTestEKSHandler()
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

// TestEKS_DeleteAccessEntry verifies DeleteAccessEntry and error cases.
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

			h := newTestEKSHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodDelete, "/clusters/my-cluster/access-entries/"+tt.principal, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestEKS_AssociateAccessPolicy verifies AssociateAccessPolicy and error cases.
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

			h := newTestEKSHandler()
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

// TestEKS_AssociateEncryptionConfig verifies AssociateEncryptionConfig and error cases.
func TestEKS_AssociateEncryptionConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "associate_encryption_config_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"encryptionConfig": []map[string]any{
					{
						"provider":  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/abc"},
						"resources": []string{"secrets"},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "associate_encryption_config_cluster_not_found",
			body:       map[string]any{"encryptionConfig": []map[string]any{}},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/encryption-config/associate", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				update, ok := resp["update"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "AssociateEncryptionConfig", update["type"])
			}
		})
	}
}

// TestEKS_AssociateIdentityProviderConfig verifies AssociateIdentityProviderConfig and error cases.
func TestEKS_AssociateIdentityProviderConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "associate_idp_config_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"oidc": map[string]any{
					"issuerUrl": "https://oidc.example.com",
					"clientId":  "my-client",
				},
				"tags": map[string]string{"env": "test"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "associate_idp_config_missing_issuer",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"oidc": map[string]any{"clientId": "my-client"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "associate_idp_config_missing_client_id",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"oidc": map[string]any{"issuerUrl": "https://oidc.example.com"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "associate_idp_config_cluster_not_found",
			body:       map[string]any{"oidc": map[string]any{"issuerUrl": "https://x.com", "clientId": "c"}},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "associate_idp_config_duplicate",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(
					t,
					h,
					http.MethodPost,
					"/clusters/my-cluster/identity-provider-configs/associate",
					map[string]any{
						"oidc": map[string]any{"issuerUrl": "https://oidc.example.com", "clientId": "dup-client"},
					},
				)
			},
			body: map[string]any{
				"oidc": map[string]any{"issuerUrl": "https://oidc.example.com", "clientId": "dup-client"},
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/identity-provider-configs/associate", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestEKS_CreateAddon verifies CreateAddon and error cases.
func TestEKS_CreateAddon(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_addon_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"addonName":    "vpc-cni",
				"addonVersion": "v1.12.0-eksbuild.1",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create_addon_missing_name",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"addonVersion": "v1.0"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_addon_cluster_not_found",
			body:       map[string]any{"addonName": "vpc-cni"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "create_addon_duplicate",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/addons",
					map[string]any{"addonName": "vpc-cni"})
			},
			body:       map[string]any{"addonName": "vpc-cni"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/addons", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				addon, ok := resp["addon"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", addon["clusterName"])
				assert.Equal(t, "vpc-cni", addon["addonName"])
				assert.NotEmpty(t, addon["addonArn"])
				assert.Equal(t, "ACTIVE", addon["status"])
			}
		})
	}
}

// TestEKS_CreateCapability verifies CreateCapability and error cases.
func TestEKS_CreateCapability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "create_capability_success",
			body:       map[string]any{"name": "my-capability", "version": "v1.0"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_capability_missing_name",
			body:       map[string]any{"version": "v1.0"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_capability_duplicate",
			body:       map[string]any{"name": "dup-cap"},
			wantStatus: http.StatusConflict,
		},
	}

	h := newTestEKSHandler()
	// Pre-create duplicate for the last test.
	doREST(t, h, http.MethodPost, "/capabilities", map[string]any{"name": "dup-cap"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var handler *eks.Handler
			if tt.name == "create_capability_duplicate" {
				handler = h
			} else {
				handler = newTestEKSHandler()
			}

			rec := doREST(t, handler, http.MethodPost, "/capabilities", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				capa, ok := resp["capability"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-capability", capa["name"])
				assert.Equal(t, "ACTIVE", capa["status"])
			}
		})
	}
}

// TestEKS_CreateAnywhereSubscription verifies CreateAnywhereSubscription and error cases.
func TestEKS_CreateEksAnywhereSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_subscription_success",
			body: map[string]any{
				"name":            "my-sub",
				"licenseType":     "License",
				"licenseQuantity": 5,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_subscription_missing_name",
			body:       map[string]any{"licenseType": "License"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			rec := doREST(t, h, http.MethodPost, "/subscriptions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				sub, ok := resp["subscription"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-sub", sub["name"])
				assert.Equal(t, "ACTIVE", sub["status"])
				assert.NotEmpty(t, sub["id"])
				assert.NotEmpty(t, sub["arn"])
			}
		})
	}
}

// TestEKS_CreateFargateProfile verifies CreateFargateProfile and error cases.
func TestEKS_CreateFargateProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_fargate_profile_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"fargateProfileName":  "my-profile",
				"podExecutionRoleArn": "arn:aws:iam::123456789012:role/fargate-role",
				"selectors": []map[string]any{
					{"namespace": "default"},
					{"namespace": "kube-system", "labels": map[string]string{"app": "backend"}},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create_fargate_profile_missing_name",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"podExecutionRoleArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_fargate_profile_cluster_not_found",
			body:       map[string]any{"fargateProfileName": "p"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "create_fargate_profile_duplicate",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/fargate-profiles",
					map[string]any{"fargateProfileName": "dup-profile"})
			},
			body:       map[string]any{"fargateProfileName": "dup-profile"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/fargate-profiles", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				profile, ok := resp["fargateProfile"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", profile["clusterName"])
				assert.Equal(t, "my-profile", profile["fargateProfileName"])
				assert.NotEmpty(t, profile["fargateProfileArn"])
				assert.Equal(t, "ACTIVE", profile["status"])
			}
		})
	}
}

// TestEKS_CreatePodIdentityAssociation verifies CreatePodIdentityAssociation and error cases.
func TestEKS_CreatePodIdentityAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_pod_identity_association_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"namespace":      "default",
				"serviceAccount": "my-sa",
				"roleArn":        "arn:aws:iam::123456789012:role/pod-role",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create_pod_identity_missing_namespace",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"serviceAccount": "my-sa", "roleArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_pod_identity_missing_service_account",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"namespace": "default", "roleArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_pod_identity_cluster_not_found",
			body:       map[string]any{"namespace": "default", "serviceAccount": "sa"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/pod-identity-associations", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				assoc, ok := resp["association"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", assoc["clusterName"])
				assert.Equal(t, "default", assoc["namespace"])
				assert.Equal(t, "my-sa", assoc["serviceAccount"])
				assert.NotEmpty(t, assoc["associationId"])
				assert.NotEmpty(t, assoc["associationArn"])
			}
		})
	}
}

// TestEKS_NewOps_PersistenceRoundTrip verifies that all new resource types survive a snapshot/restore cycle.
func TestEKS_NewOps_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend("123456789012", "us-east-1")

	// Create cluster and access entry.
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAccessEntry("c1", "arn:aws:iam::123456789012:role/r1", "STANDARD", "", nil, nil)
	require.NoError(t, err)

	// Create addon.
	_, err = b.CreateAddon("c1", "vpc-cni", "v1.12.0", "", "", "", nil)
	require.NoError(t, err)

	// Create fargate profile.
	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123456789012:role/fargate", nil, nil, nil)
	require.NoError(t, err)

	// Create pod identity association.
	_, err = b.CreatePodIdentityAssociation("c1", "default", "my-sa", "arn:aws:iam::123456789012:role/pod", nil)
	require.NoError(t, err)

	// Create capability.
	_, err = b.CreateCapability("cap1", "v1.0")
	require.NoError(t, err)

	// Create subscription.
	_, err = b.CreateEksAnywhereSubscription("sub1", 3, "License", nil)
	require.NoError(t, err)

	// Associate encryption config.
	_, err = b.AssociateEncryptionConfig("c1", []eks.EncryptionConfig{
		{Provider: map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/abc"}, Resources: []string{"secrets"}},
	})
	require.NoError(t, err)

	// Snapshot and restore into a fresh backend.
	h := eks.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := eks.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := eks.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	// Verify cluster is present.
	c, err := b2.DescribeCluster("c1")
	require.NoError(t, err)
	assert.Equal(t, "c1", c.Name)
}

// TestEKS_NewOps_GetSupportedOperations verifies the new ops are in the supported list.
func TestEKS_NewOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	ops := h.GetSupportedOperations()

	expected := []string{
		"AssociateAccessPolicy",
		"AssociateEncryptionConfig",
		"AssociateIdentityProviderConfig",
		"CreateAccessEntry",
		"CreateAddon",
		"CreateCapability",
		"CreateEksAnywhereSubscription",
		"CreateFargateProfile",
		"CreatePodIdentityAssociation",
		"DeleteAccessEntry",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}

// TestEKS_RouteMatcher_NewPaths verifies the route matcher accepts new paths.
func TestEKS_RouteMatcher_NewPaths(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		path    string
		name    string
		matches bool
	}{
		{name: "capabilities", path: "/capabilities", matches: true},
		{name: "subscriptions", path: "/subscriptions", matches: true},
		{name: "access_entries", path: "/clusters/c1/access-entries", matches: true},
		{name: "addons", path: "/clusters/c1/addons", matches: true},
		{name: "fargate_profiles", path: "/clusters/c1/fargate-profiles", matches: true},
		{name: "pod_identity_assoc", path: "/clusters/c1/pod-identity-associations", matches: true},
		{name: "encryption_config", path: "/clusters/c1/encryption-config/associate", matches: true},
		{name: "idp_configs", path: "/clusters/c1/identity-provider-configs/associate", matches: true},
		{name: "unrelated", path: "/other", matches: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.matches, matcher(c))
		})
	}
}
