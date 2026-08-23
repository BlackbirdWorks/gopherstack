package eks_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

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
					"issuerUrl":                  "https://oidc.example.com",
					"clientId":                   "my-client",
					"identityProviderConfigName": "my-oidc",
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
			name: "associate_idp_config_missing_config_name",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"oidc": map[string]any{"issuerUrl": "https://oidc.example.com", "clientId": "my-client"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "associate_idp_config_cluster_not_found",
			body: map[string]any{
				"oidc": map[string]any{
					"issuerUrl": "https://x.com", "clientId": "c", "identityProviderConfigName": "c",
				},
			},
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
						"oidc": map[string]any{
							"issuerUrl": "https://oidc.example.com", "clientId": "dup-client",
							"identityProviderConfigName": "dup-client",
						},
					},
				)
			},
			body: map[string]any{
				"oidc": map[string]any{
					"issuerUrl": "https://oidc.example.com", "clientId": "dup-client",
					"identityProviderConfigName": "dup-client",
				},
			},
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

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/identity-provider-configs/associate", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestOIDC_AssociateAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "oidc-cluster")

	params := map[string]string{
		"issuerUrl":     "https://oidc.example.com",
		"clientId":      "my-client-id",
		"usernameClaim": "email",
	}
	cfg, err := b.AssociateIdentityProviderConfig("oidc-cluster", "oidc", "my-client-id", params, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "oidc", cfg.Type)
	assert.Equal(t, "my-client-id", cfg.Name)

	described, err := b.DescribeIdentityProviderConfig("oidc-cluster", "my-client-id")
	require.NoError(t, err)
	assert.Equal(t, cfg.Name, described.Name)
}

func TestOIDC_List(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "oidc-list-cluster")

	_, _ = b.AssociateIdentityProviderConfig("oidc-list-cluster", "oidc", "client-a",
		map[string]string{"clientId": "client-a"}, nil, nil)

	configs, err := b.ListIdentityProviderConfigs("oidc-list-cluster")
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "client-a", configs[0]["name"])
}

func TestOIDC_Disassociate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "oidc-del-cluster")

	_, _ = b.AssociateIdentityProviderConfig("oidc-del-cluster", "oidc", "client-b",
		map[string]string{"clientId": "client-b"}, nil, nil)

	err := b.DisassociateIdentityProviderConfig("oidc-del-cluster", "client-b")
	require.NoError(t, err)

	configs, err := b.ListIdentityProviderConfigs("oidc-del-cluster")
	require.NoError(t, err)
	assert.Empty(t, configs)
}

func TestIDPConfigCreatesAsCreating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "idp_config_starts_creating"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateCluster(
				"cl", "1.32", "arn:aws:iam::123456789012:role/role", nil, nil, nil,
			)
			require.NoError(t, err)

			cfg, err := b.AssociateIdentityProviderConfig(
				"cl", "oidc", "my-idp",
				map[string]string{"issuerUrl": "https://example.com", "clientId": "client1"},
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, "CREATING", cfg.Status, tc.name)

			// The immediate CREATING status is right, but a machine that never
			// advances would pass that assertion forever -- confirm it
			// actually reaches ACTIVE too.
			require.Eventually(t, func() bool {
				got, descErr := b.DescribeIdentityProviderConfig("cl", "my-idp")

				return descErr == nil && got.Status == "ACTIVE"
			}, 2*time.Second, 10*time.Millisecond, tc.name)
		})
	}
}

func TestAssociateIDPUsesConfigName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
	}{
		{name: "uses_identityProviderConfigName", configName: "my-oidc-provider"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{
				"name":    "cl",
				"version": "1.32",
				"roleArn": "arn:aws:iam::123456789012:role/role",
			})

			rec := doREST(t, h, http.MethodPost, "/clusters/cl/identity-provider-configs/associate", map[string]any{
				"oidc": map[string]any{
					"identityProviderConfigName": tc.configName,
					"clientId":                   "my-client",
					"issuerUrl":                  "https://issuer.example.com",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			listRec := doREST(t, h, http.MethodGet, "/clusters/cl/identity-provider-configs", nil)
			require.Equal(t, http.StatusOK, listRec.Code, tc.name)

			listResp := parseResp(t, listRec)
			idpConfigs, ok := listResp["identityProviderConfigs"].([]any)
			require.True(t, ok, tc.name)
			require.Len(t, idpConfigs, 1, tc.name)

			entry, ok := idpConfigs[0].(map[string]any)
			require.True(t, ok, tc.name)
			assert.Equal(t, tc.configName, entry["name"], tc.name)
		})
	}
}

// TestDescribeIdentityProviderConfig_WireShape verifies DescribeIdentityProviderConfig
// nests the full OIDC config under an "oidc" key -- verified against
// aws-sdk-go-v2/service/eks/types.IdentityProviderConfigResponse, which wraps
// OidcIdentityProviderConfig rather than returning a flat object.
func TestDescribeIdentityProviderConfig_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "idp-shape-cluster"})

	assocRec := doREST(t, h, http.MethodPost, "/clusters/idp-shape-cluster/identity-provider-configs/associate",
		map[string]any{
			"oidc": map[string]any{
				"identityProviderConfigName": "shape-idp",
				"clientId":                   "shape-client",
				"issuerUrl":                  "https://issuer.example.com",
				"groupsPrefix":               "oidc:",
				"usernamePrefix":             "oidc:",
				"requiredClaims":             map[string]string{"aud": "shape-client"},
			},
			"tags": map[string]string{"env": "test"},
		})
	require.Equal(t, http.StatusOK, assocRec.Code)

	rec := doREST(t, h, http.MethodPost, "/clusters/idp-shape-cluster/identity-provider-configs/describe",
		map[string]any{
			"identityProviderConfig": map[string]any{"name": "shape-idp", "type": "oidc"},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	top, ok := resp["identityProviderConfig"].(map[string]any)
	require.True(t, ok, "response must have top-level identityProviderConfig")

	// Real shape nests everything under "oidc" -- there is no flat
	// name/type/status/clusterName at this level.
	assert.NotContains(t, top, "name")
	assert.NotContains(t, top, "status")

	oidc, ok := top["oidc"].(map[string]any)
	require.True(t, ok, "must nest the config under an oidc key")

	assert.Equal(t, "shape-idp", oidc["identityProviderConfigName"])
	assert.NotEmpty(t, oidc["identityProviderConfigArn"])
	assert.Equal(t, "idp-shape-cluster", oidc["clusterName"])
	assert.Equal(t, "shape-client", oidc["clientId"])
	assert.Equal(t, "https://issuer.example.com", oidc["issuerUrl"])
	assert.Equal(t, "oidc:", oidc["groupsPrefix"])
	assert.Equal(t, "oidc:", oidc["usernamePrefix"])
	assert.NotEmpty(t, oidc["status"])

	requiredClaims, ok := oidc["requiredClaims"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "shape-client", requiredClaims["aud"])

	tags, ok := oidc["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test", tags["env"])
}
