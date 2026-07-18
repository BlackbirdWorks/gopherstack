package eks_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// --- Shared test helpers (used across the eks_test package) ---

func newTestEKSHandler(t *testing.T) *eks.Handler {
	t.Helper()
	backend := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	return eks.NewHandler(backend)
}

func newBackend(t *testing.T) *eks.InMemoryBackend {
	t.Helper()

	return eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
}

func newHandlerAndBackend(t *testing.T) (*eks.Handler, *eks.InMemoryBackend) {
	t.Helper()
	b := newBackend(t)

	return eks.NewHandler(b), b
}

func doREST(
	t *testing.T,
	h *eks.Handler,
	method, path string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func parseClusterResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	m := parseResp(t, rec)
	cluster, ok := m["cluster"].(map[string]any)
	require.True(t, ok, "response must have 'cluster' key")

	return cluster
}

func parseNodegroupResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	m := parseResp(t, rec)
	ng, ok := m["nodegroup"].(map[string]any)
	require.True(t, ok, "response must have 'nodegroup' key")

	return ng
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

// --- Core handler metadata / routing tests ---

// TestEKSHandlerMetadata checks Handler name and supported operations.
func TestEKSHandlerMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "service_name", want: "EKS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestEKSHandler(t)
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

// TestEKSHandlerChaosAndMetrics covers ChaosServiceName, ChaosOperations, ChaosRegions,
// MatchPriority, GetSupportedOperations, and ExtractOperation/ExtractResource.
func TestEKSHandlerChaosAndMetrics(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	t.Run("chaos_service_name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "eks", h.ChaosServiceName())
	})

	t.Run("chaos_operations", func(t *testing.T) {
		t.Parallel()
		ops := h.ChaosOperations()
		assert.NotEmpty(t, ops)
		assert.Contains(t, ops, "CreateCluster")
	})

	t.Run("chaos_regions", func(t *testing.T) {
		t.Parallel()
		regions := h.ChaosRegions()
		assert.Len(t, regions, 1)
	})

	t.Run("match_priority", func(t *testing.T) {
		t.Parallel()
		assert.Positive(t, h.MatchPriority())
	})

	t.Run("supported_operations", func(t *testing.T) {
		t.Parallel()
		ops := h.GetSupportedOperations()
		for _, op := range []string{
			"CreateCluster",
			"DeleteCluster",
			"CreateNodegroup",
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
		} {
			assert.Contains(t, ops, op)
		}
	})

	t.Run("extract_operation_cluster", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/clusters", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "CreateCluster", h.ExtractOperation(c))
	})

	t.Run("extract_resource_cluster_name", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/clusters/my-cluster", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "my-cluster", h.ExtractResource(c))
	})

	t.Run("extract_resource_arn", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/tags/arn:aws:eks:us-east-1:123:cluster/foo", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.NotEmpty(t, h.ExtractResource(c))
	})
}

// TestEKSErrorPaths exercises error responses for various invalid operations.
func TestEKSErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *eks.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "delete_cluster_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/clusters/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_nodegroups_cluster_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/clusters/nonexistent/node-groups", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_nodegroup_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				rec := doREST(t, h, http.MethodDelete, "/clusters/my-cluster/node-groups/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_nodegroup_cluster_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/clusters/nonexistent/node-groups/ng", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_nodegroup_missing_name",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/node-groups", map[string]any{
					"scalingConfig": map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "tag_resource_invalid_body",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/tags/arn:aws:eks:us-east-1:123:cluster/x",
					strings.NewReader("not-json"))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "tag_resource_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/tags/arn:aws:eks:us-east-1:123:cluster/nonexistent",
					map[string]any{"tags": map[string]string{}})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "untag_resource_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete,
					"/tags/arn:aws:eks:us-east-1:123:cluster/nonexistent?tagKeys=Env",
					nil)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_capability_missing_name_is_validation_error",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})
				// CreateCapability with no capabilityName -> ErrValidation -> 400.
				rec := doREST(t, h, http.MethodPost, "/clusters/c1/capabilities", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.ops(t, newTestEKSHandler(t))
		})
	}
}

// TestEKSRouteMatcher verifies the route matcher accepts EKS paths.
func TestEKSRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		path    string
		method  string
		name    string
		matches bool
	}{
		{name: "clusters_root", path: "/clusters", method: http.MethodGet, matches: true},
		{name: "clusters_post", path: "/clusters", method: http.MethodPost, matches: true},
		{name: "cluster_by_name", path: "/clusters/my-cluster", method: http.MethodGet, matches: true},
		{name: "node_groups", path: "/clusters/my-cluster/node-groups", method: http.MethodGet, matches: true},
		{
			name:    "eks_tags",
			path:    "/tags/arn:aws:eks:us-east-1:123456789012:cluster/test",
			method:  http.MethodGet,
			matches: true,
		},
		{
			name:    "non_eks_tags",
			path:    "/tags/arn:aws:backup:us-east-1:123:vault/v",
			method:  http.MethodGet,
			matches: false,
		},
		{name: "unrelated", path: "/backup-vaults", method: http.MethodGet, matches: false},
		{name: "capabilities", path: "/clusters/c1/capabilities", method: http.MethodPost, matches: true},
		{name: "subscriptions", path: "/eks-anywhere-subscriptions", method: http.MethodPost, matches: true},
		{name: "cluster_registrations", path: "/cluster-registrations", method: http.MethodPost, matches: true},
		{
			name:    "cluster_registrations_named",
			path:    "/cluster-registrations/c1",
			method:  http.MethodPost,
			matches: true,
		},
		{
			name:    "addon_supported_versions",
			path:    "/addons/supported-versions",
			method:  http.MethodPost,
			matches: true,
		},
		{
			name:    "addon_configuration_schemas",
			path:    "/addons/configuration-schemas",
			method:  http.MethodPost,
			matches: true,
		},
		{name: "access_entries", path: "/clusters/c1/access-entries", method: http.MethodPost, matches: true},
		{name: "addons", path: "/clusters/c1/addons", method: http.MethodPost, matches: true},
		{name: "fargate_profiles", path: "/clusters/c1/fargate-profiles", method: http.MethodPost, matches: true},
		{
			name:    "pod_identity_assoc",
			path:    "/clusters/c1/pod-identity-associations",
			method:  http.MethodPost,
			matches: true,
		},
		{
			name:    "encryption_config",
			path:    "/clusters/c1/encryption-config/associate",
			method:  http.MethodPost,
			matches: true,
		},
		{
			name:    "idp_configs",
			path:    "/clusters/c1/identity-provider-configs/associate",
			method:  http.MethodPost,
			matches: true,
		},
		{
			name:    "old_subscriptions_path_no_longer_matches",
			path:    "/subscriptions",
			method:  http.MethodPost,
			matches: false,
		},
		{
			name:    "old_capabilities_path_no_longer_matches",
			path:    "/capabilities",
			method:  http.MethodPost,
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.matches, matcher(c))
		})
	}
}

// TestParseAddonAndFargatePaths verifies HTTP routing for addons and fargate profiles.
func TestParseAddonAndFargatePaths(t *testing.T) {
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

// TestParseAssocPaths verifies HTTP routing for encryption-config/IDP associate.
func TestParseAssocPaths(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	// AssociateEncryptionConfig
	rec := doREST(t, h, http.MethodPost, "/clusters/c1/encryption-config/associate",
		map[string]any{"encryptionConfig": []any{}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// AssociateIdentityProviderConfig
	rec = doREST(t, h, http.MethodPost, "/clusters/c1/identity-provider-configs/associate", map[string]any{
		"oidc": map[string]any{
			"issuerUrl":  "https://example.com",
			"clientId":   "my-client",
			"configName": "my-idp",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
