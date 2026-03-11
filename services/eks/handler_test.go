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
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func newTestEKSHandler() *eks.Handler {
	backend := eks.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return eks.NewHandler(backend)
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

// TestEKSClusterCRUD exercises CreateCluster, DescribeCluster, ListClusters, and DeleteCluster.
func TestEKSClusterCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *eks.Handler)
		name string
	}{
		{
			name: "create_cluster",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
					"name":    "my-cluster",
					"version": "1.32",
					"roleArn": "arn:aws:iam::123456789012:role/eks-role",
					"tags":    map[string]string{"Env": "test"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				cluster, ok := resp["cluster"].(map[string]any)
				require.True(t, ok, "response should have cluster key")
				assert.Equal(t, "my-cluster", cluster["name"])
				assert.NotEmpty(t, cluster["arn"])
				assert.Equal(t, "ACTIVE", cluster["status"])
				assert.Equal(t, "1.32", cluster["version"])
			},
		},
		{
			name: "describe_cluster",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				rec := doREST(t, h, http.MethodGet, "/clusters/my-cluster", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				cluster, ok := resp["cluster"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", cluster["name"])
			},
		},
		{
			name: "describe_cluster_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/clusters/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_clusters",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "cluster-a"})
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "cluster-b"})
				rec := doREST(t, h, http.MethodGet, "/clusters", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				names, ok := resp["clusters"].([]any)
				require.True(t, ok, "clusters key should be a list")
				assert.Len(t, names, 2)
			},
		},
		{
			name: "delete_cluster",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "to-delete"})
				rec := doREST(t, h, http.MethodDelete, "/clusters/to-delete", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				// verify it is gone
				rec2 := doREST(t, h, http.MethodGet, "/clusters/to-delete", nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "create_cluster_duplicate",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "dup-cluster"})
				rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "dup-cluster"})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "create_cluster_missing_name",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{"version": "1.32"})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.ops(t, newTestEKSHandler())
		})
	}
}

// TestEKSNodegroupCRUD exercises CreateNodegroup, DescribeNodegroup, ListNodegroups, and DeleteNodegroup.
func TestEKSNodegroupCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *eks.Handler)
		name string
	}{
		{
			name: "create_nodegroup",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/node-groups", map[string]any{
					"nodegroupName": "my-ng",
					"nodeRole":      "arn:aws:iam::123456789012:role/ng-role",
					"scalingConfig": map[string]any{"desiredSize": 2, "minSize": 1, "maxSize": 5},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				ng, ok := resp["nodegroup"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-ng", ng["nodegroupName"])
				assert.Equal(t, "ACTIVE", ng["status"])
			},
		},
		{
			name: "describe_nodegroup",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/node-groups", map[string]any{
					"nodegroupName": "my-ng",
					"scalingConfig": map[string]any{},
				})
				rec := doREST(t, h, http.MethodGet, "/clusters/my-cluster/node-groups/my-ng", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				ng, ok := resp["nodegroup"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-ng", ng["nodegroupName"])
			},
		},
		{
			name: "list_nodegroups",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/node-groups", map[string]any{
					"nodegroupName": "ng-1",
					"scalingConfig": map[string]any{},
				})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/node-groups", map[string]any{
					"nodegroupName": "ng-2",
					"scalingConfig": map[string]any{},
				})
				rec := doREST(t, h, http.MethodGet, "/clusters/my-cluster/node-groups", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				names, ok := resp["nodegroups"].([]any)
				require.True(t, ok)
				assert.Len(t, names, 2)
			},
		},
		{
			name: "delete_nodegroup",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/node-groups", map[string]any{
					"nodegroupName": "to-delete",
					"scalingConfig": map[string]any{},
				})
				rec := doREST(t, h, http.MethodDelete, "/clusters/my-cluster/node-groups/to-delete", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				rec2 := doREST(t, h, http.MethodGet, "/clusters/my-cluster/node-groups/to-delete", nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "nodegroup_cluster_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/clusters/nonexistent/node-groups", map[string]any{
					"nodegroupName": "ng",
					"scalingConfig": map[string]any{},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.ops(t, newTestEKSHandler())
		})
	}
}

// TestEKSTagging exercises TagResource and ListTagsForResource.
func TestEKSTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *eks.Handler)
		name string
	}{
		{
			name: "tag_and_list_cluster",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "tagged-cluster"})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				cluster := resp["cluster"].(map[string]any)
				clusterARN := cluster["arn"].(string)

				tagRec := doREST(t, h, http.MethodPost, "/tags/"+clusterARN, map[string]any{
					"tags": map[string]string{"Project": "test"},
				})
				assert.Equal(t, http.StatusOK, tagRec.Code)

				listRec := doREST(t, h, http.MethodGet, "/tags/"+clusterARN, nil)
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseResp(t, listRec)
				tagsMap, ok := listResp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "test", tagsMap["Project"])
			},
		},
		{
			name: "list_tags_not_found",
			ops: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/tags/arn:aws:eks:us-east-1:123456789012:cluster/missing", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.ops(t, newTestEKSHandler())
		})
	}
}

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
			h := newTestEKSHandler()
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

// TestEKSHandlerChaosAndMetrics covers ChaosServiceName, ChaosOperations, ChaosRegions,
// MatchPriority, GetSupportedOperations, and ExtractOperation/ExtractResource.
func TestEKSHandlerChaosAndMetrics(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()

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
		assert.Contains(t, ops, "CreateCluster")
		assert.Contains(t, ops, "DeleteCluster")
		assert.Contains(t, ops, "CreateNodegroup")
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

// TestEKSBackendRegion tests the Region getter.
func TestEKSBackendRegion(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "us-east-1", backend.Region())
}

// TestEKSBackendListAllClusters tests the ListAllClusters helper.
func TestEKSBackendListAllClusters(t *testing.T) {
	t.Parallel()

	backend := eks.NewInMemoryBackend("123456789012", "us-east-1")
	_, _ = backend.CreateCluster("a", "1.32", "", nil)
	_, _ = backend.CreateCluster("b", "1.32", "", nil)

	all := backend.ListAllClusters()
	assert.Len(t, all, 2)
}

// TestEKSUntagResource verifies the UntagResource handler returns 200.
func TestEKSUntagResource(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "tag-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	clusterARN := resp["cluster"].(map[string]any)["arn"].(string)

	untagRec := doREST(t, h, http.MethodDelete, "/tags/"+clusterARN, nil)
	assert.Equal(t, http.StatusOK, untagRec.Code)
}

// TestEKSTagNodegroup verifies that tags can be applied to a nodegroup.
func TestEKSTagNodegroup(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
	ngRec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/node-groups", map[string]any{
		"nodegroupName": "my-ng",
		"scalingConfig": map[string]any{},
	})
	require.Equal(t, http.StatusOK, ngRec.Code)
	ngResp := parseResp(t, ngRec)
	ngARN := ngResp["nodegroup"].(map[string]any)["nodegroupArn"].(string)

	tagRec := doREST(t, h, http.MethodPost, "/tags/"+ngARN, map[string]any{
		"tags": map[string]string{"Tier": "worker"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doREST(t, h, http.MethodGet, "/tags/"+ngARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)
	listResp := parseResp(t, listRec)
	tagsMap, ok := listResp["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "worker", tagsMap["Tier"])
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.ops(t, newTestEKSHandler())
		})
	}
}

// TestEKSProvider checks the Provider name and Init.
func TestEKSProvider(t *testing.T) {
	t.Parallel()

	p := &eks.Provider{}
	assert.Equal(t, "EKS", p.Name())

	appCtx := &service.AppContext{}
	h, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, h)
}

// TestEKSRouteMatcher verifies the route matcher accepts EKS paths.
func TestEKSRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
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
