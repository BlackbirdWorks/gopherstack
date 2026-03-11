package emr_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/emr"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestHandler(t *testing.T) *emr.Handler {
	t.Helper()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)

	return emr.NewHandler(backend)
}

func doEMRRequest(t *testing.T, h *emr.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ElasticMapReduce."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ----- Provider tests -----

func TestEMR_Provider_Name(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	assert.Equal(t, "EMR", p.Name())
}

func TestEMR_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "EMR", svc.Name())
}

// ----- Handler metadata tests -----

func TestEMR_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "EMR", h.Name())
}

func TestEMR_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "RunJobFlow")
	assert.Contains(t, ops, "DescribeCluster")
	assert.Contains(t, ops, "ListClusters")
	assert.Contains(t, ops, "TerminateJobFlows")
	assert.Contains(t, ops, "AddTags")
	assert.Contains(t, ops, "RemoveTags")
}

func TestEMR_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestEMR_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching EMR target",
			target: "ElasticMapReduce.RunJobFlow",
			want:   true,
		},
		{
			name:   "non-matching target",
			target: "AmazonEC2ContainerServiceV20141113.CreateCluster",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())

			matcher := h.RouteMatcher()
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestEMR_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "RunJobFlow",
			target: "ElasticMapReduce.RunJobFlow",
			want:   "RunJobFlow",
		},
		{
			name:   "DescribeCluster",
			target: "ElasticMapReduce.DescribeCluster",
			want:   "DescribeCluster",
		},
		{
			name:   "empty target",
			target: "",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

// ----- Operation tests -----

func TestEMR_RunJobFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   map[string]any
		name    string
		wantErr bool
	}{
		{
			name:  "creates cluster with name",
			input: map[string]any{"Name": "my-cluster", "ReleaseLabel": "emr-6.0.0"},
		},
		{
			name:  "creates cluster without release label uses default",
			input: map[string]any{"Name": "other-cluster"},
		},
		{
			name: "creates cluster with tags",
			input: map[string]any{
				"Name":         "tagged-cluster",
				"ReleaseLabel": "emr-6.0.0",
				"Tags":         []map[string]any{{"Key": "env", "Value": "test"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doEMRRequest(t, h, "RunJobFlow", tt.input)

			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				JobFlowID  string `json:"JobFlowId"`
				ClusterArn string `json:"ClusterArn"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out.JobFlowID)
			assert.Contains(t, out.ClusterArn, "elasticmapreduce")
		})
	}
}

func TestEMR_DescribeCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusterID string
		wantCode  int
	}{
		{
			name:      "found",
			clusterID: "",
			wantCode:  http.StatusOK,
		},
		{
			name:      "not found",
			clusterID: "j-NOTEXIST",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a cluster first
			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "test-cluster"})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}

			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			clusterID := tt.clusterID
			if clusterID == "" {
				clusterID = createOut.JobFlowID
			}

			rec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": clusterID})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					Cluster struct {
						ID   string `json:"Id"`
						Name string `json:"Name"`
					} `json:"Cluster"`
				}

				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, clusterID, out.Cluster.ID)
				assert.Equal(t, "test-cluster", out.Cluster.Name)
			}
		})
	}
}

func TestEMR_ListClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		numClusters  int
		wantMinCount int
	}{
		{
			name:         "empty list",
			numClusters:  0,
			wantMinCount: 0,
		},
		{
			name:         "lists created clusters",
			numClusters:  2,
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.numClusters {
				rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "cluster"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doEMRRequest(t, h, "ListClusters", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Clusters []any `json:"Clusters"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.GreaterOrEqual(t, len(out.Clusters), tt.wantMinCount)
		})
	}
}

func TestEMR_TerminateJobFlows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*emr.Handler) []string
		name     string
		wantCode int
	}{
		{
			name: "terminates existing cluster",
			setup: func(h *emr.Handler) []string {
				rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "to-terminate"})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					JobFlowID string `json:"JobFlowId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return []string{out.JobFlowID}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "returns error for non-existent cluster",
			setup: func(_ *emr.Handler) []string {
				return []string{"j-NOTEXIST"}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			jobFlowIDs := tt.setup(h)

			rec := doEMRRequest(t, h, "TerminateJobFlows", map[string]any{"JobFlowIds": jobFlowIDs})
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestEMR_AddAndRemoveTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "adds and removes tags on existing cluster",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "tag-cluster"})
			require.Equal(t, http.StatusOK, rec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

			addRec := doEMRRequest(t, h, "AddTags", map[string]any{
				"ResourceId": createOut.JobFlowID,
				"Tags":       []map[string]any{{"Key": "env", "Value": "dev"}},
			})
			require.Equal(t, tt.wantCode, addRec.Code)

			removeRec := doEMRRequest(t, h, "RemoveTags", map[string]any{
				"ResourceId": createOut.JobFlowID,
				"TagKeys":    []string{"env"},
			})
			require.Equal(t, tt.wantCode, removeRec.Code)
		})
	}
}

func TestEMR_ListSteps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "ListSteps", map[string]any{"ClusterId": "j-123"})

	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Steps []any `json:"Steps"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.Steps)
}

func TestEMR_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*emr.Handler) string
		checkTags func(*testing.T, map[string]string)
		name      string
		wantCode  int
	}{
		{
			name: "lists tags on existing cluster",
			setup: func(h *emr.Handler) string {
				rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
					"Name": "list-tags-cluster",
					"Tags": []map[string]any{{"Key": "env", "Value": "prod"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut struct {
					JobFlowID string `json:"JobFlowId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

				return createOut.JobFlowID
			},
			wantCode: http.StatusOK,
			checkTags: func(t *testing.T, tags map[string]string) {
				t.Helper()
				assert.Equal(t, "prod", tags["env"])
			},
		},
		{
			name: "lists empty tags on cluster without tags",
			setup: func(h *emr.Handler) string {
				rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "no-tag-cluster"})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut struct {
					JobFlowID string `json:"JobFlowId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

				return createOut.JobFlowID
			},
			wantCode: http.StatusOK,
			checkTags: func(t *testing.T, tags map[string]string) {
				t.Helper()
				assert.Empty(t, tags)
			},
		},
		{
			name: "returns error for non-existent resource",
			setup: func(_ *emr.Handler) string {
				return "j-NOTEXIST"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceID := tt.setup(h)

			listRec := doEMRRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceId": resourceID,
			})
			require.Equal(t, tt.wantCode, listRec.Code)

			if tt.checkTags != nil {
				var tagOut struct {
					Tags map[string]string `json:"Tags"`
				}
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagOut))
				tt.checkTags(t, tagOut.Tags)
			}
		})
	}
}

func TestEMR_AddJobFlowSteps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "AddJobFlowSteps", map[string]any{
		"JobFlowId": "j-123",
		"Steps":     []any{},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		StepIDs []string `json:"StepIds"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.StepIDs)
}

func TestEMR_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "NonExistentOp", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "emr", h.ChaosServiceName())
}

func TestEMR_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, []string{testRegion}, h.ChaosRegions())
}

func TestEMR_Backend_Region(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, testRegion, b.Region())
}

func TestEMR_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Contains(t, ops, "RunJobFlow")
	assert.Contains(t, ops, "DescribeCluster")
}

func TestEMR_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		want string
	}{
		{
			name: "extracts ClusterId",
			body: map[string]any{"ClusterId": "j-123"},
			want: "j-123",
		},
		{
			name: "extracts JobFlowId",
			body: map[string]any{"JobFlowId": "j-456"},
			want: "j-456",
		},
		{
			name: "extracts ResourceId",
			body: map[string]any{"ResourceId": "j-789"},
			want: "j-789",
		},
		{
			name: "returns empty for no IDs",
			body: map[string]any{"Name": "cluster"},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestEMR_AddTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "AddTags", map[string]any{
		"ResourceId": "j-NOTEXIST",
		"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_RemoveTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "RemoveTags", map[string]any{
		"ResourceId": "j-NOTEXIST",
		"TagKeys":    []string{"env"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_Backend_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags   map[string]string
		name       string
		resourceID string
		wantErr    bool
	}{
		{
			name:       "existing cluster by ID",
			resourceID: "",
			wantErr:    false,
			wantTags:   map[string]string{"env": "test"},
		},
		{
			name:       "not found",
			resourceID: "j-NOTEXIST",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emr.NewInMemoryBackend(testAccountID, testRegion)
			cluster, err := b.RunJobFlow("test-cluster", "emr-6.0.0", []emr.Tag{{Key: "env", Value: "test"}})
			require.NoError(t, err)

			resourceID := tt.resourceID
			if resourceID == "" {
				resourceID = cluster.ID
			}

			tags, err := b.ListTagsForResource(resourceID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestEMR_Backend_ListTagsForResourceByARN(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow("test-cluster", "emr-6.0.0", []emr.Tag{{Key: "key", Value: "val"}})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(cluster.ARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"key": "val"}, tags)
}
