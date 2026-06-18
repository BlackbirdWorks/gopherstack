package emr_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
		checkTags func(*testing.T, []emr.Tag)
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
			checkTags: func(t *testing.T, tags []emr.Tag) {
				t.Helper()
				require.Len(t, tags, 1)
				assert.Equal(t, "env", tags[0].Key)
				assert.Equal(t, "prod", tags[0].Value)
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
			checkTags: func(t *testing.T, tags []emr.Tag) {
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
					Tags []emr.Tag `json:"Tags"`
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

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "step-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	rec := doEMRRequest(t, h, "AddJobFlowSteps", map[string]any{
		"JobFlowId": createOut.JobFlowID,
		"Steps": []any{
			map[string]any{
				"Name":            "my-step",
				"ActionOnFailure": "CONTINUE",
				"HadoopJarStep":   map[string]any{"Jar": "command-runner.jar", "Args": []string{"spark-submit"}},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		StepIDs []string `json:"StepIds"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.StepIDs, 1)
	assert.Contains(t, out.StepIDs[0], "s-")
}

func TestEMR_ListInstanceGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns instance groups for cluster with groups",
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
				"Name":         "ig-cluster",
				"ReleaseLabel": "emr-6.0.0",
				"Instances": map[string]any{
					"InstanceGroups": []map[string]any{
						{"InstanceRole": "MASTER", "InstanceType": "m4.large", "InstanceCount": 1},
						{"InstanceRole": "CORE", "InstanceType": "m4.large", "InstanceCount": 2},
					},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			rec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{
				"ClusterId": createOut.JobFlowID,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			var out struct {
				InstanceGroups []struct {
					InstanceGroupType string `json:"InstanceGroupType"`
					Status            struct {
						State string `json:"State"`
					} `json:"Status"`
				} `json:"InstanceGroups"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.InstanceGroups, tt.wantCount)

			for _, g := range out.InstanceGroups {
				assert.Equal(t, "RUNNING", g.Status.State)
			}
		})
	}
}

func TestEMR_ListInstanceGroups_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{
		"ClusterId": "j-NOTEXIST",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_ListInstanceFleets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	rec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": createOut.JobFlowID,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		InstanceFleets []emr.InstanceFleet `json:"InstanceFleets"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.InstanceFleets)
}

func TestEMR_ListBootstrapActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "bootstrap-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	rec := doEMRRequest(t, h, "ListBootstrapActions", map[string]any{
		"ClusterId": createOut.JobFlowID,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		BootstrapActions []any `json:"BootstrapActions"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.BootstrapActions)
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
		name       string
		resourceID string
		wantTags   []emr.Tag
		wantErr    bool
	}{
		{
			name:       "existing cluster by ID",
			resourceID: "",
			wantErr:    false,
			wantTags:   []emr.Tag{{Key: "env", Value: "test"}},
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
			cluster, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{
				Name: "test-cluster", ReleaseLabel: "emr-6.0.0",
				Tags: []emr.Tag{{Key: "env", Value: "test"}},
			})
			require.NoError(t, err)

			resourceID := tt.resourceID
			if resourceID == "" {
				resourceID = cluster.ID
			}

			tags, err := b.ListTagsForResource(context.Background(), resourceID)
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
	cluster, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{
		Name: "test-cluster", ReleaseLabel: "emr-6.0.0",
		Tags: []emr.Tag{{Key: "key", Value: "val"}},
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), cluster.ARN)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "key", tags[0].Key)
	assert.Equal(t, "val", tags[0].Value)
}

func TestEMR_TerminateJobFlows_Idempotent(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "idem-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	// First termination succeeds.
	require.NoError(t, b.TerminateJobFlows(context.Background(), []string{cluster.ID}))

	// Second termination on an already-terminal cluster must be a no-op, not an error.
	require.NoError(t, b.TerminateJobFlows(context.Background(), []string{cluster.ID}))
}

func TestEMR_TerminateJobFlows_SetsEndDateTime(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "timeline-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	before := time.Now().UnixMilli()
	require.NoError(t, b.TerminateJobFlows(context.Background(), []string{cluster.ID}))

	c, err := b.DescribeCluster(context.Background(), cluster.ID)
	require.NoError(t, err)

	endRaw, ok := c.Status.Timeline["EndDateTime"]
	require.True(t, ok, "EndDateTime must be set in the timeline after termination")

	endMs, ok := endRaw.(int64)
	require.True(t, ok, "EndDateTime must be an int64 Unix milliseconds value")
	assert.GreaterOrEqual(t, endMs, before, "EndDateTime should be >= time before termination")
}

func TestEMR_AddInstanceFleet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusterID string
		wantCode  int
	}{
		{
			name:      "adds fleet to existing cluster",
			clusterID: "",
			wantCode:  http.StatusOK,
		},
		{
			name:      "returns error for non-existent cluster",
			clusterID: "j-NOTEXIST",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cluster"})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			clusterID := tt.clusterID
			if clusterID == "" {
				clusterID = createOut.JobFlowID
			}

			rec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
				"ClusterId": clusterID,
				"InstanceFleet": map[string]any{
					"InstanceFleetType": "TASK",
					"Name":              "task-fleet",
				},
			})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					ClusterArn      string `json:"ClusterArn"`
					ClusterID       string `json:"ClusterId"`
					InstanceFleetID string `json:"InstanceFleetId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.InstanceFleetID)
				assert.Equal(t, clusterID, out.ClusterID)
				assert.Contains(t, out.ClusterArn, "elasticmapreduce")
			}
		})
	}
}

func TestEMR_AddInstanceGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		jobFlowID string
		wantCode  int
	}{
		{
			name:      "adds groups to existing cluster",
			jobFlowID: "",
			wantCode:  http.StatusOK,
		},
		{
			name:      "returns error for non-existent cluster",
			jobFlowID: "j-NOTEXIST",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "ig-cluster"})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			jobFlowID := tt.jobFlowID
			if jobFlowID == "" {
				jobFlowID = createOut.JobFlowID
			}

			rec := doEMRRequest(t, h, "AddInstanceGroups", map[string]any{
				"JobFlowId": jobFlowID,
				"InstanceGroups": []map[string]any{
					{"InstanceRole": "TASK", "InstanceType": "m4.large", "InstanceCount": 1},
				},
			})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					ClusterArn       string   `json:"ClusterArn"`
					JobFlowID        string   `json:"JobFlowId"`
					InstanceGroupIDs []string `json:"InstanceGroupIds"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.InstanceGroupIDs, 1)
				assert.Equal(t, jobFlowID, out.JobFlowID)
				assert.Contains(t, out.ClusterArn, "elasticmapreduce")
			}
		})
	}
}

func TestEMR_CancelSteps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusterID string
		wantCode  int
	}{
		{
			name:      "cancels steps on existing cluster",
			clusterID: "",
			wantCode:  http.StatusOK,
		},
		{
			name:      "returns error for non-existent cluster",
			clusterID: "j-NOTEXIST",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "cancel-cluster"})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			clusterID := tt.clusterID
			if clusterID == "" {
				clusterID = createOut.JobFlowID
			}

			rec := doEMRRequest(t, h, "CancelSteps", map[string]any{
				"ClusterId": clusterID,
				"StepIds":   []string{"s-123"},
			})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					CancelStepsInfoList []any `json:"CancelStepsInfoList"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Empty(t, out.CancelStepsInfoList)
			}
		})
	}
}

func TestEMR_CreatePersistentAppUI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		targetResourceArn string
		wantCode          int
	}{
		{
			name:              "creates persistent app UI",
			targetResourceArn: "arn:aws:elasticmapreduce:us-east-1:000000000000:cluster/j-0000000000001",
			wantCode:          http.StatusOK,
		},
		{
			name:              "returns validation error when target resource ARN is missing",
			targetResourceArn: "",
			wantCode:          http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doEMRRequest(t, h, "CreatePersistentAppUI", map[string]any{
				"TargetResourceArn": tt.targetResourceArn,
			})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					PersistentAppUIID string `json:"PersistentAppUIId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.PersistentAppUIID)
			}
		})
	}
}

func TestEMR_SecurityConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*emr.Handler)
		name     string
		scName   string
		scConfig string
		testType string
		wantCode int
	}{
		{
			name:     "creates security configuration",
			testType: "create",
			scName:   "my-config",
			scConfig: `{"EncryptionConfiguration": {}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "create duplicate returns error",
			testType: "create",
			scName:   "duplicate-config",
			scConfig: "{}",
			setup: func(h *emr.Handler) {
				rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  "duplicate-config",
					"SecurityConfiguration": "{}",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create without name returns error",
			testType: "create",
			scName:   "",
			scConfig: "{}",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "deletes existing security configuration",
			testType: "delete",
			scName:   "to-delete",
			setup: func(h *emr.Handler) {
				rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  "to-delete",
					"SecurityConfiguration": "{}",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete non-existent returns error",
			testType: "delete",
			scName:   "nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			var rec *httptest.ResponseRecorder
			switch tt.testType {
			case "create":
				rec = doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  tt.scName,
					"SecurityConfiguration": tt.scConfig,
				})
			case "delete":
				rec = doEMRRequest(t, h, "DeleteSecurityConfiguration", map[string]any{
					"Name": tt.scName,
				})
			}

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.testType == "create" && tt.wantCode == http.StatusOK {
				var out struct {
					Name             string `json:"Name"`
					CreationDateTime string `json:"CreationDateTime"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.scName, out.Name)
				assert.NotEmpty(t, out.CreationDateTime)
			}
		})
	}
}

func TestEMR_Studio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*emr.Handler) string
		name     string
		testType string
		wantCode int
	}{
		{
			name:     "creates studio",
			testType: "create",
			wantCode: http.StatusOK,
		},
		{
			name:     "create studio without name returns error",
			testType: "create_no_name",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "deletes existing studio",
			testType: "delete",
			setup: func(h *emr.Handler) string {
				rec := doEMRRequest(t, h, "CreateStudio", map[string]any{
					"Name":                     "my-studio",
					"AuthMode":                 "IAM",
					"DefaultS3Location":        "s3://bucket/path",
					"EngineSecurityGroupId":    "sg-engine",
					"ServiceRole":              "arn:aws:iam::000000000000:role/studio-role",
					"SubnetIds":                []string{"subnet-1"},
					"VpcId":                    "vpc-123",
					"WorkspaceSecurityGroupId": "sg-workspace",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					StudioID string `json:"StudioId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.StudioID
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete non-existent studio returns error",
			testType: "delete_notfound",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var studioID string
			if tt.setup != nil {
				studioID = tt.setup(h)
			}

			var rec *httptest.ResponseRecorder
			switch tt.testType {
			case "create":
				rec = doEMRRequest(t, h, "CreateStudio", map[string]any{
					"Name":                     "my-studio",
					"AuthMode":                 "IAM",
					"DefaultS3Location":        "s3://bucket/path",
					"EngineSecurityGroupId":    "sg-engine",
					"ServiceRole":              "arn:aws:iam::000000000000:role/studio-role",
					"SubnetIds":                []string{"subnet-1"},
					"VpcId":                    "vpc-123",
					"WorkspaceSecurityGroupId": "sg-workspace",
				})
			case "create_no_name":
				rec = doEMRRequest(t, h, "CreateStudio", map[string]any{
					"Name":              "",
					"AuthMode":          "IAM",
					"DefaultS3Location": "s3://bucket/path",
				})
			case "delete":
				rec = doEMRRequest(t, h, "DeleteStudio", map[string]any{
					"StudioId": studioID,
				})
			case "delete_notfound":
				rec = doEMRRequest(t, h, "DeleteStudio", map[string]any{
					"StudioId": "es-NOTEXIST",
				})
			}

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.testType == "create" && tt.wantCode == http.StatusOK {
				var out struct {
					StudioID string `json:"StudioId"`
					URL      string `json:"Url"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.StudioID)
				assert.NotEmpty(t, out.URL)
			}
		})
	}
}

func TestEMR_StudioSessionMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		testType string
		wantCode int
	}{
		{
			name:     "creates session mapping",
			testType: "create",
			wantCode: http.StatusOK,
		},
		{
			name:     "create session mapping for non-existent studio fails",
			testType: "create_notstudio",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create session mapping without studio ID fails",
			testType: "create_nostudio",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "deletes existing session mapping",
			testType: "delete",
			wantCode: http.StatusOK,
		},
		{
			name:     "delete non-existent session mapping fails",
			testType: "delete_notfound",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a studio for tests that need one.
			var studioID string
			studioRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
				"Name":                     "session-studio",
				"AuthMode":                 "SSO",
				"DefaultS3Location":        "s3://bucket/path",
				"EngineSecurityGroupId":    "sg-engine",
				"ServiceRole":              "arn:aws:iam::000000000000:role/role",
				"SubnetIds":                []string{"subnet-1"},
				"VpcId":                    "vpc-123",
				"WorkspaceSecurityGroupId": "sg-workspace",
			})
			require.Equal(t, http.StatusOK, studioRec.Code)

			var studioOut struct {
				StudioID string `json:"StudioId"`
			}
			require.NoError(t, json.Unmarshal(studioRec.Body.Bytes(), &studioOut))
			studioID = studioOut.StudioID

			var rec *httptest.ResponseRecorder
			switch tt.testType {
			case "create":
				rec = doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         studioID,
					"IdentityType":     "USER",
					"IdentityId":       "user-123",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
			case "create_notstudio":
				rec = doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         "es-NOTEXIST",
					"IdentityType":     "USER",
					"IdentityId":       "user-123",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
			case "create_nostudio":
				rec = doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         "",
					"IdentityType":     "USER",
					"IdentityId":       "user-123",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
			case "delete":
				// First create a mapping.
				cRec := doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
					"StudioId":         studioID,
					"IdentityType":     "USER",
					"IdentityId":       "user-to-delete",
					"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
				})
				require.Equal(t, http.StatusOK, cRec.Code)

				rec = doEMRRequest(t, h, "DeleteStudioSessionMapping", map[string]any{
					"StudioId":     studioID,
					"IdentityType": "USER",
					"IdentityId":   "user-to-delete",
				})
			case "delete_notfound":
				rec = doEMRRequest(t, h, "DeleteStudioSessionMapping", map[string]any{
					"StudioId":     studioID,
					"IdentityType": "USER",
					"IdentityId":   "nonexistent-user",
				})
			}

			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestEMR_AddInstanceFleet_ListInstanceFleets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	// Add a fleet.
	addRec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
		"ClusterId": createOut.JobFlowID,
		"InstanceFleet": map[string]any{
			"InstanceFleetType": "TASK",
			"Name":              "task-fleet",
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	// List fleets - should now have one.
	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": createOut.JobFlowID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		InstanceFleets []struct {
			ID   string `json:"Id"`
			Name string `json:"Name"`
		} `json:"InstanceFleets"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.InstanceFleets, 1)
	assert.Equal(t, "task-fleet", listOut.InstanceFleets[0].Name)
	assert.NotEmpty(t, listOut.InstanceFleets[0].ID)
}

func TestEMR_ListInstanceFleets_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": "j-NOTEXIST",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_DeleteStudio_CascadesSessionMappings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a studio.
	studioRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "cascade-studio",
		"AuthMode":                 "SSO",
		"DefaultS3Location":        "s3://bucket/path",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/role",
		"SubnetIds":                []string{"subnet-1"},
		"VpcId":                    "vpc-123",
		"WorkspaceSecurityGroupId": "sg-workspace",
	})
	require.Equal(t, http.StatusOK, studioRec.Code)

	var studioOut struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(studioRec.Body.Bytes(), &studioOut))

	// Create a session mapping.
	mappingRec := doEMRRequest(t, h, "CreateStudioSessionMapping", map[string]any{
		"StudioId":         studioOut.StudioID,
		"IdentityType":     "USER",
		"IdentityId":       "user-123",
		"SessionPolicyArn": "arn:aws:iam::000000000000:policy/policy",
	})
	require.Equal(t, http.StatusOK, mappingRec.Code)

	// Delete the studio - should cascade.
	deleteRec := doEMRRequest(t, h, "DeleteStudio", map[string]any{
		"StudioId": studioOut.StudioID,
	})
	require.Equal(t, http.StatusOK, deleteRec.Code)

	// Deleting studio again should fail.
	deleteRec2 := doEMRRequest(t, h, "DeleteStudio", map[string]any{
		"StudioId": studioOut.StudioID,
	})
	assert.Equal(t, http.StatusBadRequest, deleteRec2.Code)
}

func TestEMR_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AddInstanceFleet",
		"AddInstanceGroups",
		"CancelSteps",
		"CreatePersistentAppUI",
		"CreateSecurityConfiguration",
		"CreateStudio",
		"CreateStudioSessionMapping",
		"DeleteSecurityConfiguration",
		"DeleteStudio",
		"DeleteStudioSessionMapping",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op)
	}
}
