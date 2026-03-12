package batch_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/batch"
)

func newTestHandler(t *testing.T) *batch.Handler {
	t.Helper()

	return batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(
	t *testing.T,
	h *batch.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func post(t *testing.T, h *batch.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	return doRequest(t, h, http.MethodPost, path, body)
}

func mustUnmarshal(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

// --- Handler metadata tests ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Batch", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "batch", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityPathVersioned, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"CreateComputeEnvironment",
		"DescribeComputeEnvironments",
		"UpdateComputeEnvironment",
		"DeleteComputeEnvironment",
		"CreateJobQueue",
		"DescribeJobQueues",
		"UpdateJobQueue",
		"DeleteJobQueue",
		"RegisterJobDefinition",
		"DescribeJobDefinitions",
		"DeregisterJobDefinition",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	} {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "batch_path", path: "/v1/createcomputeenvironment", wantMatch: true},
		{name: "tags_path", path: "/v1/tags/some-arn", wantMatch: true},
		{
			name:      "tags_batch_arn",
			path:      "/v1/tags/arn%3Aaws%3Abatch%3Aus-east-1%3A123%3Acompute-environment%2Ftest",
			wantMatch: true,
		},
		{
			name:      "tags_kafka_arn_excluded",
			path:      "/v1/tags/arn%3Aaws%3Akafka%3Aus-east-1%3A123%3Acluster%2Ftest%2Fuuid",
			wantMatch: false,
		},
		{name: "kafka_cluster_excluded", path: "/v1/clusters", wantMatch: false},
		{name: "kafka_config_excluded", path: "/v1/configurations", wantMatch: false},
		{name: "appsync_path_excluded", path: "/v1/apis", wantMatch: false},
		{name: "appsync_path_excluded_with_id", path: "/v1/apis/abc123/datasources", wantMatch: false},
		{name: "other_prefix", path: "/v2/apis", wantMatch: false},
		{name: "root", path: "/", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.RouteMatcher()(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_ce",
			path:   "/v1/createcomputeenvironment",
			method: http.MethodPost,
			wantOp: "CreateComputeEnvironment",
		},
		{
			name:   "describe_ce",
			path:   "/v1/describecomputeenvironments",
			method: http.MethodPost,
			wantOp: "DescribeComputeEnvironments",
		},
		{name: "tags_get", path: "/v1/tags/some-arn", method: http.MethodGet, wantOp: "ListTagsForResource"},
		{name: "tags_post", path: "/v1/tags/some-arn", method: http.MethodPost, wantOp: "TagResource"},
		{name: "tags_delete", path: "/v1/tags/some-arn", method: http.MethodDelete, wantOp: "UntagResource"},
		{name: "unknown", path: "/v1/unknown", method: http.MethodPost, wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// --- Compute Environment tests ---

func TestHandler_ComputeEnvironment_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler)
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "create_success",
			wantStatus: http.StatusOK,
			wantARN:    true,
		},
		{
			name: "create_duplicate",
			setup: func(t *testing.T, h *batch.Handler) {
				t.Helper()
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": "test-ce",
					"type":                   "MANAGED",
					"state":                  "ENABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
				"computeEnvironmentName": "test-ce",
				"type":                   "MANAGED",
				"state":                  "ENABLED",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out["computeEnvironmentArn"], "test-ce")
				assert.Equal(t, "test-ce", out["computeEnvironmentName"])
			}
		})
	}
}

func TestHandler_DescribeComputeEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     []string
		wantCount  int
		wantStatus int
	}{
		{name: "describe_all", filter: nil, wantCount: 2, wantStatus: http.StatusOK},
		{name: "describe_one", filter: []string{"ce-1"}, wantCount: 1, wantStatus: http.StatusOK},
		{name: "describe_missing", filter: []string{"nonexistent"}, wantCount: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"ce-1", "ce-2"} {
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": name,
					"type":                   "MANAGED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filter != nil {
				body["computeEnvironments"] = tt.filter
			}

			rec := post(t, h, "/v1/describecomputeenvironments", body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			list, ok := out["computeEnvironments"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_UpdateComputeEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ce         string
		state      string
		wantStatus int
	}{
		{name: "update_success", ce: "test-ce", state: "DISABLED", wantStatus: http.StatusOK},
		{name: "update_not_found", ce: "missing-ce", state: "DISABLED", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
				"computeEnvironmentName": "test-ce",
				"type":                   "MANAGED",
				"state":                  "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = post(t, h, "/v1/updatecomputeenvironment", map[string]any{
				"computeEnvironment": tt.ce,
				"state":              tt.state,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteComputeEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ce         string
		wantStatus int
	}{
		{name: "delete_success", ce: "test-ce", wantStatus: http.StatusOK},
		{name: "delete_not_found", ce: "missing", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
				"computeEnvironmentName": "test-ce",
				"type":                   "MANAGED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = post(t, h, "/v1/deletecomputeenvironment", map[string]any{
				"computeEnvironment": tt.ce,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Job Queue tests ---

func TestHandler_JobQueue_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler)
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "create_success",
			wantStatus: http.StatusOK,
			wantARN:    true,
		},
		{
			name: "create_duplicate",
			setup: func(t *testing.T, h *batch.Handler) {
				t.Helper()
				rec := post(t, h, "/v1/createjobqueue", map[string]any{
					"jobQueueName": "test-jq",
					"priority":     10,
					"state":        "ENABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := post(t, h, "/v1/createjobqueue", map[string]any{
				"jobQueueName": "test-jq",
				"priority":     10,
				"state":        "ENABLED",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out["jobQueueArn"], "test-jq")
				assert.Equal(t, "test-jq", out["jobQueueName"])
			}
		})
	}
}

func TestHandler_DescribeJobQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     []string
		wantCount  int
		wantStatus int
	}{
		{name: "describe_all", filter: nil, wantCount: 2, wantStatus: http.StatusOK},
		{name: "describe_one", filter: []string{"jq-1"}, wantCount: 1, wantStatus: http.StatusOK},
		{name: "describe_missing", filter: []string{"no-such-queue"}, wantCount: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"jq-1", "jq-2"} {
				rec := post(t, h, "/v1/createjobqueue", map[string]any{
					"jobQueueName": name,
					"priority":     1,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filter != nil {
				body["jobQueues"] = tt.filter
			}

			rec := post(t, h, "/v1/describejobqueues", body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			list, ok := out["jobQueues"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_UpdateJobQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		priority   *int32
		name       string
		jq         string
		state      string
		wantStatus int
	}{
		{
			name:       "update_state",
			jq:         "test-jq",
			state:      "DISABLED",
			wantStatus: http.StatusOK,
		},
		{
			name: "update_priority",
			jq:   "test-jq",
			priority: func() *int32 {
				v := int32(20)

				return &v
			}(),
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			jq:         "no-such-queue",
			state:      "DISABLED",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createjobqueue", map[string]any{
				"jobQueueName": "test-jq",
				"priority":     10,
				"state":        "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			body := map[string]any{"jobQueue": tt.jq}
			if tt.state != "" {
				body["state"] = tt.state
			}

			if tt.priority != nil {
				body["priority"] = *tt.priority
			}

			rec = post(t, h, "/v1/updatejobqueue", body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteJobQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jq         string
		wantStatus int
	}{
		{name: "delete_success", jq: "test-jq", wantStatus: http.StatusOK},
		{name: "delete_not_found", jq: "missing", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createjobqueue", map[string]any{
				"jobQueueName": "test-jq",
				"priority":     1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = post(t, h, "/v1/deletejobqueue", map[string]any{
				"jobQueue": tt.jq,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Job Definition tests ---

func TestHandler_JobDefinition_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jdName     string
		wantStatus int
		wantRev    int32
	}{
		{name: "register_success", jdName: "test-jd", wantStatus: http.StatusOK, wantRev: 1},
		{name: "register_second_revision", jdName: "test-jd-rev", wantStatus: http.StatusOK, wantRev: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
				"jobDefinitionName": tt.jdName,
				"type":              "container",
			})

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			assert.Contains(t, out["jobDefinitionArn"].(string), tt.jdName)
			assert.Equal(t, tt.jdName, out["jobDefinitionName"])
			assert.InEpsilon(t, float64(tt.wantRev), out["revision"].(float64), 0.001)
		})
	}
}

func TestHandler_RegisterJobDefinition_MultipleRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
			"jobDefinitionName": "my-jd",
			"type":              "container",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		mustUnmarshal(t, rec, &out)
		assert.InEpsilon(t, float64(i+1), out["revision"].(float64), 0.001)
	}
}

func TestHandler_DescribeJobDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     []string
		wantMin    int
		wantStatus int
	}{
		{name: "describe_all", filter: nil, wantMin: 2, wantStatus: http.StatusOK},
		{name: "describe_by_name", filter: []string{"jd-1"}, wantMin: 1, wantStatus: http.StatusOK},
		{name: "describe_missing", filter: []string{"nope"}, wantMin: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"jd-1", "jd-2"} {
				rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
					"jobDefinitionName": name,
					"type":              "container",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filter != nil {
				body["jobDefinitions"] = tt.filter
			}

			rec := post(t, h, "/v1/describejobdefinitions", body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			list, ok := out["jobDefinitions"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(list), tt.wantMin)
		})
	}
}

func TestHandler_DeregisterJobDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useARN     bool
		wantStatus int
	}{
		{name: "deregister_success", useARN: true, wantStatus: http.StatusOK},
		{name: "deregister_not_found", useARN: false, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
				"jobDefinitionName": "deregtest-jd",
				"type":              "container",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			jdARN := out["jobDefinitionArn"].(string)

			jd := jdARN
			if !tt.useARN {
				jd = "arn:aws:batch:us-east-1:000000000000:job-definition/nonexistent:1"
			}

			rec = post(t, h, "/v1/deregisterjobdefinition", map[string]any{
				"jobDefinition": jd,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Tags tests ---

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler) string
		addTags    map[string]string
		wantTags   map[string]string
		name       string
		removeKeys []string
		wantStatus int
	}{
		{
			name: "list_tags_on_ce",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": "tag-ce",
					"type":                   "MANAGED",
					"tags":                   map[string]string{"env": "prod"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["computeEnvironmentArn"]
			},
			wantTags:   map[string]string{"env": "prod"},
			wantStatus: http.StatusOK,
		},
		{
			name: "tag_and_untag_ce",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": "untag-ce",
					"type":                   "MANAGED",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["computeEnvironmentArn"]
			},
			addTags:    map[string]string{"key1": "val1", "key2": "val2"},
			removeKeys: []string{"key1"},
			wantTags:   map[string]string{"key2": "val2"},
			wantStatus: http.StatusOK,
		},
		{
			name: "list_tags_not_found",
			setup: func(_ *testing.T, _ *batch.Handler) string {
				return "arn:aws:batch:us-east-1:000000000000:compute-environment/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			if tt.addTags != nil {
				rec := doRequest(t, h, http.MethodPost, "/v1/tags/"+resourceARN, map[string]any{
					"tags": tt.addTags,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.removeKeys != nil {
				path := fmt.Sprintf("/v1/tags/%s?tagKeys=%s", resourceARN, tt.removeKeys[0])
				var queryBuilder strings.Builder
				for _, k := range tt.removeKeys[1:] {
					queryBuilder.WriteString("&tagKeys=" + k)
				}
				path += queryBuilder.String()

				rec := doRequest(t, h, http.MethodDelete, path, nil)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/v1/tags/"+resourceARN, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantTags != nil {
				var out map[string]map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, tt.wantTags, out["tags"])
			}
		})
	}
}

func TestHandler_Tags_OnJobQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "tagged-jq",
		"priority":     1,
		"tags":         map[string]string{"team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var jqOut map[string]string
	mustUnmarshal(t, rec, &jqOut)
	jqARN := jqOut["jobQueueArn"]

	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+jqARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "platform", out["tags"]["team"])
}

func TestHandler_Tags_OnJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "tagged-jd",
		"type":              "container",
		"tags":              map[string]string{"owner": "alice"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var jdOut map[string]any
	mustUnmarshal(t, rec, &jdOut)
	jdARN := jdOut["jobDefinitionArn"].(string)

	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+jdARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "alice", out["tags"]["owner"])
}

// --- Stub operation tests ---

func TestHandler_StubOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "list_jobs",
			path:       "/v1/listjobs",
			body:       map[string]any{"jobQueue": "my-queue"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe_jobs",
			path:       "/v1/describejobs",
			body:       map[string]any{"jobs": []string{"job-id-1"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "submit_job",
			path: "/v1/submitjob",
			body: map[string]any{
				"jobName":       "my-job",
				"jobQueue":      "my-queue",
				"jobDefinition": "my-jd",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "terminate_job",
			path:       "/v1/terminatejob",
			body:       map[string]any{"jobId": "job-id-1", "reason": "testing"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "cancel_job",
			path:       "/v1/canceljob",
			body:       map[string]any{"jobId": "job-id-1", "reason": "testing"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := post(t, h, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/v1/createcomputeenvironment", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/unknownoperation", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateComputeEnvironment")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.Equal(t, []string{"us-east-1"}, regions)
}

func TestHandler_Tags_InvalidARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/v1/tags/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_JobQueueWithComputeEnvironmentOrder(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "ordered-jq",
		"priority":     5,
		"state":        "ENABLED",
		"computeEnvironmentOrder": []map[string]any{
			{"computeEnvironment": "ce-1", "order": 1},
			{"computeEnvironment": "ce-2", "order": 2},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobqueues", map[string]any{
		"jobQueues": []string{"ordered-jq"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jqs := out["jobQueues"].([]any)
	require.Len(t, jqs, 1)

	jq := jqs[0].(map[string]any)
	ceOrder := jq["computeEnvironmentOrder"].([]any)
	assert.Len(t, ceOrder, 2)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "tags_arn",
			path: "/v1/tags/arn:aws:batch:us-east-1:000000000000:compute-environment/my-ce",
			want: "arn:aws:batch:us-east-1:000000000000:compute-environment/my-ce",
		},
		{
			name: "non_tags_path",
			path: "/v1/createcomputeenvironment",
			want: "",
		},
		{
			name: "tags_empty",
			path: "/v1/tags/",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			h := newTestHandler(t)
			got := h.ExtractResource(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_Tags_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a CE so we have a valid ARN to tag.
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "body-ce",
		"type":                   "MANAGED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	ceARN := out["computeEnvironmentArn"]

	// Send invalid JSON as tag body — expect 400.
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/tags/"+ceARN, strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	recW := httptest.NewRecorder()
	c := e.NewContext(req, recW)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, recW.Code)
}

func TestHandler_Tags_UntagNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nonExistentARN := "arn:aws:batch:us-east-1:000000000000:compute-environment/ghost"
	path := "/v1/tags/" + nonExistentARN + "?tagKeys=k1"

	rec := doRequest(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Tags_TagNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nonExistentARN := "arn:aws:batch:us-east-1:000000000000:compute-environment/ghost"

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/tags/"+nonExistentARN, strings.NewReader(`{"tags":{"k":"v"}}`))
	req.Header.Set("Content-Type", "application/json")
	recW := httptest.NewRecorder()
	c := e.NewContext(req, recW)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, recW.Code)
}

func TestHandler_ComputeEnvironmentByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "arn-lookup-ce",
		"type":                   "MANAGED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	ceARN := out["computeEnvironmentArn"]

	// Update using ARN instead of name.
	rec = post(t, h, "/v1/updatecomputeenvironment", map[string]any{
		"computeEnvironment": ceARN,
		"state":              "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe by ARN.
	rec = post(t, h, "/v1/describecomputeenvironments", map[string]any{
		"computeEnvironments": []string{ceARN},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut map[string]any
	mustUnmarshal(t, rec, &descOut)
	ces := descOut["computeEnvironments"].([]any)
	require.Len(t, ces, 1)
	assert.Equal(t, "DISABLED", ces[0].(map[string]any)["state"])
}

func TestHandler_JobQueueByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "arn-lookup-jq",
		"priority":     1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	jqARN := out["jobQueueArn"]

	// Update by ARN.
	rec = post(t, h, "/v1/updatejobqueue", map[string]any{
		"jobQueue": jqARN,
		"state":    "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete by ARN.
	rec = post(t, h, "/v1/deletejobqueue", map[string]any{
		"jobQueue": jqARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeregisterJobDefinition_ByNameRevision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "namerev-jd",
		"type":              "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Deregister by name:revision.
	rec = post(t, h, "/v1/deregisterjobdefinition", map[string]any{
		"jobDefinition": "namerev-jd:1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
