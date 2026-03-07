package ecs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecs"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestHandler(t *testing.T) *ecs.Handler {
	t.Helper()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	return ecs.NewHandler(backend)
}

func doECSRequest(t *testing.T, h *ecs.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ----- Provider tests -----

func TestECS_Provider_Name(t *testing.T) {
	t.Parallel()

	p := &ecs.Provider{}
	assert.Equal(t, "ECS", p.Name())
}

func TestECS_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &ecs.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "ECS", svc.Name())
}

// ----- Handler metadata tests -----

func TestECS_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ECS", h.Name())
}

func TestECS_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateCluster")
	assert.Contains(t, ops, "RegisterTaskDefinition")
	assert.Contains(t, ops, "CreateService")
	assert.Contains(t, ops, "RunTask")
}

func TestECS_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestECS_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matching ECS target",
			target:    "AmazonEC2ContainerServiceV20141113.CreateCluster",
			wantMatch: true,
		},
		{
			name:      "non-matching target",
			target:    "OtherService.Action",
			wantMatch: false,
		},
		{
			name:      "empty target",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestECS_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "create cluster",
			target: "AmazonEC2ContainerServiceV20141113.CreateCluster",
			want:   "CreateCluster",
		},
		{
			name:   "empty target",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "other service",
			target: "OtherService.Action",
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

func TestECS_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body string
		name string
		want string
	}{
		{
			name: "clusterName field",
			body: `{"clusterName":"my-cluster"}`,
			want: "my-cluster",
		},
		{
			name: "cluster field",
			body: `{"cluster":"my-cluster"}`,
			want: "my-cluster",
		},
		{
			name: "serviceName field",
			body: `{"serviceName":"my-service"}`,
			want: "my-service",
		},
		{
			name: "service field",
			body: `{"service":"my-service"}`,
			want: "my-service",
		},
		{
			name: "family field",
			body: `{"family":"my-family"}`,
			want: "my-family",
		},
		{
			name: "empty body",
			body: `{}`,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(tt.body)))
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestECS_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "UnknownAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "UnknownOperationException")
}

// ----- Cluster tests -----

func TestECS_CreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name:     "with explicit name",
			input:    map[string]any{"clusterName": "my-cluster"},
			wantCode: http.StatusOK,
			wantName: "my-cluster",
		},
		{
			name:     "with empty name defaults to 'default'",
			input:    map[string]any{},
			wantCode: http.StatusOK,
			wantName: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "CreateCluster", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cluster, ok := resp["cluster"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, cluster["clusterName"])
			assert.NotEmpty(t, cluster["clusterArn"])
			assert.Equal(t, "ACTIVE", cluster["status"])
		})
	}
}

func TestECS_CreateCluster_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dupe"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dupe"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "ClusterAlreadyExistsException")
}

func TestECS_DescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusters  []string
		filter    []string
		wantCode  int
		wantCount int
	}{
		{
			name:      "list all",
			clusters:  []string{"cluster-a", "cluster-b"},
			filter:    nil,
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:      "filter by name",
			clusters:  []string{"cluster-a", "cluster-b"},
			filter:    []string{"cluster-a"},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
		{
			name:     "not found",
			clusters: []string{},
			filter:   []string{"nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.clusters {
				rec := doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": name})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			input := map[string]any{}
			if tt.filter != nil {
				input["clusters"] = tt.filter
			}

			rec := doECSRequest(t, h, "DescribeClusters", input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				clusters, ok := resp["clusters"].([]any)
				require.True(t, ok)
				assert.Len(t, clusters, tt.wantCount)
			}
		})
	}
}

func TestECS_DeleteCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		create   string
		delete   string
		wantCode int
	}{
		{
			name:     "success",
			create:   "my-cluster",
			delete:   "my-cluster",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			create:   "",
			delete:   "nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.create != "" {
				rec := doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": tt.create})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doECSRequest(t, h, "DeleteCluster", map[string]any{"cluster": tt.delete})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ----- Task definition tests -----

func TestECS_RegisterTaskDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
		wantRev  int
	}{
		{
			name: "success",
			input: map[string]any{
				"family": "nginx-task",
				"containerDefinitions": []map[string]any{
					{"name": "nginx", "image": "nginx:latest", "essential": true},
				},
			},
			wantCode: http.StatusOK,
			wantRev:  1,
		},
		{
			name:     "missing family",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "RegisterTaskDefinition", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				td, ok := resp["taskDefinition"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, td["taskDefinitionArn"])
				assert.Equal(t, tt.wantRev, int(td["revision"].(float64)))
				assert.Equal(t, "ACTIVE", td["status"])
			}
		})
	}
}

func TestECS_RegisterTaskDefinition_MultipleRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 3; i++ {
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family": "my-task",
			"containerDefinitions": []map[string]any{
				{"name": "app", "image": "app:latest"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		td := resp["taskDefinition"].(map[string]any)
		assert.Equal(t, i, int(td["revision"].(float64)))
	}
}

func TestECS_DescribeTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a task definition first.
	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "web",
		"containerDefinitions": []map[string]any{
			{"name": "web", "image": "nginx:latest"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe by family name.
	rec2 := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{"taskDefinition": "web"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	td := resp["taskDefinition"].(map[string]any)
	assert.Equal(t, "web", td["family"])

	// Not found.
	rec3 := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{"taskDefinition": "nonexistent"})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

func TestECS_DeregisterTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "temp-task",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "busybox"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	tdArn := createResp["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	rec2 := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{"taskDefinition": tdArn})
	require.Equal(t, http.StatusOK, rec2.Code)

	var deregResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &deregResp))

	td := deregResp["taskDefinition"].(map[string]any)
	assert.Equal(t, "INACTIVE", td["status"])
}

func TestECS_ListTaskDefinitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	families := []string{"app-a", "app-b", "other"}
	for _, f := range families {
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               f,
			"containerDefinitions": []map[string]any{{"name": "c", "image": "busybox"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List all.
	rec := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arns := resp["taskDefinitionArns"].([]any)
	assert.GreaterOrEqual(t, len(arns), 3)

	// Filter by prefix.
	rec2 := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{"familyPrefix": "app"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	arns2 := resp2["taskDefinitionArns"].([]any)
	assert.Len(t, arns2, 2)
}

// ----- Service tests -----

func registerTestTaskDef(t *testing.T, h *ecs.Handler, family string) string {
	t.Helper()

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               family,
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx:latest"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)
}

func TestECS_CreateService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success with default cluster",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "svc-task")

				return map[string]any{
					"serviceName":    "my-service",
					"taskDefinition": tdArn,
					"desiredCount":   2,
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "success with explicit cluster",
			setup: func(h *ecs.Handler) map[string]any {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "prod"})
				tdArn := registerTestTaskDef(t, h, "svc-task2")

				return map[string]any{
					"serviceName":    "prod-service",
					"cluster":        "prod",
					"taskDefinition": tdArn,
					"desiredCount":   1,
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing service name",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{"taskDefinition": "some-task"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing task definition",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{"serviceName": "svc"}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			input := tt.setup(h)
			rec := doECSRequest(t, h, "CreateService", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				svc := resp["service"].(map[string]any)
				assert.NotEmpty(t, svc["serviceArn"])
				assert.Equal(t, "ACTIVE", svc["status"])
			}
		})
	}
}

func TestECS_CreateService_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "dup-task")

	input := map[string]any{
		"serviceName":    "dup-svc",
		"taskDefinition": tdArn,
		"desiredCount":   1,
	}

	rec := doECSRequest(t, h, "CreateService", input)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doECSRequest(t, h, "CreateService", input)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "ServiceAlreadyExistsException")
}

func TestECS_DescribeServices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "desc-task")

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "desc-svc",
		"taskDefinition": tdArn,
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe all services.
	rec2 := doECSRequest(t, h, "DescribeServices", map[string]any{"services": []string{}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	svcs := resp["services"].([]any)
	assert.GreaterOrEqual(t, len(svcs), 1)

	// Describe by name.
	rec3 := doECSRequest(t, h, "DescribeServices", map[string]any{"services": []string{"desc-svc"}})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))

	svcs3 := resp3["services"].([]any)
	require.Len(t, svcs3, 1)
	assert.Equal(t, "desc-svc", svcs3[0].(map[string]any)["serviceName"])
}

func TestECS_UpdateService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) (string, map[string]any)
		name     string
		wantCode int
		wantDC   int
	}{
		{
			name: "update desiredCount",
			setup: func(h *ecs.Handler) (string, map[string]any) {
				tdArn := registerTestTaskDef(t, h, "upd-task")
				doECSRequest(t, h, "CreateService", map[string]any{
					"serviceName":    "upd-svc",
					"taskDefinition": tdArn,
					"desiredCount":   1,
				})

				count := 5

				return "upd-svc", map[string]any{
					"service":      "upd-svc",
					"desiredCount": count,
				}
			},
			wantCode: http.StatusOK,
			wantDC:   5,
		},
		{
			name: "service not found",
			setup: func(_ *ecs.Handler) (string, map[string]any) {
				return "", map[string]any{"service": "nonexistent"}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, input := tt.setup(h)
			rec := doECSRequest(t, h, "UpdateService", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				svc := resp["service"].(map[string]any)
				assert.Equal(t, tt.wantDC, int(svc["desiredCount"].(float64)))
			}
		})
	}
}

func TestECS_DeleteService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "del-task")

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "del-svc",
		"taskDefinition": tdArn,
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doECSRequest(t, h, "DeleteService", map[string]any{"service": "del-svc"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	svc := resp["service"].(map[string]any)
	assert.Equal(t, "del-svc", svc["serviceName"])

	// Confirm deletion.
	rec3 := doECSRequest(t, h, "DescribeServices", map[string]any{"services": []string{"del-svc"}})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// ----- Task tests -----

func TestECS_RunTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "run single task",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "run-task")

				return map[string]any{
					"taskDefinition": tdArn,
					"count":          1,
				}
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "run multiple tasks",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "run-multi")

				return map[string]any{
					"taskDefinition": tdArn,
					"count":          3,
				}
			},
			wantCode: http.StatusOK,
			wantLen:  3,
		},
		{
			name: "default count is 1",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "run-default")

				return map[string]any{"taskDefinition": tdArn}
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "missing task definition",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			input := tt.setup(h)
			rec := doECSRequest(t, h, "RunTask", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				tasks := resp["tasks"].([]any)
				assert.Len(t, tasks, tt.wantLen)

				task := tasks[0].(map[string]any)
				assert.NotEmpty(t, task["taskArn"])
				assert.Equal(t, "RUNNING", task["lastStatus"])
				assert.Equal(t, "RUNNING", task["desiredStatus"])
			}
		})
	}
}

func TestECS_DescribeTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "desc-task-t")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &runResp))

	taskArns := make([]string, 0, 2)
	for _, t := range runResp["tasks"].([]any) {
		taskArns = append(taskArns, t.(map[string]any)["taskArn"].(string))
	}

	// Describe all tasks on cluster.
	rec2 := doECSRequest(t, h, "DescribeTasks", map[string]any{"tasks": []string{}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.Len(t, resp2["tasks"].([]any), 2)

	// Describe specific task.
	rec3 := doECSRequest(t, h, "DescribeTasks", map[string]any{"tasks": []string{taskArns[0]}})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	assert.Len(t, resp3["tasks"].([]any), 1)
}

func TestECS_StopTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "stop-task-def")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &runResp))

	taskArn := runResp["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	rec2 := doECSRequest(t, h, "StopTask", map[string]any{
		"task":   taskArn,
		"reason": "manual stop",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	task := resp2["task"].(map[string]any)
	assert.Equal(t, "STOPPED", task["lastStatus"])
	assert.Equal(t, "manual stop", task["stoppedReason"])
}

func TestECS_ListTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "list-task-def")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doECSRequest(t, h, "ListTasks", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	arns := resp["taskArns"].([]any)
	assert.Len(t, arns, 3)
}

// ----- Backend direct tests -----

func TestECS_Backend_DefaultClusterAutoCreated(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	// Register a task definition.
	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "auto-cluster-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	// Run a task - default cluster should be auto-created.
	tasks, err := backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "RUNNING", tasks[0].LastStatus)
}

func TestECS_Backend_TaskDefinitionByRevision(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "rev-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "v1"}},
	})
	require.NoError(t, err)

	_, err = backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "rev-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "v2"}},
	})
	require.NoError(t, err)

	// Describe by family:revision shorthand.
	td, err := backend.DescribeTaskDefinition("rev-task:1")
	require.NoError(t, err)
	assert.Equal(t, 1, td.Revision)
	assert.Equal(t, "v1", td.ContainerDefinitions[0].Image)

	td2, err := backend.DescribeTaskDefinition("rev-task:2")
	require.NoError(t, err)
	assert.Equal(t, 2, td2.Revision)
	assert.Equal(t, "v2", td2.ContainerDefinitions[0].Image)
}

func TestECS_Backend_CountRunningTasksForService(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "svc-task-count",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	// Create a cluster and service.
	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "test-cluster"})
	require.NoError(t, err)

	_, err = backend.CreateService(ecs.CreateServiceInput{
		ServiceName:    "count-svc",
		Cluster:        "test-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   2,
	})
	require.NoError(t, err)

	// Run tasks with the service group.
	_, err = backend.RunTask(ecs.RunTaskInput{
		Cluster:        "test-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		Count:          2,
		Group:          "service:count-svc",
	})
	require.NoError(t, err)

	count := backend.CountRunningTasksForService("test-cluster", "count-svc")
	assert.Equal(t, 2, count)
}

func TestECS_Backend_StopOldestServiceTask(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "oldest-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "oldest-cluster"})
	require.NoError(t, err)

	// Start 3 tasks for the service.
	for range 3 {
		err = backend.StartTaskForService("oldest-cluster", "oldest-svc", td.TaskDefinitionArn)
		require.NoError(t, err)
	}

	// Stop the oldest one.
	err = backend.StopOldestServiceTask("oldest-cluster", "oldest-svc")
	require.NoError(t, err)

	count := backend.CountRunningTasksForService("oldest-cluster", "oldest-svc")
	assert.Equal(t, 2, count)
}

func TestECS_Reconciler(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "reconcile-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "reconcile-cluster"})
	require.NoError(t, err)

	_, err = backend.CreateService(ecs.CreateServiceInput{
		ServiceName:    "reconcile-svc",
		Cluster:        "reconcile-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   2,
	})
	require.NoError(t, err)

	reconciler := ecs.NewReconciler(backend)

	// Run one reconcile cycle manually.
	ctx := t.Context()
	reconciler.RunOnce(ctx)

	// Should have started 2 tasks.
	count := backend.CountRunningTasksForService("reconcile-cluster", "reconcile-svc")
	assert.Equal(t, 2, count)

	// Scale down to 1.
	count1 := 1
	_, err = backend.UpdateService(ecs.UpdateServiceInput{
		Cluster:      "reconcile-cluster",
		Service:      "reconcile-svc",
		DesiredCount: &count1,
	})
	require.NoError(t, err)

	reconciler.RunOnce(ctx)

	count = backend.CountRunningTasksForService("reconcile-cluster", "reconcile-svc")
	assert.Equal(t, 1, count)
}

func TestECS_Backend_ClusterKey_ARN(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	// CreateCluster, then describe using ARN.
	c, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "arn-test"})
	require.NoError(t, err)

	clusters, err := backend.DescribeClusters([]string{c.ClusterArn})
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "arn-test", clusters[0].ClusterName)
}

func TestECS_Backend_DescribeServices_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.DescribeServices("nonexistent-cluster", nil)
	require.Error(t, err)
}

func TestECS_Backend_UpdateService_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	count := 1
	_, err := backend.UpdateService(ecs.UpdateServiceInput{
		Cluster:      "nonexistent-cluster",
		Service:      "any-service",
		DesiredCount: &count,
	})
	require.Error(t, err)
}

func TestECS_Backend_DeleteService_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.DeleteService("nonexistent-cluster", "any-service")
	require.Error(t, err)
}

func TestECS_Backend_StopTask_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.StopTask("nonexistent-cluster", "task-arn", "reason")
	require.Error(t, err)
}

func TestECS_Backend_ListTasks_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.ListTasks("nonexistent-cluster")
	require.Error(t, err)
}

func TestECS_Backend_DescribeTasks_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.DescribeTasks("nonexistent-cluster", []string{"task-arn"})
	require.Error(t, err)
}

func TestECS_Handler_DescribeServices_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "nonexistent-cluster",
		"services": []string{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Handler_DeleteService_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DeleteService", map[string]any{
		"cluster": "nonexistent-cluster",
		"service": "any-service",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Handler_StopTask_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "StopTask", map[string]any{
		"cluster": "nonexistent-cluster",
		"task":    "task-arn",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Handler_ListTasks_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "ListTasks", map[string]any{
		"cluster": "nonexistent-cluster",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Handler_ListTaskDefinitions_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arns, ok := resp["taskDefinitionArns"].([]any)
	require.True(t, ok)
	assert.Empty(t, arns)
}

func TestECS_Provider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	// Test Init with no config (just default).
	p := &ecs.Provider{}
	svc, err := p.Init(&service.AppContext{
		Logger: slog.Default(),
		// Config is nil — should use defaults.
	})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestECS_Reconciler_Start_ContextCancel(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
	reconciler := ecs.NewReconciler(backend)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		reconciler.Start(ctx)
		close(done)
	}()

	// Cancel context — Start should return.
	cancel()

	select {
	case <-done:
	// expected
	case <-t.Context().Done():
		t.Fatal("Start did not return after context cancellation")
	}
}

func TestECS_Backend_ServiceKey_ARN(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "arn-svc-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "arn-svc-cluster"})
	require.NoError(t, err)

	svc, err := backend.CreateService(ecs.CreateServiceInput{
		ServiceName:    "arn-svc",
		Cluster:        "arn-svc-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   1,
	})
	require.NoError(t, err)

	// Describe using the full service ARN.
	svcs, err := backend.DescribeServices("arn-svc-cluster", []string{svc.ServiceArn})
	require.NoError(t, err)
	require.Len(t, svcs, 1)
	assert.Equal(t, "arn-svc", svcs[0].ServiceName)
}

func TestECS_Backend_UpdateService_TaskDefinition(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td1, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "td-update-family",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "v1"}},
	})
	require.NoError(t, err)

	td2, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "td-update-family",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "v2"}},
	})
	require.NoError(t, err)

	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "td-update-cluster"})
	require.NoError(t, err)

	_, err = backend.CreateService(ecs.CreateServiceInput{
		ServiceName:    "td-update-svc",
		Cluster:        "td-update-cluster",
		TaskDefinition: td1.TaskDefinitionArn,
		DesiredCount:   1,
	})
	require.NoError(t, err)

	updated, err := backend.UpdateService(ecs.UpdateServiceInput{
		Cluster:        "td-update-cluster",
		Service:        "td-update-svc",
		TaskDefinition: td2.TaskDefinitionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, td2.TaskDefinitionArn, updated.TaskDefinition)
}

func TestECS_Backend_EnrichCluster_PendingTasks(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "enrich-cluster"})
	require.NoError(t, err)

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "enrich-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	// Run tasks so cluster has running count.
	_, err = backend.RunTask(ecs.RunTaskInput{
		Cluster:        "enrich-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		Count:          2,
	})
	require.NoError(t, err)

	clusters, err := backend.DescribeClusters([]string{"enrich-cluster"})
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, 2, clusters[0].RunningTasksCount)
}

func TestECS_Backend_EnrichService_PendingTasks(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "enrich-svc-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "enrich-svc-cluster"})
	require.NoError(t, err)

	_, err = backend.CreateService(ecs.CreateServiceInput{
		ServiceName:    "enrich-svc",
		Cluster:        "enrich-svc-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   2,
	})
	require.NoError(t, err)

	// Run tasks to populate service running count.
	_, err = backend.RunTask(ecs.RunTaskInput{
		Cluster:        "enrich-svc-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		Count:          2,
		Group:          "service:enrich-svc",
	})
	require.NoError(t, err)

	svcs, err := backend.DescribeServices("enrich-svc-cluster", []string{"enrich-svc"})
	require.NoError(t, err)
	require.Len(t, svcs, 1)
	assert.Equal(t, 2, svcs[0].RunningCount)
}

func TestECS_Handler_DeregisterTaskDefinition_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": "nonexistent-family",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Provider_Init_WithJanitorCtx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())

	p := &ecs.Provider{}
	svc, err := p.Init(&service.AppContext{
		Logger:     slog.Default(),
		JanitorCtx: ctx,
	})
	require.NoError(t, err)
	assert.NotNil(t, svc)

	// Cancel the janitor context to stop the reconciler.
	cancel()
}

func TestECS_Handler_RunTask_WithGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "group-task")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          1,
		"group":          "service:my-svc",
		"launchType":     "EC2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks := resp["tasks"].([]any)
	task := tasks[0].(map[string]any)
	assert.Equal(t, "service:my-svc", task["group"])
	assert.Equal(t, "EC2", task["launchType"])
}

func TestECS_Handler_ExtractResource_TaskField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/",
		bytes.NewReader([]byte(`{"task":"arn:aws:ecs:us-east-1:000000000000:task/c1/abc"}`)))
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:task/c1/abc", h.ExtractResource(c))
}

func TestECS_Backend_DeregisterTaskDefinition_NotFoundByARN(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.DeregisterTaskDefinition("arn:aws:ecs:us-east-1:000000000000:task-definition/nonexistent:1")
	require.Error(t, err)
}

func TestECS_Reconciler_RunOnce_NoServices(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
	reconciler := ecs.NewReconciler(backend)

	// Should not panic with no services.
	reconciler.RunOnce(t.Context())
}

func TestECS_Backend_CreateService_LaunchTypeDefault(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "default-lt-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	svc, err := backend.CreateService(ecs.CreateServiceInput{
		ServiceName:    "default-lt-svc",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   1,
	})
	require.NoError(t, err)
	// Default launch type is FARGATE.
	assert.Equal(t, "FARGATE", svc.LaunchType)
}

func TestECS_Backend_RunTask_LaunchTypeDefault(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "default-lt-run",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	tasks, err := backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	// Default launch type is FARGATE.
	assert.Equal(t, "FARGATE", tasks[0].LaunchType)
}

func TestECS_Backend_StopOldestServiceTask_NoTasks(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "empty-cluster"})
	require.NoError(t, err)

	// Should not error when no tasks exist.
	err = backend.StopOldestServiceTask("empty-cluster", "nonexistent-svc")
	require.NoError(t, err)
}

func TestECS_Handler_DescribeTasks_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"tasks": []string{"arn:aws:ecs:us-east-1:000000000000:task/default/nonexistent"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Handler_StopTask_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Create the default cluster first via RunTask initialization.
	tdArn := registerTestTaskDef(t, h, "stop-not-found")
	doECSRequest(t, h, "RunTask", map[string]any{"taskDefinition": tdArn})

	rec := doECSRequest(t, h, "StopTask", map[string]any{
		"task": "arn:aws:ecs:us-east-1:000000000000:task/default/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----- TaskRunner mock helpers -----

// errContainerStart is the sentinel error used by failingRunner.
var errContainerStart = errors.New("container start failed")

// failingRunner is a TaskRunner that always returns an error from RunTask, causing
// the task to remain at PROVISIONING rather than transitioning to RUNNING.
type failingRunner struct{}

func (r *failingRunner) RunTask(_ *ecs.Task, _ *ecs.TaskDefinition) error { return errContainerStart }
func (r *failingRunner) StopTask(_ *ecs.Task) error                       { return nil }

// TestECS_Backend_RunTask_ProvisioningStaysOnRunnerError verifies that when the
// TaskRunner returns an error, the task status remains at PROVISIONING.
func TestECS_Backend_RunTask_ProvisioningStaysOnRunnerError(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, &failingRunner{})

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "prov-err-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx:latest"}},
	})
	require.NoError(t, err)

	tasks, err := backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// Task should remain PROVISIONING because the runner failed.
	assert.Equal(t, "PROVISIONING", tasks[0].LastStatus)
	assert.Equal(t, "RUNNING", tasks[0].DesiredStatus)
}

// TestECS_Backend_RunTask_TransitionToRunningWithRunner verifies that when the
// TaskRunner succeeds, the task transitions from PROVISIONING to RUNNING.
func TestECS_Backend_RunTask_TransitionToRunningWithRunner(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "prov-ok-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx:latest"}},
	})
	require.NoError(t, err)

	tasks, err := backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// Noop runner succeeds → task transitions to RUNNING.
	assert.Equal(t, "RUNNING", tasks[0].LastStatus)
}
