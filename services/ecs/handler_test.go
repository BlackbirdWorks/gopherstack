package ecs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ----- shared test helpers -----

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestHandler(t *testing.T) *ecs.Handler {
	t.Helper()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	return ecs.NewHandler(backend)
}

func doECSRequest(
	t *testing.T,
	h *ecs.Handler,
	action string,
	body any,
) *httptest.ResponseRecorder {
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

func TestECS_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ECS", h.Name())
}

func TestECS_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ops  []string
	}{
		{
			name: "core",
			ops:  []string{"CreateCluster", "RegisterTaskDefinition", "CreateService", "RunTask"},
		},
		{
			name: "capacity_providers_and_express_gateway",
			ops: []string{
				"CreateCapacityProvider",
				"DeleteCapacityProvider",
				"DescribeCapacityProviders",
				"DeleteAccountSetting",
				"DeleteAttributes",
				"DeleteTaskDefinitions",
				"DescribeServiceDeployments",
				"CreateExpressGatewayService",
				"DeleteExpressGatewayService",
				"DescribeExpressGatewayService",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ops := h.GetSupportedOperations()
			for _, op := range tt.ops {
				assert.Contains(t, ops, op, "expected %s in supported operations", op)
			}
		})
	}
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

func TestECS_Handler_ExtractResource_TaskField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/",
		bytes.NewReader([]byte(`{"task":"arn:aws:ecs:us-east-1:000000000000:task/c1/abc"}`)))
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:task/c1/abc", h.ExtractResource(c))
}

func TestECS_Reconciler_RunOnce_NoServices(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
	reconciler := ecs.NewReconciler(backend)

	// Should not panic with no services.
	reconciler.RunOnce(t.Context())
}

func TestConcurrent_CreateServices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "concurrent-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "concurrent-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	var wg sync.WaitGroup
	results := make([]int, 10)

	for i := range 10 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			body, _ := json.Marshal(map[string]any{
				"cluster":        "concurrent-cluster",
				"serviceName":    "concurrent-svc-" + string(rune('a'+idx)),
				"taskDefinition": "concurrent-task",
				"desiredCount":   1,
			})
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113.CreateService")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			_ = h.Handler()(c)
			results[idx] = rec.Code
		}(i)
	}

	wg.Wait()

	for i, code := range results {
		assert.Equal(t, http.StatusOK, code, "service %d", i)
	}
}

func TestConcurrent_RegisterContainerInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "concurrent-ci-cluster"})

	var wg sync.WaitGroup
	results := make([]int, 20)

	for i := range 20 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			body, _ := json.Marshal(map[string]any{
				"cluster":       "concurrent-ci-cluster",
				"ec2InstanceId": "i-concurrent-" + string(rune('a'+idx)),
			})
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set(
				"X-Amz-Target",
				"AmazonEC2ContainerServiceV20141113.RegisterContainerInstance",
			)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			_ = h.Handler()(c)
			results[idx] = rec.Code
		}(i)
	}

	wg.Wait()

	for i, code := range results {
		assert.Equal(t, http.StatusOK, code, "container instance %d", i)
	}

	listResp := doECSRequest(t, h, "ListContainerInstances", map[string]any{
		"cluster": "concurrent-ci-cluster",
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["containerInstanceArns"].([]any)
	assert.Len(t, arns, 20)
}

func TestHandler_Reset_ClearsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "reset-cluster"})
	doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "reset-cp"})

	h.Reset()

	// Default cluster should still exist
	listResp := doECSRequest(t, h, "ListClusters", map[string]any{})
	require.Equal(t, http.StatusOK, listResp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &out))
	// No custom clusters after reset
	arns := out["clusterArns"].([]any)
	for _, arn := range arns {
		assert.NotContains(t, arn.(string), "reset-cluster")
	}
}

func TestECS_ExtractResource_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body string
		name string
		want string
	}{
		{
			name: "containerInstance field",
			body: `{"containerInstance":"arn:aws:ecs:us-east-1:000000000000:container-instance/c/abc"}`,
			want: "arn:aws:ecs:us-east-1:000000000000:container-instance/c/abc",
		},
		{
			name: "taskSet field",
			body: `{"taskSet":"arn:aws:ecs:us-east-1:000000000000:task-set/c/s/ecs-svc-abc"}`,
			want: "arn:aws:ecs:us-east-1:000000000000:task-set/c/s/ecs-svc-abc",
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

func TestBackend_Reset_ClearsResourceMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFunc func(*ecs.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name: "reset clears capacity providers",
			setupFunc: func(b *ecs.InMemoryBackend) {
				b.AddCapacityProviderInternal(&ecs.CapacityProvider{
					Name:                "test-cp",
					CapacityProviderArn: "arn:aws:ecs:us-east-1:000000000000:capacity-provider/test-cp",
					Status:              "ACTIVE",
				})
			},
			wantCount: 0,
		},
		{
			name:      "reset on empty backend is a no-op",
			setupFunc: func(*ecs.InMemoryBackend) {},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			tt.setupFunc(b)
			b.Reset()

			assert.Equal(t, tt.wantCount, b.CapacityProviderCount())
			assert.Equal(t, tt.wantCount, b.ServiceDeploymentCount())
			assert.Equal(t, tt.wantCount, b.AccountSettingCount())
			assert.Equal(t, tt.wantCount, b.ExpressGatewayServiceCount())
		})
	}
}

func TestHandler_Reset_ClearsCapacityProviders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears backend state via handler"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "cp1"})
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "cp2"})

			h.Reset()

			rec := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cps, ok := resp["capacityProviders"].([]any)
			require.True(t, ok)
			assert.Empty(t, cps)
		})
	}
}

func TestHandler_CachedOps_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input  map[string]any
		name   string
		action string
		want   int
	}{
		{
			name:   "CreateCapacityProvider is available via cached ops",
			action: "CreateCapacityProvider",
			input:  map[string]any{"name": "ops-test-cp"},
			want:   http.StatusOK,
		},
		{
			name:   "unknown action returns 400",
			action: "NonExistentOperation",
			input:  map[string]any{},
			want:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, tt.action, tt.input)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestBackend_CountHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFunc   func(*ecs.InMemoryBackend)
		name        string
		attrCluster string
		wantCP      int
		wantSD      int
		wantAS      int
		wantEGS     int
		wantAttr    int
	}{
		{
			name:      "empty backend has zero counts",
			setupFunc: func(*ecs.InMemoryBackend) {},
		},
		{
			name: "one of each increments count",
			setupFunc: func(b *ecs.InMemoryBackend) {
				b.AddCapacityProviderInternal(&ecs.CapacityProvider{
					Name:                "cp1",
					CapacityProviderArn: "arn:aws:ecs:us-east-1:000000000000:capacity-provider/cp1",
				})

				now := time.Now()
				b.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
					ServiceDeploymentArn: "arn:aws:ecs:us-east-1:000000000000:service-deployment/sd1",
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/default",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/default/svc1",
					Status:               "COMPLETED",
					CreatedAt:            &now,
				})

				b.AddAccountSettingInternal(":containerInsights", &ecs.AccountSetting{
					Name:  "containerInsights",
					Value: "enabled",
				})

				b.AddAttributeInternal("default", &ecs.Attribute{
					Name:     "custom",
					Value:    "value",
					TargetID: "ci-abc",
				})
			},
			wantCP:      1,
			wantSD:      1,
			wantAS:      1,
			wantEGS:     0,
			wantAttr:    1,
			attrCluster: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			tt.setupFunc(b)

			assert.Equal(t, tt.wantCP, b.CapacityProviderCount())
			assert.Equal(t, tt.wantSD, b.ServiceDeploymentCount())
			assert.Equal(t, tt.wantAS, b.AccountSettingCount())
			assert.Equal(t, tt.wantEGS, b.ExpressGatewayServiceCount())

			if tt.attrCluster != "" {
				assert.Equal(t, tt.wantAttr, b.AttributeCount(tt.attrCluster))
			}
		})
	}
}

func TestProvider_NilAppContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		nilAppCtx bool
	}{
		{
			name:      "nil AppContext returns ErrNilAppContext",
			nilAppCtx: true,
			wantErr:   ecs.ErrNilAppContext,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ecs.Provider{}
			_, err := p.Init(nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandler_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cpName   string
		wantName string
	}{
		{
			name:     "roundtrip preserves capacity provider",
			cpName:   "snap-cp",
			wantName: "snap-cp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": tt.cpName})

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(t.Context(), snap))

			rec := doECSRequest(t, h2, "DescribeCapacityProviders", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cps, ok := resp["capacityProviders"].([]any)
			require.True(t, ok)
			require.Len(t, cps, 1)

			cp := cps[0].(map[string]any)
			assert.Equal(t, tt.wantName, cp["name"])
		})
	}
}

func TestBackend_NonNilEmptySlices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "DescribeCapacityProviders returns non-nil empty slice when no providers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			cps, _, err := b.DescribeCapacityProviders(nil)
			require.NoError(t, err)
			assert.NotNil(t, cps)
			assert.Empty(t, cps)
		})
	}
}
