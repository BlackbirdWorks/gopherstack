package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestServiceDeployment_DescribeList_Roundtrip(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
	h := ecs.NewHandler(backend)

	depArn := "arn:aws:ecs:us-east-1:000000000000:service-deployment/sd-dep-cluster/sd-dep-svc/dep-1"
	backend.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
		ServiceDeploymentArn: depArn,
		ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/sd-dep-cluster",
		ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/sd-dep-cluster/sd-dep-svc",
		Status:               "IN_PROGRESS",
	})

	listResp := doECSRequest(t, h, "ListServiceDeployments", map[string]any{
		"cluster": "sd-dep-cluster",
		"service": "sd-dep-svc",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	briefs := listOut["serviceDeployments"].([]any)
	require.NotEmpty(t, briefs)
	firstArn := briefs[0].(map[string]any)["serviceDeploymentArn"].(string)

	descResp := doECSRequest(t, h, "DescribeServiceDeployments", map[string]any{
		"serviceDeploymentArns": []string{firstArn},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	deps := descOut["serviceDeployments"].([]any)
	assert.Len(t, deps, 1)
	dep := deps[0].(map[string]any)
	assert.NotEmpty(t, dep["serviceDeploymentArn"])
}

func TestStopServiceDeployment(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
	h := ecs.NewHandler(backend)

	depArn := "arn:aws:ecs:us-east-1:000000000000:service-deployment/stop-dep-cluster/stop-dep-svc/dep-2"
	backend.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
		ServiceDeploymentArn: depArn,
		ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/stop-dep-cluster",
		ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/stop-dep-cluster/stop-dep-svc",
		Status:               "IN_PROGRESS",
	})

	stopResp := doECSRequest(t, h, "StopServiceDeployment", map[string]any{
		"serviceDeploymentArn": depArn,
	})
	require.Equal(t, http.StatusOK, stopResp.Code)
	var stopOut map[string]any
	require.NoError(t, json.Unmarshal(stopResp.Body.Bytes(), &stopOut))
	dep := stopOut["serviceDeployment"].(map[string]any)
	assert.Equal(t, "STOPPED", dep["status"])

	// Stop again should fail
	stopAgain := doECSRequest(t, h, "StopServiceDeployment", map[string]any{
		"serviceDeploymentArn": depArn,
	})
	assert.Equal(t, http.StatusBadRequest, stopAgain.Code)
}

func TestECS_DescribeServiceDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input            map[string]any
		name             string
		seedDeployments  []ecs.ServiceDeployment
		wantDeployCount  int
		wantFailureCount int
		wantCode         int
	}{
		{
			name:             "describe non-existent returns failure",
			input:            map[string]any{"serviceDeploymentArns": []string{"nonexistent-arn"}},
			wantCode:         http.StatusOK,
			wantDeployCount:  0,
			wantFailureCount: 1,
		},
		{
			name: "describe existing deployment",
			seedDeployments: []ecs.ServiceDeployment{
				{
					ServiceDeploymentArn: "arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-1",
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/cluster",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/cluster/svc",
					Status:               "SUCCESSFUL",
				},
			},
			input: map[string]any{
				"serviceDeploymentArns": []string{
					"arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-1",
				},
			},
			wantCode:         http.StatusOK,
			wantDeployCount:  1,
			wantFailureCount: 0,
		},
		{
			name: "mixed found and not found",
			seedDeployments: []ecs.ServiceDeployment{
				{
					ServiceDeploymentArn: "arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-2",
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/cluster",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/cluster/svc",
					Status:               "IN_PROGRESS",
				},
			},
			input: map[string]any{
				"serviceDeploymentArns": []string{
					"arn:aws:ecs:us-east-1:000000000000:service-deployment/cluster/svc/deploy-2",
					"nonexistent",
				},
			},
			wantCode:         http.StatusOK,
			wantDeployCount:  1,
			wantFailureCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			for i := range tt.seedDeployments {
				backend.AddServiceDeploymentInternal(&tt.seedDeployments[i])
			}

			h := ecs.NewHandler(backend)
			rec := doECSRequest(t, h, "DescribeServiceDeployments", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			deployments := resp["serviceDeployments"].([]any)
			failures := resp["failures"].([]any)

			assert.Len(t, deployments, tt.wantDeployCount)
			assert.Len(t, failures, tt.wantFailureCount)
		})
	}
}

// TestECS_ListServiceDeployments verifies ListServiceDeployments.
func TestECS_ListServiceDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty cluster",
			input:    map[string]any{"cluster": "default"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "ListServiceDeployments", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			briefs, ok := resp["serviceDeployments"].([]any)
			require.True(t, ok)
			assert.Empty(t, briefs)
		})
	}
}

// TestECS_ListServiceDeployments_Shape proves ListServiceDeployments returns
// the real ServiceDeploymentBrief shape (an object list under
// "serviceDeployments") rather than a bare "serviceDeploymentArns" string
// list, and that the honestly-sourceable Brief fields are populated.
func TestECS_ListServiceDeployments_Shape(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
	h := ecs.NewHandler(backend)

	created := time.Unix(1700000000, 0)
	backend.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
		ServiceDeploymentArn:     "arn:aws:ecs:us-east-1:000000000000:service-deployment/shape-cluster/shape-svc/dep-1",
		ClusterArn:               "arn:aws:ecs:us-east-1:000000000000:cluster/shape-cluster",
		ServiceArn:               "arn:aws:ecs:us-east-1:000000000000:service/shape-cluster/shape-svc",
		Status:                   "IN_PROGRESS",
		CreatedAt:                &created,
		TargetServiceRevisionArn: "arn:aws:ecs:us-east-1:000000000000:service-revision/shape-cluster/shape-svc/1",
	})

	rec := doECSRequest(t, h, "ListServiceDeployments", map[string]any{
		"cluster": "shape-cluster",
		"service": "shape-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasOldShape := resp["serviceDeploymentArns"]
	assert.False(t, hasOldShape, "response must not carry the bare serviceDeploymentArns shape")

	briefs, ok := resp["serviceDeployments"].([]any)
	require.True(t, ok, "response must carry a serviceDeployments object list")
	require.Len(t, briefs, 1)

	brief := briefs[0].(map[string]any)
	assert.Equal(t,
		"arn:aws:ecs:us-east-1:000000000000:service-deployment/shape-cluster/shape-svc/dep-1",
		brief["serviceDeploymentArn"])
	assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:cluster/shape-cluster", brief["clusterArn"])
	assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:service/shape-cluster/shape-svc", brief["serviceArn"])
	assert.Equal(t, "IN_PROGRESS", brief["status"])
	assert.InDelta(t, float64(1700000000), brief["createdAt"], 0)
	assert.InDelta(t, float64(1700000000), brief["startedAt"], 0)
	assert.Equal(t,
		"arn:aws:ecs:us-east-1:000000000000:service-revision/shape-cluster/shape-svc/1",
		brief["targetServiceRevisionArn"])
	assert.Nil(t, brief["finishedAt"], "in-progress deployment must not report a finishedAt")
}

// TestECS_StopServiceDeployment verifies StopServiceDeployment.
func TestECS_StopServiceDeployment(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:ecs:us-east-1:000000000000:service-deployment/test-cluster/my-service/deploy-1"

	tests := []struct {
		input    map[string]any
		setup    func(h *ecs.Handler)
		name     string
		wantCode int
	}{
		{
			name: "stop in-progress deployment",
			setup: func(h *ecs.Handler) {
				now := time.Now()
				b, ok := h.Backend.(*ecs.InMemoryBackend)
				if !ok {
					return
				}
				b.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
					ServiceDeploymentArn: arn,
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/test-cluster",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/test-cluster/my-service",
					Status:               "IN_PROGRESS",
					CreatedAt:            &now,
				})
			},
			input:    map[string]any{"serviceDeploymentArn": arn},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			input:    map[string]any{"serviceDeploymentArn": "arn:not-exist"},
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

			rec := doECSRequest(t, h, "StopServiceDeployment", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			sd, ok := resp["serviceDeployment"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "STOPPED", sd["status"])
		})
	}
}

// TestECS_ContinueServiceDeployment verifies real-vs-honest-error behavior:
// this backend never pauses a deployment at a lifecycle hook (blue/green
// PAUSE stages aren't modeled), so ContinueServiceDeployment must validate
// the deployment/hookId inputs for real and report ClientException rather
// than fabricating a successful continue/rollback.
func TestECS_ContinueServiceDeployment(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:ecs:us-east-1:000000000000:service-deployment/test-cluster/my-service/deploy-1"

	tests := []struct {
		input        map[string]any
		setup        func(h *ecs.Handler)
		name         string
		wantTypeIsIn []string
		wantCode     int
	}{
		{
			name: "existing deployment, no paused hook",
			setup: func(h *ecs.Handler) {
				now := time.Now()
				b, ok := h.Backend.(*ecs.InMemoryBackend)
				if !ok {
					return
				}
				b.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
					ServiceDeploymentArn: arn,
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/test-cluster",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/test-cluster/my-service",
					Status:               "IN_PROGRESS",
					CreatedAt:            &now,
				})
			},
			input:        map[string]any{"serviceDeploymentArn": arn, "hookId": "hook-1"},
			wantCode:     http.StatusBadRequest,
			wantTypeIsIn: []string{"ClientException"},
		},
		{
			name:         "deployment not found",
			input:        map[string]any{"serviceDeploymentArn": "arn:not-exist", "hookId": "hook-1"},
			wantCode:     http.StatusBadRequest,
			wantTypeIsIn: []string{"ServiceDeploymentNotFoundException"},
		},
		{
			name:         "missing hookId",
			input:        map[string]any{"serviceDeploymentArn": arn},
			wantCode:     http.StatusBadRequest,
			wantTypeIsIn: []string{"InvalidParameterException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECSRequest(t, h, "ContinueServiceDeployment", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, tt.wantTypeIsIn, resp["__type"])
		})
	}
}
