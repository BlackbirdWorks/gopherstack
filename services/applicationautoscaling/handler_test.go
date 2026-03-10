package applicationautoscaling_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

func newTestHandler(t *testing.T) *applicationautoscaling.Handler {
	t.Helper()

	return applicationautoscaling.NewHandler(applicationautoscaling.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *applicationautoscaling.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AnyScaleFrontendService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func doInvalidRequest(t *testing.T, h *applicationautoscaling.Handler, action string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AnyScaleFrontendService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ApplicationAutoscaling", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "RegisterScalableTarget")
	assert.Contains(t, ops, "DeregisterScalableTarget")
	assert.Contains(t, ops, "DescribeScalableTargets")
	assert.Contains(t, ops, "PutScalingPolicy")
	assert.Contains(t, ops, "DeleteScalingPolicy")
	assert.Contains(t, ops, "DescribeScalingPolicies")
	assert.Contains(t, ops, "DescribeScalingActivities")
	assert.Contains(t, ops, "PutScheduledAction")
	assert.Contains(t, ops, "DeleteScheduledAction")
	assert.Contains(t, ops, "DescribeScheduledActions")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{name: "match", target: "AnyScaleFrontendService.RegisterScalableTarget", wantMatch: true},
		{name: "no_match", target: "AWSScheduler.CreateSchedule", wantMatch: false},
		{name: "empty", target: "", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AnyScaleFrontendService.RegisterScalableTarget")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "RegisterScalableTarget", h.ExtractOperation(c))
}

func TestHandler_RegisterScalableTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name: "create",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       int32(1),
				"MaxCapacity":       int32(10),
			},
			wantCode: http.StatusOK,
			wantKey:  "ScalableTargetARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "RegisterScalableTarget", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, tt.wantKey)
			assert.Contains(t, resp[tt.wantKey].(string), "arn:aws:application-autoscaling:")
		})
	}
}

func TestHandler_RegisterScalableTarget_Upsert(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	}

	// Create
	rec1 := doRequest(t, h, "RegisterScalableTarget", body)
	require.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]string
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	// Update (upsert) - should update, not error
	body["MaxCapacity"] = int32(20)
	rec2 := doRequest(t, h, "RegisterScalableTarget", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	// Verify the updated capacity
	descRec := doRequest(t, h, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	targets := descResp["ScalableTargets"].([]any)
	require.Len(t, targets, 1)
	target := targets[0].(map[string]any)
	assert.InDelta(t, float64(20), target["MaxCapacity"], 0)
}

func TestHandler_DeregisterScalableTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  int
		preCreate bool
	}{
		{name: "success", preCreate: true, wantCode: http.StatusOK},
		{name: "not_found", preCreate: false, wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.preCreate {
				doRequest(t, h, "RegisterScalableTarget", map[string]any{
					"ServiceNamespace":  "ecs",
					"ResourceId":        "service/default/my-svc",
					"ScalableDimension": "ecs:service:DesiredCount",
					"MinCapacity":       int32(1),
					"MaxCapacity":       int32(10),
				})
			}

			rec := doRequest(t, h, "DeregisterScalableTarget", map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeScalableTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{name: "all", filter: "", wantCount: 2},
		{name: "filtered_ecs", filter: "ecs", wantCount: 1},
		{name: "filtered_dynamodb", filter: "dynamodb", wantCount: 1},
		{name: "filtered_no_match", filter: "rds", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "RegisterScalableTarget", map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/svc1",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       int32(1),
				"MaxCapacity":       int32(5),
			})
			doRequest(t, h, "RegisterScalableTarget", map[string]any{
				"ServiceNamespace":  "dynamodb",
				"ResourceId":        "table/my-table",
				"ScalableDimension": "dynamodb:table:ReadCapacityUnits",
				"MinCapacity":       int32(5),
				"MaxCapacity":       int32(100),
			})

			rec := doRequest(t, h, "DescribeScalableTargets", map[string]any{"ServiceNamespace": tt.filter})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			targets, ok := resp["ScalableTargets"].([]any)
			require.True(t, ok)
			assert.Len(t, targets, tt.wantCount)
		})
	}
}

func TestHandler_PutScalingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "my-policy",
		"PolicyType":        "TargetTrackingScaling",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["PolicyARN"], "arn:aws:autoscaling:")
	assert.Contains(t, resp["PolicyARN"], "scalingPolicy:")
}

func TestHandler_DeleteScalingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preCreate bool
		wantCode  int
	}{
		{name: "success", preCreate: true, wantCode: http.StatusOK},
		{name: "not_found", preCreate: false, wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.preCreate {
				doRequest(t, h, "PutScalingPolicy", map[string]any{
					"ServiceNamespace":  "ecs",
					"ResourceId":        "service/default/my-svc",
					"ScalableDimension": "ecs:service:DesiredCount",
					"PolicyName":        "my-policy",
					"PolicyType":        "TargetTrackingScaling",
				})
			}

			rec := doRequest(t, h, "DeleteScalingPolicy", map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "my-policy",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeScalingPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{name: "all", filter: "", wantCount: 2},
		{name: "filtered", filter: "ecs", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "PutScalingPolicy", map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/svc1",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "policy-ecs",
				"PolicyType":        "TargetTrackingScaling",
			})
			doRequest(t, h, "PutScalingPolicy", map[string]any{
				"ServiceNamespace":  "dynamodb",
				"ResourceId":        "table/t1",
				"ScalableDimension": "dynamodb:table:ReadCapacityUnits",
				"PolicyName":        "policy-ddb",
				"PolicyType":        "TargetTrackingScaling",
			})

			rec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{"ServiceNamespace": tt.filter})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policies, ok := resp["ScalingPolicies"].([]any)
			require.True(t, ok)
			assert.Len(t, policies, tt.wantCount)
		})
	}
}

func TestHandler_DescribeScalingActivities(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeScalingActivities", map[string]any{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	activities, ok := resp["ScalingActivities"].([]any)
	require.True(t, ok)
	assert.Empty(t, activities)
}

func TestHandler_PutScheduledAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "scale-up",
		"Schedule":            "cron(0 9 * * ? *)",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ScheduledActionARN"], "arn:aws:autoscaling:")
	assert.Contains(t, resp["ScheduledActionARN"], "scheduledAction:")
}

func TestHandler_PutScheduledAction_Upsert(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	base := map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "scale-up",
		"Schedule":            "cron(0 9 * * ? *)",
	}

	rec1 := doRequest(t, h, "PutScheduledAction", base)
	require.Equal(t, http.StatusOK, rec1.Code)

	base["Schedule"] = "cron(0 10 * * ? *)"
	rec2 := doRequest(t, h, "PutScheduledAction", base)
	require.Equal(t, http.StatusOK, rec2.Code)

	// Should only have one action
	descRec := doRequest(t, h, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	actions := descResp["ScheduledActions"].([]any)
	assert.Len(t, actions, 1)
}

func TestHandler_DeleteScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preCreate bool
		wantCode  int
	}{
		{name: "success", preCreate: true, wantCode: http.StatusOK},
		{name: "not_found", preCreate: false, wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.preCreate {
				doRequest(t, h, "PutScheduledAction", map[string]any{
					"ServiceNamespace":    "ecs",
					"ResourceId":          "service/default/my-svc",
					"ScalableDimension":   "ecs:service:DesiredCount",
					"ScheduledActionName": "scale-up",
					"Schedule":            "cron(0 9 * * ? *)",
				})
			}

			rec := doRequest(t, h, "DeleteScheduledAction", map[string]any{
				"ServiceNamespace":    "ecs",
				"ResourceId":          "service/default/my-svc",
				"ScalableDimension":   "ecs:service:DesiredCount",
				"ScheduledActionName": "scale-up",
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{name: "all", filter: "", wantCount: 2},
		{name: "filtered", filter: "ecs", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "PutScheduledAction", map[string]any{
				"ServiceNamespace":    "ecs",
				"ResourceId":          "service/default/svc1",
				"ScalableDimension":   "ecs:service:DesiredCount",
				"ScheduledActionName": "action-ecs",
				"Schedule":            "rate(1 hour)",
			})
			doRequest(t, h, "PutScheduledAction", map[string]any{
				"ServiceNamespace":    "dynamodb",
				"ResourceId":          "table/t1",
				"ScalableDimension":   "dynamodb:table:ReadCapacityUnits",
				"ScheduledActionName": "action-ddb",
				"Schedule":            "rate(2 hours)",
			})

			rec := doRequest(t, h, "DescribeScheduledActions", map[string]any{"ServiceNamespace": tt.filter})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			actions, ok := resp["ScheduledActions"].([]any)
			require.True(t, ok)
			assert.Len(t, actions, tt.wantCount)
		})
	}
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	resourceARN := createResp["ScalableTargetARN"]

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": resourceARN,
		"Tags":        map[string]string{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	resourceARN := createResp["ScalableTargetARN"]

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": resourceARN,
		"Tags":        map[string]string{"env": "prod"},
	})

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": resourceARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	resourceARN := createResp["ScalableTargetARN"]

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": resourceARN,
		"Tags":        map[string]string{"env": "prod", "team": "platform"},
	})

	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": resourceARN,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": resourceARN})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "platform", tags["team"])
}

func TestHandler_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "DeregisterScalableTarget_NotFound",
			action: "DeregisterScalableTarget",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/nonexistent",
				"ScalableDimension": "ecs:service:DesiredCount",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteScalingPolicy_NotFound",
			action: "DeleteScalingPolicy",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "nonexistent",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteScheduledAction_NotFound",
			action: "DeleteScheduledAction",
			body: map[string]any{
				"ServiceNamespace":    "ecs",
				"ResourceId":          "service/default/my-svc",
				"ScalableDimension":   "ecs:service:DesiredCount",
				"ScheduledActionName": "nonexistent",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "TagResource_NotFound",
			action: "TagResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:application-autoscaling:us-east-1:000000000000:scalable-target/nonexistent",
				"Tags":        map[string]string{"env": "test"},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListTagsForResource_NotFound",
			action: "ListTagsForResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:application-autoscaling:us-east-1:000000000000:scalable-target/nonexistent",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UntagResource_NotFound",
			action: "UntagResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:application-autoscaling:us-east-1:000000000000:scalable-target/nonexistent",
				"TagKeys":     []string{"env"},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UnknownAction",
			action:   "UnknownAction",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{name: "RegisterScalableTarget", action: "RegisterScalableTarget", wantCode: http.StatusBadRequest},
		{name: "DeregisterScalableTarget", action: "DeregisterScalableTarget", wantCode: http.StatusBadRequest},
		{name: "PutScalingPolicy", action: "PutScalingPolicy", wantCode: http.StatusBadRequest},
		{name: "DeleteScalingPolicy", action: "DeleteScalingPolicy", wantCode: http.StatusBadRequest},
		{name: "TagResource", action: "TagResource", wantCode: http.StatusBadRequest},
		{name: "UntagResource", action: "UntagResource", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doInvalidRequest(t, h, tt.action)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestProvider(t *testing.T) {
	t.Parallel()

	p := &applicationautoscaling.Provider{}
	assert.Equal(t, "ApplicationAutoscaling", p.Name())
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &applicationautoscaling.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "ApplicationAutoscaling", svc.Name())
}

func TestPersistence_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(snap))

	rec := doRequest(t, h2, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	targets := resp["ScalableTargets"].([]any)
	assert.Len(t, targets, 1)
}
