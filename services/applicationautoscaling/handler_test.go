package applicationautoscaling_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func TestHandler_DescribeScalingActivities_AfterRegister(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	regRec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	rec := doRequest(t, h, "DescribeScalingActivities", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	activities, ok := resp["ScalingActivities"].([]any)
	require.True(t, ok)
	require.Len(t, activities, 1)

	act, ok := activities[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ecs", act["ServiceNamespace"])
	assert.Equal(t, "service/default/my-svc", act["ResourceId"])
	assert.Equal(t, "Successful", act["StatusCode"])
}

func TestHandler_DescribeScalingActivities_MissingNamespace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeScalingActivities", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestApplicationAutoScaling_Handler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		createTargets int
		wantAfter     int
	}{
		{
			name:          "reset clears all scalable targets",
			createTargets: 2,
			wantAfter:     0,
		},
		{
			name:          "reset on empty backend is a no-op",
			createTargets: 0,
			wantAfter:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createTargets {
				rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
					"ServiceNamespace":  "ecs",
					"ResourceId":        fmt.Sprintf("service/cluster/svc-%d", i),
					"ScalableDimension": "ecs:service:DesiredCount",
					"MinCapacity":       1,
					"MaxCapacity":       10,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			rec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
				"ServiceNamespace": "ecs",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			targets, _ := out["ScalableTargets"].([]any)
			assert.Len(t, targets, tt.wantAfter)
		})
	}
}

func TestHandler_GetPredictiveScalingForecast(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCode  int
		preCreate bool
	}{
		{
			name:      "success",
			preCreate: true,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "2024-01-01T00:00:00Z",
				"EndTime":           "2024-01-01T03:00:00Z",
			},
			wantCode: http.StatusOK,
		},
		{
			name:      "policy_not_found",
			preCreate: false,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "nonexistent-policy",
				"StartTime":         "2024-01-01T00:00:00Z",
				"EndTime":           "2024-01-01T03:00:00Z",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:      "invalid_start_time",
			preCreate: false,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "not-a-time",
				"EndTime":           "2024-01-01T03:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:      "invalid_end_time",
			preCreate: false,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "2024-01-01T00:00:00Z",
				"EndTime":           "not-a-time",
			},
			wantCode: http.StatusBadRequest,
		},
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
					"PolicyName":        "predictive-policy",
					"PolicyType":        "PredictiveScaling",
				})
			}

			rec := doRequest(t, h, "GetPredictiveScalingForecast", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetPredictiveScalingForecast_DataPoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"PolicyType":        "PredictiveScaling",
	})

	rec := doRequest(t, h, "GetPredictiveScalingForecast", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"StartTime":         "2024-01-01T00:00:00Z",
		"EndTime":           "2024-01-01T02:00:00Z",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// CapacityForecast should be present with timestamps and values
	cf, ok := resp["CapacityForecast"].(map[string]any)
	require.True(t, ok, "expected CapacityForecast in response")

	timestamps, ok := cf["Timestamps"].([]any)
	require.True(t, ok, "expected Timestamps in CapacityForecast")
	// 00:00→02:00 = exactly 2 hourly timestamps (00:00 and 01:00); EndTime is excluded.
	assert.Len(t, timestamps, 2, "expected exactly 2 hourly timestamps for 00:00→02:00 window")
	assert.Equal(t, "2024-01-01T00:00:00Z", timestamps[0], "first timestamp should be start of window")
	assert.Equal(t, "2024-01-01T01:00:00Z", timestamps[1], "second timestamp should be start+1h")

	values, ok := cf["Values"].([]any)
	require.True(t, ok, "expected Values in CapacityForecast")
	assert.Len(t, values, 2, "values count must match timestamps count")

	// LoadForecast should be a non-empty array
	lf, ok := resp["LoadForecast"].([]any)
	require.True(t, ok, "expected LoadForecast in response")
	assert.NotEmpty(t, lf)

	// UpdateTime should be a non-empty string
	updateTime, ok := resp["UpdateTime"].(string)
	require.True(t, ok, "expected UpdateTime in response")
	assert.NotEmpty(t, updateTime)
}

func TestHandler_RegisterScalableTarget_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_namespace",
			body: map[string]any{
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       int32(1),
				"MaxCapacity":       int32(10),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_resource_id",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       int32(1),
				"MaxCapacity":       int32(10),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_scalable_dimension",
			body: map[string]any{
				"ServiceNamespace": "ecs",
				"ResourceId":       "service/default/my-svc",
				"MinCapacity":      int32(1),
				"MaxCapacity":      int32(10),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "min_exceeds_max",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       int32(20),
				"MaxCapacity":       int32(5),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "equal_min_max",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       int32(5),
				"MaxCapacity":       int32(5),
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "RegisterScalableTarget", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutScalingPolicy_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_namespace",
			body: map[string]any{
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "my-policy",
				"PolicyType":        "TargetTrackingScaling",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_policy_name",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyType":        "TargetTrackingScaling",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_policy_type",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "my-policy",
				"PolicyType":        "InvalidType",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_policy_type_allowed",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "my-policy",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "PutScalingPolicy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutScheduledAction_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_namespace",
			body: map[string]any{
				"ResourceId":          "service/default/my-svc",
				"ScalableDimension":   "ecs:service:DesiredCount",
				"ScheduledActionName": "my-action",
				"Schedule":            "rate(5 minutes)",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_action_name",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"Schedule":          "rate(5 minutes)",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "PutScheduledAction", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetPredictiveScalingForecast_TimeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "end_before_start",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "2024-01-02T00:00:00Z",
				"EndTime":           "2024-01-01T00:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "equal_start_end",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "2024-01-01T00:00:00Z",
				"EndTime":           "2024-01-01T00:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "window_exceeds_14_days",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "2024-01-01T00:00:00Z",
				"EndTime":           "2024-01-16T00:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "PutScalingPolicy", map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"PolicyType":        "PredictiveScaling",
			})

			rec := doRequest(t, h, "GetPredictiveScalingForecast", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeScalableTargets_HasTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})

	rec := doRequest(t, h, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	targets := resp["ScalableTargets"].([]any)
	require.Len(t, targets, 1)

	target := targets[0].(map[string]any)
	assert.NotEmpty(t, target["CreationTime"], "expected CreationTime in response")
	assert.NotEmpty(t, target["LastModifiedTime"], "expected LastModifiedTime in response")
}

func TestHandler_DescribeScalableTargets_HasTags(t *testing.T) {
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

	var registerResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &registerResp))

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": registerResp["ScalableTargetARN"],
		"Tags":        map[string]string{"env": "prod"},
	})

	descRec := doRequest(t, h, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	targets := resp["ScalableTargets"].([]any)
	require.Len(t, targets, 1)

	target := targets[0].(map[string]any)
	tags, ok := target["Tags"].(map[string]any)
	require.True(t, ok, "expected Tags in DescribeScalableTargets response")
	assert.Equal(t, "prod", tags["env"])
}

func TestHandler_DescribeScalingPolicies_RicherFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name:      "filter_by_resource_id",
			body:      map[string]any{"ResourceId": "service/default/svc1"},
			wantCount: 1,
		},
		{
			name:      "filter_by_policy_names",
			body:      map[string]any{"PolicyNames": []string{"policy-ecs"}},
			wantCount: 1,
		},
		{
			name:      "filter_by_scalable_dimension",
			body:      map[string]any{"ScalableDimension": "dynamodb:table:ReadCapacityUnits"},
			wantCount: 1,
		},
		{
			name:      "no_filter_returns_all",
			body:      map[string]any{},
			wantCount: 2,
		},
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

			rec := doRequest(t, h, "DescribeScalingPolicies", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policies, ok := resp["ScalingPolicies"].([]any)
			require.True(t, ok)
			assert.Len(t, policies, tt.wantCount)
		})
	}
}

func TestHandler_DescribeScheduledActions_RicherFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name:      "filter_by_resource_id",
			body:      map[string]any{"ResourceId": "service/default/svc1"},
			wantCount: 1,
		},
		{
			name:      "filter_by_action_names",
			body:      map[string]any{"ScheduledActionNames": []string{"action-ecs"}},
			wantCount: 1,
		},
		{
			name:      "no_filter_returns_all",
			body:      map[string]any{},
			wantCount: 2,
		},
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

			rec := doRequest(t, h, "DescribeScheduledActions", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			actions, ok := resp["ScheduledActions"].([]any)
			require.True(t, ok)
			assert.Len(t, actions, tt.wantCount)
		})
	}
}

func TestHandler_PersistenceRebuildsSecondaryIndexes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "my-policy",
		"PolicyType":        "TargetTrackingScaling",
	})
	doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "my-action",
		"Schedule":            "rate(1 hour)",
	})

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(snap))

	// Delete should work via secondary index (O(1) lookup)
	rec := doRequest(t, h2, "DeleteScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "my-policy",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h2, "DeleteScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "my-action",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_RegisterScalableTarget_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/tagged-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
		"Tags":              map[string]string{"env": "prod", "team": "infra"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var regResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
	targetARN, _ := regResp["ScalableTargetARN"].(string)
	require.NotEmpty(t, targetARN)

	// Tags should be visible via ListTagsForResource
	tagRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": targetARN})
	require.Equal(t, http.StatusOK, tagRec.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(tagRec.Body.Bytes(), &tagResp))
	tags, ok := tagResp["Tags"].(map[string]any)
	require.True(t, ok, "expected Tags in response")
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])
}

func TestHandler_RegisterScalableTarget_WithRoleARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	roleARN := "arn:aws:iam::123456789012:role/ApplicationAutoScalingRole"
	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
		"RoleARN":           roleARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// RoleARN should be visible in DescribeScalableTargets
	descRec := doRequest(t, h, "DescribeScalableTargets", map[string]any{"ServiceNamespace": "ecs"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	targets, ok := descResp["ScalableTargets"].([]any)
	require.True(t, ok)
	require.Len(t, targets, 1)
	target := targets[0].(map[string]any)
	assert.Equal(t, roleARN, target["RoleARN"])
}

func TestHandler_DescribeScalableTargets_ResourceIdsFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, svc := range []string{"svc-a", "svc-b", "svc-c"} {
		doRequest(t, h, "RegisterScalableTarget", map[string]any{
			"ServiceNamespace":  "ecs",
			"ResourceId":        "service/default/" + svc,
			"ScalableDimension": "ecs:service:DesiredCount",
			"MinCapacity":       int32(1),
			"MaxCapacity":       int32(5),
		})
	}

	// Filter by two specific resource IDs
	rec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
		"ServiceNamespace": "ecs",
		"ResourceIds":      []string{"service/default/svc-a", "service/default/svc-c"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	targets, ok := resp["ScalableTargets"].([]any)
	require.True(t, ok)
	assert.Len(t, targets, 2, "expected exactly 2 targets when filtering by ResourceIds")
}

func TestHandler_DescribeScalableTargets_ScalableDimensionFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
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

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name:      "filter_by_dimension_ecs",
			body:      map[string]any{"ServiceNamespace": "ecs", "ScalableDimension": "ecs:service:DesiredCount"},
			wantCount: 1,
		},
		{
			name: "filter_by_dimension_no_match",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ScalableDimension": "dynamodb:table:ReadCapacityUnits",
			},
			wantCount: 0,
		},
		{
			name:      "no_dimension_filter_returns_all",
			body:      map[string]any{},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "DescribeScalableTargets", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			targets, ok := resp["ScalableTargets"].([]any)
			require.True(t, ok)
			assert.Len(t, targets, tt.wantCount)
		})
	}
}

func TestHandler_PutScheduledAction_WithScalableTargetAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	minCap := int32(2)
	maxCap := int32(20)
	rec := doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "scale-up-morning",
		"Schedule":            "cron(0 8 * * ? *)",
		"ScalableTargetAction": map[string]any{
			"MinCapacity": minCap,
			"MaxCapacity": maxCap,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify ScalableTargetAction is returned in DescribeScheduledActions
	descRec := doRequest(t, h, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": "ecs",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	actions, ok := resp["ScheduledActions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	sta, ok := action["ScalableTargetAction"].(map[string]any)
	require.True(t, ok, "expected ScalableTargetAction in response")
	assert.InDelta(t, float64(2), sta["MinCapacity"], 0)
	assert.InDelta(t, float64(20), sta["MaxCapacity"], 0)
}

func TestHandler_PutScalingPolicy_DefaultPolicyType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Omit PolicyType — should default to TargetTrackingScaling
	rec := doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "default-type-policy",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "ecs",
		"PolicyNames":      []string{"default-type-policy"},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	policies, ok := resp["ScalingPolicies"].([]any)
	require.True(t, ok)
	require.Len(t, policies, 1)
	assert.Equal(t, "TargetTrackingScaling", policies[0].(map[string]any)["PolicyType"])
}

func TestHandler_TagResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_arn",
			body:     map[string]any{"ResourceARN": "", "Tags": map[string]string{"k": "v"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: map[string]any{
				"ResourceARN": "arn:aws:autoscaling:us-east-1:000000000000:scalable-target/no-such",
				"Tags":        map[string]string{"k": "v"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_UntagResource_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "",
		"TagKeys":     []string{"key"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeregisterScalableTarget_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_service_namespace",
			body: map[string]any{
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_resource_id",
			body:     map[string]any{"ServiceNamespace": "ecs", "ScalableDimension": "ecs:service:DesiredCount"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_scalable_dimension",
			body:     map[string]any{"ServiceNamespace": "ecs", "ResourceId": "service/default/my-svc"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DeregisterScalableTarget", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteScalingPolicy_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_policy_name",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_service_namespace",
			body: map[string]any{
				"PolicyName":        "my-policy",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DeleteScalingPolicy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteScheduledAction_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_action_name",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_service_namespace",
			body: map[string]any{
				"ScheduledActionName": "my-action",
				"ResourceId":          "service/default/my-svc",
				"ScalableDimension":   "ecs:service:DesiredCount",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DeleteScheduledAction", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetPredictiveScalingForecast_NonHourBoundaryStart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"PolicyType":        "PredictiveScaling",
	})

	// StartTime is mid-hour (00:30). First complete hour boundary >= 00:30 is 01:00.
	// EndTime is 03:00. So expected timestamps: 01:00, 02:00 (2 points).
	rec := doRequest(t, h, "GetPredictiveScalingForecast", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"StartTime":         "2024-01-01T00:30:00Z",
		"EndTime":           "2024-01-01T03:00:00Z",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cf, ok := resp["CapacityForecast"].(map[string]any)
	require.True(t, ok)

	timestamps, ok := cf["Timestamps"].([]any)
	require.True(t, ok)

	// All timestamps must be >= StartTime (no timestamp before 00:30)
	for _, ts := range timestamps {
		tsStr, isStr := ts.(string)
		require.True(t, isStr)
		assert.GreaterOrEqual(t, tsStr, "2024-01-01T00:30:00Z",
			"timestamp %s must not precede StartTime 00:30", tsStr)
	}
	assert.Len(t, timestamps, 2, "expected 2 hourly points (01:00, 02:00) for 00:30→03:00 window")
	assert.Equal(t, "2024-01-01T01:00:00Z", timestamps[0])
	assert.Equal(t, "2024-01-01T02:00:00Z", timestamps[1])
}

func TestHandler_MaxResults_DescribeScalableTargets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 5 {
		doRequest(t, h, "RegisterScalableTarget", map[string]any{
			"ServiceNamespace":  "ecs",
			"ResourceId":        fmt.Sprintf("service/default/svc-%d", i),
			"ScalableDimension": "ecs:service:DesiredCount",
			"MinCapacity":       int32(1),
			"MaxCapacity":       int32(5),
		})
	}

	tests := []struct {
		name       string
		maxResults int32
		wantLen    int
	}{
		{name: "no_limit", maxResults: 0, wantLen: 5},
		{name: "limit_2", maxResults: 2, wantLen: 2},
		{name: "limit_3", maxResults: 3, wantLen: 3},
		{name: "limit_exceeds_count", maxResults: 100, wantLen: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{"ServiceNamespace": "ecs"}
			if tt.maxResults > 0 {
				body["MaxResults"] = tt.maxResults
			}

			rec := doRequest(t, h, "DescribeScalableTargets", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			targets, ok := resp["ScalableTargets"].([]any)
			require.True(t, ok)
			assert.Len(t, targets, tt.wantLen)
		})
	}
}

func TestHandler_MaxResults_DescribeScalingPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 4 {
		doRequest(t, h, "PutScalingPolicy", map[string]any{
			"ServiceNamespace":  "ecs",
			"ResourceId":        "service/default/my-svc",
			"ScalableDimension": "ecs:service:DesiredCount",
			"PolicyName":        fmt.Sprintf("policy-%d", i),
			"PolicyType":        "TargetTrackingScaling",
		})
	}

	rec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "ecs",
		"MaxResults":       int32(2),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	policies, ok := resp["ScalingPolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, policies, 2)
}

func TestHandler_MaxResults_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 4 {
		doRequest(t, h, "PutScheduledAction", map[string]any{
			"ServiceNamespace":    "ecs",
			"ResourceId":          "service/default/my-svc",
			"ScalableDimension":   "ecs:service:DesiredCount",
			"ScheduledActionName": fmt.Sprintf("action-%d", i),
			"Schedule":            "rate(1 hour)",
		})
	}

	rec := doRequest(t, h, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": "ecs",
		"MaxResults":       int32(2),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	actions, ok := resp["ScheduledActions"].([]any)
	require.True(t, ok)
	assert.Len(t, actions, 2)
}

func TestHandler_DescribeScalingPolicies_PolicyARNsFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arns := make([]string, 3)
	for i := range 3 {
		rec := doRequest(t, h, "PutScalingPolicy", map[string]any{
			"ServiceNamespace":  "ecs",
			"ResourceId":        "service/default/my-svc",
			"ScalableDimension": "ecs:service:DesiredCount",
			"PolicyName":        fmt.Sprintf("policy-%d", i),
			"PolicyType":        "TargetTrackingScaling",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		arns[i] = out["PolicyARN"].(string)
	}

	// Filter by first two ARNs only
	rec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "ecs",
		"PolicyARNs":       []string{arns[0], arns[1]},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	policies, ok := resp["ScalingPolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, policies, 2, "expected exactly 2 policies when filtering by 2 ARNs")
}

func TestHandler_PutScheduledAction_StartEndTimeTimezone(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "morning-scale",
		"Schedule":            "cron(0 8 * * ? *)",
		"Timezone":            "America/New_York",
		"StartTime":           "2024-01-01T08:00:00Z",
		"EndTime":             "2024-12-31T08:00:00Z",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doRequest(t, h, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": "ecs",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	actions, ok := resp["ScheduledActions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	assert.Equal(t, "America/New_York", action["Timezone"])
	assert.NotNil(t, action["StartTime"], "expected StartTime in response")
	assert.NotNil(t, action["EndTime"], "expected EndTime in response")
}

func TestHandler_PutScheduledAction_InvalidStartTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "bad-action",
		"Schedule":            "rate(1 hour)",
		"StartTime":           "not-a-timestamp",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_RegisterScalableTarget_WithSuspendedState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
		"SuspendedState": map[string]any{
			"DynamicScalingInSuspended":  true,
			"DynamicScalingOutSuspended": false,
			"ScheduledScalingSuspended":  true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
		"ServiceNamespace": "ecs",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	targets, ok := resp["ScalableTargets"].([]any)
	require.True(t, ok)
	require.Len(t, targets, 1)

	ss, ok := targets[0].(map[string]any)["SuspendedState"].(map[string]any)
	require.True(t, ok, "expected SuspendedState in response")
	assert.Equal(t, true, ss["DynamicScalingInSuspended"])
	assert.Equal(t, false, ss["DynamicScalingOutSuspended"])
	assert.Equal(t, true, ss["ScheduledScalingSuspended"])
}

func TestHandler_ListTagsForResource_EmptyARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_TagResource_ExceedsLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a scalable target
	regRec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	var regResp map[string]any
	require.NoError(t, json.Unmarshal(regRec.Body.Bytes(), &regResp))
	targetARN, _ := regResp["ScalableTargetARN"].(string)
	require.NotEmpty(t, targetARN)

	// Add 50 tags (the maximum allowed)
	tags := make(map[string]string, 50)
	for i := range 50 {
		tags[fmt.Sprintf("key-%d", i)] = "value"
	}
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": targetARN,
		"Tags":        tags,
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// Adding one more distinct tag should fail with 400
	overRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": targetARN,
		"Tags":        map[string]string{"new-key": "value"},
	})
	assert.Equal(t, http.StatusBadRequest, overRec.Code)
}

func TestHandler_RegisterScalableTarget_TagLimitEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Build exactly 51 tags (one over the limit)
	tags := make(map[string]string, 51)
	for i := range 51 {
		tags[fmt.Sprintf("key-%d", i)] = "value"
	}

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
		"Tags":              tags,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 when tag count exceeds 50")
}

func TestHandler_Backend_Purge(t *testing.T) {
	t.Parallel()

	b := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.RegisterScalableTarget(
		"ecs", "service/default/my-svc", "ecs:service:DesiredCount", 1, 5, nil, "", nil,
	)
	require.NoError(t, err)

	b.Purge()
	targets, _ := b.DescribeScalableTargets(applicationautoscaling.DescribeScalableTargetsFilter{})
	assert.Empty(t, targets, "Purge should clear all scalable targets")
}

func TestHandler_ChaosAndRegion(t *testing.T) {
	t.Parallel()

	b := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	h := applicationautoscaling.NewHandler(b)

	tests := []struct {
		want any
		fn   func() any
		name string
	}{
		{
			name: "ChaosServiceName",
			fn:   func() any { return h.ChaosServiceName() },
			want: "applicationautoscaling",
		},
		{
			name: "ChaosOperations",
			fn:   func() any { return len(h.ChaosOperations()) > 0 },
			want: true,
		},
		{
			name: "ChaosRegions",
			fn:   func() any { return h.ChaosRegions() },
			want: []string{"us-east-1"},
		},
		{
			name: "Region",
			fn:   func() any { return b.Region() },
			want: "us-east-1",
		},
		{
			name: "ExtractResource",
			fn:   func() any { return h.ExtractResource(nil) },
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.fn())
		})
	}
}

func TestHandler_DeregisterScalableTarget_CascadeDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a scalable target
	doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
	})

	// Add a policy
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "my-policy",
		"PolicyType":        "TargetTrackingScaling",
	})

	// Add a scheduled action
	doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "my-action",
		"Schedule":            "rate(1 hour)",
	})

	// Deregister the target
	rec := doRequest(t, h, "DeregisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
	})
	require.Equal(t, http.StatusOK, rec.Code, "deregister should succeed")

	// Verify the target is gone
	targetsRec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
		"ServiceNamespace": "ecs",
	})
	var targetsResp map[string]any
	require.NoError(t, json.Unmarshal(targetsRec.Body.Bytes(), &targetsResp))
	targets, _ := targetsResp["ScalableTargets"].([]any)
	assert.Empty(t, targets, "scalable targets should be empty after deregister")

	// Verify cascade: policies should also be gone
	policiesRec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "ecs",
	})
	var policiesResp map[string]any
	require.NoError(t, json.Unmarshal(policiesRec.Body.Bytes(), &policiesResp))
	policies, _ := policiesResp["ScalingPolicies"].([]any)
	assert.Empty(t, policies, "scaling policies should be cascade-deleted")

	// Verify cascade: scheduled actions should also be gone
	actionsRec := doRequest(t, h, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": "ecs",
	})
	var actionsResp map[string]any
	require.NoError(t, json.Unmarshal(actionsRec.Body.Bytes(), &actionsResp))
	actions, _ := actionsResp["ScheduledActions"].([]any)
	assert.Empty(t, actions, "scheduled actions should be cascade-deleted")
}

func TestHandler_PutScalingPolicy_PreservesTypeOnUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create policy with StepScaling type
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "step-policy",
		"PolicyType":        "StepScaling",
	})

	// Update policy without specifying PolicyType - should keep StepScaling
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "step-policy",
	})

	rec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "ecs",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	policies, _ := resp["ScalingPolicies"].([]any)
	require.Len(t, policies, 1)
	policy := policies[0].(map[string]any)
	assert.Equal(t, "StepScaling", policy["PolicyType"], "PolicyType should be preserved on update")
}

func TestHandler_DescribeScalingPolicies_IncludesConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a TargetTracking policy with config
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "my-policy",
		"PolicyType":        "TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration": map[string]any{
			"TargetValue": 70.0,
		},
	})

	rec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "ecs",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	policies, _ := resp["ScalingPolicies"].([]any)
	require.Len(t, policies, 1)
	policy := policies[0].(map[string]any)
	ttConfig, ok := policy["TargetTrackingScalingPolicyConfiguration"].(map[string]any)
	require.True(t, ok, "expected TargetTrackingScalingPolicyConfiguration in response")
	assert.InDelta(t, 70.0, ttConfig["TargetValue"], 0.001)
}

func TestHandler_GetPredictiveScalingForecast_WrongPolicyType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a TargetTracking policy (not PredictiveScaling)
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "tt-policy",
		"PolicyType":        "TargetTrackingScaling",
	})

	rec := doRequest(t, h, "GetPredictiveScalingForecast", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "tt-policy",
		"StartTime":         "2024-01-01T00:00:00Z",
		"EndTime":           "2024-01-02T00:00:00Z",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 for non-PredictiveScaling policy")
}

func TestHandler_PutScheduledAction_MissingSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    "ecs",
		"ResourceId":          "service/default/my-svc",
		"ScalableDimension":   "ecs:service:DesiredCount",
		"ScheduledActionName": "my-action",
		// Schedule intentionally omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 when Schedule is missing")
}

func TestHandler_DescribeScalingActivities_WithInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeScalingActivities", map[string]any{
		"ServiceNamespace":           "ecs",
		"ResourceId":                 "service/default/my-svc",
		"ScalableDimension":          "ecs:service:DesiredCount",
		"MaxResults":                 int32(10),
		"IncludeNotScaledActivities": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	acts, ok := resp["ScalingActivities"].([]any)
	require.True(t, ok, "ScalingActivities should be an array")
	assert.Empty(t, acts, "expected empty array not null")
}

func TestHandler_RegisterScalableTarget_UpdateSuspendedState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create target without SuspendedState
	doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
	})

	// Update with SuspendedState
	doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(2),
		"MaxCapacity":       int32(10),
		"SuspendedState": map[string]any{
			"DynamicScalingInSuspended": true,
		},
	})

	rec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
		"ServiceNamespace": "ecs",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	targets, _ := resp["ScalableTargets"].([]any)
	require.Len(t, targets, 1)

	ss, ok := targets[0].(map[string]any)["SuspendedState"].(map[string]any)
	require.True(t, ok, "expected SuspendedState after update")
	assert.Equal(t, true, ss["DynamicScalingInSuspended"])
	// Also verify capacity was updated
	assert.InDelta(t, float64(2), targets[0].(map[string]any)["MinCapacity"], 0.001)
}

func TestHandler_RegisterScalableTarget_UpdateTagsMergeLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register with 45 tags
	initial := make(map[string]string, 45)
	for i := range 45 {
		initial[fmt.Sprintf("k%d", i)] = "v"
	}

	doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
		"Tags":              initial,
	})

	// Upsert with 10 new tags - total would be 55, exceeds limit
	extra := make(map[string]string, 10)
	for i := range 10 {
		extra[fmt.Sprintf("new-%d", i)] = "v"
	}

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
		"Tags":              extra,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 when tag limit exceeded on upsert")
}

func TestHandler_ApplyMaxResults_CapAt100(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register 110 scalable targets
	for i := range 110 {
		doRequest(t, h, "RegisterScalableTarget", map[string]any{
			"ServiceNamespace":  "ecs",
			"ResourceId":        fmt.Sprintf("service/default/svc-%d", i),
			"ScalableDimension": "ecs:service:DesiredCount",
			"MinCapacity":       int32(1),
			"MaxCapacity":       int32(5),
		})
	}

	// Request MaxResults=200 (over cap)
	rec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
		"ServiceNamespace": "ecs",
		"MaxResults":       int32(200),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	targets, _ := resp["ScalableTargets"].([]any)
	assert.Len(t, targets, 100, "MaxResults=200 should be capped at 100")
}
