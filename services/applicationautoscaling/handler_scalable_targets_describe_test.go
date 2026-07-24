package applicationautoscaling_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DeregisterScalableTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  int
		preCreate bool
	}{
		{name: "success", preCreate: true, wantCode: http.StatusOK},
		{name: "not_found", preCreate: false, wantCode: http.StatusBadRequest},
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

func TestDeregisterScalableTarget_Cascades(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const (
		ns  = "ecs"
		res = "service/default/cascade"
		dim = "ecs:service:DesiredCount"
	)

	seedTarget(t, h, res, 1, 10)

	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  ns,
		"ResourceId":        res,
		"ScalableDimension": dim,
		"PolicyName":        "p1",
		"PolicyType":        "TargetTrackingScaling",
	})
	doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    ns,
		"ResourceId":          res,
		"ScalableDimension":   dim,
		"ScheduledActionName": "a1",
		"Schedule":            "rate(1 hour)",
	})

	doRequest(t, h, "DeregisterScalableTarget", map[string]any{
		"ServiceNamespace":  ns,
		"ResourceId":        res,
		"ScalableDimension": dim,
	})

	polRec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": ns,
	})
	var polOut map[string]any
	require.NoError(t, json.Unmarshal(polRec.Body.Bytes(), &polOut))
	assert.Empty(t, polOut["ScalingPolicies"], "policies must be deleted when target is deregistered")

	actRec := doRequest(t, h, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": ns,
	})
	var actOut map[string]any
	require.NoError(t, json.Unmarshal(actRec.Body.Bytes(), &actOut))
	assert.Empty(t, actOut["ScheduledActions"], "scheduled actions must be deleted when target is deregistered")
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

func TestHandler_DescribeScalableTargets_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const (
		ns  = "ecs"
		dim = "ecs:service:DesiredCount"
	)

	for i := range 5 {
		seedTarget(t, h, "service/default/svc"+string(rune('a'+i)), 1, 10)
	}

	tests := []struct {
		name      string
		maxRes    int
		wantCount int
		wantNext  bool
	}{
		{name: "page1 of 2", maxRes: 3, wantCount: 3, wantNext: true},
		{name: "all at once", maxRes: 10, wantCount: 5, wantNext: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
				"ServiceNamespace": ns,
				"MaxResults":       tc.maxRes,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			targets := out["ScalableTargets"].([]any)
			assert.Len(t, targets, tc.wantCount)
			if tc.wantNext {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
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
