package applicationautoscaling_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

// seedTarget is a helper that registers an ECS scalable target and asserts success.
func seedTarget(t *testing.T, h *applicationautoscaling.Handler, resID string, minCap, maxCap int) string {
	t.Helper()

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        resID,
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       minCap,
		"MaxCapacity":       maxCap,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["ScalableTargetARN"].(string)
}

// TestParity_RegisterDeregister verifies full register → describe → deregister lifecycle.
func TestParity_RegisterDeregister(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const (
		ns  = "ecs"
		res = "service/default/web"
		dim = "ecs:service:DesiredCount"
	)

	arn := seedTarget(t, h, res, 1, 10)
	assert.NotEmpty(t, arn)

	descRec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
		"ServiceNamespace": ns,
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	targets := descOut["ScalableTargets"].([]any)
	require.Len(t, targets, 1)
	t0 := targets[0].(map[string]any)
	assert.Equal(t, res, t0["ResourceId"])
	assert.Equal(t, arn, t0["ScalableTargetARN"])
	assert.NotNil(t, t0["CreationTime"], "CreationTime must be present")

	deregRec := doRequest(t, h, "DeregisterScalableTarget", map[string]any{
		"ServiceNamespace":  ns,
		"ResourceId":        res,
		"ScalableDimension": dim,
	})
	assert.Equal(t, http.StatusOK, deregRec.Code)

	afterRec := doRequest(t, h, "DescribeScalableTargets", map[string]any{
		"ServiceNamespace": ns,
	})
	var afterOut map[string]any
	require.NoError(t, json.Unmarshal(afterRec.Body.Bytes(), &afterOut))
	assert.Empty(t, afterOut["ScalableTargets"])
}

// TestParity_ScalingPolicyCRUD verifies put → describe → delete for scaling policies.
func TestParity_ScalingPolicyCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const (
		ns     = "ecs"
		res    = "service/default/api"
		dim    = "ecs:service:DesiredCount"
		policy = "scale-out-policy"
	)

	seedTarget(t, h, res, 1, 20)

	putRec := doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  ns,
		"ResourceId":        res,
		"ScalableDimension": dim,
		"PolicyName":        policy,
		"PolicyType":        "TargetTrackingScaling",
		"TargetTrackingScalingPolicyConfiguration": map[string]any{
			"TargetValue": 75.0,
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)
	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	policyARN := putOut["PolicyARN"].(string)
	assert.NotEmpty(t, policyARN)

	descRec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": ns,
	})
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	policies := descOut["ScalingPolicies"].([]any)
	require.Len(t, policies, 1)
	p0 := policies[0].(map[string]any)
	assert.Equal(t, policy, p0["PolicyName"])
	assert.Equal(t, "TargetTrackingScaling", p0["PolicyType"])
	assert.Equal(t, policyARN, p0["PolicyARN"])
	assert.NotNil(t, p0["CreationTime"])

	delRec := doRequest(t, h, "DeleteScalingPolicy", map[string]any{
		"ServiceNamespace":  ns,
		"ResourceId":        res,
		"ScalableDimension": dim,
		"PolicyName":        policy,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	afterRec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": ns,
	})
	var afterOut map[string]any
	require.NoError(t, json.Unmarshal(afterRec.Body.Bytes(), &afterOut))
	assert.Empty(t, afterOut["ScalingPolicies"])
}

// TestParity_ScheduledActionCRUD verifies put → describe → delete for scheduled actions.
func TestParity_ScheduledActionCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const (
		ns     = "ecs"
		res    = "service/default/batch"
		dim    = "ecs:service:DesiredCount"
		action = "scale-down-night"
	)

	seedTarget(t, h, res, 0, 10)

	putRec := doRequest(t, h, "PutScheduledAction", map[string]any{
		"ServiceNamespace":    ns,
		"ResourceId":          res,
		"ScalableDimension":   dim,
		"ScheduledActionName": action,
		"Schedule":            "cron(0 0 * * ? *)",
		"ScalableTargetAction": map[string]any{
			"MinCapacity": 0,
			"MaxCapacity": 0,
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)
	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	actionARN := putOut["ScheduledActionARN"].(string)
	assert.NotEmpty(t, actionARN)

	descRec := doRequest(t, h, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": ns,
	})
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	actions := descOut["ScheduledActions"].([]any)
	require.Len(t, actions, 1)
	a0 := actions[0].(map[string]any)
	assert.Equal(t, action, a0["ScheduledActionName"])
	assert.Equal(t, actionARN, a0["ScheduledActionARN"])
	assert.NotNil(t, a0["CreationTime"])

	delRec := doRequest(t, h, "DeleteScheduledAction", map[string]any{
		"ServiceNamespace":    ns,
		"ResourceId":          res,
		"ScalableDimension":   dim,
		"ScheduledActionName": action,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	afterRec := doRequest(t, h, "DescribeScheduledActions", map[string]any{
		"ServiceNamespace": ns,
	})
	var afterOut map[string]any
	require.NoError(t, json.Unmarshal(afterRec.Body.Bytes(), &afterOut))
	assert.Empty(t, afterOut["ScheduledActions"])
}

// TestParity_DescribeScalingActivities_Pagination verifies MaxResults and NextToken
// for DescribeScalingActivities (previously unimplemented).
func TestParity_DescribeScalingActivities_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const (
		ns  = "ecs"
		dim = "ecs:service:DesiredCount"
	)

	for i := range 5 {
		seedTarget(t, h, "service/default/svc"+string(rune('a'+i)), 1, 10)
	}

	rec1 := doRequest(t, h, "DescribeScalingActivities", map[string]any{
		"ServiceNamespace": ns,
		"MaxResults":       2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1 := out1["ScalingActivities"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["NextToken"].(string)
	assert.True(t, ok && nextToken != "", "NextToken must be present after partial page")

	rec2 := doRequest(t, h, "DescribeScalingActivities", map[string]any{
		"ServiceNamespace": ns,
		"MaxResults":       2,
		"NextToken":        nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2 := out2["ScalingActivities"].([]any)
	assert.Len(t, page2, 2)
}

// TestParity_DescribeScalingActivities_TypedFields verifies the response uses
// typed ActivityId, ResourceId fields (not []any items).
func TestParity_DescribeScalingActivities_TypedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/typed", 1, 5)

	rec := doRequest(t, h, "DescribeScalingActivities", map[string]any{
		"ServiceNamespace": "ecs",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	activities := out["ScalingActivities"].([]any)
	require.Len(t, activities, 1)

	a := activities[0].(map[string]any)
	assert.NotEmpty(t, a["ActivityId"], "ActivityId must be present")
	assert.NotEmpty(t, a["ResourceId"], "ResourceId must be present")
	assert.NotEmpty(t, a["StatusCode"], "StatusCode must be present")
	assert.NotNil(t, a["StartTime"], "StartTime must be present as epoch seconds")
}

// TestParity_TagResourceRoundTrip verifies tag → list → untag round-trip.
func TestParity_TagResourceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := seedTarget(t, h, "service/default/tagged", 1, 5)

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        map[string]any{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	tags := listOut["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	afterRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arn,
	})
	var afterOut map[string]any
	require.NoError(t, json.Unmarshal(afterRec.Body.Bytes(), &afterOut))
	afterTags := afterOut["Tags"].(map[string]any)
	assert.NotContains(t, afterTags, "env")
	assert.Equal(t, "platform", afterTags["team"])
}

// TestParity_DeregisterCascades verifies that deregistering a target deletes its
// associated scaling policies and scheduled actions (AWS cascade behaviour).
func TestParity_DeregisterCascades(t *testing.T) {
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

// TestParity_DescribeScalableTargets_Pagination verifies NextToken pagination.
func TestParity_DescribeScalableTargets_Pagination(t *testing.T) {
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

// TestParity_ErrorCodes verifies correct HTTP status codes for error cases.
func TestParity_ErrorCodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "DeregisterScalableTarget notfound",
			action: "DeregisterScalableTarget",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/nonexistent",
				"ScalableDimension": "ecs:service:DesiredCount",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteScalingPolicy notfound",
			action: "DeleteScalingPolicy",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/x",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "no-such-policy",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteScheduledAction notfound",
			action: "DeleteScheduledAction",
			body: map[string]any{
				"ServiceNamespace":    "ecs",
				"ResourceId":          "service/default/x",
				"ScalableDimension":   "ecs:service:DesiredCount",
				"ScheduledActionName": "no-such-action",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "RegisterScalableTarget min>max",
			action: "RegisterScalableTarget",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/x",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       10,
				"MaxCapacity":       5,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeScalingActivities missing namespace",
			action:   "DescribeScalingActivities",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, "action=%s", tc.action)
		})
	}
}
