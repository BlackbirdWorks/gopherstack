package applicationautoscaling_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutScalingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)
	rec := doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "my-policy",
		"PolicyType":        "TargetTrackingScaling",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	policyARN, _ := resp["PolicyARN"].(string)
	assert.Contains(t, policyARN, "arn:aws:autoscaling:")
	assert.Contains(t, policyARN, "scalingPolicy:")

	// Alarms is honestly left empty: gopherstack has no cross-service
	// reference to a real cloudwatch backend, so it cannot create the
	// genuine backing CloudWatch alarm real AWS would (see PARITY.md gaps).
	// omitempty means the field is absent entirely, not present-but-empty.
	_, ok := resp["Alarms"]
	assert.False(t, ok, "Alarms should be omitted (honest-empty), not fabricated")
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
			seedTarget(t, h, "service/default/my-svc", 1, 10)
			rec := doRequest(t, h, "PutScalingPolicy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutScalingPolicy_DefaultPolicyType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)
	// Omit PolicyType — should default to StepScaling, matching real AWS/Terraform
	// behavior (the aws_appautoscaling_policy resource documents "StepScaling"
	// as its default policy_type).
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
	assert.Equal(t, "StepScaling", policies[0].(map[string]any)["PolicyType"])
}

func TestHandler_PutScalingPolicy_PreservesTypeOnUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)

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

func TestHandler_DeleteScalingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preCreate bool
		wantCode  int
	}{
		{name: "success", preCreate: true, wantCode: http.StatusOK},
		{name: "not_found", preCreate: false, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.preCreate {
				seedTarget(t, h, "service/default/my-svc", 1, 10)
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
			seedTarget(t, h, "service/default/svc1", 1, 10)
			seedTargetNS(t, h, "dynamodb", "table/t1", "dynamodb:table:ReadCapacityUnits", 1, 10)
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
			seedTarget(t, h, "service/default/svc1", 1, 10)
			seedTargetNS(t, h, "dynamodb", "table/t1", "dynamodb:table:ReadCapacityUnits", 1, 10)
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

func TestHandler_DescribeScalingPolicies_IncludesConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)

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

func TestHandler_MaxResults_DescribeScalingPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)
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

func TestScalingPolicyCRUD(t *testing.T) {
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

// TestHandler_PutScalingPolicy_Alarms verifies that the Alarms field (both
// PutScalingPolicy and DescribeScalingPolicies return it) is honestly left
// empty for every policy type. Real AWS attaches genuine backing CloudWatch
// alarms server-side; gopherstack has no cross-service reference to a real
// cloudwatch backend to create one (see PARITY.md gaps), so it must not
// fabricate alarm names/ARNs that resolve to nothing.
func TestHandler_PutScalingPolicy_Alarms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policyType string
		body       map[string]any
		name       string
		policyName string
	}{
		{
			name:       "target_tracking_scale_in_allowed",
			policyName: "tt-policy",
			policyType: "TargetTrackingScaling",
		},
		{
			name:       "target_tracking_scale_in_disabled",
			policyName: "tt-policy-disabled",
			policyType: "TargetTrackingScaling",
			body: map[string]any{
				"TargetTrackingScalingPolicyConfiguration": map[string]any{
					"TargetValue":    50.0,
					"DisableScaleIn": true,
				},
			},
		},
		{
			name:       "step_scaling",
			policyName: "step-policy",
			policyType: "StepScaling",
		},
		{
			name:       "predictive_scaling_no_alarms",
			policyName: "predictive-policy",
			policyType: "PredictiveScaling",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedTarget(t, h, "service/default/my-svc", 1, 10)

			body := map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        tt.policyName,
				"PolicyType":        tt.policyType,
			}
			maps.Copy(body, tt.body)

			rec := doRequest(t, h, "PutScalingPolicy", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var putOut map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putOut))
			_, putHasAlarms := putOut["Alarms"]
			assert.False(t, putHasAlarms, "PutScalingPolicy response Alarms should be omitted")

			descRec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
				"ServiceNamespace": "ecs",
				"PolicyNames":      []string{tt.policyName},
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
			policies := descOut["ScalingPolicies"].([]any)
			require.Len(t, policies, 1)
			_, descHasAlarms := policies[0].(map[string]any)["Alarms"]
			assert.False(t, descHasAlarms, "DescribeScalingPolicies response Alarms should be omitted")
		})
	}
}

// TestHandler_PutScalingPolicy_PredictiveScalingConfiguration verifies
// PredictiveScalingPolicyConfiguration -- present on PutScalingPolicy's real
// request shape but previously dropped entirely by gopherstack -- is now
// captured and echoed back by DescribeScalingPolicies.
func TestHandler_PutScalingPolicy_PredictiveScalingConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)

	rec := doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"PolicyType":        "PredictiveScaling",
		"PredictiveScalingPolicyConfiguration": map[string]any{
			"MetricSpecifications": []any{},
			"Mode":                 "ForecastOnly",
			"MaxCapacityBuffer":    int32(10),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doRequest(t, h, "DescribeScalingPolicies", map[string]any{
		"ServiceNamespace": "ecs",
		"PolicyNames":      []string{"predictive-policy"},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	policies := descOut["ScalingPolicies"].([]any)
	require.Len(t, policies, 1)
	p0 := policies[0].(map[string]any)
	predictiveConfig, ok := p0["PredictiveScalingPolicyConfiguration"].(map[string]any)
	require.True(t, ok, "expected PredictiveScalingPolicyConfiguration in response")
	assert.Equal(t, "ForecastOnly", predictiveConfig["Mode"])
	assert.InDelta(t, 10, predictiveConfig["MaxCapacityBuffer"], 0)
}
