package applicationautoscaling_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHandler_PutScalingPolicy_DefaultPolicyType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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
