package applicationautoscaling_test

import (
	"net/http"
	"strings"
	"testing"

	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDimensionEnums_SDKDimensionsBelongToANamespace(t *testing.T) {
	t.Parallel()

	namespaces := map[string]bool{}
	for _, ns := range aastypes.ServiceNamespace("").Values() {
		namespaces[string(ns)] = true
	}

	for _, dim := range aastypes.ScalableDimension("").Values() {
		prefix, _, _ := strings.Cut(string(dim), ":")
		assert.True(t, namespaces[prefix], "dimension %s must lead with a ServiceNamespace", dim)
	}
}

func TestHandler_NamespaceDimensionValidation(t *testing.T) {
	t.Parallel()

	const (
		ecsDim = "ecs:service:DesiredCount"
		res    = "service/default/my-svc"
	)

	cases := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name: "register invalid namespace", op: "RegisterScalableTarget", wantCode: http.StatusBadRequest,
			body: map[string]any{
				"ServiceNamespace": "bogus", "ResourceId": res, "ScalableDimension": ecsDim,
				"MinCapacity": 1, "MaxCapacity": 2,
			},
		},
		{
			name: "register invalid dimension", op: "RegisterScalableTarget", wantCode: http.StatusBadRequest,
			body: map[string]any{
				"ServiceNamespace": "ecs", "ResourceId": res, "ScalableDimension": "ecs:service:Bogus",
				"MinCapacity": 1, "MaxCapacity": 2,
			},
		},
		{
			name: "register dimension of another namespace", op: "RegisterScalableTarget",
			wantCode: http.StatusBadRequest,
			body: map[string]any{
				"ServiceNamespace": "ecs", "ResourceId": res, "ScalableDimension": "dynamodb:table:ReadCapacityUnits",
				"MinCapacity": 1, "MaxCapacity": 2,
			},
		},
		{
			name: "register valid", op: "RegisterScalableTarget", wantCode: http.StatusOK,
			body: map[string]any{
				"ServiceNamespace": "ecs", "ResourceId": res, "ScalableDimension": ecsDim,
				"MinCapacity": 1, "MaxCapacity": 2,
			},
		},
		{
			name: "policy dimension of another namespace", op: "PutScalingPolicy", wantCode: http.StatusBadRequest,
			body: map[string]any{
				"ServiceNamespace": "ecs", "ResourceId": res, "ScalableDimension": "rds:cluster:ReadReplicaCount",
				"PolicyName": "p", "PolicyType": "StepScaling",
			},
		},
		{
			name: "scheduled action invalid namespace", op: "PutScheduledAction", wantCode: http.StatusBadRequest,
			body: map[string]any{
				"ServiceNamespace": "bogus", "ResourceId": res, "ScalableDimension": ecsDim,
				"ScheduledActionName": "a", "Schedule": "rate(1 hour)",
			},
		},
		{
			name: "describe invalid namespace", op: "DescribeScalableTargets", wantCode: http.StatusBadRequest,
			body: map[string]any{"ServiceNamespace": "bogus"},
		},
		{
			name: "describe policies invalid dimension", op: "DescribeScalingPolicies", wantCode: http.StatusBadRequest,
			body: map[string]any{"ServiceNamespace": "ecs", "ScalableDimension": "bogus"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.op, tc.body)
			require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())

			if tc.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "ValidationException")
			}
		})
	}
}

func TestHandler_DescribeScheduledActions_OmitsInventedMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		absent  string
		present string
	}{
		{name: "scheduled action", absent: "LastModifiedTime", present: "CreationTime"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedTarget(t, h, "service/default/my-svc", 1, 10)
			rec := doRequest(t, h, "PutScheduledAction", map[string]any{
				"ServiceNamespace": "ecs", "ResourceId": "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount", "ScheduledActionName": "a",
				"Schedule": "rate(1 hour)",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			desc := doRequest(t, h, "DescribeScheduledActions", map[string]any{"ServiceNamespace": "ecs"})
			require.Equal(t, http.StatusOK, desc.Code)
			assert.Contains(t, desc.Body.String(), tc.present)
			assert.NotContains(t, desc.Body.String(), tc.absent)
		})
	}
}
