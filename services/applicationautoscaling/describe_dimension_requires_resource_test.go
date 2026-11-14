package applicationautoscaling_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_Describe_ScalableDimensionRequiresResourceID(t *testing.T) {
	t.Parallel()

	ops := []string{
		"DescribeScalableTargets", "DescribeScalingPolicies", "DescribeScalingActivities", "DescribeScheduledActions",
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "dimension_without_resource",
			body: map[string]any{
				"ServiceNamespace": "ecs", "ScalableDimension": "ecs:service:DesiredCount",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "namespace_only",
			body:     map[string]any{"ServiceNamespace": "ecs"},
			wantCode: http.StatusOK,
		},
	}

	for _, op := range ops {
		for _, tt := range tests {
			t.Run(op+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				rec := doRequest(t, h, op, tt.body)
				assert.Equal(t, tt.wantCode, rec.Code)
			})
		}
	}
}
