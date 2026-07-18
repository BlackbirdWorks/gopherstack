package autoscaling_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestAutoscalingHandler_DescribeScalingActivities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "with_group",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=act-asg&MinSize=1&MaxSize=3",
				)
			},
			body:       "Action=DescribeScalingActivities&Version=2011-01-01&AutoScalingGroupName=act-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "no_group_filter",
			body:       "Action=DescribeScalingActivities&Version=2011-01-01",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_group",
			body:       "Action=DescribeScalingActivities&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
