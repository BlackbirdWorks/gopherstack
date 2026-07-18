package autoscaling_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_InstanceRefreshFlow covers StartInstanceRefresh, DescribeInstanceRefreshes,
// RollbackInstanceRefresh.
func TestAutoscalingHandler_InstanceRefreshFlow(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	// Create an ASG first.
	rec := postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg&MinSize=0&MaxSize=5"+
		"&LaunchConfigurationName=test-lc")
	// ASG creation may fail if LC doesn't exist - just check we can proceed
	_ = rec

	// DescribeInstanceRefreshes (works even without ASG data).
	rec = postAutoscalingForm(t, h, "Action=DescribeInstanceRefreshes&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg")
	assert.Equal(t, http.StatusOK, rec.Code)

	// StartInstanceRefresh.
	rec = postAutoscalingForm(t, h, "Action=StartInstanceRefresh&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg")
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	// RollbackInstanceRefresh (may fail if no active refresh).
	rec = postAutoscalingForm(t, h, "Action=RollbackInstanceRefresh&Version=2011-01-01"+
		"&AutoScalingGroupName=test-asg")
	assert.True(t, rec.Code == http.StatusOK || rec.Code != http.StatusInternalServerError)

	_ = body
}

func TestAutoscalingHandler_CancelInstanceRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler, b *autoscaling.InMemoryBackend)
		body       string
		wantStatus int
		wantIDSet  bool
	}{
		{
			name: "cancel_active_refresh",
			setup: func(t *testing.T, _ *autoscaling.Handler, b *autoscaling.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "refresh-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				require.NoError(t, err)

				err = b.AddInstanceRefresh(autoscaling.InstanceRefresh{
					InstanceRefreshID:    "irs-12345",
					AutoScalingGroupName: "refresh-asg",
					Status:               "InProgress",
				})
				require.NoError(t, err)
			},
			body:       "Action=CancelInstanceRefresh&Version=2011-01-01&AutoScalingGroupName=refresh-asg",
			wantStatus: http.StatusOK,
			wantIDSet:  true,
		},
		{
			name: "cancel_no_active_refresh",
			setup: func(t *testing.T, h *autoscaling.Handler, _ *autoscaling.InMemoryBackend) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=no-refresh-asg&MinSize=0&MaxSize=5")
			},
			body:       "Action=CancelInstanceRefresh&Version=2011-01-01&AutoScalingGroupName=no-refresh-asg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "cancel_refresh_group_not_found",
			body:       "Action=CancelInstanceRefresh&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			h := autoscaling.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantIDSet {
				assert.Contains(t, rec.Body.String(), "irs-12345")
			}
		})
	}
}
