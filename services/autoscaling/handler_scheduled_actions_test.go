package autoscaling_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_ScheduledActionARNPresent verifies that PutScheduledUpdateGroupAction
// results in a ScheduledActionARN being set on DescribeScheduledActions.
func TestAutoscalingHandler_ScheduledActionARNPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		asgName    string
		actionName string
	}{
		{"first action", "sched-asg-1", "daily-scale-up"},
		{"second action", "sched-asg-2", "nightly-scale-down"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {tc.asgName},
				"MinSize":                    {"1"},
				"MaxSize":                    {"10"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "PutScheduledUpdateGroupAction", url.Values{
				"AutoScalingGroupName": {tc.asgName},
				"ScheduledActionName":  {tc.actionName},
				"Recurrence":           {"0 8 * * *"},
				"DesiredCapacity":      {"5"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeScheduledActions", url.Values{
				"AutoScalingGroupName": {tc.asgName},
			})
			require.Equal(t, 200, code)

			assert.Contains(t, body, "<ScheduledActionARN>",
				"DescribeScheduledActions must include ScheduledActionARN; got: %s", body)
			assert.Contains(t, body, "scheduledUpdateGroupAction",
				"ScheduledActionARN must use correct ARN format")
		})
	}
}

// TestAutoscalingHandler_ScheduledActionStartEndTimeRoundTrip verifies that PutScheduledUpdateGroupAction
// and BatchPutScheduledUpdateGroupAction both persist StartTime/EndTime -- previously
// both silently dropped these fields, so DescribeScheduledActions never reflected the
// schedule the caller actually requested.
func TestAutoscalingHandler_ScheduledActionStartEndTimeRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		useBatch bool
	}{
		{name: "single PutScheduledUpdateGroupAction", useBatch: false},
		{name: "BatchPutScheduledUpdateGroupAction", useBatch: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			asgName := "sched-time-asg-" + tc.name

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"5"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			if tc.useBatch {
				code, body = doAS(t, h, "BatchPutScheduledUpdateGroupAction", url.Values{
					"AutoScalingGroupName": {asgName},
					"ScheduledUpdateGroupActions.member.1.ScheduledActionName": {"batch-action"},
					"ScheduledUpdateGroupActions.member.1.StartTime":           {"2030-01-01T00:00:00Z"},
					"ScheduledUpdateGroupActions.member.1.EndTime":             {"2030-01-02T00:00:00Z"},
					"ScheduledUpdateGroupActions.member.1.DesiredCapacity":     {"4"},
				})
			} else {
				code, body = doAS(t, h, "PutScheduledUpdateGroupAction", url.Values{
					"AutoScalingGroupName": {asgName},
					"ScheduledActionName":  {"single-action"},
					"StartTime":            {"2030-01-01T00:00:00Z"},
					"EndTime":              {"2030-01-02T00:00:00Z"},
					"DesiredCapacity":      {"4"},
				})
			}
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeScheduledActions", url.Values{
				"AutoScalingGroupName": {asgName},
			})
			require.Equal(t, 200, code, body)

			assert.Contains(t, body, "<StartTime>2030-01-01T00:00:00Z</StartTime>",
				"StartTime must round-trip; got: %s", body)
			assert.Contains(t, body, "<EndTime>2030-01-02T00:00:00Z</EndTime>",
				"EndTime must round-trip; got: %s", body)
		})
	}
}

func TestAutoscalingHandler_BatchDeleteScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "batch_delete_existing_action",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=sched-asg&MinSize=0&MaxSize=5")
				postAutoscalingForm(t, h,
					"Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01"+
						"&AutoScalingGroupName=sched-asg"+
						"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=action-1"+
						"&ScheduledUpdateGroupActions.member.1.DesiredCapacity=3")
			},
			body: "Action=BatchDeleteScheduledAction&Version=2011-01-01" +
				"&AutoScalingGroupName=sched-asg" +
				"&ScheduledActionNames.member.1=action-1",
			wantStatus: http.StatusOK,
		},
		{
			name: "batch_delete_nonexistent_action_returns_failures",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=sched-fail-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=BatchDeleteScheduledAction&Version=2011-01-01" +
				"&AutoScalingGroupName=sched-fail-asg" +
				"&ScheduledActionNames.member.1=no-such-action",
			wantStatus: http.StatusOK,
		},
		{
			name: "batch_delete_group_not_found",
			body: "Action=BatchDeleteScheduledAction&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such&ScheduledActionNames.member.1=a",
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

func TestAutoscalingHandler_BatchPutScheduledUpdateGroupAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "batch_put_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=put-sched-asg&MinSize=0&MaxSize=10")
			},
			body: "Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01" +
				"&AutoScalingGroupName=put-sched-asg" +
				"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=scale-up" +
				"&ScheduledUpdateGroupActions.member.1.DesiredCapacity=5" +
				"&ScheduledUpdateGroupActions.member.1.Recurrence=0 9 * * *",
			wantStatus: http.StatusOK,
		},
		{
			name: "batch_put_group_not_found",
			body: "Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such" +
				"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=a",
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

func TestAutoscalingHandler_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "describe_scheduled_actions_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=sa-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t, h,
					"Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=sa-asg"+
						"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=scale-out"+
						"&ScheduledUpdateGroupActions.member.1.DesiredCapacity=5",
				)
			},
			body:       "Action=DescribeScheduledActions&Version=2011-01-01&AutoScalingGroupName=sa-asg",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "scale-out")
			},
		},
		{
			name:       "describe_scheduled_actions_group_not_found",
			body:       "Action=DescribeScheduledActions&Version=2011-01-01&AutoScalingGroupName=no-such",
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

			if tt.checkBody != nil {
				tt.checkBody(t, rec.Body.String())
			}
		})
	}
}
