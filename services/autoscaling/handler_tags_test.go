package autoscaling_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestAutoscalingHandler_CreateOrUpdateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "create_or_update_tags_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=tag-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CreateOrUpdateTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=tag-asg" +
				"&Tags.member.1.ResourceType=auto-scaling-group" +
				"&Tags.member.1.Key=env" +
				"&Tags.member.1.Value=production",
			wantStatus: http.StatusOK,
		},
		{
			name: "create_or_update_tags_group_not_found",
			body: "Action=CreateOrUpdateTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=no-such" +
				"&Tags.member.1.ResourceType=auto-scaling-group" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=test",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_or_update_tags_unknown_resource_type_ignored",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=tag-asg2&MinSize=0&MaxSize=5")
			},
			body: "Action=CreateOrUpdateTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=tag-asg2" +
				"&Tags.member.1.ResourceType=other-type" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=test",
			wantStatus: http.StatusOK,
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

func TestAutoscalingHandler_DeleteAndDescribeTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "delete_tags_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=tag-asg&MinSize=0&MaxSize=5"+
						"&Tags.member.1.Key=env&Tags.member.1.Value=prod",
				)
			},
			body: "Action=DeleteTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=tag-asg&Tags.member.1.ResourceType=auto-scaling-group&Tags.member.1.Key=env",
			wantStatus: http.StatusOK,
		},
		{
			name: "describe_tags_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dtag-asg&MinSize=0&MaxSize=5"+
						"&Tags.member.1.Key=team&Tags.member.1.Value=platform",
				)
			},
			body:       "Action=DescribeTags&Version=2011-01-01",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "platform")
			},
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
