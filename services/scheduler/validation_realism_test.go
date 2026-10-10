package scheduler_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSchedule_ValidationRealism(t *testing.T) {
	t.Parallel()

	target := &types.Target{
		Arn:     aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
		RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
	}

	tests := []struct {
		target  *types.Target
		name    string
		expr    string
		desc    string
		ftw     types.FlexibleTimeWindow
		wantErr bool
	}{
		{name: "singular rate", expr: "rate(1 minute)", ftw: ftwOff(), target: target},
		{name: "plural rate", expr: "rate(5 minutes)", ftw: ftwOff(), target: target},
		{name: "singular unit with many", expr: "rate(5 minute)", ftw: ftwOff(), target: target, wantErr: true},
		{name: "plural unit with one", expr: "rate(1 minutes)", ftw: ftwOff(), target: target, wantErr: true},
		{
			name: "window too large", expr: "rate(5 minutes)", target: target, wantErr: true,
			ftw: types.FlexibleTimeWindow{
				Mode: types.FlexibleTimeWindowModeFlexible, MaximumWindowInMinutes: aws.Int32(1441),
			},
		},
		{
			name: "window at limit", expr: "rate(5 minutes)", target: target,
			ftw: types.FlexibleTimeWindow{
				Mode: types.FlexibleTimeWindowModeFlexible, MaximumWindowInMinutes: aws.Int32(1440),
			},
		},
		{
			name: "description too long", expr: "rate(5 minutes)", desc: strings.Repeat("x", 513),
			ftw: ftwOff(), target: target, wantErr: true,
		},
		{
			name: "description at limit", expr: "rate(5 minutes)", desc: strings.Repeat("x", 512),
			ftw: ftwOff(), target: target,
		},
		{
			name: "expression too long", expr: "cron(" + strings.Repeat("1,", 140) + "1 * * * ? *)",
			ftw: ftwOff(), target: target, wantErr: true,
		},
		{name: "target not arn", expr: "rate(5 minutes)", ftw: ftwOff(), wantErr: true, target: &types.Target{
			Arn: aws.String("queue"), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		}},
		{name: "role not arn", expr: "rate(5 minutes)", ftw: ftwOff(), wantErr: true, target: &types.Target{
			Arn: aws.String("arn:aws:sqs:us-east-1:000000000000:q"), RoleArn: aws.String("role"),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.CreateSchedule(t.Context(), &schedulersdk.CreateScheduleInput{
				Name:               aws.String("s"),
				ScheduleExpression: aws.String(tt.expr),
				Description:        aws.String(tt.desc),
				FlexibleTimeWindow: &tt.ftw,
				Target:             tt.target,
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
			assert.NotContains(t, ve.ErrorMessage(), "ValidationException:")
		})
	}
}

func ftwOff() types.FlexibleTimeWindow {
	return types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff}
}

func TestSchedulerErrors_MessageWording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *schedulersdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "get missing schedule",
			call: func(c *schedulersdk.Client) error {
				_, err := c.GetSchedule(t.Context(), &schedulersdk.GetScheduleInput{Name: aws.String("nope")})

				return err
			},
			wantCode: "ResourceNotFoundException",
			wantMsg:  "Schedule nope does not exist.",
		},
		{
			name: "get missing group",
			call: func(c *schedulersdk.Client) error {
				_, err := c.GetScheduleGroup(t.Context(), &schedulersdk.GetScheduleGroupInput{Name: aws.String("nope")})

				return err
			},
			wantCode: "ResourceNotFoundException",
			wantMsg:  "ScheduleGroup nope does not exist.",
		},
		{
			name: "bad max results",
			call: func(c *schedulersdk.Client) error {
				_, err := c.ListSchedules(t.Context(), &schedulersdk.ListSchedulesInput{MaxResults: aws.Int32(101)})

				return err
			},
			wantCode: "ValidationException",
			wantMsg:  "MaxResults must be between 1 and 100",
		},
		{
			name: "bad next token",
			call: func(c *schedulersdk.Client) error {
				_, err := c.ListScheduleGroups(
					t.Context(), &schedulersdk.ListScheduleGroupsInput{NextToken: aws.String("!!")},
				)

				return err
			},
			wantCode: "ValidationException",
			wantMsg:  "Invalid pagination token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newTestHandlerAndClient(t))

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
		})
	}
}

func TestListSchedules_OpaqueTokenWalk(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	for _, n := range []string{"a", "b", "c"} {
		_, err := client.CreateSchedule(t.Context(), &schedulersdk.CreateScheduleInput{
			Name:               aws.String(n),
			ScheduleExpression: aws.String("rate(5 minutes)"),
			FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
			Target: &types.Target{
				Arn:     aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
				RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
			},
		})
		require.NoError(t, err)
	}

	var seen []string
	var token *string

	for {
		out, err := client.ListSchedules(t.Context(), &schedulersdk.ListSchedulesInput{
			MaxResults: aws.Int32(1), NextToken: token,
		})
		require.NoError(t, err)

		for _, s := range out.Schedules {
			seen = append(seen, aws.ToString(s.Name))
		}

		if out.NextToken == nil {
			break
		}

		assert.NotContains(t, aws.ToString(out.NextToken), "default/", "token must be opaque")
		token = out.NextToken
	}

	assert.Equal(t, []string{"a", "b", "c"}, seen)
}
