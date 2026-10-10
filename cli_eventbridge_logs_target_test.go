package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceResourceAuthzEventBridgeLogs(t *testing.T) {
	t.Parallel()

	const group = "/aws/events/authz"

	groupARN := "arn:aws:logs:us-east-1:000000000000:log-group:" + group

	tests := []struct {
		name      string
		principal string
		resource  string
		wantCode  string
		enforce   bool
		noPolicy  bool
	}{
		{name: "allowed_wildcard", enforce: true, principal: "events.amazonaws.com",
			resource: "arn:aws:logs:us-east-1:000000000000:log-group:/aws/events/*:*"},
		{name: "allowed_exact", enforce: true, principal: "events.amazonaws.com", resource: groupARN},
		{name: "denied_no_policy", enforce: true, noPolicy: true, wantCode: "NO_PERMISSIONS"},
		{name: "denied_other_principal", enforce: true, principal: "sns.amazonaws.com",
			resource: groupARN + ":*", wantCode: "NO_PERMISSIONS"},
		{name: "denied_other_group", enforce: true, principal: "events.amazonaws.com",
			resource: "arn:aws:logs:us-east-1:000000000000:log-group:/other/*:*", wantCode: "NO_PERMISSIONS"},
		{name: "enforcement_off_unchanged", noPolicy: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			authzStartWorkers(t, fx, "EventBridge")

			dlqURL, dlqARN := authzQueue(t, fx, "logs-dlq")
			authzQueuePolicy(t, fx, dlqURL, dlqARN, "events.amazonaws.com", "")

			cwl := cloudwatchlogs.NewFromConfig(fx.cfg)
			_, err := cwl.CreateLogGroup(
				t.Context(),
				&cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(group)},
			)
			require.NoError(t, err)

			if !tt.noPolicy {
				_, err = cwl.PutResourcePolicy(t.Context(), &cloudwatchlogs.PutResourcePolicyInput{
					PolicyName: aws.String("events-to-logs"),
					PolicyDocument: aws.String(authzResourcePolicy(
						tt.principal, "logs:PutLogEvents", tt.resource, "")),
				})
				require.NoError(t, err)
			}

			ebc := eventbridge.NewFromConfig(fx.cfg)
			_, err = ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["authz.logs"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"),
				Targets: []ebtypes.Target{{
					Id: aws.String("t"), Arn: aws.String(groupARN),
					DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(dlqARN)},
					RetryPolicy:      &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)},
				}},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("authz.logs"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
			}}})
			require.NoError(t, err)

			logged := func() bool {
				out, ferr := cwl.FilterLogEvents(t.Context(), &cloudwatchlogs.FilterLogEventsInput{
					LogGroupName: aws.String(group),
				})

				return ferr == nil && len(out.Events) > 0
			}

			if tt.wantCode == "" {
				require.Eventually(t, logged, authzDeadline, authzTick)

				return
			}

			var code string

			require.Eventually(t, func() bool {
				code = authzDLQErrorCode(t, fx, dlqURL)

				return code != ""
			}, authzDeadline, authzTick)
			assert.Equal(t, tt.wantCode, code)
			assert.False(t, logged())
		})
	}
}
