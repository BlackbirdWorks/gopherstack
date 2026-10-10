package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestAPIErrors_CodeAndMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *ebsdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "rule_not_found",
			call: func(c *ebsdk.Client) error {
				_, err := c.DescribeRule(t.Context(), &ebsdk.DescribeRuleInput{Name: aws.String("nope")})

				return err
			},
			wantCode: "ResourceNotFoundException", wantMsg: "Rule nope does not exist on EventBus default.",
		},
		{
			name: "bus_not_found",
			call: func(c *ebsdk.Client) error {
				_, err := c.DescribeEventBus(t.Context(), &ebsdk.DescribeEventBusInput{Name: aws.String("nope")})

				return err
			},
			wantCode: "ResourceNotFoundException", wantMsg: "Event bus nope does not exist.",
		},
		{
			name: "pattern_scalar",
			call: func(c *ebsdk.Client) error {
				_, err := c.PutRule(t.Context(), &ebsdk.PutRuleInput{
					Name: aws.String("r"), EventPattern: aws.String(`{"source":"a"}`),
				})

				return err
			},
			wantCode: "InvalidEventPatternException",
			wantMsg:  `Event pattern is not valid. Reason: "source" must be an object or an array`,
		},
		{
			name: "pattern_bad_json",
			call: func(c *ebsdk.Client) error {
				_, err := c.PutRule(t.Context(), &ebsdk.PutRuleInput{
					Name: aws.String("r"), EventPattern: aws.String(`{bad`),
				})

				return err
			},
			wantCode: "InvalidEventPatternException",
			wantMsg:  "Event pattern is not valid. Reason: Invalid JSON",
		},
		{
			name: "rule_name_chars",
			call: func(c *ebsdk.Client) error {
				_, err := c.PutRule(t.Context(), &ebsdk.PutRuleInput{
					Name: aws.String("bad name!"), EventPattern: aws.String(rulePattern),
				})

				return err
			},
			wantCode: "ValidationException",
			wantMsg:  "1 validation error detected: Value 'bad name!' at 'name' failed to satisfy constraint",
		},
		{
			name: "schedule_plural_for_one",
			call: func(c *ebsdk.Client) error {
				_, err := c.PutRule(t.Context(), &ebsdk.PutRuleInput{
					Name: aws.String("r"), ScheduleExpression: aws.String("rate(1 minutes)"),
				})

				return err
			},
			wantCode: "ValidationException", wantMsg: "Parameter ScheduleExpression is not valid.",
		},
		{
			name: "schedule_singular_for_many",
			call: func(c *ebsdk.Client) error {
				_, err := c.PutRule(t.Context(), &ebsdk.PutRuleInput{
					Name: aws.String("r"), ScheduleExpression: aws.String("rate(5 minute)"),
				})

				return err
			},
			wantCode: "ValidationException", wantMsg: "Parameter ScheduleExpression is not valid.",
		},
		{
			name: "target_id_chars",
			call: func(c *ebsdk.Client) error {
				_, err := c.PutRule(
					t.Context(),
					&ebsdk.PutRuleInput{Name: aws.String("r"), EventPattern: aws.String(rulePattern)},
				)
				if err != nil {
					return err
				}
				_, err = c.PutTargets(t.Context(), &ebsdk.PutTargetsInput{
					Rule: aws.String("r"),
					Targets: []ebtypes.Target{{
						Id: aws.String("bad id"), Arn: aws.String("arn:aws:sqs:us-east-1:123456789012:q"),
					}},
				})

				return err
			},
			wantCode: "ValidationException", wantMsg: "'targets.1.member.id'",
		},
		{
			name: "target_arn_format",
			call: func(c *ebsdk.Client) error {
				_, err := c.PutRule(
					t.Context(),
					&ebsdk.PutRuleInput{Name: aws.String("r"), EventPattern: aws.String(rulePattern)},
				)
				if err != nil {
					return err
				}
				_, err = c.PutTargets(t.Context(), &ebsdk.PutTargetsInput{
					Rule:    aws.String("r"),
					Targets: []ebtypes.Target{{Id: aws.String("1"), Arn: aws.String("notarn")}},
				})

				return err
			},
			wantCode: "ValidationException", wantMsg: "Provided Arn is not in correct format.",
		},
		{
			name: "tag_missing_rule",
			call: func(c *ebsdk.Client) error {
				_, err := c.TagResource(t.Context(), &ebsdk.TagResourceInput{
					ResourceARN: aws.String(ruleARNPrefix + "nope"),
					Tags:        []ebtypes.Tag{{Key: aws.String("a"), Value: aws.String("b")}},
				})

				return err
			},
			wantCode: "ResourceNotFoundException", wantMsg: "Rule nope does not exist on EventBus default.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

			err := tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}

func TestPutEvents_PerEntryFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		entry    ebtypes.PutEventsRequestEntry
		name     string
		wantCode string
	}{
		{
			name: "malformed_detail",
			entry: ebtypes.PutEventsRequestEntry{
				Source: aws.String("a"), DetailType: aws.String("t"), Detail: aws.String("{bad"),
			},
			wantCode: "MalformedDetail",
		},
		{
			name: "missing_bus",
			entry: ebtypes.PutEventsRequestEntry{
				Source: aws.String("a"), DetailType: aws.String("t"), Detail: aws.String("{}"),
				EventBusName: aws.String("nope"),
			},
			wantCode: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

			out, err := client.PutEvents(t.Context(), &ebsdk.PutEventsInput{
				Entries: []ebtypes.PutEventsRequestEntry{
					tt.entry,
					{Source: aws.String("a"), DetailType: aws.String("t"), Detail: aws.String("{}")},
				},
			})
			require.NoError(t, err)
			assert.EqualValues(t, 1, out.FailedEntryCount)
			require.Len(t, out.Entries, 2)
			assert.Equal(t, tt.wantCode, aws.ToString(out.Entries[0].ErrorCode))
			assert.NotEmpty(t, aws.ToString(out.Entries[1].EventId))
		})
	}
}
