package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDK_PutAlarmMuteRule drives PutAlarmMuteRule/GetAlarmMuteRule through a
// real aws-sdk-go-v2 cloudwatch@v1.66.3 client. cloudwatch's whole client
// speaks smithy rpc-v2-cbor exclusively (options.Protocol is hardcoded to
// rpcv2.NewCBOR in api_client.go:214), so the client's own PutAlarmMuteRuleInput
// validation runs before anything is serialized: a request missing Name or
// Rule.Schedule never reaches the handler at all. This exercises the wire
// shape a real client can actually construct and send.
func TestSDK_PutAlarmMuteRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expression string
		duration   string
		wantStatus cwtypes.AlarmMuteRuleStatus
	}{
		{
			name:       "recurring cron schedule",
			expression: "cron(0 2 * * *)",
			duration:   "PT1H",
			wantStatus: cwtypes.AlarmMuteRuleStatusActive,
		},
		{
			name:       "one time at schedule",
			expression: "at(2030-05-10T14:00)",
			duration:   "PT30M",
			wantStatus: cwtypes.AlarmMuteRuleStatusActive,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.PutAlarmMuteRule(ctx, &cwsdk.PutAlarmMuteRuleInput{
				Name:        aws.String("mute-" + tc.name),
				Description: aws.String("integration test rule"),
				Rule: &cwtypes.Rule{
					Schedule: &cwtypes.Schedule{
						Expression: aws.String(tc.expression),
						Duration:   aws.String(tc.duration),
					},
				},
				MuteTargets: &cwtypes.MuteTargets{
					AlarmNames: []string{"alarm-a", "alarm-b"},
				},
			})
			require.NoError(t, err, "PutAlarmMuteRule should succeed against the real wire shape")

			out, err := client.GetAlarmMuteRule(ctx, &cwsdk.GetAlarmMuteRuleInput{
				AlarmMuteRuleName: aws.String("mute-" + tc.name),
			})
			require.NoError(t, err)

			assert.Equal(t, "mute-"+tc.name, aws.ToString(out.Name))
			assert.NotEmpty(t, aws.ToString(out.AlarmMuteRuleArn))
			assert.Equal(t, "integration test rule", aws.ToString(out.Description))
			require.NotNil(t, out.Rule)
			require.NotNil(t, out.Rule.Schedule)
			assert.Equal(t, tc.expression, aws.ToString(out.Rule.Schedule.Expression))
			assert.Equal(t, tc.duration, aws.ToString(out.Rule.Schedule.Duration))
			require.NotNil(t, out.MuteTargets)
			assert.ElementsMatch(t, []string{"alarm-a", "alarm-b"}, out.MuteTargets.AlarmNames)
			assert.Equal(t, tc.wantStatus, out.Status)
			assert.NotNil(t, out.LastUpdatedTimestamp)
		})
	}
}

// TestSDK_GetAlarmMuteRule_MuteType verifies the derived MuteType matches the
// schedule expression style (types.Schedule doc, aws-sdk-go-v2
// cloudwatch@v1.66.3 types/types.go:3461).
func TestSDK_GetAlarmMuteRule_MuteType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		expression   string
		wantMuteType string
	}{
		{name: "cron is recurring", expression: "cron(0 2 * * *)", wantMuteType: "RECURRING"},
		{name: "at is one time", expression: "at(2030-05-10T14:00)", wantMuteType: "ONE_TIME"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.PutAlarmMuteRule(ctx, &cwsdk.PutAlarmMuteRuleInput{
				Name: aws.String("mute-type-" + tc.name),
				Rule: &cwtypes.Rule{
					Schedule: &cwtypes.Schedule{
						Expression: aws.String(tc.expression),
						Duration:   aws.String("PT1H"),
					},
				},
			})
			require.NoError(t, err)

			out, err := client.GetAlarmMuteRule(ctx, &cwsdk.GetAlarmMuteRuleInput{
				AlarmMuteRuleName: aws.String("mute-type-" + tc.name),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantMuteType, aws.ToString(out.MuteType))
		})
	}
}

// TestSDK_ListAlarmMuteRules_FilterByAlarmName verifies the AlarmName filter
// (ListAlarmMuteRulesInput.AlarmName, aws-sdk-go-v2 cloudwatch@v1.66.3
// api_op_ListAlarmMuteRules.go:44) and the summary shape, which carries an ARN
// but no Name (types.AlarmMuteRuleSummary, types/types.go:181).
func TestSDK_ListAlarmMuteRules_FilterByAlarmName(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	putRule := func(t *testing.T, name string, alarms []string) {
		t.Helper()
		_, err := client.PutAlarmMuteRule(ctx, &cwsdk.PutAlarmMuteRuleInput{
			Name: aws.String(name),
			Rule: &cwtypes.Rule{
				Schedule: &cwtypes.Schedule{
					Expression: aws.String("cron(0 2 * * *)"),
					Duration:   aws.String("PT1H"),
				},
			},
			MuteTargets: &cwtypes.MuteTargets{AlarmNames: alarms},
		})
		require.NoError(t, err)
	}

	putRule(t, "list-mute-a", []string{"target-alarm"})
	putRule(t, "list-mute-b", []string{"other-alarm"})

	out, err := client.ListAlarmMuteRules(ctx, &cwsdk.ListAlarmMuteRulesInput{
		AlarmName: aws.String("target-alarm"),
	})
	require.NoError(t, err)
	require.Len(t, out.AlarmMuteRuleSummaries, 1)

	summary := out.AlarmMuteRuleSummaries[0]
	assert.NotEmpty(t, aws.ToString(summary.AlarmMuteRuleArn))
	assert.Contains(t, aws.ToString(summary.AlarmMuteRuleArn), "list-mute-a")
	assert.Equal(t, "RECURRING", aws.ToString(summary.MuteType))
}

// TestSDK_DeleteAlarmMuteRule_Idempotent verifies deleting a mute rule that
// does not exist succeeds without error (api_op_DeleteAlarmMuteRule.go:19:
// "This operation is idempotent").
func TestSDK_DeleteAlarmMuteRule_Idempotent(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.DeleteAlarmMuteRule(t.Context(), &cwsdk.DeleteAlarmMuteRuleInput{
		AlarmMuteRuleName: aws.String("never-existed"),
	})
	require.NoError(t, err)
}
