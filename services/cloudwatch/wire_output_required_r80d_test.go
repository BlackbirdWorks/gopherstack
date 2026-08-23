package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAlarmMuteRule_MuteTargets_EmptyAlarmNames_RealClient covers
// gopherstack-r80d batch 33: types.MuteTargets.AlarmNames is required
// (cloudwatch@v1.66.3 types/types.go:3223), but the real client-side
// validator only null-checks it (validateMuteTargets, validators.go:1418-1425),
// so a real client can legally send PutAlarmMuteRuleInput.MuteTargets with a
// non-nil, empty AlarmNames array. Pre-fix, buildAlarmMuteRuleCBOR gated
// emission on len(rule.AlarmNames) > 0, so the entire MuteTargets wrapper
// vanished from GetAlarmMuteRule's response instead of coming back as
// {AlarmNames: []} -- indistinguishable, on the wire, from a rule that never
// had MuteTargets set at all.
func TestGetAlarmMuteRule_MuteTargets_EmptyAlarmNames_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutAlarmMuteRule(ctx, &cwsdk.PutAlarmMuteRuleInput{
		Name: aws.String("mute-empty-targets"),
		Rule: &cwtypes.Rule{
			Schedule: &cwtypes.Schedule{
				Expression: aws.String("cron(0 2 * * *)"),
				Duration:   aws.String("PT1H"),
			},
		},
		MuteTargets: &cwtypes.MuteTargets{
			AlarmNames: []string{},
		},
	})
	require.NoError(t, err, "PutAlarmMuteRule should succeed: the real validator only null-checks AlarmNames")

	out, err := client.GetAlarmMuteRule(ctx, &cwsdk.GetAlarmMuteRuleInput{
		AlarmMuteRuleName: aws.String("mute-empty-targets"),
	})
	require.NoError(t, err)

	// The provable, observable distinction is the wrapper object's presence
	// (a *types.MuteTargets pointer): the smithy-go rpc-v2-cbor deserializer
	// collapses a present-but-zero-length list to a nil Go slice the same
	// way it collapses an absent key, so AlarmNames itself cannot
	// distinguish "explicitly empty" from "never set" through this client --
	// only MuteTargets's own presence can.
	require.NotNil(t, out.MuteTargets,
		"MuteTargets must still be present when explicitly set with an empty AlarmNames array")
	assert.Empty(t, out.MuteTargets.AlarmNames)
}

// TestGetMetricStream_StatisticsConfigurations_RealClient covers
// gopherstack-r80d batch 33: types.MetricStreamStatisticsConfiguration's
// AdditionalStatistics/IncludeMetrics are both required (cloudwatch@v1.66.3
// types/types.go:3270), but PutMetricStreamInput.StatisticsConfigurations was
// structurally absent from gopherstack's model entirely -- never parsed on
// Put, never stored, never emitted on Get. A real client configuring
// additional statistics had that configuration silently discarded.
func TestGetMetricStream_StatisticsConfigurations_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutMetricStream(ctx, &cwsdk.PutMetricStreamInput{
		Name:         aws.String("stream-stats"),
		FirehoseArn:  aws.String("arn:aws:firehose:us-east-1:123456789012:deliverystream/test"),
		RoleArn:      aws.String("arn:aws:iam::123456789012:role/test"),
		OutputFormat: cwtypes.MetricStreamOutputFormatJson,
		StatisticsConfigurations: []cwtypes.MetricStreamStatisticsConfiguration{
			{
				AdditionalStatistics: []string{"p99"},
				IncludeMetrics: []cwtypes.MetricStreamStatisticsMetric{
					{Namespace: aws.String("AWS/EC2"), MetricName: aws.String("CPUUtilization")},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetMetricStream(ctx, &cwsdk.GetMetricStreamInput{
		Name: aws.String("stream-stats"),
	})
	require.NoError(t, err)

	require.Len(t, out.StatisticsConfigurations, 1,
		"StatisticsConfigurations must round-trip through Put -> Get; pre-fix this field did not exist")
	cfg := out.StatisticsConfigurations[0]
	assert.Equal(t, []string{"p99"}, cfg.AdditionalStatistics)
	require.Len(t, cfg.IncludeMetrics, 1)
	assert.Equal(t, "AWS/EC2", aws.ToString(cfg.IncludeMetrics[0].Namespace))
	assert.Equal(t, "CPUUtilization", aws.ToString(cfg.IncludeMetrics[0].MetricName))
}
