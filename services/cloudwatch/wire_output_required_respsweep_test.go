package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestMetricAndCompositeAlarm_StateUpdatedTimestamp_RealClient proves the
// required-output-member sweep fix: MetricAlarm/CompositeAlarm both have a
// StateUpdatedTimestamp member (cloudwatch@v1.66.3 types/types.go:2178 and
// :579) that neither domain struct had a field for at all, so neither wire
// protocol (rpcv2cbor -- this SDK's only protocol, api_client.go:214 -- or
// the legacy XML/form path) ever emitted it. Verified through a real SDK
// client round trip: create both alarm kinds, DescribeAlarms, and confirm
// the field is present and non-nil on both.
func TestMetricAndCompositeAlarm_StateUpdatedTimestamp_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutMetricAlarm(ctx, &cwsdk.PutMetricAlarmInput{
		AlarmName:          aws.String("sut-metric"),
		Namespace:          aws.String("NS"),
		MetricName:         aws.String("M"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(1),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
	})
	require.NoError(t, err)

	_, err = client.PutCompositeAlarm(ctx, &cwsdk.PutCompositeAlarmInput{
		AlarmName: aws.String("sut-composite"),
		AlarmRule: aws.String(`ALARM("nonexistent")`),
	})
	require.NoError(t, err)

	out, err := client.DescribeAlarms(ctx, &cwsdk.DescribeAlarmsInput{
		AlarmTypes: []cwtypes.AlarmType{cwtypes.AlarmTypeMetricAlarm, cwtypes.AlarmTypeCompositeAlarm},
	})
	require.NoError(t, err)

	require.Len(t, out.MetricAlarms, 1)
	require.NotNil(t, out.MetricAlarms[0].StateUpdatedTimestamp, "MetricAlarm.StateUpdatedTimestamp must be present")
	assert.False(t, out.MetricAlarms[0].StateUpdatedTimestamp.IsZero())

	require.Len(t, out.CompositeAlarms, 1)
	require.NotNil(
		t,
		out.CompositeAlarms[0].StateUpdatedTimestamp,
		"CompositeAlarm.StateUpdatedTimestamp must be present",
	)
	assert.False(t, out.CompositeAlarms[0].StateUpdatedTimestamp.IsZero())
}

// getMetricAlarm and getCompositeAlarm read an alarm back through the
// backend's own exported DescribeAlarms, at full Go time.Time precision --
// unlike the wire protocols above, cborFromTime truncates to whole seconds
// (rpcv2cbor.go), which would make same-second updates indistinguishable at
// the wire layer. The semantic under test here (StateUpdatedTimestamp moves
// on every state update, even a no-op one, while StateTransitionedTimestamp
// only moves on an actual value change) is proved at the domain-model layer
// instead, where no precision is lost.
func getMetricAlarm(t *testing.T, b *cloudwatch.InMemoryBackend, name string) cloudwatch.MetricAlarm {
	t.Helper()

	pg, _, _, err := b.DescribeAlarms([]string{name}, nil, "", "", "", 0, "", "", "")
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)

	return pg.Data[0]
}

func getCompositeAlarm(t *testing.T, b *cloudwatch.InMemoryBackend, name string) cloudwatch.CompositeAlarm {
	t.Helper()

	_, pg, _, err := b.DescribeAlarms([]string{name}, []string{"CompositeAlarm"}, "", "", "", 0, "", "", "")
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)

	return pg.Data[0]
}

// TestAlarmStateUpdatedTimestamp_SurvivesNoOpSetAlarmState proves the
// distinguishing semantic named in both fields' own SDK doc comments
// ("Tracks the timestamp of any state update, even if StateValue doesn't
// change" -- types/types.go:578): a second SetAlarmState call that
// re-affirms the alarm's current state must still advance
// StateUpdatedTimestamp, while StateTransitionedTimestamp -- which only
// moves on an actual value change -- must not.
func TestAlarmStateUpdatedTimestamp_SurvivesNoOpSetAlarmState(t *testing.T) {
	t.Parallel()

	t.Run("metric alarm", func(t *testing.T) {
		t.Parallel()

		b := cloudwatch.NewInMemoryBackend()
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName:          "noop-metric",
			Namespace:          "NS",
			MetricName:         "M",
			ComparisonOperator: "GreaterThanThreshold",
			Threshold:          1,
			EvaluationPeriods:  1,
			Period:             60,
		}))

		ctx := t.Context()
		require.NoError(t, b.SetAlarmState(ctx, "noop-metric", "ALARM", "reason", ""))
		first := getMetricAlarm(t, b, "noop-metric")

		require.NoError(t, b.SetAlarmState(ctx, "noop-metric", "ALARM", "reason again", ""))
		second := getMetricAlarm(t, b, "noop-metric")

		assert.Equal(t, first.StateTransitionedTimestamp, second.StateTransitionedTimestamp,
			"StateTransitionedTimestamp must not move when StateValue doesn't change")
		assert.True(t, second.StateUpdatedTimestamp.After(first.StateUpdatedTimestamp),
			"StateUpdatedTimestamp must advance on every state update, even a no-op one")
	})

	// PutCompositeAlarm, not SetAlarmState, is the no-op-update event under
	// test here: composite alarms self-evaluate their rule on every Put, and
	// setAlarmStateLocked's own reevaluateCompositeAlarms pass would
	// immediately override a manually-SetAlarmState'd composite alarm back to
	// its rule-computed state, making a SetAlarmState-based no-op scenario
	// unreliable for this alarm type specifically.
	t.Run("composite alarm", func(t *testing.T) {
		t.Parallel()

		b := cloudwatch.NewInMemoryBackend()
		alarm := &cloudwatch.CompositeAlarm{
			AlarmName: "noop-composite",
			AlarmRule: `ALARM("nonexistent")`,
		}
		require.NoError(t, b.PutCompositeAlarm(alarm))
		first := getCompositeAlarm(t, b, "noop-composite")

		alarm2 := &cloudwatch.CompositeAlarm{
			AlarmName: "noop-composite",
			AlarmRule: `ALARM("nonexistent")`,
		}
		require.NoError(t, b.PutCompositeAlarm(alarm2))
		second := getCompositeAlarm(t, b, "noop-composite")

		require.Equal(t, first.StateValue, second.StateValue, "rule result must be unchanged across both Puts")
		assert.Equal(t, first.StateTransitionedTimestamp, second.StateTransitionedTimestamp,
			"StateTransitionedTimestamp must not move when StateValue doesn't change")
		assert.True(t, second.StateUpdatedTimestamp.After(first.StateUpdatedTimestamp),
			"StateUpdatedTimestamp must advance on every state update, even a no-op one")
	})
}
