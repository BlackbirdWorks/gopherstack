package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func v2Metric(name string) types.MetricV2 { return types.MetricV2{Name: aws.String(name)} }

func queueFilter(q string) []types.FilterV2 {
	return []types.FilterV2{{FilterKey: aws.String("QUEUE"), FilterValues: []string{q}}}
}

// historyEnv answers one queued call after 20s and abandons another after 40s.
func historyEnv(t *testing.T) (*contactEnv, time.Time) {
	t.Helper()

	e := newContactEnv(t)
	start := e.f.clock.now().Add(-time.Minute)

	answered := e.outbound(t, "+442071230001", true)
	e.f.clock.advance(20 * time.Second)

	_, err := e.f.client.UpdateContact(t.Context(), &sdk.UpdateContactInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(answered),
		UserInfo: &types.UserInfo{UserId: aws.String(e.user)},
	})
	require.NoError(t, err)

	abandoned := e.outbound(t, "+442071230002", true)
	e.f.clock.advance(40 * time.Second)
	e.stop(t, abandoned)
	e.f.clock.advance(30 * time.Second)
	e.stop(t, answered)

	return e, start
}

func (e *contactEnv) instARN() string {
	return "arn:aws:connect:us-east-1:" + testAccount + ":instance/" + e.inst
}

func TestGetMetricDataV2Values(t *testing.T) {
	t.Parallel()

	e, start := historyEnv(t)
	slo := types.MetricV2{
		Name:      aws.String("SERVICE_LEVEL"),
		Threshold: []types.ThresholdV2{{Comparison: aws.String("LT"), ThresholdValue: aws.Float64(30)}},
	}
	names := []string{
		"CONTACTS_CREATED", "CONTACTS_QUEUED", "CONTACTS_HANDLED", "CONTACTS_ABANDONED", "ABANDONMENT_RATE",
		"AVG_QUEUE_ANSWER_TIME", "AVG_ABANDON_TIME", "AVG_HANDLE_TIME", "AVG_CONTACT_DURATION", "MAX_QUEUED_TIME",
	}
	want := []float64{2, 2, 1, 1, 50, 20, 40, 70, 65, 40, 50}

	metrics := make([]types.MetricV2, 0, len(want))
	for _, n := range names {
		metrics = append(metrics, v2Metric(n))
	}

	out, err := e.f.client.GetMetricDataV2(t.Context(), &sdk.GetMetricDataV2Input{
		ResourceArn: aws.String(e.instARN()), StartTime: aws.Time(start), EndTime: aws.Time(e.f.clock.now()),
		Filters: queueFilter(e.queue), Metrics: append(metrics, slo),
	})
	require.NoError(t, err)
	require.Len(t, out.MetricResults, 1)

	res := out.MetricResults[0]
	require.Len(t, res.Collections, len(want))
	assert.Empty(t, res.Dimensions)
	assert.Equal(t, types.IntervalPeriodTotal, res.MetricInterval.Interval)

	for i, c := range res.Collections {
		assert.InDelta(t, want[i], aws.ToFloat64(c.Value), 0.001, aws.ToString(c.Metric.Name))
	}

	assert.Equal(t, "LT", aws.ToString(res.Collections[len(want)-1].Metric.Threshold[0].Comparison))
}

func TestGetMetricDataV2FiltersGroupingsIntervals(t *testing.T) {
	t.Parallel()

	e, start := historyEnv(t)
	call := func(mut func(*sdk.GetMetricDataV2Input)) *sdk.GetMetricDataV2Output {
		in := &sdk.GetMetricDataV2Input{
			ResourceArn: aws.String(e.instARN()), StartTime: aws.Time(start), EndTime: aws.Time(e.f.clock.now()),
			Filters: queueFilter(e.queue), Metrics: []types.MetricV2{v2Metric("CONTACTS_CREATED")},
		}
		mut(in)

		out, err := e.f.client.GetMetricDataV2(t.Context(), in)
		require.NoError(t, err)

		return out
	}

	byChannel := call(func(in *sdk.GetMetricDataV2Input) { in.Groupings = []string{"CHANNEL", "QUEUE"} })
	require.Len(t, byChannel.MetricResults, 1)
	assert.Equal(t, "VOICE", byChannel.MetricResults[0].Dimensions["CHANNEL"])
	assert.Equal(t, e.queue, byChannel.MetricResults[0].Dimensions["QUEUE"])
	assert.Contains(t, byChannel.MetricResults[0].Dimensions["QUEUE_ARN"], "/queue/"+e.queue)

	chatOnly := call(func(in *sdk.GetMetricDataV2Input) {
		in.Filters = append(
			in.Filters,
			types.FilterV2{FilterKey: aws.String("CHANNEL"), FilterValues: []string{"CHAT"}},
		)
	})
	assert.InDelta(t, 0, aws.ToFloat64(chatOnly.MetricResults[0].Collections[0].Value), 0.001)

	byMethod := call(func(in *sdk.GetMetricDataV2Input) {
		in.Metrics = []types.MetricV2{
			{
				Name: aws.String("CONTACTS_CREATED"),
				MetricFilters: []types.MetricFilterV2{
					{MetricFilterKey: aws.String("INITIATION_METHOD"), MetricFilterValues: []string{"OUTBOUND"}},
				},
			},
		}
	})
	assert.InDelta(t, 2, aws.ToFloat64(byMethod.MetricResults[0].Collections[0].Value), 0.001)

	negated := call(func(in *sdk.GetMetricDataV2Input) {
		in.Metrics = []types.MetricV2{{
			Name: aws.String("CONTACTS_CREATED"),
			MetricFilters: []types.MetricFilterV2{
				{
					MetricFilterKey:    aws.String("INITIATION_METHOD"),
					MetricFilterValues: []string{"OUTBOUND"},
					Negate:             true,
				},
			},
		}}
	})
	assert.InDelta(t, 0, aws.ToFloat64(negated.MetricResults[0].Collections[0].Value), 0.001)

	byAgent := call(func(in *sdk.GetMetricDataV2Input) { in.Groupings = []string{"AGENT"} })
	require.Len(t, byAgent.MetricResults, 1, "contacts without an agent have no AGENT group")

	e.f.clock.advance(3 * time.Hour)

	hourly := call(func(in *sdk.GetMetricDataV2Input) {
		in.StartTime = aws.Time(start)
		in.EndTime = aws.Time(start.Add(150 * time.Minute))
		in.Interval = &types.IntervalDetails{
			IntervalPeriod: types.IntervalPeriodHour,
			TimeZone:       aws.String("America/Chicago"),
		}
	})
	require.Len(t, hourly.MetricResults, 3)
	assert.InDelta(t, 2, aws.ToFloat64(hourly.MetricResults[0].Collections[0].Value), 0.001)
	assert.InDelta(t, 0, aws.ToFloat64(hourly.MetricResults[1].Collections[0].Value), 0.001)
	assert.Equal(t, start.Add(time.Hour).Unix(), hourly.MetricResults[1].MetricInterval.StartTime.Unix())
	assert.Equal(t, start.Add(150*time.Minute).Unix(), hourly.MetricResults[2].MetricInterval.EndTime.Unix())

	page := call(func(in *sdk.GetMetricDataV2Input) {
		in.EndTime = aws.Time(start.Add(150 * time.Minute))
		in.Interval = &types.IntervalDetails{IntervalPeriod: types.IntervalPeriodHour}
		in.MaxResults = aws.Int32(2)
	})
	require.Len(t, page.MetricResults, 2)
	require.NotNil(t, page.NextToken)
}

func TestGetMetricDataV2Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(e *contactEnv, in *sdk.GetMetricDataV2Input)
		name string
		code string
	}{
		{
			name: "bad arn",
			code: "InvalidParameterException",
			mut:  func(_ *contactEnv, in *sdk.GetMetricDataV2Input) { in.ResourceArn = aws.String("nope") },
		},
		{
			name: "unknown instance",
			code: "ResourceNotFoundException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.ResourceArn = aws.String("arn:aws:connect:us-east-1:123456789012:instance/missing")
			},
		},
		{
			name: "start after end",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.StartTime, in.EndTime = in.EndTime, in.StartTime
			},
		},
		{
			name: "end in future",
			code: "InvalidParameterException",
			mut: func(e *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.EndTime = aws.Time(e.f.clock.now().Add(time.Hour))
			},
		},
		{
			name: "no scoping filter",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Filters = []types.FilterV2{{FilterKey: aws.String("CHANNEL"), FilterValues: []string{"VOICE"}}}
			},
		},
		{
			name: "unsupported filter key",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Filters = append(
					in.Filters,
					types.FilterV2{FilterKey: aws.String("AGENT_HIERARCHY_LEVEL_ONE"), FilterValues: []string{"x"}},
				)
			},
		},
		{
			name: "unsupported grouping",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Groupings = []string{"FEATURE"}
			},
		},
		{
			name: "unsupported metric",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Metrics = []types.MetricV2{v2Metric("AGENT_ONLINE_TIME")}
			},
		},
		{
			name: "threshold missing",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Metrics = []types.MetricV2{v2Metric("SERVICE_LEVEL")}
			},
		},
		{
			name: "threshold on plain metric",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Metrics = []types.MetricV2{
					{
						Name:      aws.String("CONTACTS_QUEUED"),
						Threshold: []types.ThresholdV2{{Comparison: aws.String("LT"), ThresholdValue: aws.Float64(5)}},
					},
				}
			},
		},
		{
			name: "metric filter not valid for metric",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Metrics = []types.MetricV2{
					{
						Name: aws.String("CONTACTS_QUEUED"),
						MetricFilters: []types.MetricFilterV2{
							{MetricFilterKey: aws.String("INITIATION_METHOD"), MetricFilterValues: []string{"API"}},
						},
					},
				}
			},
		},
		{
			name: "name and id",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Metrics = []types.MetricV2{{Name: aws.String("CONTACTS_QUEUED"), MetricId: aws.String("x")}}
			},
		},
		{
			name: "unknown custom metric",
			code: "ResourceNotFoundException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Metrics = []types.MetricV2{{MetricId: aws.String("nope")}}
			},
		},
		{
			name: "span too long for hour",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.StartTime = aws.Time(in.EndTime.Add(-4 * 24 * time.Hour))
				in.Interval = &types.IntervalDetails{IntervalPeriod: types.IntervalPeriodHour}
			},
		},
		{
			name: "bad time zone",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.Interval = &types.IntervalDetails{TimeZone: aws.String("Mars/Base")}
			},
		},
		{
			name: "bad max results",
			code: "InvalidParameterException",
			mut: func(_ *contactEnv, in *sdk.GetMetricDataV2Input) {
				in.MaxResults = aws.Int32(500)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			in := &sdk.GetMetricDataV2Input{
				ResourceArn: aws.String(e.instARN()), StartTime: aws.Time(e.f.clock.now().Add(-time.Hour)),
				EndTime: aws.Time(e.f.clock.now()), Filters: queueFilter(e.queue),
				Metrics: []types.MetricV2{v2Metric("CONTACTS_CREATED")},
			}
			tt.mut(e, in)

			_, err := e.f.client.GetMetricDataV2(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestGetMetricDataV2CustomMetric(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	created, err := e.f.client.CreateMetric(t.Context(), &sdk.CreateMetricInput{
		InstanceId: aws.String(e.inst), Name: aws.String("rate"), Unit: types.MetricUnitPercent,
		MetricCalculation: calc("SUM(M1)", "M1"),
	})
	require.NoError(t, err)

	_, err = e.f.client.GetMetricDataV2(t.Context(), &sdk.GetMetricDataV2Input{
		ResourceArn: aws.String(e.instARN()), StartTime: aws.Time(e.f.clock.now().Add(-time.Hour)),
		EndTime: aws.Time(e.f.clock.now()), Filters: queueFilter(e.queue),
		Metrics: []types.MetricV2{{MetricId: created.MetricId}},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "existing custom metrics are not computed")
}

func TestGetContactMetrics(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	first := e.outbound(t, "+442071230001", true)
	e.f.clock.advance(5 * time.Second)
	second := e.outbound(t, "+442071230002", true)
	task := e.task(t)

	get := func(id string, names ...types.ContactMetricName) (*sdk.GetContactMetricsOutput, error) {
		in := &sdk.GetContactMetricsInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)}
		for _, n := range names {
			in.Metrics = append(in.Metrics, types.ContactMetricInfo{Name: n})
		}

		return e.f.client.GetContactMetrics(t.Context(), in)
	}
	number := func(r types.ContactMetricResult) float64 {
		return r.Value.(*types.ContactMetricValueMemberNumber).Value
	}

	out, err := get(second, types.ContactMetricNamePositionInQueue, types.ContactMetricNameEstimatedWaitTime)
	require.NoError(t, err)
	assert.Equal(t, second, aws.ToString(out.Id))
	assert.Contains(t, aws.ToString(out.Arn), second)
	require.Len(t, out.MetricResults, 2)
	assert.InDelta(t, 2, number(out.MetricResults[0]), 0.001)
	assert.InDelta(t, 0, number(out.MetricResults[1]), 0.001, "no answered contact to learn a wait from")

	_, err = e.f.client.UpdateContact(t.Context(), &sdk.UpdateContactInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(first),
		UserInfo: &types.UserInfo{UserId: aws.String(e.user)},
	})
	require.NoError(t, err)

	out, err = get(second, types.ContactMetricNamePositionInQueue, types.ContactMetricNameEstimatedWaitTime)
	require.NoError(t, err)
	assert.InDelta(t, 1, number(out.MetricResults[0]), 0.001)
	assert.InDelta(t, 5, number(out.MetricResults[1]), 0.001, "one slot at the 5s mean queue answer time")

	_, err = get(first, types.ContactMetricNamePositionInQueue)
	assert.Equal(t, "InvalidRequestException", apiCode(err), "connected contacts are not waiting")

	_, err = get(task, types.ContactMetricNamePositionInQueue)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = get("missing", types.ContactMetricNamePositionInQueue)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = get(second, "BOGUS")
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}
