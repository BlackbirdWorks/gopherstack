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

func currentValues(out *sdk.GetCurrentMetricDataOutput) map[types.CurrentMetricName]float64 {
	vals := make(map[types.CurrentMetricName]float64)

	for _, r := range out.MetricResults {
		for _, c := range r.Collections {
			vals[c.Metric.Name] = aws.ToFloat64(c.Value)
		}
	}

	return vals
}

func TestCurrentMetricsFromContacts(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)

	query := func() *sdk.GetCurrentMetricDataOutput {
		out, err := e.f.client.GetCurrentMetricData(t.Context(), &sdk.GetCurrentMetricDataInput{
			InstanceId: inst, Filters: &types.Filters{Queues: []string{e.queue}},
			CurrentMetrics: []types.CurrentMetric{
				{Name: types.CurrentMetricNameContactsInQueue, Unit: types.UnitCount},
				{Name: types.CurrentMetricNameOldestContactAge, Unit: types.UnitSeconds},
			},
		})
		require.NoError(t, err)

		return out
	}

	assert.InDelta(t, 0.0, currentValues(query())[types.CurrentMetricNameContactsInQueue], 0.001)

	first := e.outbound(t, "+442071230001", true)
	e.f.clock.advance(30 * time.Second)
	second := e.outbound(t, "+442071230002", true)
	e.f.clock.advance(30 * time.Second)

	vals := currentValues(query())
	assert.InDelta(t, 2.0, vals[types.CurrentMetricNameContactsInQueue], 0.001)
	assert.InDelta(t, 60.0, vals[types.CurrentMetricNameOldestContactAge], 0.001)

	_, err := e.f.client.UpdateContact(
		t.Context(),
		&sdk.UpdateContactInput{
			InstanceId: inst,
			ContactId:  aws.String(first),
			UserInfo:   &types.UserInfo{UserId: aws.String(e.user)},
		},
	)
	require.NoError(t, err)

	vals = currentValues(query())
	assert.InDelta(t, 1.0, vals[types.CurrentMetricNameContactsInQueue], 0.001)
	assert.InDelta(t, 30.0, vals[types.CurrentMetricNameOldestContactAge], 0.001)

	_, err = e.f.client.StopContact(t.Context(), &sdk.StopContactInput{InstanceId: inst, ContactId: aws.String(second)})
	require.NoError(t, err)
	assert.InDelta(t, 0.0, currentValues(query())[types.CurrentMetricNameContactsInQueue], 0.001)

	grouped, err := e.f.client.GetCurrentMetricData(t.Context(), &sdk.GetCurrentMetricDataInput{
		InstanceId: inst,
		Filters: &types.Filters{
			Queues:   []string{e.queue},
			Channels: []types.Channel{types.ChannelVoice, types.ChannelChat},
		},
		Groupings:      []types.Grouping{types.GroupingQueue, types.GroupingChannel},
		CurrentMetrics: []types.CurrentMetric{{Name: types.CurrentMetricNameContactsInQueue, Unit: types.UnitCount}},
	})
	require.NoError(t, err)
	require.Len(t, grouped.MetricResults, 2)
	assert.Equal(t, e.queue, aws.ToString(grouped.MetricResults[0].Dimensions.Queue.Id))
}

func TestCurrentMetricsForAgents(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)

	statuses, err := e.f.client.ListAgentStatuses(
		t.Context(),
		&sdk.ListAgentStatusesInput{
			InstanceId:       inst,
			AgentStatusTypes: []types.AgentStatusType{types.AgentStatusTypeRoutable},
		},
	)
	require.NoError(t, err)

	_, err = e.f.client.PutUserStatus(
		t.Context(),
		&sdk.PutUserStatusInput{
			InstanceId:    inst,
			UserId:        aws.String(e.user),
			AgentStatusId: statuses.AgentStatusSummaryList[0].Id,
		},
	)
	require.NoError(t, err)

	metrics := []types.CurrentMetric{
		{Name: types.CurrentMetricNameAgentsOnline, Unit: types.UnitCount},
		{Name: types.CurrentMetricNameAgentsAvailable, Unit: types.UnitCount},
		{Name: types.CurrentMetricNameAgentsOnCall, Unit: types.UnitCount},
		{Name: types.CurrentMetricNameSlotsAvailable, Unit: types.UnitCount},
	}
	get := func() map[types.CurrentMetricName]float64 {
		out, gerr := e.f.client.GetCurrentMetricData(
			t.Context(),
			&sdk.GetCurrentMetricDataInput{
				InstanceId:     inst,
				Filters:        &types.Filters{Queues: []string{e.queue}},
				CurrentMetrics: metrics,
			},
		)
		require.NoError(t, gerr)

		return currentValues(out)
	}

	vals := get()
	assert.InDelta(t, 1.0, vals[types.CurrentMetricNameAgentsOnline], 0.001)
	assert.InDelta(t, 1.0, vals[types.CurrentMetricNameAgentsAvailable], 0.001)
	assert.InDelta(t, 0.0, vals[types.CurrentMetricNameAgentsOnCall], 0.001)
	assert.InDelta(t, 4.0, vals[types.CurrentMetricNameSlotsAvailable], 0.001, "1 voice + 2 chat + 1 task slots")

	contact := e.outbound(t, "+442071230001", true)
	_, err = e.f.client.UpdateContact(
		t.Context(),
		&sdk.UpdateContactInput{
			InstanceId: inst,
			ContactId:  aws.String(contact),
			UserInfo:   &types.UserInfo{UserId: aws.String(e.user)},
		},
	)
	require.NoError(t, err)

	vals = get()
	assert.InDelta(t, 0.0, vals[types.CurrentMetricNameAgentsAvailable], 0.001)
	assert.InDelta(t, 1.0, vals[types.CurrentMetricNameAgentsOnCall], 0.001)
	assert.InDelta(t, 3.0, vals[types.CurrentMetricNameSlotsAvailable], 0.001)

	byProfile, err := e.f.client.GetCurrentMetricData(t.Context(), &sdk.GetCurrentMetricDataInput{
		InstanceId:     inst,
		Filters:        &types.Filters{Queues: []string{e.queue}},
		Groupings:      []types.Grouping{types.GroupingRoutingProfile},
		CurrentMetrics: metrics[:1],
	})
	require.NoError(t, err)
	require.Len(t, byProfile.MetricResults, 1)
	assert.NotNil(t, byProfile.MetricResults[0].Dimensions.RoutingProfile)

	data, err := e.f.client.GetCurrentUserData(
		t.Context(),
		&sdk.GetCurrentUserDataInput{InstanceId: inst, Filters: &types.UserDataFilters{Queues: []string{e.queue}}},
	)
	require.NoError(t, err)
	require.Len(t, data.UserDataList, 1)
	require.Len(t, data.UserDataList[0].Contacts, 1)
	assert.Equal(t, contact, aws.ToString(data.UserDataList[0].Contacts[0].ContactId))
	assert.Equal(t, int32(1), data.UserDataList[0].ActiveSlotsByChannel["VOICE"])
	assert.Equal(t, int32(0), data.UserDataList[0].AvailableSlotsByChannel["VOICE"])
}

func TestCurrentMetricValidation(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	ok := []types.CurrentMetric{{Name: types.CurrentMetricNameContactsInQueue, Unit: types.UnitCount}}

	tests := []struct {
		in   *sdk.GetCurrentMetricDataInput
		name string
	}{
		{name: "no scope", in: &sdk.GetCurrentMetricDataInput{Filters: &types.Filters{}, CurrentMetrics: ok}},
		{
			name: "unsupported metric",
			in: &sdk.GetCurrentMetricDataInput{
				Filters: &types.Filters{Queues: []string{e.queue}},
				CurrentMetrics: []types.CurrentMetric{
					{Name: types.CurrentMetricNameEstimatedWaitTime, Unit: types.UnitSeconds},
				},
			},
		},
		{
			name: "wrong unit",
			in: &sdk.GetCurrentMetricDataInput{
				Filters: &types.Filters{Queues: []string{e.queue}},
				CurrentMetrics: []types.CurrentMetric{
					{Name: types.CurrentMetricNameContactsInQueue, Unit: types.UnitSeconds},
				},
			},
		},
		{
			name: "contact metric by profile",
			in: &sdk.GetCurrentMetricDataInput{
				Filters:        &types.Filters{Queues: []string{e.queue}},
				Groupings:      []types.Grouping{types.GroupingRoutingProfile},
				CurrentMetrics: ok,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.in.InstanceId = aws.String(e.inst)
			_, err := e.f.client.GetCurrentMetricData(t.Context(), tt.in)
			assert.Equal(t, "InvalidParameterException", apiCode(err))
		})
	}
}

func TestHistoricalMetrics(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)
	start := e.f.clock.now().Add(-time.Minute)

	answered := e.outbound(t, "+442071230001", true)
	e.f.clock.advance(20 * time.Second)

	_, err := e.f.client.UpdateContact(
		t.Context(),
		&sdk.UpdateContactInput{
			InstanceId: inst,
			ContactId:  aws.String(answered),
			UserInfo:   &types.UserInfo{UserId: aws.String(e.user)},
		},
	)
	require.NoError(t, err)

	abandoned := e.outbound(t, "+442071230002", true)
	e.f.clock.advance(40 * time.Second)

	_, err = e.f.client.StopContact(
		t.Context(),
		&sdk.StopContactInput{InstanceId: inst, ContactId: aws.String(abandoned)},
	)
	require.NoError(t, err)

	out, err := e.f.client.GetMetricData(t.Context(), &sdk.GetMetricDataInput{
		InstanceId: inst, StartTime: aws.Time(start), EndTime: aws.Time(e.f.clock.now().Add(time.Minute)),
		Filters: &types.Filters{Queues: []string{e.queue}},
		HistoricalMetrics: []types.HistoricalMetric{
			{Name: types.HistoricalMetricNameContactsQueued, Statistic: types.StatisticSum, Unit: types.UnitCount},
			{Name: types.HistoricalMetricNameContactsHandled, Statistic: types.StatisticSum, Unit: types.UnitCount},
			{Name: types.HistoricalMetricNameContactsAbandoned, Statistic: types.StatisticSum, Unit: types.UnitCount},
			{
				Name:      types.HistoricalMetricNameContactsHandledOutbound,
				Statistic: types.StatisticSum,
				Unit:      types.UnitCount,
			},
			{Name: types.HistoricalMetricNameQueuedTime, Statistic: types.StatisticMax, Unit: types.UnitSeconds},
			{Name: types.HistoricalMetricNameQueuedTime, Statistic: types.StatisticAvg, Unit: types.UnitSeconds},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.MetricResults, 1)

	got := make([]float64, 0, len(out.MetricResults[0].Collections))
	for _, c := range out.MetricResults[0].Collections {
		got = append(got, aws.ToFloat64(c.Value))
	}

	assert.Equal(t, []float64{2, 1, 1, 1, 40, 30}, got)

	_, err = e.f.client.GetMetricData(t.Context(), &sdk.GetMetricDataInput{
		InstanceId: inst,
		StartTime:  aws.Time(start),
		EndTime:    aws.Time(start.Add(-time.Hour)),
		Filters:    &types.Filters{Queues: []string{e.queue}},
		HistoricalMetrics: []types.HistoricalMetric{
			{Name: types.HistoricalMetricNameContactsQueued, Statistic: types.StatisticSum, Unit: types.UnitCount},
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = e.f.client.GetMetricData(t.Context(), &sdk.GetMetricDataInput{
		InstanceId: inst,
		StartTime:  aws.Time(start),
		EndTime:    aws.Time(start.Add(time.Hour)),
		Filters:    &types.Filters{Queues: []string{e.queue}},
		HistoricalMetrics: []types.HistoricalMetric{
			{Name: types.HistoricalMetricNameQueuedTime, Statistic: types.StatisticSum, Unit: types.UnitSeconds},
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}
