package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateThing_ExpectedVersionOmittedVsZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expected *int64
		name     string
		wantErr  bool
	}{
		{name: "omitted_applies", expected: nil},
		{name: "matching_applies", expected: aws.Int64(1)},
		{name: "explicit_zero_conflicts", expected: aws.Int64(0), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newIoTDroppedEnv(t)
			_, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("t")})
			require.NoError(t, err)

			_, err = e.client.UpdateThing(t.Context(), &iotsdk.UpdateThingInput{
				ThingName:        aws.String("t"),
				ExpectedVersion:  tt.expected,
				AttributePayload: &types.AttributePayload{Attributes: map[string]string{"k": "v"}},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateThingGroup_ExpectedVersionOmittedVsZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expected *int64
		name     string
		wantErr  bool
	}{
		{name: "omitted_applies", expected: nil},
		{name: "matching_applies", expected: aws.Int64(1)},
		{name: "explicit_zero_conflicts", expected: aws.Int64(0), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newIoTDroppedEnv(t)
			_, err := e.client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
				ThingGroupName: aws.String("g"),
			})
			require.NoError(t, err)

			_, err = e.client.UpdateThingGroup(t.Context(), &iotsdk.UpdateThingGroupInput{
				ThingGroupName:       aws.String("g"),
				ExpectedVersion:      tt.expected,
				ThingGroupProperties: &types.ThingGroupProperties{ThingGroupDescription: aws.String("d")},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateFleetMetric_PeriodAndVersionOmittedVsZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		period     *int32
		version    *int64
		name       string
		wantPeriod int32
		wantErr    bool
	}{
		{name: "omitted_keeps_period", wantPeriod: 300},
		{name: "explicit_period_applies", period: aws.Int32(120), wantPeriod: 120},
		{name: "explicit_zero_period_rejected", period: aws.Int32(0), wantErr: true},
		{name: "explicit_zero_version_conflicts", version: aws.Int64(0), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newIoTDroppedEnv(t)
			_, err := e.client.CreateFleetMetric(t.Context(), &iotsdk.CreateFleetMetricInput{
				MetricName:  aws.String("m"),
				QueryString: aws.String("thingName:*"),
				AggregationType: &types.AggregationType{
					Name: types.AggregationTypeNameStatistics, Values: []string{"count"},
				},
				AggregationField: aws.String("registry.version"),
				Period:           aws.Int32(300),
			})
			require.NoError(t, err)

			_, err = e.client.UpdateFleetMetric(t.Context(), &iotsdk.UpdateFleetMetricInput{
				MetricName:      aws.String("m"),
				IndexName:       aws.String("AWS_Things"),
				Period:          tt.period,
				ExpectedVersion: tt.version,
				Description:     aws.String("d"),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := e.client.DescribeFleetMetric(t.Context(), &iotsdk.DescribeFleetMetricInput{
				MetricName: aws.String("m"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantPeriod, aws.ToInt32(got.Period))
		})
	}
}
