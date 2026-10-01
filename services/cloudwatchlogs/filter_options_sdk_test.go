package cloudwatchlogs_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestMetricFilter_SystemFieldOptions_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		criteria string
		dims     []string
		wantErr  bool
	}{
		{name: "valid", dims: []string{"@aws.account", "@aws.region"}, criteria: `@aws.region = "us-east-1"`},
		{name: "bad_dimension", dims: []string{"@source.log"}, wantErr: true},
		{name: "criteria_too_long", criteria: strings.Repeat("a", 2001), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			ctx := t.Context()
			_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("g")})
			require.NoError(t, err)

			in := &cwlsdk.PutMetricFilterInput{
				LogGroupName:  aws.String("g"),
				FilterName:    aws.String("f"),
				FilterPattern: aws.String("ERROR"),
				MetricTransformations: []cwltypes.MetricTransformation{{
					MetricName: aws.String("m"), MetricNamespace: aws.String("ns"), MetricValue: aws.String("1"),
				}},
				ApplyOnTransformedLogs:    true,
				EmitSystemFieldDimensions: tt.dims,
			}
			if tt.criteria != "" {
				in.FieldSelectionCriteria = aws.String(tt.criteria)
			}
			_, err = client.PutMetricFilter(ctx, in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			out, err := client.DescribeMetricFilters(
				ctx,
				&cwlsdk.DescribeMetricFiltersInput{LogGroupName: aws.String("g")},
			)
			require.NoError(t, err)
			require.Len(t, out.MetricFilters, 1)
			mf := out.MetricFilters[0]
			assert.True(t, mf.ApplyOnTransformedLogs)
			assert.Equal(t, tt.dims, mf.EmitSystemFieldDimensions)
			assert.Equal(t, tt.criteria, aws.ToString(mf.FieldSelectionCriteria))
		})
	}
}

func TestSubscriptionFilter_SystemFieldOptions_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		fields  []string
		wantErr bool
	}{
		{name: "valid", fields: []string{"@aws.account", "@aws.region", "@source.log"}},
		{name: "bad_field", fields: []string{"@aws.nope"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			ctx := t.Context()
			_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("g")})
			require.NoError(t, err)

			_, err = client.PutSubscriptionFilter(ctx, &cwlsdk.PutSubscriptionFilterInput{
				LogGroupName:           aws.String("g"),
				FilterName:             aws.String("f"),
				FilterPattern:          aws.String(""),
				DestinationArn:         aws.String("arn:aws:lambda:us-east-1:000000000000:function:x"),
				ApplyOnTransformedLogs: true,
				EmitSystemFields:       tt.fields,
				FieldSelectionCriteria: aws.String(`@aws.region NOT IN ["cn-north-1"]`),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			out, err := client.DescribeSubscriptionFilters(ctx, &cwlsdk.DescribeSubscriptionFiltersInput{
				LogGroupName: aws.String("g"),
			})
			require.NoError(t, err)
			require.Len(t, out.SubscriptionFilters, 1)
			sf := out.SubscriptionFilters[0]
			assert.True(t, sf.ApplyOnTransformedLogs)
			assert.Equal(t, tt.fields, sf.EmitSystemFields)
			assert.Equal(t, `@aws.region NOT IN ["cn-north-1"]`, aws.ToString(sf.FieldSelectionCriteria))
		})
	}
}

func TestListLogGroups_LogGroupTags_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []cwltypes.TagFilter
		want    []string
	}{
		{name: "key_only", filters: []cwltypes.TagFilter{{Key: aws.String("env")}}, want: []string{"a", "b", "c"}},
		{
			name:    "exact_value",
			filters: []cwltypes.TagFilter{{Key: aws.String("env"), Values: []string{"prod"}}},
			want:    []string{"a"},
		},
		{
			name:    "wildcard_any_case",
			filters: []cwltypes.TagFilter{{Key: aws.String("env"), Values: []string{"PRO*", "dev"}}},
			want:    []string{"a", "b"},
		},
		{
			name:    "negation",
			filters: []cwltypes.TagFilter{{Key: aws.String("env"), Values: []string{"!prod"}}},
			want:    []string{"b", "c"},
		},
		{
			name: "and_across_filters",
			filters: []cwltypes.TagFilter{
				{Key: aws.String("env")}, {Key: aws.String("team"), Values: []string{"x"}},
			},
			want: []string{"a"},
		},
		{name: "missing_key", filters: []cwltypes.TagFilter{{Key: aws.String("nope")}}, want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			ctx := t.Context()
			seed := map[string]map[string]string{
				"a": {"env": "prod", "team": "x"},
				"b": {"env": "dev"},
				"c": {"env": "staging"},
				"d": {},
			}
			for name, tags := range seed {
				_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String(name)})
				require.NoError(t, err)
				if len(tags) > 0 {
					desc, derr := client.DescribeLogGroups(ctx, &cwlsdk.DescribeLogGroupsInput{
						LogGroupNamePrefix: aws.String(name),
					})
					require.NoError(t, derr)
					_, err = client.TagResource(ctx, &cwlsdk.TagResourceInput{
						ResourceArn: desc.LogGroups[0].Arn, Tags: tags,
					})
					require.NoError(t, err)
				}
			}

			out, err := client.ListLogGroups(ctx, &cwlsdk.ListLogGroupsInput{LogGroupTags: tt.filters})
			require.NoError(t, err)

			got := make([]string, 0, len(out.LogGroups))
			for _, g := range out.LogGroups {
				got = append(got, aws.ToString(g.LogGroupName))
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateImportTask_ImportFilter_EchoedByDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter *cwltypes.ImportFilter
		name   string
	}{
		{name: "range", filter: &cwltypes.ImportFilter{StartEventTime: aws.Int64(1000), EndEventTime: aws.Int64(2000)}},
		{name: "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			ctx := t.Context()

			created, err := client.CreateImportTask(ctx, &cwlsdk.CreateImportTaskInput{
				ImportRoleArn:   aws.String("arn:aws:iam::000000000000:role/r"),
				ImportSourceArn: aws.String("arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/x"),
				ImportFilter:    tt.filter,
			})
			require.NoError(t, err)

			out, err := client.DescribeImportTasks(ctx, &cwlsdk.DescribeImportTasksInput{ImportId: created.ImportId})
			require.NoError(t, err)
			require.Len(t, out.Imports, 1)
			if tt.filter == nil {
				assert.Nil(t, out.Imports[0].ImportFilter)

				return
			}
			require.NotNil(t, out.Imports[0].ImportFilter)
			assert.Equal(t, int64(1000), aws.ToInt64(out.Imports[0].ImportFilter.StartEventTime))
			assert.Equal(t, int64(2000), aws.ToInt64(out.Imports[0].ImportFilter.EndEventTime))
		})
	}
}
