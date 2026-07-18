package cloudwatch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// MetricFilter: CRUD
// ---------------------------------------------------------------------------

func TestBackend_MetricFilter_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutMetricFilter(&cloudwatch.MetricFilter{
		FilterName:    "f1",
		LogGroupName:  "/app/logs",
		FilterPattern: "[level=ERROR]",
		MetricTransformations: []cloudwatch.MetricTransformation{
			{MetricName: "Errors", MetricNamespace: "App", MetricValue: "1"},
		},
	}))

	p, err := b.DescribeMetricFilters("", "/app/logs", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "f1", p.Data[0].FilterName)

	require.NoError(t, b.DeleteMetricFilter("f1", "/app/logs"))
	p, err = b.DescribeMetricFilters("", "/app/logs", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data)
}

func TestCloudWatchBackend_PutMetricFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter  *cloudwatch.MetricFilter
		name    string
		wantErr bool
	}{
		{
			name: "valid",
			filter: &cloudwatch.MetricFilter{
				FilterName:    "my-filter",
				LogGroupName:  "/aws/lambda/fn",
				FilterPattern: "[host, ident, authuser, date, request, status]",
				MetricTransformations: []cloudwatch.MetricTransformation{
					{MetricName: "ReqCount", MetricNamespace: "MyApp", MetricValue: "1"},
				},
			},
		},
		{
			name:    "missing_filter_name",
			filter:  &cloudwatch.MetricFilter{LogGroupName: "/aws/lambda/fn"},
			wantErr: true,
		},
		{
			name:    "missing_log_group",
			filter:  &cloudwatch.MetricFilter{FilterName: "my-filter"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutMetricFilter(tt.filter)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCloudWatchBackend_DescribeMetricFilters(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	filters := []cloudwatch.MetricFilter{
		{FilterName: "alpha", LogGroupName: "/aws/lambda/fn1", FilterPattern: "[a]"},
		{FilterName: "beta", LogGroupName: "/aws/lambda/fn1", FilterPattern: "[b]"},
		{FilterName: "gamma", LogGroupName: "/aws/ec2", FilterPattern: "[c]"},
	}
	for i := range filters {
		require.NoError(t, b.PutMetricFilter(&filters[i]))
	}

	tests := []struct {
		name             string
		filterNamePrefix string
		logGroupName     string
		wantCount        int
	}{
		{name: "all", wantCount: 3},
		{name: "by_log_group", logGroupName: "/aws/lambda/fn1", wantCount: 2},
		{name: "by_prefix", filterNamePrefix: "al", wantCount: 1},
		{
			name:             "prefix_and_group",
			filterNamePrefix: "b",
			logGroupName:     "/aws/lambda/fn1",
			wantCount:        1,
		},
		{name: "no_match", logGroupName: "/aws/nonexistent", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := b.DescribeMetricFilters(tt.filterNamePrefix, tt.logGroupName, "", 0)
			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)
		})
	}
}

func TestCloudWatchBackend_DeleteMetricFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricFilter(&cloudwatch.MetricFilter{
		FilterName:   "del-filter",
		LogGroupName: "/aws/lambda/fn",
	}))

	// delete should succeed
	require.NoError(t, b.DeleteMetricFilter("del-filter", "/aws/lambda/fn"))

	// second delete should fail
	require.ErrorIs(
		t,
		b.DeleteMetricFilter("del-filter", "/aws/lambda/fn"),
		cloudwatch.ErrMetricFilterNotFound,
	)
}
