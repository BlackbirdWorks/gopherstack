package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// StreamAllowsMetric filter logic (gap #8)
// ---------------------------------------------------------------------------

func TestStreamAllowsMetric(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stream     *cloudwatch.MetricStream
		name       string
		namespace  string
		metricName string
		want       bool
	}{
		{
			name:       "no filters allows AWS/EC2",
			stream:     &cloudwatch.MetricStream{Name: "s"},
			namespace:  "AWS/EC2",
			metricName: "CPU",
			want:       true,
		},
		{
			name:       "no filters allows Custom/App",
			stream:     &cloudwatch.MetricStream{Name: "s"},
			namespace:  "Custom/App",
			metricName: "RPM",
			want:       true,
		},
		{
			name: "include filter allows matching namespace",
			stream: &cloudwatch.MetricStream{
				Name:           "s",
				IncludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
			},
			namespace:  "AWS/EC2",
			metricName: "CPU",
			want:       true,
		},
		{
			name: "include filter rejects non-matching namespace",
			stream: &cloudwatch.MetricStream{
				Name:           "s",
				IncludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
			},
			namespace:  "Custom/App",
			metricName: "RPM",
			want:       false,
		},
		{
			name: "exclude filter rejects matching namespace",
			stream: &cloudwatch.MetricStream{
				Name:           "s",
				ExcludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
			},
			namespace:  "AWS/EC2",
			metricName: "CPU",
			want:       false,
		},
		{
			name: "exclude filter allows non-matching namespace",
			stream: &cloudwatch.MetricStream{
				Name:           "s",
				ExcludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
			},
			namespace:  "Custom/App",
			metricName: "RPM",
			want:       true,
		},
		{
			// Exclude overrides include.
			name: "exclude takes precedence over include",
			stream: &cloudwatch.MetricStream{
				Name:           "s",
				IncludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
				ExcludeFilters: []cloudwatch.MetricStreamFilter{{Namespace: "AWS/EC2"}},
			},
			namespace:  "AWS/EC2",
			metricName: "CPU",
			want:       false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := cloudwatch.StreamAllowsMetricForTest(tc.stream, tc.namespace, tc.metricName)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// MetricStream: full lifecycle
// ---------------------------------------------------------------------------

func TestBackend_MetricStream_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutMetricStream(&cloudwatch.MetricStream{
		Name:         "stream1",
		FirehoseArn:  "arn:aws:firehose:us-east-1:123:deliverystream/ds",
		RoleArn:      "arn:aws:iam::123:role/r",
		OutputFormat: "json",
	}))

	s, err := b.GetMetricStream("stream1")
	require.NoError(t, err)
	assert.Equal(t, "stream1", s.Name)
	assert.NotEmpty(t, s.Arn)

	p, err := b.ListMetricStreams("", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1)

	require.NoError(t, b.DeleteMetricStream("stream1"))
	_, err = b.GetMetricStream("stream1")
	assert.Error(t, err)
}

func TestBackend_MetricStream_StartStop(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricStream(&cloudwatch.MetricStream{
		Name:         "s",
		FirehoseArn:  "arn:aws:firehose:us-east-1:123:deliverystream/ds",
		RoleArn:      "arn:aws:iam::123:role/r",
		OutputFormat: "json",
	}))

	require.NoError(t, b.StopMetricStreams([]string{"s"}))
	s, _ := b.GetMetricStream("s")
	assert.Equal(t, "STOPPED", s.State)

	require.NoError(t, b.StartMetricStreams([]string{"s"}))
	s, _ = b.GetMetricStream("s")
	assert.Equal(t, "RUNNING", s.State)
}

func TestCloudWatchBackend_ListMetricStreams(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:         "stream-a",
		FirehoseArn:  "arn:aws:firehose:us-east-1:123456789012:deliverystream/a",
		OutputFormat: "json",
		State:        "running",
	})
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:         "stream-b",
		FirehoseArn:  "arn:aws:firehose:us-east-1:123456789012:deliverystream/b",
		OutputFormat: "opentelemetry0.7",
		State:        "running",
	})

	p, err := b.ListMetricStreams("", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 2)
	assert.Equal(t, "stream-a", p.Data[0].Name)
	assert.Equal(t, "stream-b", p.Data[1].Name)
}

func TestCloudWatchBackend_MetricStream_IncludeFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:        "stream-include",
		FirehoseArn: "arn:aws:firehose:us-east-1:123:deliverystream/s",
		State:       "RUNNING",
		IncludeFilters: []cloudwatch.MetricStreamFilter{
			{Namespace: "AWS/EC2", MetricNames: []string{"CPUUtilization"}},
		},
	})

	before, err := b.GetMetricStream("stream-include")
	require.NoError(t, err)

	ts := time.Now().UTC()
	// This metric matches the include filter.
	err = b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
		{MetricName: "CPUUtilization", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
	})
	require.NoError(t, err)

	after, err := b.GetMetricStream("stream-include")
	require.NoError(t, err)
	assert.True(t, after.LastUpdateDate.After(before.LastUpdateDate),
		"matching metric should bump stream last-update date")

	// Record baseline after previous update.
	baseline := after.LastUpdateDate

	// Non-matching namespace should NOT change LastUpdateDate.
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:  "stream-include2",
		State: "RUNNING",
		IncludeFilters: []cloudwatch.MetricStreamFilter{
			{Namespace: "AWS/EC2"},
		},
	})
	beforeNonMatch, _ := b.GetMetricStream("stream-include2")
	err = b.PutMetricData("AWS/RDS", []cloudwatch.MetricDatum{
		{MetricName: "FreeStorageSpace", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: ts},
	})
	require.NoError(t, err)
	afterNonMatch, err := b.GetMetricStream("stream-include2")
	require.NoError(t, err)
	assert.Equal(t, beforeNonMatch.LastUpdateDate, afterNonMatch.LastUpdateDate,
		"non-matching namespace should not update stream")
	_ = baseline
}

func TestCloudWatchBackend_MetricStream_ExcludeFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:  "stream-exclude",
		State: "RUNNING",
		ExcludeFilters: []cloudwatch.MetricStreamFilter{
			{Namespace: "AWS/EC2"},
		},
	})

	ts := time.Now().UTC()
	baseline, _ := b.GetMetricStream("stream-exclude")

	// Excluded namespace: should not update stream.
	err := b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
		{MetricName: "CPUUtilization", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
	})
	require.NoError(t, err)
	afterExcluded, err := b.GetMetricStream("stream-exclude")
	require.NoError(t, err)
	assert.Equal(t, baseline.LastUpdateDate, afterExcluded.LastUpdateDate,
		"excluded namespace should not update stream")

	// Non-excluded namespace: should update.
	err = b.PutMetricData("AWS/RDS", []cloudwatch.MetricDatum{
		{MetricName: "FreeStorageSpace", Value: 99, Count: 1, Sum: 99, Min: 99, Max: 99, Timestamp: ts},
	})
	require.NoError(t, err)
	afterAllowed, err := b.GetMetricStream("stream-exclude")
	require.NoError(t, err)
	assert.True(t, afterAllowed.LastUpdateDate.After(baseline.LastUpdateDate),
		"non-excluded namespace should update stream")
}
