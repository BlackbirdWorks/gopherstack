package cloudwatchlogs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsBackend_MetricFilterLifecycle(t *testing.T) {
	t.Parallel()

	transformation := cloudwatchlogs.MetricTransformation{
		MetricName:      "ErrorCount",
		MetricNamespace: "MyApp",
		MetricValue:     "1",
	}

	tests := []struct {
		wantErr    error
		setup      func(b *cloudwatchlogs.InMemoryBackend)
		name       string
		groupName  string
		filterName string
		pattern    string
		op         string
		transforms []cloudwatchlogs.MetricTransformation
		wantLen    int
	}{
		{
			name:       "put_and_describe",
			groupName:  "grp",
			filterName: "f1",
			pattern:    "ERROR",
			transforms: []cloudwatchlogs.MetricTransformation{transformation},
			op:         "put_then_describe",
			wantLen:    1,
		},
		{
			name: "describe_with_prefix",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"grp",
					"err-filter",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
				_ = b.PutMetricFilter(
					context.Background(),
					"grp",
					"warn-filter",
					"WARN",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
			},
			op:      "describe_prefix",
			wantLen: 1,
		},
		{
			name: "delete_filter",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"grp",
					"f1",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
			},
			groupName:  "grp",
			filterName: "f1",
			op:         "delete",
		},
		{
			name:       "put_missing_group",
			groupName:  "nonexistent",
			filterName: "f1",
			pattern:    "ERROR",
			transforms: []cloudwatchlogs.MetricTransformation{transformation},
			op:         "put",
			wantErr:    cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name:       "put_missing_filter_name",
			groupName:  "grp",
			filterName: "",
			pattern:    "ERROR",
			transforms: []cloudwatchlogs.MetricTransformation{transformation},
			op:         "put_no_setup",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name: "delete_missing_filter",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "grp", "", "")
			},
			groupName:  "grp",
			filterName: "nonexistent",
			op:         "delete",
			wantErr:    cloudwatchlogs.ErrMetricFilterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "put_then_describe":
				_, innerErr := b.CreateLogGroup(context.Background(), tt.groupName, "", "")
				require.NoError(t, innerErr)
				err = b.PutMetricFilter(
					context.Background(),
					tt.groupName,
					tt.filterName,
					tt.pattern,
					tt.transforms,
				)
				require.NoError(t, err)
				var filters []cloudwatchlogs.MetricFilter
				filters, _, err = b.DescribeMetricFilters(
					context.Background(),
					tt.groupName,
					"",
					"",
					"",
					"",
					50,
				)
				require.NoError(t, err)
				assert.Len(t, filters, tt.wantLen)

				return
			case "describe_prefix":
				var filters []cloudwatchlogs.MetricFilter
				filters, _, err = b.DescribeMetricFilters(
					context.Background(),
					"grp",
					"err",
					"",
					"",
					"",
					50,
				)
				require.NoError(t, err)
				assert.Len(t, filters, tt.wantLen)

				return
			case "delete":
				err = b.DeleteMetricFilter(context.Background(), tt.groupName, tt.filterName)
			case "put":
				err = b.PutMetricFilter(
					context.Background(),
					tt.groupName,
					tt.filterName,
					tt.pattern,
					tt.transforms,
				)
			case "put_no_setup":
				err = b.PutMetricFilter(
					context.Background(),
					tt.groupName,
					tt.filterName,
					tt.pattern,
					tt.transforms,
				)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_TestMetricFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		pattern   string
		messages  []string
		wantCount int
	}{
		{
			name:      "matches_substring",
			pattern:   "ERROR",
			messages:  []string{"this is an ERROR message", "this is fine", "another ERROR"},
			wantCount: 2,
		},
		{
			name:      "no_matches",
			pattern:   "CRITICAL",
			messages:  []string{"info message", "debug message"},
			wantCount: 0,
		},
		{
			name:     "empty_pattern",
			pattern:  "",
			messages: []string{"any message"},
			wantErr:  cloudwatchlogs.ErrValidation,
		},
		{
			name:      "empty_messages",
			pattern:   "ERROR",
			messages:  []string{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()

			matches, err := b.TestMetricFilter(tt.pattern, tt.messages)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, matches, tt.wantCount)
			for i, m := range matches {
				assert.NotEmpty(t, m.EventMessage)
				assert.Positive(t, m.EventNumber)
				assert.NotNil(t, m.ExtractedValues)
				_ = i
			}
		})
	}
}

func TestCloudWatchLogsBackend_MetricFilterCount(t *testing.T) {
	t.Parallel()

	transformation := cloudwatchlogs.MetricTransformation{
		MetricName:      "Errors",
		MetricNamespace: "App",
		MetricValue:     "1",
	}

	tests := []struct {
		setup     func(b *cloudwatchlogs.InMemoryBackend)
		name      string
		wantCount int32
	}{
		{
			name: "no_filters",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
			},
			wantCount: 0,
		},
		{
			name: "two_filters",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"g",
					"f1",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
				_ = b.PutMetricFilter(
					context.Background(),
					"g",
					"f2",
					"WARN",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
			},
			wantCount: 2,
		},
		{
			name: "after_delete",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateLogGroup(context.Background(), "g", "", "")
				_ = b.PutMetricFilter(
					context.Background(),
					"g",
					"f1",
					"ERROR",
					[]cloudwatchlogs.MetricTransformation{transformation},
				)
				_ = b.DeleteMetricFilter(context.Background(), "g", "f1")
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 10)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			assert.Equal(t, tt.wantCount, groups[0].MetricFilterCount)
		})
	}
}

func TestCloudWatchLogsBackend_DeleteLogGroup_CleansMetricFilters(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	err = b.PutMetricFilter(
		context.Background(),
		"g",
		"f1",
		"",
		[]cloudwatchlogs.MetricTransformation{
			{MetricName: "m", MetricNamespace: "ns", MetricValue: "1"},
		},
	)
	require.NoError(t, err)

	err = b.DeleteLogGroup(context.Background(), "g")
	require.NoError(t, err)

	// Re-create the group and check metric filters are gone.
	_, err = b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	filters, _, err := b.DescribeMetricFilters(context.Background(), "g", "", "", "", "", 50)
	require.NoError(t, err)
	assert.Empty(t, filters)
}

func TestCloudWatchLogsBackend_MetricFilterEmission(t *testing.T) {
	t.Parallel()

	type emittedMetric struct {
		namespace string
		name      string
		value     float64
	}

	var mu sync.Mutex
	var emitted []emittedMetric

	emitter := cloudwatchlogs.MetricEmitterFunc(
		func(namespace, name string, value float64, _ string) error {
			mu.Lock()
			emitted = append(emitted, emittedMetric{namespace: namespace, name: name, value: value})
			mu.Unlock()

			return nil
		},
	)

	b := cloudwatchlogs.NewInMemoryBackend()
	b.SetMetricEmitter(emitter)

	_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "grp", "stream")
	require.NoError(t, err)

	err = b.PutMetricFilter(
		context.Background(),
		"grp",
		"errors",
		"ERROR",
		[]cloudwatchlogs.MetricTransformation{
			{MetricNamespace: "MyApp", MetricName: "ErrorCount", MetricValue: "1"},
		},
	)
	require.NoError(t, err)

	// Two events: one matches the filter pattern, one does not.
	_, err = b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "ERROR: something went wrong", Timestamp: time.Now().UnixMilli()},
			{Message: "INFO: all good", Timestamp: time.Now().UnixMilli()},
		},
	)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, emitted, 1, "expected exactly one metric emission for the ERROR event")
	assert.Equal(t, "MyApp", emitted[0].namespace)
	assert.Equal(t, "ErrorCount", emitted[0].name)
	assert.InDelta(t, 1.0, emitted[0].value, 0.001)
}

func TestCloudWatchLogsBackend_MetricFilterEmission_NoEmitterNoPanic(t *testing.T) {
	t.Parallel()

	// No emitter set — PutLogEvents should succeed silently.
	b := cloudwatchlogs.NewInMemoryBackend()

	_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "grp", "stream")
	require.NoError(t, err)

	err = b.PutMetricFilter(
		context.Background(),
		"grp",
		"errors",
		"ERROR",
		[]cloudwatchlogs.MetricTransformation{
			{MetricNamespace: "MyApp", MetricName: "ErrorCount", MetricValue: "1"},
		},
	)
	require.NoError(t, err)

	_, err = b.PutLogEvents(
		context.Background(),
		"grp",
		"stream",
		"",
		[]cloudwatchlogs.InputLogEvent{
			{Message: "ERROR: kaboom", Timestamp: time.Now().UnixMilli()},
		},
	)
	require.NoError(t, err)
}

// ---- Metric filter field extraction (MetricValue "$name" / "$.path" references) ----
//
// A metric filter's MetricValue may be a literal number (published as-is for every
// matched event) or a "$"-prefixed field reference that must be extracted from each
// individual matched log event: "$size" for a named field in a space-delimited pattern
// ("[ip, level, size]"), "$.bytes" for a JSON selector pattern. Real CloudWatch Logs
// silently skips publishing a data point for a matched event whose referenced field is
// absent or non-numeric -- it does not fabricate a value (DefaultValue is documented for
// periods with zero *matching* events, not failed per-event extraction), so these cases
// assert zero emissions rather than a fallback constant.

func TestCloudWatchLogsBackend_MetricFilterEmission_FieldExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		filterPattern string
		metricValue   string
		message       string
		wantEmitted   []float64 // nil means "no emission for this event"
	}{
		{
			name:          "json_field_extracted_per_event",
			filterPattern: `{ $.level = "ERROR" }`,
			metricValue:   "$.bytes",
			message:       `{"level":"ERROR","bytes":512}`,
			wantEmitted:   []float64{512},
		},
		{
			name:          "space_field_extracted_per_event",
			filterPattern: "[ip, level, bytes]",
			metricValue:   "$bytes",
			message:       "1.2.3.4 ERROR 256",
			wantEmitted:   []float64{256},
		},
		{
			name:          "missing_json_field_skips_emission",
			filterPattern: `{ $.level = "ERROR" }`,
			metricValue:   "$.bytes",
			message:       `{"level":"ERROR"}`,
			wantEmitted:   nil,
		},
		{
			name:          "non_numeric_field_skips_emission",
			filterPattern: `{ $.level = "ERROR" }`,
			metricValue:   "$.bytes",
			message:       `{"level":"ERROR","bytes":"lots"}`,
			wantEmitted:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex
			var emitted []float64

			emitter := cloudwatchlogs.MetricEmitterFunc(
				func(_, _ string, value float64, _ string) error {
					mu.Lock()
					emitted = append(emitted, value)
					mu.Unlock()

					return nil
				},
			)

			b := cloudwatchlogs.NewInMemoryBackend()
			b.SetMetricEmitter(emitter)

			_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
			require.NoError(t, err)
			_, err = b.CreateLogStream(context.Background(), "grp", "stream")
			require.NoError(t, err)

			err = b.PutMetricFilter(
				context.Background(),
				"grp",
				"mf",
				tt.filterPattern,
				[]cloudwatchlogs.MetricTransformation{
					{MetricNamespace: "MyApp", MetricName: "Bytes", MetricValue: tt.metricValue},
				},
			)
			require.NoError(t, err)

			_, err = b.PutLogEvents(
				context.Background(),
				"grp",
				"stream",
				"",
				[]cloudwatchlogs.InputLogEvent{
					{Message: tt.message, Timestamp: time.Now().UnixMilli()},
				},
			)
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()

			if tt.wantEmitted == nil {
				assert.Empty(t, emitted)

				return
			}

			require.Len(t, emitted, len(tt.wantEmitted))
			for i, want := range tt.wantEmitted {
				assert.InDelta(t, want, emitted[i], 0.001)
			}
		})
	}
}

func TestCloudWatchLogsBackend_TestMetricFilter_ExtractedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pattern    string
		messages   []string
		wantValues []map[string]string // one entry per expected match, in order
	}{
		{
			name:    "json_pattern_extracts_referenced_selector",
			pattern: `{ $.level = "ERROR" }`,
			messages: []string{
				`{"level":"ERROR","bytes":512}`,
				`{"level":"INFO","bytes":10}`,
			},
			wantValues: []map[string]string{
				{"$.level": "ERROR"},
			},
		},
		{
			name:    "space_pattern_extracts_named_fields",
			pattern: "[ip, level, bytes]",
			messages: []string{
				"1.2.3.4 ERROR 256",
				"5.6.7.8 INFO 10",
			},
			wantValues: []map[string]string{
				{"$ip": "1.2.3.4", "$level": "ERROR", "$bytes": "256"},
				{"$ip": "5.6.7.8", "$level": "INFO", "$bytes": "10"},
			},
		},
		{
			name:    "plain_text_pattern_has_no_addressable_fields",
			pattern: "ERROR",
			messages: []string{
				"an ERROR occurred",
			},
			wantValues: []map[string]string{
				{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()

			matches, err := b.TestMetricFilter(tt.pattern, tt.messages)
			require.NoError(t, err)
			require.Len(t, matches, len(tt.wantValues))

			for i, want := range tt.wantValues {
				assert.Equal(t, want, matches[i].ExtractedValues)
			}
		})
	}
}

func TestCloudWatchLogsBackend_MetricTransformation_DimensionsAndUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDimensions  map[string]string
		name            string
		wantUnit        string
		transformations []cloudwatchlogs.MetricTransformation
	}{
		{
			name: "with_dimensions_and_unit",
			transformations: []cloudwatchlogs.MetricTransformation{
				{
					MetricNamespace: "MyApp",
					MetricName:      "Errors",
					MetricValue:     "1",
					Unit:            "Count",
					Dimensions: map[string]string{
						"Service": "api",
						"Env":     "prod",
					},
				},
			},
			wantDimensions: map[string]string{"Service": "api", "Env": "prod"},
			wantUnit:       "Count",
		},
		{
			name: "without_dimensions",
			transformations: []cloudwatchlogs.MetricTransformation{
				{
					MetricNamespace: "MyApp",
					MetricName:      "Requests",
					MetricValue:     "1",
				},
			},
			wantUnit: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			err = b.PutMetricFilter(
				context.Background(),
				"g",
				"filter1",
				"ERROR",
				tt.transformations,
			)
			require.NoError(t, err)

			filters, _, err := b.DescribeMetricFilters(
				context.Background(),
				"g",
				"",
				"",
				"",
				"",
				50,
			)
			require.NoError(t, err)
			require.Len(t, filters, 1)
			require.Len(t, filters[0].MetricTransformations, 1)

			mf := filters[0].MetricTransformations[0]
			assert.Equal(t, tt.wantUnit, mf.Unit)

			if tt.wantDimensions != nil {
				assert.Equal(t, tt.wantDimensions, mf.Dimensions)
			}
		})
	}
}
