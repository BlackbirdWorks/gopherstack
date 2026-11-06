package azurearm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func utc(y int, m time.Month, d, h, mi int) time.Time {
	return time.Date(y, m, d, h, mi, 0, 0, time.UTC)
}

func TestLogicRecurrenceNext(t *testing.T) {
	t.Parallel()

	anchor := utc(2026, 1, 1, 0, 0)

	tests := []struct {
		after time.Time
		want  time.Time
		rec   map[string]any
		name  string
		none  bool
	}{
		{
			name: "simple minute", after: utc(2026, 1, 1, 0, 0),
			rec:  map[string]any{"frequency": "Minute", "interval": int64(1)},
			want: utc(2026, 1, 1, 0, 1),
		},
		{
			name: "anchor included when after precedes it", after: anchor.Add(-time.Nanosecond),
			rec:  map[string]any{"frequency": "Hour", "interval": int64(2)},
			want: anchor,
		},
		{
			name: "far future skips periods", after: utc(2030, 6, 1, 12, 7),
			rec:  map[string]any{"frequency": "Minute", "interval": int64(15)},
			want: utc(2030, 6, 1, 12, 15),
		},
		{
			name: "week interval two", after: utc(2026, 1, 2, 0, 0),
			rec:  map[string]any{"frequency": "Week", "interval": int64(2)},
			want: utc(2026, 1, 15, 0, 0),
		},
		{
			name: "month end clamps by skipping short months", after: utc(2026, 1, 31, 0, 0),
			rec: map[string]any{
				"frequency": "Month",
				"interval":  int64(1),
				"schedule": map[string]any{
					"monthDays": []any{int64(31)},
					"hours":     []any{int64(0)},
					"minutes":   []any{int64(0)},
				},
			},
			want: utc(2026, 3, 31, 0, 0),
		},
		{
			name: "negative month day is from the end", after: utc(2026, 1, 1, 0, 0),
			rec: map[string]any{
				"frequency": "Month",
				"interval":  int64(1),
				"schedule": map[string]any{
					"monthDays": []any{int64(-1)},
					"hours":     []any{int64(0)},
					"minutes":   []any{int64(0)},
				},
			},
			want: utc(2026, 1, 31, 0, 0),
		},
		{
			name: "multiple weekdays", after: utc(2026, 1, 1, 0, 0),
			rec: map[string]any{
				"frequency": "Week",
				"interval":  int64(1),
				"schedule": map[string]any{
					"weekDays": []any{"Saturday", "Friday"},
					"hours":    []any{int64(8)},
					"minutes":  []any{int64(0)},
				},
			},
			want: utc(2026, 1, 2, 8, 0),
		},
		{
			name: "after end time", after: utc(2026, 1, 1, 1, 0),
			rec:  map[string]any{"frequency": "Hour", "interval": int64(1), "endTime": "2026-01-01T01:00:00Z"},
			none: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rc := parseLogicRecurrence(map[string]any{"recurrence": tc.rec}, anchor)
			got, ok := rc.next(tc.after)

			if tc.none {
				assert.False(t, ok)

				return
			}

			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicSchemaValidation(t *testing.T) {
	t.Parallel()

	schema := map[string]any{
		"type": "object", "required": []any{"id"},
		"properties": map[string]any{
			"id":   map[string]any{"type": "Integer", "minimum": int64(1)},
			"tags": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "maxItems": int64(2)},
			"kind": map[string]any{"type": "string", "enum": []any{"a", "b"}},
			"name": map[string]any{"type": "string", "minLength": int64(2), "pattern": "^[a-z]+$"},
			"opt":  map[string]any{"type": []any{"string", "null"}},
		},
	}

	tests := []struct {
		value   any
		name    string
		wantErr string
	}{
		{name: "valid", value: map[string]any{"id": int64(3), "tags": []any{"x"}, "kind": "a", "name": "ab"}},
		{name: "null optional", value: map[string]any{"id": int64(3), "opt": nil}},
		{
			name:    "missing required",
			value:   map[string]any{},
			wantErr: "Required properties are missing from object: id.",
		},
		{
			name:    "wrong type",
			value:   map[string]any{"id": "x"},
			wantErr: "Invalid type. Expected Integer but got String. Path 'id'.",
		},
		{name: "below minimum", value: map[string]any{"id": int64(0)}, wantErr: "less than minimum"},
		{name: "enum", value: map[string]any{"id": int64(1), "kind": "z"}, wantErr: "not defined in enum"},
		{
			name:    "item type",
			value:   map[string]any{"id": int64(1), "tags": []any{int64(1)}},
			wantErr: "Expected string but got Integer",
		},
		{
			name:    "too many items",
			value:   map[string]any{"id": int64(1), "tags": []any{"a", "b", "c"}},
			wantErr: "exceeds maximum count",
		},
		{name: "min length", value: map[string]any{"id": int64(1), "name": "a"}, wantErr: "less than minimum length"},
		{name: "pattern", value: map[string]any{"id": int64(1), "name": "AB"}, wantErr: "does not match regex pattern"},
		{name: "not an object", value: "text", wantErr: "Invalid type. Expected object but got String."},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateLogicSchema(schema, tc.value)
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestParseISODuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "PT1H", want: time.Hour},
		{in: "PT30S", want: 30 * time.Second},
		{in: "PT1H30M", want: 90 * time.Minute},
		{in: "P1D", want: 24 * time.Hour},
		{in: "P1DT12H", want: 36 * time.Hour},
		{in: "PT0.5S", want: 500 * time.Millisecond},
		{in: "1H", wantErr: true},
		{in: "PTXS", wantErr: true},
		{in: "PT5Q", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			got, err := parseISODuration(tc.in)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLogicFilterParsing(t *testing.T) {
	t.Parallel()

	conds, err := parseLogicFilter("status eq 'Failed' and startTime ge 2026-01-01T00:00:00Z")
	require.NoError(t, err)
	require.Len(t, conds, 2)

	run := logicFilterable{status: "Failed", start: utc(2026, 1, 2, 0, 0)}
	assert.True(t, filterMatches(conds, run))
	assert.False(t, filterMatches(conds, logicFilterable{status: "Succeeded", start: run.start}))
	assert.False(t, filterMatches(conds, logicFilterable{status: "Failed", start: utc(2025, 12, 31, 0, 0)}))

	for _, bad := range []string{"status", "status like 'x'"} {
		_, err = parseLogicFilter(bad)
		require.Error(t, err, bad)
	}
}
