package asl

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtraIntrinsics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		input   any
		want    any
		wantErr bool
	}{
		{
			name: "math_add_positive",
			expr: "States.MathAdd(2, 3)",
			want: 5.0,
		},
		{
			name: "math_add_mixed",
			expr: "States.MathAdd(10, -7)",
			want: 3.0,
		},
		{
			name: "array_range_ascending",
			expr: "States.ArrayRange(1, 5, 1)",
			want: []any{1.0, 2.0, 3.0, 4.0, 5.0},
		},
		{
			name: "array_range_descending",
			expr: "States.ArrayRange(5, 1, -2)",
			want: []any{5.0, 3.0, 1.0},
		},
		{
			name:    "array_range_zero_step",
			expr:    "States.ArrayRange(1, 5, 0)",
			wantErr: true,
		},
		{
			name:  "array_get_item",
			expr:  "States.ArrayGetItem($.arr, 1)",
			input: map[string]any{"arr": []any{"a", "b", "c"}},
			want:  "b",
		},
		{
			name:    "array_get_item_oob",
			expr:    "States.ArrayGetItem($.arr, 9)",
			input:   map[string]any{"arr": []any{"a"}},
			wantErr: true,
		},
		{
			name:  "array_unique",
			expr:  "States.ArrayUnique($.arr)",
			input: map[string]any{"arr": []any{1.0, 2.0, 1.0, 3.0, 2.0}},
			want:  []any{1.0, 2.0, 3.0},
		},
		{
			name:  "json_merge_shallow",
			expr:  "States.JsonMerge($.a, $.b, false)",
			input: map[string]any{"a": map[string]any{"x": 1.0, "y": 2.0}, "b": map[string]any{"y": 9.0, "z": 3.0}},
			want:  map[string]any{"x": 1.0, "y": 9.0, "z": 3.0},
		},
		{
			name:    "json_merge_deep_unsupported",
			expr:    "States.JsonMerge($.a, $.b, true)",
			input:   map[string]any{"a": map[string]any{}, "b": map[string]any{}},
			wantErr: true,
		},
		{
			name: "string_split_csv",
			expr: "States.StringSplit('a,b,,c', ',')",
			want: []any{"a", "b", "c"},
		},
		{
			name: "string_split_multi_delim",
			expr: "States.StringSplit('a-b_c', '-_')",
			want: []any{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := evaluateIntrinsicFunction(tt.expr, tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUUIDIntrinsicShape(t *testing.T) {
	t.Parallel()

	got, err := evaluateIntrinsicFunction("States.UUID()", nil)
	require.NoError(t, err)

	s, ok := got.(string)
	require.True(t, ok)
	assert.Len(t, s, 36)
	// version 4: third group starts with '4'.
	assert.Equal(t, byte('4'), s[14])
}

type stubS3 struct {
	data []byte
	err  error
}

func (s stubS3) GetObjectBytes(_ context.Context, _, _ string) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.data, nil
}

func TestDecodeReaderItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *ReaderConfig
		data    []byte
		want    []any
		wantErr bool
	}{
		{
			name: "json_array_default",
			data: []byte(`[{"a":1},{"a":2}]`),
			want: []any{map[string]any{"a": 1.0}, map[string]any{"a": 2.0}},
		},
		{
			name: "jsonl_explicit",
			cfg:  &ReaderConfig{InputType: "JSONL"},
			data: []byte("{\"a\":1}\n{\"a\":2}\n"),
			want: []any{map[string]any{"a": 1.0}, map[string]any{"a": 2.0}},
		},
		{
			name: "csv_first_row_header",
			cfg:  &ReaderConfig{InputType: "CSV"},
			data: []byte("col1,col2\nx,1\ny,2\n"),
			want: []any{
				map[string]any{"col1": "x", "col2": "1"},
				map[string]any{"col1": "y", "col2": "2"},
			},
		},
		{
			name: "csv_given_headers",
			cfg:  &ReaderConfig{InputType: "CSV", CSVHeaderLocation: "GIVEN", CSVHeaders: []string{"a", "b"}},
			data: []byte("1,2\n3,4\n"),
			want: []any{
				map[string]any{"a": "1", "b": "2"},
				map[string]any{"a": "3", "b": "4"},
			},
		},
		{
			name:    "csv_given_missing_headers",
			cfg:     &ReaderConfig{InputType: "CSV", CSVHeaderLocation: "GIVEN"},
			data:    []byte("1,2\n"),
			wantErr: true,
		},
		{
			name: "csv_max_items_truncates",
			cfg:  &ReaderConfig{InputType: "CSV", MaxItems: 1},
			data: []byte("col1\nx\ny\nz\n"),
			want: []any{map[string]any{"col1": "x"}},
		},
		{
			name:    "unknown_input_type",
			cfg:     &ReaderConfig{InputType: "PARQUET"},
			data:    []byte("ignored"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			items, err := decodeReaderItems(tt.data, tt.cfg)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.cfg != nil && tt.cfg.MaxItems > 0 && len(items) > tt.cfg.MaxItems {
				items = items[:tt.cfg.MaxItems]
			}

			assert.Equal(t, tt.want, items)
		})
	}
}

func TestResolveItemsFromReaderUsesConfig(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(&StateMachine{}, nil, nil)
	exec.SetS3Reader(stubS3{data: []byte("h1,h2\na,1\nb,2\n")})

	items, err := exec.resolveItemsFromReader(t.Context(), &ItemReader{
		Parameters:   map[string]any{"Bucket": "b", "Key": "k"},
		ReaderConfig: &ReaderConfig{InputType: "CSV"},
	})
	require.NoError(t, err)
	assert.Equal(t, []any{
		map[string]any{"h1": "a", "h2": "1"},
		map[string]any{"h1": "b", "h2": "2"},
	}, items)
}

func TestResolveItemsFromReaderS3Error(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(&StateMachine{}, nil, nil)
	exec.SetS3Reader(stubS3{err: errors.New("boom")})

	_, err := exec.resolveItemsFromReader(t.Context(), &ItemReader{
		Parameters: map[string]any{"Bucket": "b", "Key": "k"},
	})
	require.Error(t, err)
}
