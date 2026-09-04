package azuretable_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func entityWith(props map[string]azuretable.EntityProperty) azuretable.EntityInfo {
	return azuretable.EntityInfo{
		PartitionKey: "p",
		RowKey:       "r",
		Timestamp:    time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
		Properties:   props,
	}
}

func evalFilter(t *testing.T, expr string, entity azuretable.EntityInfo) bool {
	t.Helper()

	node, err := azuretable.ParseFilter(expr)
	require.NoError(t, err, expr)

	return azuretable.EvaluateFilter(node, entity)
}

func TestParseFilter_Operators(t *testing.T) {
	t.Parallel()

	entity := entityWith(map[string]azuretable.EntityProperty{
		"Age":    {Type: azuretable.EdmInt32, Value: int32(30)},
		"Name":   {Type: azuretable.EdmString, Value: "bob"},
		"Active": {Type: azuretable.EdmBoolean, Value: true},
	})

	tests := []struct {
		name string
		expr string
		want bool
	}{
		{name: "eq_true", expr: "Age eq 30", want: true},
		{name: "eq_false", expr: "Age eq 31", want: false},
		{name: "ne_true", expr: "Age ne 31", want: true},
		{name: "lt_true", expr: "Age lt 31", want: true},
		{name: "le_true", expr: "Age le 30", want: true},
		{name: "gt_true", expr: "Age gt 29", want: true},
		{name: "ge_true", expr: "Age ge 30", want: true},
		{name: "string_eq", expr: "Name eq 'bob'", want: true},
		{name: "string_ne", expr: "Name eq 'alice'", want: false},
		{name: "string_lt", expr: "Name lt 'zoe'", want: true},
		{name: "bool_eq", expr: "Active eq true", want: true},
		{name: "partition_key", expr: "PartitionKey eq 'p'", want: true},
		{name: "row_key", expr: "RowKey eq 'r'", want: true},
		{name: "missing_property_false", expr: "Nonexistent eq 'x'", want: false},
		{name: "and", expr: "Age eq 30 and Name eq 'bob'", want: true},
		{name: "and_false", expr: "Age eq 30 and Name eq 'alice'", want: false},
		{name: "or", expr: "Age eq 1 or Name eq 'bob'", want: true},
		{name: "not", expr: "not (Age eq 1)", want: true},
		{name: "parens", expr: "(Age eq 30 or Age eq 1) and Name eq 'bob'", want: true},
		{
			name: "precedence_and_binds_tighter",
			expr: "Age eq 1 or Age eq 30 and Name eq 'bob'", want: true,
		},
		{name: "int64_literal", expr: "Age eq 30L", want: true},
		{name: "float_literal_no_match", expr: "Age eq 30.5", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, evalFilter(t, tt.expr, entity), tt.expr)
		})
	}
}

func TestParseFilter_LiteralForms(t *testing.T) {
	t.Parallel()

	dt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	entity := entityWith(map[string]azuretable.EntityProperty{
		"When":  {Type: azuretable.EdmDateTime, Value: dt},
		"ID":    {Type: azuretable.EdmGUID, Value: "550e8400-e29b-41d4-a716-446655440000"},
		"Blob":  {Type: azuretable.EdmBinary, Value: []byte("hi")},
		"Big":   {Type: azuretable.EdmInt64, Value: int64(9223372036854775807)},
		"Score": {Type: azuretable.EdmDouble, Value: 3.14},
	})

	tests := []struct {
		name string
		expr string
		want bool
	}{
		{name: "datetime_eq", expr: "When eq datetime'2024-01-02T03:04:05.0000000Z'", want: true},
		{name: "datetime_lt", expr: "When lt datetime'2025-01-01T00:00:00.0000000Z'", want: true},
		{name: "guid_eq", expr: "ID eq guid'550e8400-e29b-41d4-a716-446655440000'", want: true},
		{name: "binary_base64_eq", expr: "Blob eq binary'aGk='", want: true},
		{name: "binary_hex_eq", expr: "Blob eq X'6869'", want: true},
		{name: "int64_eq", expr: "Big eq 9223372036854775807L", want: true},
		{name: "double_eq", expr: "Score eq 3.14", want: true},
		{name: "escaped_quote_string", expr: "PartitionKey eq 'p'", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, evalFilter(t, tt.expr, entity), tt.expr)
		})
	}
}

func TestParseFilter_EscapedQuoteInStringLiteral(t *testing.T) {
	t.Parallel()

	entity := entityWith(map[string]azuretable.EntityProperty{
		"Name": {Type: azuretable.EdmString, Value: "O'Brien"},
	})

	assert.True(t, evalFilter(t, "Name eq 'O''Brien'", entity))
}

func TestParseFilter_MalformedInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "empty", expr: ""},
		{name: "unbalanced_open_paren", expr: "(Age eq 1"},
		{name: "unbalanced_close_paren", expr: "Age eq 1)"},
		{name: "trailing_operator", expr: "Age eq 1 and"},
		{name: "trailing_and", expr: "Age eq 1 and or"},
		{name: "dangling_eq", expr: "Age eq"},
		{name: "double_operator", expr: "Age eq eq 1"},
		{name: "unterminated_string", expr: "Name eq 'unterminated"},
		{name: "invalid_token", expr: "Age eq @"},
		{name: "just_and", expr: "and"},
		{name: "just_paren", expr: "("},
		{name: "empty_parens", expr: "()"},
		{name: "missing_operand_after_not", expr: "not"},
		{name: "bad_datetime", expr: "Age eq datetime'not-a-date'"},
		{name: "bad_hex", expr: "Age eq X'zz'"},
		{name: "trailing_garbage", expr: "Age eq 1 garbage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := azuretable.ParseFilter(tt.expr)
			require.Error(t, err, tt.expr)
		})
	}
}

// TestParseFilter_DeepNestingBounded is a regression test against an
// unbounded recursive-descent parser stack-overflowing on adversarial input:
// a filter nested well past maxFilterDepth must return a parse error, never
// panic.
func TestParseFilter_DeepNestingBounded(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat("(", 500) + "Age eq 1" + strings.Repeat(")", 500)

	assert.NotPanics(t, func() {
		_, err := azuretable.ParseFilter(deep)
		require.Error(t, err)
	})
}

func TestEvaluateFilter_TypeMismatchIsFalse(t *testing.T) {
	t.Parallel()

	entity := entityWith(map[string]azuretable.EntityProperty{
		"Name": {Type: azuretable.EdmString, Value: "bob"},
	})

	assert.False(t, evalFilter(t, "Name eq 1", entity))
	assert.False(t, evalFilter(t, "Name gt true", entity))
}

func TestEvaluateFilter_BoolOnlySupportsEqNe(t *testing.T) {
	t.Parallel()

	entity := entityWith(map[string]azuretable.EntityProperty{
		"Active": {Type: azuretable.EdmBoolean, Value: true},
	})

	assert.False(t, evalFilter(t, "Active gt false", entity))
	assert.True(t, evalFilter(t, "Active ne false", entity))
}
