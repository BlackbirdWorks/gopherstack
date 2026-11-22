package timestreamquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedTranslationKeepsEveryOwnedName(t *testing.T) {
	t.Parallel()

	registerTimestreamFuncs()

	require.Greater(t, len(plainFuncs), 30)

	for name := range plainFuncs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			out, err := translate("SELECT "+name+"(1)", loadedSet{})
			if err != nil {
				t.Skipf("%s has special call syntax: %v", name, err)
			}

			got := qualifyEngineFuncs(out)
			assert.Contains(t, got, funcPrefix+name+"(", "Timestream must run its own %s", name)
			assert.NotContains(t, got, "athena_"+name+"(")
		})
	}
}

func TestAthenaOnlyFunctionsAreUnknown(t *testing.T) {
	t.Parallel()

	tests := []string{
		"flatten(ARRAY[ARRAY[1], ARRAY[2]])",
		"array_first(ARRAY[1, 2])",
		"array_last(ARRAY[1, 2])",
		"trim_array(ARRAY[1, 2], 1)",
		"contains_sequence(ARRAY[1, 2], ARRAY[1])",
		"url_encode('a b')",
		"url_decode('a%20b')",
		"translate('abc', 'a', 'x')",
		"ends_with('abc', 'c')",
		"width_bucket(1.5, 0, 3, 3)",
		"bitwise_left_shift(1, 2)",
		"bitwise_right_shift(4, 1)",
		"bitwise_right_shift_arithmetic(4, 1)",
		"kurtosis(x) FROM (SELECT 1 AS x)",
		"bitwise_xor_agg(x) FROM (SELECT 1 AS x)",
	}

	for _, expr := range tests {
		t.Run(strings.SplitN(expr, "(", 2)[0], func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.QueryWithOptions(t.Context(), QueryOptions{QueryString: "SELECT " + expr})
			require.Error(t, err)
			assert.Contains(t, strings.ToLower(err.Error()), strings.SplitN(expr, "(", 2)[0])
		})
	}
}

func TestOwnedFunctionsKeepTimestreamResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want string
	}{
		{"skewness", "round(skewness(x), 4) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 9)", "0.6655"},
		{"infinity", "infinity()", "Infinity"},
		{"is_infinite", "is_infinite(infinity())", "true"},
		{"is_nan", "is_nan(nan())", "true"},
		{"arrays_overlap", "arrays_overlap(ARRAY[1, 2], ARRAY[2, 3])", "true"},
		{
			"zip", "CAST(zip(ARRAY[1, 2], ARRAY['a', 'b']) AS VARCHAR)",
			`[{"\u00000":1,"\u00001":"a"},{"\u00000":2,"\u00001":"b"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			pg, err := b.QueryWithOptions(t.Context(), QueryOptions{QueryString: "SELECT " + tt.expr})
			require.NoError(t, err)
			require.Len(t, pg.Rows, 1)
			require.NotNil(t, pg.Rows[0].Data[0].ScalarValue)
			assert.Equal(t, tt.want, strings.TrimPrefix(*pg.Rows[0].Data[0].ScalarValue, "\x01"))
		})
	}
}
