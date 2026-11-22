package timestreamquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestQualifyEngineFuncs(t *testing.T) {
	t.Parallel()

	registerTimestreamFuncs()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain call", `SELECT repeat(1, 2)`, `SELECT ts_repeat(1, 2)`},
		{"upper case", `SELECT REPEAT(1, 2)`, `SELECT ts_REPEAT(1, 2)`},
		{"space before paren", "SELECT zip\n  (a, b)", "SELECT ts_zip\n  (a, b)"},
		{
			"nested",
			`SELECT nan(), cardinality(shuffle(sequence(1, 3)))`,
			`SELECT ts_nan(), cardinality(ts_shuffle(ts_sequence(1, 3)))`,
		},
		{"string literal", `SELECT 'repeat(1, 2)'`, `SELECT 'repeat(1, 2)'`},
		{"escaped quote", `SELECT 'it''s zip(a)', zip(a)`, `SELECT 'it''s zip(a)', ts_zip(a)`},
		{"quoted identifier", `SELECT "repeat"(1) FROM t`, `SELECT "repeat"(1) FROM t`},
		{"column named like a function", `SELECT repeat, nan FROM t`, `SELECT repeat, nan FROM t`},
		{"member access", `SELECT t.repeat(1)`, `SELECT t.repeat(1)`},
		{"comment", "SELECT 1 -- zip(a)\n, repeat(1,2)", "SELECT 1 -- zip(a)\n, ts_repeat(1,2)"},
		{"block comment", `SELECT /* nan() */ nan()`, `SELECT /* nan() */ ts_nan()`},
		{"already prefixed", `SELECT ts_sum(x), athena_array(1)`, `SELECT ts_sum(x), athena_array(1)`},
		{
			"longer identifier",
			`SELECT repeat_x(1), xrepeat(1), repeat1(2)`,
			`SELECT repeat_x(1), xrepeat(1), repeat1(2)`,
		},
		{
			"sqlite builtin",
			`SELECT replace('a', 'b', 'c'), reverse('a'), abs(1)`,
			`SELECT replace('a', 'b', 'c'), reverse('a'), abs(1)`,
		},
		{"unterminated string", `SELECT 'repeat(`, `SELECT 'repeat(`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, qualifyEngineFuncs(tt.in))
		})
	}
}

func TestEveryPlainFunctionIsRegisteredPrefixed(t *testing.T) {
	t.Parallel()

	registerTimestreamFuncs()
	require.NotEmpty(t, plainFuncs)

	registered := map[string]bool{}
	for _, f := range registeredTimestreamFuncs(t) {
		registered[f.name] = true
	}

	for plain := range plainFuncs {
		assert.True(t, registered[funcPrefix+plain], plain)
	}

	db, err := athena.OpenEngineDB()
	require.NoError(t, err)

	defer db.Close()

	for plain := range plainFuncs {
		var n int

		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT count(*) FROM pragma_function_list WHERE name = ?", plain).Scan(&n))
		assert.Zero(t, n, "%s must not be callable unprefixed", plain)
	}
}
