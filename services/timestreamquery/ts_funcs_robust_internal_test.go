package timestreamquery

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

type registeredFunc struct {
	name  string
	narg  int
	aggFn bool
}

func registeredTimestreamFuncs(t *testing.T) []registeredFunc {
	t.Helper()

	db, err := athena.OpenEngineDB()
	require.NoError(t, err)

	defer db.Close()

	rows, err := db.QueryContext(t.Context(),
		"SELECT name, narg, type FROM pragma_function_list WHERE builtin = 0 AND name LIKE 'ts\\_%' ESCAPE '\\'")
	require.NoError(t, err)

	defer rows.Close()

	var out []registeredFunc

	for rows.Next() {
		var (
			f   registeredFunc
			typ string
		)

		require.NoError(t, rows.Scan(&f.name, &f.narg, &typ))

		f.aggFn = typ == "a"
		out = append(out, f)
	}

	require.NoError(t, rows.Err())

	return out
}

// Every registered function must reject hostile arguments with an error or NULL, never panic or run away.
func TestRegisteredFunctionsSurviveHostileArguments(t *testing.T) {
	t.Parallel()

	funcs := registeredTimestreamFuncs(t)
	require.Greater(t, len(funcs), 100)

	args := []struct {
		val  any
		name string
	}{
		{nil, "null"},
		{[]byte{0xF3, 'i', 'v', 0x1D, 'Y', 1, 2}, "blob"},
		{int64(math.MaxInt64), "huge int"},
		{int64(math.MinInt64), "min int"},
		{"NaN", "nan text"},
		{"", "empty text"},
		{strings.Repeat("ab", 1000), "long text"},
		{1e300, "float"},
		{`[1,2,3]`, "json"},
	}

	for _, f := range funcs {
		arities := []int{f.narg}
		if f.narg < 0 {
			arities = []int{0, 1, 2, 3, 4}
		}

		for _, n := range arities {
			t.Run(fmt.Sprintf("%s/%d", f.name, n), func(t *testing.T) {
				t.Parallel()

				db, err := athena.OpenEngineDB()
				require.NoError(t, err)

				defer db.Close()

				marks := strings.TrimSuffix(strings.Repeat("?,", n), ",")
				q := fmt.Sprintf("SELECT %s(%s)", f.name, marks)

				for _, a := range args {
					params := make([]any, n)
					for i := range params {
						params[i] = a.val
					}

					start := time.Now()

					assert.NotPanics(t, func() {
						var v any

						_ = db.QueryRowContext(t.Context(), q, params...).Scan(&v)
					}, a.name)
					assert.Less(t, time.Since(start), 5*time.Second, a.name)
				}
			})
		}
	}
}
