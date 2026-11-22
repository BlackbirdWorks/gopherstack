package timestreamquery

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func abusiveQueries() map[string]string {
	return map[string]string{
		"1mb string":         "SELECT '" + strings.Repeat("a", 1<<20) + "'",
		"1mb unterminated":   "SELECT '" + strings.Repeat("a", 1<<20),
		"1mb ident":          "SELECT " + strings.Repeat("a", 1<<20),
		"thousands of items": "SELECT " + strings.Repeat("1,", 20000) + "1",
		"many unions":        strings.Repeat("SELECT 1 UNION ALL ", 5000) + "SELECT 1",
		"deep case": "SELECT " + strings.Repeat(
			"CASE WHEN true THEN ",
			3000,
		) + "1" + strings.Repeat(
			" END",
			3000,
		),
		"deep arrays": "SELECT " + strings.Repeat("ARRAY[", 3000) + "1" + strings.Repeat("]", 3000),
		"deep calls":  "SELECT " + strings.Repeat("abs(", 3000) + "1" + strings.Repeat(")", 3000),
		"deep unary":  "SELECT " + strings.Repeat("-", 100000) + "1",
		"deep not":    "SELECT " + strings.Repeat("NOT ", 100000) + "true",
		"deep ctes": strings.Repeat(
			"WITH a AS (",
			3000,
		) + "SELECT 1" + strings.Repeat(
			") SELECT * FROM a",
			3000,
		),
		"huge numeric":         "SELECT " + strings.Repeat("9", 5000) + ", 1e999999, 0x, 1e, .e1",
		"nul bytes":            "SELECT '\x00', \"\x00\", \x00 FROM \x00",
		"unicode":              "SELECT '日本語', \"ключ\", 😀 FROM t",
		"invalid utf8":         "SELECT '\xff\xfe', \xc3\x28",
		"unterminated quoted":  `SELECT "abc`,
		"unterminated comment": "SELECT 1 /* never closed",
		"comment only":         "-- nothing",
		"block comment only":   "/* nothing */",
		"line comment at end":  "SELECT 1 --",
		"many lambdas": "SELECT " + strings.Repeat(
			"filter(ARRAY[1], x -> ",
			2000,
		) + "true" + strings.Repeat(
			")",
			2000,
		),
		"huge in list":       "SELECT 1 IN (" + strings.Repeat("1,", 50000) + "1)",
		"huge between chain": "SELECT " + strings.Repeat("1 BETWEEN 0 AND ", 2000) + "2",
		"operators":          "SELECT " + strings.Repeat("1 + ", 50000) + "1",
		"parens balance":     strings.Repeat(")", 50000),
		"lone quote":         "'",
		"empty":              "",
		"only semicolons":    ";;;;",
		"interval junk":      "SELECT 1d2h3m, 99999999999999999999d, interval '' day",
		"cast garbage":       "SELECT cast(1 as ), cast( as bigint), cast(1 as array(array(array(",
		"deep row type": "SELECT cast(1 as " + strings.Repeat(
			"array(",
			3000,
		) + "bigint" + strings.Repeat(
			")",
			3000,
		) + ")",
		"dollar quote":          "SELECT $$ -- x $$, $a$ /* y */ $a$",
		"comment markers adj":   "SELECT 1-/**/-1, 1/-/*x*/-1",
		"backslash":             `SELECT '\', "\\" -- \`,
		"non-ascii whitespace":  "SELECT 1 +　2",
		"identifier like names": `SELECT typeof_x, "typeof"(1), ts_filter, filter_x FROM t`,
	}
}

func TestAnnotateAbusiveQueriesDoNotPanicOrHang(t *testing.T) {
	t.Parallel()

	for name, q := range abusiveQueries() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			start := time.Now()

			assert.NotPanics(t, func() {
				_ = stripSQLComments(q)
				_, _ = parseSQL(q)
				_ = buildAnnotation(q, loadedSet{}, time.Time{}, nil)
			})
			assert.Less(t, time.Since(start), 10*time.Second)
		})
	}
}

func FuzzAnnotateQuery(f *testing.F) {
	for _, s := range []string{
		"SELECT 1", "SELECT a, count(*) FROM t WHERE time > ago(1h) GROUP BY a", "SELECT 'x' -- c",
		"SELECT /* c */ CASE WHEN a THEN 1 END",
		"WITH a AS (SELECT 1) SELECT * FROM a", "SELECT filter(ARRAY[1], x -> x > 0)", `SELECT "a--b"`,
	} {
		f.Add(s)
	}

	f.Fuzz(func(_ *testing.T, q string) {
		_ = stripSQLComments(q)
		_, _ = parseSQL(q)
		_ = buildAnnotation(q, loadedSet{}, time.Time{}, nil)
	})
}

func TestAdmitInsightsLimiterStaysBounded(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	b := NewInMemoryBackend("000000000000", "us-east-1")
	b.SetClock(func() time.Time { return now })

	for i := range 100 {
		require.NoError(t, b.admitInsights("region-"+strconv.Itoa(i), insightsModeEnabled))
	}

	require.ErrorIs(t, b.admitInsights("region-0", insightsModeEnabled), ErrThrottling)
	require.NoError(t, b.admitInsights("region-0", insightsModeDisabled), "queries without insights are never limited")

	now = now.Add(time.Second)

	require.NoError(t, b.admitInsights("region-0", insightsModeEnabled))
	assert.Len(t, b.insightsLast, 1)
}
