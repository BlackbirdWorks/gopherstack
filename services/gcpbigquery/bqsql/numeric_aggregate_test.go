package bqsql_test

import (
	"fmt"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

type frameCase struct {
	lo, hi func(i int) int
	sql    string
}

func frameCases() []frameCase {
	const unb = 1 << 30

	return []frameCase{
		{func(i int) int { return i - 1 }, func(i int) int { return i }, "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"},
		{
			func(i int) int { return i - 2 },
			func(i int) int { return i + 1 },
			"ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
		},
		{
			func(int) int { return -unb },
			func(i int) int { return i },
			"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
		},
		{
			func(i int) int { return i },
			func(int) int { return unb },
			"ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
		},
		{
			func(i int) int { return i + 1 },
			func(i int) int { return i + 3 },
			"ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING",
		},
		{
			func(i int) int { return i - 3 },
			func(i int) int { return i - 1 },
			"ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
		},
		{
			func(int) int { return -unb },
			func(int) int { return unb },
			"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
		},
	}
}

func windowCatalog(t *testing.T, vals []string, nullEvery int) *fakeCatalog {
	t.Helper()

	fs := fieldsJSON(
		t,
		`[{"name":"id","type":"INTEGER"},{"name":"n","type":"NUMERIC"},{"name":"b","type":"BIGNUMERIC"}]`,
	)

	var sb strings.Builder

	sb.WriteString("[")

	for i, v := range vals {
		if i > 0 {
			sb.WriteString(",")
		}

		cell := `"` + v + `"`
		if nullEvery > 0 && i%nullEvery == nullEvery-1 {
			cell = "null"
		}

		fmt.Fprintf(&sb, `{"id":%d,"n":%s,"b":%s}`, i, cell, cell)
	}

	sb.WriteString("]")

	return &fakeCatalog{tables: map[string]*bqsql.TableData{
		"ds.w": {
			Ref:    bqsql.TableRef{Project: "p", Dataset: "ds", Table: "w"},
			Fields: fs,
			Rows:   rowsJSON(t, fs, sb.String()),
		},
	}}
}

func windowValue(r *rand.Rand) string {
	digits := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('0' + r.IntN(10))
		}

		return string(b)
	}

	s := "0"
	if n := r.IntN(25); n > 0 {
		s = digits(n)
	}

	if f := r.IntN(10); f > 0 {
		s += "." + digits(f)
	}

	if r.IntN(2) == 0 {
		s = "-" + s
	}

	return s
}

func TestNumericWindowFramesMatchBruteForce(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(11, 13))
	nk := oracleKinds()["numeric"]
	vals := make([]string, 40)

	for i := range vals {
		vals[i] = windowValue(r)
	}

	for _, nullEvery := range []int{0, 3} {
		cat := windowCatalog(t, vals, nullEvery)

		for _, fc := range frameCases() {
			t.Run(fmt.Sprintf("%d/%s", nullEvery, fc.sql), func(t *testing.T) {
				t.Parallel()

				sql := "SELECT id, SUM(n) OVER (ORDER BY id " + fc.sql + "), AVG(n) OVER (ORDER BY id " + fc.sql +
					"), SUM(b) OVER (ORDER BY id " + fc.sql + ") FROM w ORDER BY id"

				res, err := runSQL(t, cat, sql)
				require.NoError(t, err)

				got := wireRows(res)
				require.Len(t, got, len(vals))

				for i := range vals {
					sum, cnt := new(big.Rat), 0

					for j := max(fc.lo(i), 0); j <= min(fc.hi(i), len(vals)-1); j++ {
						if nullEvery > 0 && j%nullEvery == nullEvery-1 {
							continue
						}

						sum.Add(sum, ratOf(t, vals[j]))

						cnt++
					}

					if cnt == 0 {
						assert.Equal(t, []any{strconv.Itoa(i), nil, nil, nil}, got[i], "row %d", i)

						continue
					}

					sn, _ := nk.unscaled(sum)
					avg, _ := nk.unscaled(new(big.Rat).Quo(sum, big.NewRat(int64(cnt), 1)))
					bk := oracleKinds()["bignumeric"]
					sb, _ := bk.unscaled(sum)

					assert.Equal(
						t,
						[]any{strconv.Itoa(i), nk.canonical(sn), nk.canonical(avg), bk.canonical(sb)},
						got[i],
						"row %d frame %s",
						i,
						fc.sql,
					)
				}
			})
		}
	}
}

func TestNumericAggregateEdges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		err  string
		want [][]any
	}{
		{
			"avg tie up",
			"SELECT AVG(x) FROM UNNEST([NUMERIC '0.000000001', NUMERIC '0']) x",
			"",
			[][]any{{"0.000000001"}},
		},
		{
			"avg tie down",
			"SELECT AVG(x) FROM UNNEST([NUMERIC '-0.000000001', NUMERIC '0']) x",
			"",
			[][]any{{"-0.000000001"}},
		},
		{
			"avg third",
			"SELECT AVG(x) FROM UNNEST([NUMERIC '1', NUMERIC '1', NUMERIC '2']) x",
			"",
			[][]any{{"1.333333333"}},
		},
		{
			"avg big",
			"SELECT AVG(x) FROM UNNEST([BIGNUMERIC '1', BIGNUMERIC '1', BIGNUMERIC '2']) x",
			"",
			[][]any{{"1.33333333333333333333333333333333333333"}},
		},
		{"empty", "SELECT SUM(x), AVG(x) FROM UNNEST(ARRAY<NUMERIC>[]) x", "", [][]any{{nil, nil}}},
		{
			"all null",
			"SELECT SUM(x), AVG(x), MIN(x), MAX(x) FROM UNNEST([CAST(NULL AS NUMERIC)]) x",
			"",
			[][]any{{nil, nil, nil, nil}},
		},
		{
			"skips null",
			"SELECT SUM(x), AVG(x), COUNT(x) FROM UNNEST([NUMERIC '1', NULL, NUMERIC '2']) x",
			"",
			[][]any{{"3", "1.5", "2"}},
		},
		{"distinct", "SELECT SUM(DISTINCT x), AVG(DISTINCT x), COUNT(DISTINCT x) " +
			"FROM UNNEST([NUMERIC '1', NUMERIC '1.0', NUMERIC '2']) x", "", [][]any{{"3", "1.5", "2"}}},
		{
			"overflow",
			"SELECT SUM(x) FROM UNNEST([NUMERIC '99999999999999999999999999999.999999999', NUMERIC '0.000000001']) x",
			"numeric overflow",
			nil,
		},
		{
			"big overflow",
			"SELECT SUM(x) FROM UNNEST([BIGNUMERIC '" + bigMax + "', BIGNUMERIC '1']) x",
			"BIGNUMERIC overflow",
			nil,
		},
		{
			"minmax type",
			"SELECT MIN(x), MAX(x) FROM UNNEST([NUMERIC '1.5', NUMERIC '-2']) x",
			"",
			[][]any{{"-2", "1.5"}},
		},
		{
			"count distinct kinds",
			"SELECT COUNT(DISTINCT x) FROM UNNEST([CAST(NUMERIC '1.5' AS BIGNUMERIC), BIGNUMERIC '1.5', BIGNUMERIC '2']) x",
			"",
			[][]any{{"2"}},
		},
		{
			"count distinct union",
			"SELECT COUNT(DISTINCT x) FROM (SELECT NUMERIC '1.5' AS x UNION ALL SELECT BIGNUMERIC '1.5' UNION ALL SELECT 1.5)",
			"",
			[][]any{{"1"}},
		},
		{"sum int numeric", "SELECT SUM(x) FROM UNNEST([1, 2, 3]) x", "", [][]any{{"6"}}},
		{
			"stddev",
			"SELECT STDDEV(x), VARIANCE(x) FROM UNNEST([NUMERIC '2', NUMERIC '4']) x",
			"",
			[][]any{{"1.4142135623730951", "2.0"}},
		},
		{"stddev single", "SELECT STDDEV(x), STDDEV_POP(x) FROM UNNEST([NUMERIC '2']) x", "", [][]any{{nil, "0.0"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, numericCatalog(t), tt.sql)
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, wireRows(res))
		})
	}
}

func TestNormalizeRowsLegacyCells(t *testing.T) {
	t.Parallel()

	fs := fieldsJSON(t, `[
		{"name":"n","type":"NUMERIC"},
		{"name":"b","type":"BIGNUMERIC"},
		{"name":"arr","type":"NUMERIC","mode":"REPEATED"},
		{"name":"rec","type":"RECORD","fields":[{"name":"x","type":"NUMERIC"},{"name":"s","type":"STRING"}]}
	]`)

	rows := [][]any{
		{float64(0.1), "2.250000000", []any{float64(1.5), "2", nil}, []any{int64(3), "7"}},
		{"5e-1", float64(1e21), []any{}, []any{"x", "keep"}},
		{nil, nil, nil, nil},
	}

	bqsql.NormalizeRows(fs, rows)

	assert.Equal(t, [][]any{
		{"0.1", "2.25", []any{"1.5", "2", nil}, []any{"3", "7"}},
		{"0.5", "1000000000000000000000", []any{}, []any{"x", "keep"}},
		{nil, nil, nil, nil},
	}, rows)
}
