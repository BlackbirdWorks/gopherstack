package ocilogging

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkRows(n int, f func(i int) map[string]any) []*row {
	rows := make([]*row, 0, n)
	for i := range n {
		rows = append(rows, &row{ce: map[string]any{"data": f(i)}, seq: int64(i)})
	}

	return rows
}

func runQuery(t *testing.T, tail string, rows []*row) ([]*row, *APIError) {
	t.Helper()

	q, err := ParseQuery(`search "c"` + tail)
	require.NoError(t, err)

	out, _, _, aerr := q.apply(rows)

	return out, aerr
}

func TestGroupKeyDoesNotCollide(t *testing.T) {
	t.Parallel()

	a := groupKey([]any{"a\x001b", "c"}, []bool{true, true})
	b := groupKey([]any{"a", "b\x001c"}, []bool{true, true})
	assert.NotEqual(t, a, b)

	rows := mkRows(2, func(i int) map[string]any {
		if i == 0 {
			return map[string]any{"x": "a\x001b", "y": "c"}
		}

		return map[string]any{"x": "a", "y": "b\x001c"}
	})
	out, aerr := runQuery(t, ` | dedup data.x, data.y`, rows)
	require.Nil(t, aerr)
	assert.Len(t, out, 2)
}

func TestWildcardMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"a*", "a", true},
		{"*c", "abc", true},
		{"a*c", "abc", true},
		{"ab*bc", "abc", false},
		{"a*b*c", "a--b--c", true},
		{"a*b*c", "a--c--b", false},
		{"*", "", true},
		{"**", "x", true},
		{"a.c*", "abc", false},
		{"a.c*", "a.cd", true},
		{"*\n*", "x\ny", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"/"+tt.s, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, wildcard(tt.pattern, tt.s))
		})
	}
}

func TestParseQueryMalformedInput(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat("(", 4000)
	tests := []struct {
		name  string
		query string
	}{
		{"empty", ``},
		{"no scope", `search`},
		{"unterminated string", `search "c" | where data.a = 'x`},
		{"unterminated path", `search "c" | where "data.a = 1`},
		{"deep parens", `search "c" | where ` + deep + `data.a = 1`},
		{"deep parens closed", `search "c" | where ` + deep + `data.a = 1` + strings.Repeat(")", 4000)},
		{"deep arithmetic", `search "c" | select ` + deep + `1` + strings.Repeat(")", 4000)},
		{"deep not", `search "c" | where ` + strings.Repeat("not ", 3000) + `data.a`},
		{"deep neg", `search "c" | select ` + strings.Repeat("- ", 3000) + `1`},
		{"too many ops", `search "c"` + strings.Repeat(` | limit 1`, 100)},
		{"huge number", `search "c" | where data.a = ` + strings.Repeat("9", 400)},
		{"dotted number", `search "c" | where data.a = 1.2.3`},
		{"bad agg", `search "c" | summarize foo(data.a)`},
		{"agg no paren", `search "c" | summarize sum`},
		{"count arg missing", `search "c" | summarize sum()`},
		{"substr arity", `search "c" | select substr(data.a)`},
		{"rounddown bad", `search "c" | select rounddown(datetime, 'x')`},
		{"rounddown huge", `search "c" | select rounddown(datetime, '999999999d')`},
		{"extend two", `search "c" | extend 1 as a, 2 as b`},
		{"as missing", `search "c" | select data.a as`},
		{"control chars", "search \"c\" | where \x00\x01"},
		{"unicode", `search "c" | where données.é = 'ü'`},
		{"in unclosed", `search "c" | where data.a in (1, 2`},
		{"in empty", `search "c" | where data.a in ()`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.NotPanics(t, func() {
				q, err := ParseQuery(tt.query)
				if err != nil || q == nil {
					return
				}

				q.apply(mkRows(3, func(int) map[string]any { return map[string]any{"a": "x", "n": 1.0} }))
			})
		})
	}
}

func TestQueryEvalEdgeCases(t *testing.T) {
	t.Parallel()

	huge := "1" + strings.Repeat("0", 300)
	rows := func() []*row {
		return mkRows(4, func(i int) map[string]any {
			return map[string]any{"n": float64(i + 1), "s": "inf", "t": "NaN", "w": "héllo"}
		})
	}

	tests := []struct {
		wantVal    any
		name, tail string
		wantAbsent bool
	}{
		{name: "overflow multiply", tail: ` | select data.n * ` + huge + ` * ` + huge + ` as x`, wantAbsent: true},
		{name: "divide by zero", tail: ` | select data.n / 0 as x`, wantAbsent: true},
		{name: "zero over zero", tail: ` | select 0 / 0 as x`, wantAbsent: true},
		{name: "string inf is not numeric", tail: ` | select data.s + 1 as x`, wantAbsent: true},
		{name: "string nan is not numeric", tail: ` | select data.t * 2 as x`, wantAbsent: true},
		{name: "substr past end", tail: ` | select substr(data.w, 99) as x`, wantVal: ""},
		{name: "substr huge count", tail: ` | select substr(data.w, 1, 999999999999999999999) as x`, wantVal: "éllo"},
		{name: "substr negative", tail: ` | select substr(data.w, 0 - 1) as x`, wantAbsent: true},
		{name: "substr unicode", tail: ` | select substr(data.w, 1, 2) as x`, wantVal: "él"},
		{
			name:       "sum overflow",
			tail:       ` | summarize sum(data.n * ` + huge + ` * 0.000001 * ` + huge + ` * 0.000000001) as x`,
			wantAbsent: true,
		},
		{name: "avg of non-numeric", tail: ` | summarize avg(data.w) as x`, wantAbsent: true},
		{name: "min mixed", tail: ` | summarize min(data.n) as x`, wantVal: 1.0},
		{name: "first of null", tail: ` | summarize first(data.zz) as x`, wantAbsent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, aerr := runQuery(t, tt.tail, rows())
			require.Nil(t, aerr)
			require.NotEmpty(t, out)

			v, ok := out[0].cols["x"]
			assert.Equal(t, !tt.wantAbsent, ok)

			if !tt.wantAbsent {
				assert.Equal(t, tt.wantVal, v)
			}
		})
	}
}

func TestSumOverflowIsNotInfinite(t *testing.T) {
	t.Parallel()

	rows := mkRows(3, func(int) map[string]any { return map[string]any{"n": 1e308} })
	out, aerr := runQuery(t, ` | summarize sum(data.n) as s, avg(data.n) as a, max(data.n) as m`, rows)
	require.Nil(t, aerr)
	require.Len(t, out, 1)
	assert.NotContains(t, out[0].cols, "s")
	assert.NotContains(t, out[0].cols, "a")
	assert.InDelta(t, 1e308, out[0].cols["m"], 0)
}

func TestQueryScalesLinearly(t *testing.T) {
	t.Parallel()

	const n = 200000

	rows := func() []*row {
		return mkRows(n, func(i int) map[string]any {
			return map[string]any{"a": strconv.Itoa(i % 5000), "b": strconv.Itoa(i % 2), "m": "x" + strconv.Itoa(i)}
		})
	}

	out, aerr := runQuery(t, ` | dedup data.a, data.b`, rows())
	require.Nil(t, aerr)
	assert.Len(t, out, 5000)

	out, aerr = runQuery(t, ` | summarize count() by data.a, data.b`, rows())
	require.Nil(t, aerr)
	assert.Len(t, out, 5000)

	_, aerr = runQuery(t, ` | summarize count() by data.m`, rows())
	require.NotNil(t, aerr, "more than the group cap is rejected")

	out, aerr = runQuery(t, ` | where data.m = '*9*9*9*9'`, rows())
	require.Nil(t, aerr)
	assert.NotEmpty(t, out)
}

func TestSubtreeLookupRunsOutsideBackendLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list func(b *InMemoryBackend) error
		name string
	}{
		{name: "log groups", list: func(b *InMemoryBackend) error {
			_, err := b.ListLogGroupsIn("root", "", true, ListOpts{})

			return err
		}},
		{name: "agents", list: func(b *InMemoryBackend) error {
			_, err := b.ListAgentConfigs(AgentFilter{Compartment: "root", Subtree: true}, ListOpts{})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			b.groups["g"] = &LogGroup{ID: "g", CompartmentID: "child", Name: "g"}
			b.agents["a"] = &AgentConfig{ID: "a", CompartmentID: "child", Name: "a"}

			calls := 0
			b.SetCompartmentTree(func(id, ancestor string) bool {
				calls++

				locked, _, _ := b.mu.GetLockStatus()
				assert.False(t, locked, "compartment tree called with the backend lock held")

				return id == "child" && ancestor == "root"
			})

			require.NoError(t, tt.list(b))
			assert.Equal(t, 1, calls)
		})
	}
}
