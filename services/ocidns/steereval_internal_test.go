package ocidns

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func steerAns(name string) map[string]any {
	return map[string]any{"name": name, "rtype": "A", "rdata": name}
}

func weightedRule(w map[string]float64) map[string]any {
	data := make([]any, 0, len(w))
	for n, v := range w {
		data = append(data, map[string]any{"answerCondition": "answer.name == '" + n + "'", "value": v})
	}

	return map[string]any{"ruleType": "WEIGHTED", "defaultAnswerData": data}
}

func names(as []map[string]any) []string {
	out := make([]string, 0, len(as))
	for _, a := range as {
		out = append(out, a["name"].(string))
	}

	return out
}

func TestSteerExprMalformedInput(t *testing.T) {
	t.Parallel()

	q := steerQuery{client: netip.MustParseAddr("10.1.2.3")}
	tests := []struct {
		name string
		expr string
	}{
		{"empty", ""},
		{"only space", "   \t "},
		{"one token", "query.client.address"},
		{"lone equals", "query.client.address = '1.1.1.1'"},
		{"trailing bang", "query.client.address !"},
		{"trailing equals", "query.client.address =="},
		{"unterminated quote", "query.client.address == '10.0.0.1"},
		{"open paren", "query.client.address in ("},
		{"close paren only", "query.client.address in )"},
		{"empty list", "query.client.address in ()"},
		{"comma only", "query.client.address in (,)"},
		{"trailing comma", "query.client.address in ('10.0.0.0/8',)"},
		{"leading comma", "query.client.address in (,'10.0.0.0/8')"},
		{"nested parens", "query.client.address in (((('10.0.0.0/8'))))"},
		{"bad prefix", "query.client.address in (subnet '10.0.0.0/99')"},
		{"bad type word", "query.client.address in (a b c '10.0.0.0/8')"},
		{"unknown attr", "foo.bar == 'x'"},
		{"unknown op", "query.client.address ~= '10.0.0.1'"},
		{"nul bytes", "query.client.address == '\x00'"},
		{"invalid utf8", "query.client.address == '\xff\xfe'"},
		{"too long", "query.client.address == '" + string(make([]byte, 2000)) + "'"},
		{"huge list", "query.client.address in (" + repeatJoin("'1.1.1.1'", ",", 5000) + ")"},
		{"many parens", repeatJoin("(", "", 5000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.NotPanics(t, func() { assert.False(t, evalSteerExpr(tt.expr, nil, q)) })
		})
	}
}

func repeatJoin(s, sep string, n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = s
	}

	return strings.Join(parts, sep)
}

func TestSteerWeightedDistribution(t *testing.T) {
	t.Parallel()

	answers := []map[string]any{steerAns("a"), steerAns("b"), steerAns("c")}
	rule := weightedRule(map[string]float64{"a": 1, "b": 3, "c": 0})
	first := map[string]int{}

	for r := range 4 {
		calls := 0
		got := applySteerRule(rule, answers, steerQuery{}, func(n int) int {
			calls++
			if calls == 1 {
				require.Equal(t, 4, n)

				return r
			}

			return 0
		})
		first[names(got)[0]]++
		assert.Equal(t, "c", names(got)[2], "zero weight goes last")
	}

	assert.Equal(t, map[string]int{"a": 1, "b": 3}, first)
}

func TestSteerWeightedOutOfRangePickTerminates(t *testing.T) {
	t.Parallel()

	calls := 0
	pick := func(int) int {
		calls++
		require.Less(t, calls, 100, "pick called without progress")

		return 1 << 30
	}

	got := applySteerRule(
		weightedRule(map[string]float64{"a": 1, "b": 2}),
		[]map[string]any{steerAns("a"), steerAns("b")}, steerQuery{}, pick)
	assert.Len(t, got, 2)
}

func TestSteeredAnswersShufflesInput(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	b.pick = func(int) int { return 0 }
	p := &SteeringPolicy{Answers: []map[string]any{steerAns("a"), steerAns("b"), steerAns("c")}}

	assert.Equal(t, []string{"b", "c", "a"}, names(b.steeredAnswers(p, steerQuery{})))
}

func TestZoneArchiveBoundsRetainedRRSets(t *testing.T) {
	t.Parallel()

	z := &Zone{Records: map[string]*RRSet{}}
	for i := range maxArchivedRRSets / 4 {
		z.Records[string(rune('a'+i%26))+string(rune(i))] = &RRSet{Rdata: []string{"x"}}
	}

	for range 10 {
		z.archive()
		z.Version++
	}

	total := 0
	for _, r := range z.History {
		total += len(r.Records)
	}

	assert.LessOrEqual(t, total, maxArchivedRRSets)
	assert.NotEmpty(t, z.History)
	assert.Equal(t, z.Version-1, z.History[len(z.History)-1].Version)
}
