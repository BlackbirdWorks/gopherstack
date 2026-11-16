package ocidns

import (
	"cmp"
	"context"
	"crypto/rand"
	"math"
	"math/big"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxSteerExprLen  = 1024
	maxSteerListLen  = 64
	maxSteerAnswers  = 1000
	steerNoValue     = math.MaxInt
	steerRuleFilter  = "FILTER"
	steerRulePrio    = "PRIORITY"
	steerRuleWeight  = "WEIGHTED"
	steerRuleLimit   = "LIMIT"
	steerOpEq        = "=="
	steerOpNe        = "!="
	steerOpIn        = "in"
	steerAttrAddress = "query.client.address"
	steerOpLen       = 2
)

// steerQuery is what a steering rule can see of the query.
type steerQuery struct {
	client netip.Addr
}

func randIntN(n int) int {
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}

	return int(v.Int64())
}

// steeredAnswers runs the policy's rules over its answers for one query.
func (b *InMemoryBackend) steeredAnswers(p *SteeringPolicy, q steerQuery) []map[string]any {
	answers := make([]map[string]any, 0, min(len(p.Answers), maxSteerAnswers))
	for _, a := range p.Answers {
		if len(answers) == maxSteerAnswers {
			break
		}

		answers = append(answers, a)
	}

	pick := b.pick
	if pick == nil {
		pick = randIntN
	}

	for _, r := range p.Rules {
		answers = applySteerRule(r, answers, q, pick)
	}

	return answers
}

func applySteerRule(rule map[string]any, answers []map[string]any, q steerQuery, pick func(int) int) []map[string]any {
	kind := strings.ToUpper(strField(rule, "ruleType"))
	if kind == "HEALTH" {
		logger.Load(context.Background()).Debug("ocidns: would drop unhealthy answers using the health check monitor")

		return answers
	}

	cases, hasCases := rule["cases"].([]any)
	var chosen map[string]any

	if hasCases {
		for _, c := range cases {
			cm, _ := c.(map[string]any)
			if cm == nil {
				continue
			}

			if cond := strField(cm, "caseCondition"); cond == "" || evalSteerExpr(cond, nil, q) {
				chosen = cm

				break
			}
		}

		if chosen == nil {
			return answers
		}
	}

	switch kind {
	case steerRuleLimit:
		return steerLimit(rule, chosen, answers)
	case steerRuleFilter:
		return steerFilter(rule, chosen, answers, q)
	case steerRulePrio:
		return steerPriority(rule, chosen, answers, q)
	case steerRuleWeight:
		return steerWeighted(rule, chosen, answers, q, pick)
	}

	return answers
}

func steerInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		if n != math.Trunc(n) || n < 0 || n > math.MaxInt32 {
			return 0, false
		}

		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	}

	return 0, false
}

func steerLimit(rule, chosen map[string]any, answers []map[string]any) []map[string]any {
	src := rule["defaultCount"]
	if chosen != nil && chosen["count"] != nil {
		src = chosen["count"]
	}

	n, ok := steerInt(src)
	if !ok || n >= len(answers) {
		return answers
	}

	return answers[:n]
}

// steerData returns the first answerData entry that applies to the answer: the case's, then the rule default.
func steerData(rule, chosen map[string]any, defaultKey string, a map[string]any, q steerQuery) map[string]any {
	lists := []any{}
	if chosen != nil {
		lists = append(lists, chosen["answerData"])
	}

	lists = append(lists, rule[defaultKey])

	for _, l := range lists {
		items, _ := l.([]any)
		for _, it := range items {
			m, _ := it.(map[string]any)
			if m == nil {
				continue
			}

			if cond := strField(m, "answerCondition"); cond == "" || evalSteerExpr(cond, a, q) {
				return m
			}
		}
	}

	return nil
}

func steerFilter(rule, chosen map[string]any, answers []map[string]any, q steerQuery) []map[string]any {
	configured := steerHasData(rule["defaultAnswerData"])
	if chosen != nil {
		configured = configured || steerHasData(chosen["answerData"])
	}

	if !configured {
		return answers
	}

	out := make([]map[string]any, 0, len(answers))

	for _, a := range answers {
		d := steerData(rule, chosen, "defaultAnswerData", a, q)
		if keep, _ := d["shouldKeep"].(bool); keep {
			out = append(out, a)
		}
	}

	return out
}

func steerHasData(v any) bool {
	l, _ := v.([]any)

	return len(l) > 0
}

func steerPriority(rule, chosen map[string]any, answers []map[string]any, q steerQuery) []map[string]any {
	type ranked struct {
		a map[string]any
		v int
	}

	rs := make([]ranked, 0, len(answers))

	for _, a := range answers {
		v := steerNoValue
		if n, ok := steerInt(steerData(rule, chosen, "defaultAnswerData", a, q)["value"]); ok {
			v = n
		}

		rs = append(rs, ranked{a: a, v: v})
	}

	slices.SortStableFunc(rs, func(x, y ranked) int { return cmp.Compare(x.v, y.v) })

	out := make([]map[string]any, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.a)
	}

	return out
}

func steerWeighted(
	rule, chosen map[string]any, answers []map[string]any, q steerQuery, pick func(int) int,
) []map[string]any {
	type item struct {
		a map[string]any
		w int
	}

	pool := make([]item, 0, len(answers))
	zero := make([]map[string]any, 0)

	for _, a := range answers {
		w, ok := steerInt(steerData(rule, chosen, "defaultAnswerData", a, q)["value"])
		if !ok || w <= 0 {
			zero = append(zero, a)

			continue
		}

		pool = append(pool, item{a: a, w: w})
	}

	out := make([]map[string]any, 0, len(answers))

	for len(pool) > 0 {
		total := 0
		for _, it := range pool {
			total += it.w
		}

		r := pick(total)
		for i, it := range pool {
			if r < it.w {
				out = append(out, it.a)
				pool = slices.Delete(pool, i, i+1)

				break
			}

			r -= it.w
		}
	}

	return append(out, zero...)
}
