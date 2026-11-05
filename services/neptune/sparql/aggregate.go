package sparql

import "strings"

func (ev *evaluator) aggregateValues(c exprCall, en *env) []Term {
	var vals []Term

	seen := map[Term]struct{}{}

	for _, row := range en.rows {
		t, err := ev.eval(c.Args[0], &env{b: row, gc: en.gc})
		if err != nil {
			continue
		}

		if c.Distinct {
			if _, dup := seen[t]; dup {
				continue
			}

			seen[t] = struct{}{}
		}

		vals = append(vals, t)
	}

	return vals
}

func (ev *evaluator) evalAggregate(c exprCall, en *env) (Term, error) {
	if !en.agg {
		return Term{}, errNoAggCtx
	}

	if c.Star {
		return ev.countRows(c, en), nil
	}

	if len(c.Args) != 1 {
		return Term{}, errType
	}

	vals := ev.aggregateValues(c, en)

	switch c.Name {
	case "COUNT":
		return intTerm(int64(len(vals))), nil
	case "SUM":
		return sumTerms(vals)
	case "AVG":
		return avgTerms(vals)
	case "MIN", "MAX":
		return extremum(vals, c.Name == "MAX")
	case "SAMPLE":
		if len(vals) == 0 {
			return Term{}, errUnbound
		}

		return vals[0], nil
	default:
		return groupConcat(vals, c.Sep)
	}
}

func (ev *evaluator) countRows(c exprCall, en *env) Term {
	if !c.Distinct {
		return intTerm(int64(len(en.rows)))
	}

	seen := map[string]struct{}{}
	for _, r := range en.rows {
		seen[bindingKey(r, nil)] = struct{}{}
	}

	return intTerm(int64(len(seen)))
}

func sumTerms(vals []Term) (Term, error) {
	acc := numVal{k: nInt}

	for _, v := range vals {
		n, ok := asNumeric(v)
		if !ok {
			return Term{}, errType
		}

		var err error
		if acc, err = numArith("+", acc, n); err != nil {
			return Term{}, err
		}
	}

	return acc.term(), nil
}

func avgTerms(vals []Term) (Term, error) {
	if len(vals) == 0 {
		return intTerm(0), nil
	}

	sum, err := sumTerms(vals)
	if err != nil {
		return Term{}, err
	}

	s, _ := asNumeric(sum)

	avg, err := numArith("/", s, numVal{k: nInt, i: int64(len(vals))})
	if err != nil {
		return Term{}, err
	}

	return avg.term(), nil
}

func extremum(vals []Term, wantMax bool) (Term, error) {
	if len(vals) == 0 {
		return Term{}, errUnbound
	}

	best := vals[0]

	for _, v := range vals[1:] {
		c := orderCompare(v, best)
		if (wantMax && c > 0) || (!wantMax && c < 0) {
			best = v
		}
	}

	return best, nil
}

func groupConcat(vals []Term, sep string) (Term, error) {
	parts := make([]string, 0, len(vals))
	lang, same := "", true

	for i, v := range vals {
		if v.Kind == KindBlank {
			return Term{}, errType
		}

		parts = append(parts, v.Value)

		if i == 0 {
			lang = v.Lang
		} else if v.Lang != lang {
			same = false
		}
	}

	out := strings.Join(parts, sep)
	if same && lang != "" && len(vals) > 0 {
		return LangLiteral(out, lang), nil
	}

	return Literal(out), nil
}

// bindingKey is a stable string identity for the given variables of b (all variables when vars is nil).
func bindingKey(b Binding, vars []string) string {
	if vars == nil {
		vars = sortedKeys(b)
	}

	var sb strings.Builder

	for _, v := range vars {
		sb.WriteString(v)
		sb.WriteByte('=')
		sb.WriteString(b[v].String())
		sb.WriteByte(0)
	}

	return sb.String()
}
