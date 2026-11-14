package sparql

import (
	"slices"
	"strconv"
	"strings"
)

// HintNS is the Neptune query-hint vocabulary namespace.
const HintNS = "http://aws.amazon.com/neptune/vocab/v01/QueryHints#"

// Documented DESCRIBE modes and limits (sparql-query-hints-for-describe.html).
const (
	DescribeForwardOneStep   = "ForwardOneStep"
	DescribeSymmetricOneStep = "SymmetricOneStep"
	DescribeCBD              = "CBD"
	DescribeSCBD             = "SCBD"

	defaultIterationLimit = 5
	defaultStatementLimit = 5000
)

// Hints are the Neptune query hints found in a request.
type Hints struct {
	QueryID      string
	DescribeMode string
	TimeoutMs    int64
	IterLimit    int
	StmtLimit    int
	HasTimeout   bool
	iterSet      bool
	stmtSet      bool
}

func (h *Hints) iterationLimit() int {
	if h.iterSet {
		return h.IterLimit
	}

	return defaultIterationLimit
}

func (h *Hints) statementLimit() int {
	if h.stmtSet {
		return h.StmtLimit
	}

	return defaultStatementLimit
}

func hintTerm(n node) (string, bool) {
	if n.isVar() || n.Term.Kind != KindIRI || !strings.HasPrefix(n.Term.Value, HintNS) {
		return "", false
	}

	return strings.TrimPrefix(n.Term.Value, HintNS), true
}

const hintScopeQuery = "Query"

func knownHint(scope, name string) bool {
	switch name {
	case "joinOrder":
		return scope == hintScopeQuery || scope == "Group"
	case "rangeSafe":
		return scope == hintScopeQuery || scope == "Prior"
	case "evaluationStrategy":
		return scope == "SubQuery"
	case "queryTimeout", "queryId", "useDFE", "describeMode", "describeIterationLimit", "describeStatementLimit":
		return scope == hintScopeQuery
	default:
		return false
	}
}

// takeHints removes the query-hint triple patterns from tps, recording them on the parser.
func (p *parser) takeHints(tps []triplePat) ([]triplePat, error) {
	kept := tps[:0]

	for _, tp := range tps {
		scope, sok := hintTerm(tp.S)
		name, nok := hintTerm(tp.P)

		if !sok || !nok || tp.Path != nil || !knownHint(scope, name) {
			kept = append(kept, tp)

			continue
		}

		if err := p.applyHint(name, tp.O); err != nil {
			return nil, err
		}
	}

	return kept, nil
}

func hintValue(n node) (string, bool) {
	if n.isVar() || n.Term.Kind != KindLiteral {
		return "", false
	}

	return n.Term.Value, true
}

func (p *parser) applyHint(name string, o node) error {
	val, ok := hintValue(o)
	if !ok {
		return syntaxf("Malformed query: Query hint '%s' requires a literal value", name)
	}

	h := &p.hints

	switch name {
	case "queryTimeout":
		return h.setTimeout(val)
	case "queryId":
		if h.QueryID != "" {
			return syntaxf("Malformed query: Query hint 'queryId' must be set only once")
		}

		h.QueryID = val
	case "describeMode":
		return h.setDescribeMode(val)
	case "describeIterationLimit":
		n, err := nonNegative(name, val)
		h.IterLimit, h.iterSet = n, true

		return err
	case "describeStatementLimit":
		n, err := nonNegative(name, val)
		h.StmtLimit, h.stmtSet = n, true

		return err
	case "joinOrder":
		return requireOneOf(name, val, "Ordered")
	case "evaluationStrategy":
		return requireOneOf(name, val, "BottomUp")
	default:
		return requireOneOf(name, strings.ToLower(val), "true", "false")
	}

	return nil
}

func requireOneOf(name, val string, allowed ...string) error {
	if slices.Contains(allowed, val) {
		return nil
	}

	return syntaxf("Malformed query: Query hint '%s' has invalid value '%s'", name, val)
}

func nonNegative(name, val string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil || n < 0 {
		return 0, syntaxf("Malformed query: Query hint '%s' must be a non-negative integer", name)
	}

	return n, nil
}

func (h *Hints) setTimeout(val string) error {
	if h.HasTimeout {
		return syntaxf("Malformed query: Query hint 'queryTimeout' must be set only once")
	}

	n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
	if err != nil || n <= 0 {
		return syntaxf("Malformed query: Query hint 'queryTimeout' must be a positive integer number of milliseconds")
	}

	h.TimeoutMs, h.HasTimeout = n, true

	return nil
}

func (h *Hints) setDescribeMode(val string) error {
	for _, m := range []string{DescribeForwardOneStep, DescribeSymmetricOneStep, DescribeCBD, DescribeSCBD} {
		if val == m {
			h.DescribeMode = m

			return nil
		}
	}

	return syntaxf("Malformed query: Query hint 'describeMode' has invalid value '%s'", val)
}
