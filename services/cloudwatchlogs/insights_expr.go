package cloudwatchlogs

import (
	"errors"
	"fmt"
	"regexp"
)

// ErrParseExpr indicates a malformed Insights expression.
var ErrParseExpr = errors.New("parse expression")

// exprNode is a node in a parsed Insights expression tree.
type exprNode interface {
	eval(rec *insightsRecord) cwValue
}

// litNode is a constant literal.
type litNode struct {
	v cwValue
}

func (n *litNode) eval(_ *insightsRecord) cwValue { return n.v }

// fieldNode references a record field by name.
type fieldNode struct {
	name string
}

func (n *fieldNode) eval(rec *insightsRecord) cwValue { return rec.resolve(n.name) }

// arithNode is a binary arithmetic operation.
type arithNode struct {
	left  exprNode
	right exprNode
	op    byte
}

func (n *arithNode) eval(rec *insightsRecord) cwValue {
	l := n.left.eval(rec)
	r := n.right.eval(rec)

	switch n.op {
	case '+':
		return numFromFloat(l.num + r.num)
	case '-':
		return numFromFloat(l.num - r.num)
	case '*':
		return numFromFloat(l.num * r.num)
	case '/':
		if r.num == 0 {
			return numFromFloat(0)
		}

		return numFromFloat(l.num / r.num)
	case '%':
		if r.num == 0 {
			return numFromFloat(0)
		}

		return numFromFloat(float64(int64(l.num) % int64(r.num)))
	default:
		return cwValue{}
	}
}

// negNode is unary numeric negation.
type negNode struct {
	inner exprNode
}

func (n *negNode) eval(rec *insightsRecord) cwValue {
	return numFromFloat(-n.inner.eval(rec).num)
}

// cmpNode is a comparison producing a boolean value.
type cmpNode struct {
	left  exprNode
	right exprNode
	op    string
}

func (n *cmpNode) eval(rec *insightsRecord) cwValue {
	l := n.left.eval(rec)
	r := n.right.eval(rec)

	return boolValue(evalCompare(n.op, l, r))
}

// evalCompare applies a comparison operator to two values.
func evalCompare(op string, l, r cwValue) bool {
	switch op {
	case "=", "==":
		return valuesEqual(l, r)
	case "!=", "<>":
		return !valuesEqual(l, r)
	case "<":
		return compareValues(l, r) < 0
	case "<=":
		return compareValues(l, r) <= 0
	case ">":
		return compareValues(l, r) > 0
	case ">=":
		return compareValues(l, r) >= 0
	default:
		return false
	}
}

// logicNode is a boolean AND/OR.
type logicNode struct {
	left  exprNode
	right exprNode
	isOr  bool
}

func (n *logicNode) eval(rec *insightsRecord) cwValue {
	if n.isOr {
		return boolValue(n.left.eval(rec).truthy() || n.right.eval(rec).truthy())
	}

	return boolValue(n.left.eval(rec).truthy() && n.right.eval(rec).truthy())
}

// notNode is a boolean negation.
type notNode struct {
	inner exprNode
}

func (n *notNode) eval(rec *insightsRecord) cwValue {
	return boolValue(!n.inner.eval(rec).truthy())
}

// likeNode implements like / not like against a regex.
type likeNode struct {
	left   exprNode
	re     *regexp.Regexp
	negate bool
}

func (n *likeNode) eval(rec *insightsRecord) cwValue {
	match := n.re.MatchString(n.left.eval(rec).String())
	if n.negate {
		match = !match
	}

	return boolValue(match)
}

// inNode implements membership: expr in (v1, v2, ...).
type inNode struct {
	left   exprNode
	values []exprNode
	negate bool
}

func (n *inNode) eval(rec *insightsRecord) cwValue {
	l := n.left.eval(rec)
	found := false
	for _, ve := range n.values {
		if valuesEqual(l, ve.eval(rec)) {
			found = true

			break
		}
	}

	if n.negate {
		found = !found
	}

	return boolValue(found)
}

// boolValue maps a Go bool to a numeric cwValue (1/0).
func boolValue(b bool) cwValue {
	if b {
		return numFromFloat(1)
	}

	return numFromFloat(0)
}

// parseExpression parses a full boolean/arithmetic expression.
func parseExpression(s string) (exprNode, error) {
	toks, err := tokenizeExpr(s)
	if err != nil {
		return nil, err
	}

	p := &exprParser{toks: toks, pos: 0}

	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.pos != len(p.toks) {
		return nil, fmt.Errorf("%w: unexpected token %q", ErrParseExpr, p.toks[p.pos].text)
	}

	return node, nil
}
