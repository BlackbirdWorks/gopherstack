package cloudtrail

import (
	"regexp"
	"strings"
)

// whereExpr is a WHERE-clause boolean expression node, evaluated against a
// single flattened event row (see eventToRow). Each node type below covers
// exactly one SQL construct (AND/OR/NOT/comparison/LIKE/IN) and stays small
// on purpose, keeping the WHERE evaluator's cyclomatic complexity low
// without needing a suppression.
type whereExpr interface {
	eval(row map[string]string) bool
}

type andNode struct{ left, right whereExpr }

func (n andNode) eval(row map[string]string) bool { return n.left.eval(row) && n.right.eval(row) }

type orNode struct{ left, right whereExpr }

func (n orNode) eval(row map[string]string) bool { return n.left.eval(row) || n.right.eval(row) }

type notNode struct{ inner whereExpr }

func (n notNode) eval(row map[string]string) bool { return !n.inner.eval(row) }

// cmpNode is a "<col> [!]= <value>" equality/inequality comparison.
type cmpNode struct {
	column string
	value  string
	negate bool
}

func (n cmpNode) eval(row map[string]string) bool {
	return (row[n.column] == n.value) != n.negate
}

// inNode is a "<col> [NOT] IN (<value>, ...)" membership test.
type inNode struct {
	values map[string]struct{}
	column string
	negate bool
}

func (n inNode) eval(row map[string]string) bool {
	_, ok := n.values[row[n.column]]

	return ok != n.negate
}

// likeNode is a "<col> [NOT] LIKE '<pattern>'" match. CloudTrail Lake is
// Trino-derived -- "CloudTrail Lake supports all valid Trino SQL SELECT
// statements, functions, and operators" and LIKE is one of the documented
// supported condition operators (docs.aws.amazon.com/awscloudtrail/latest/
// userguide/query-limitations.html#query-aggregates-condition-operators).
// Trino's LIKE is case-sensitive, with "%" matching zero or more characters
// and "_" matching exactly one (trino.io/docs/current/functions/
// comparison.html), which is also standard SQL LIKE semantics.
type likeNode struct {
	pattern *regexp.Regexp
	column  string
	negate  bool
}

func (n likeNode) eval(row map[string]string) bool {
	return n.pattern.MatchString(row[n.column]) != n.negate
}

// likePatternToRegexp compiles a SQL LIKE pattern into an equivalent
// case-sensitive, fully-anchored regexp ("%" -> ".*", "_" -> ".", everything
// else literal).
func likePatternToRegexp(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder

	b.WriteString("^")

	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	b.WriteString("$")

	return regexp.Compile(b.String())
}
