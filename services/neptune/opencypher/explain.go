package opencypher

import (
	"fmt"
	"strings"
)

// Explain renders the operator pipeline of src as Neptune's explain table (mode is static, dynamic or details).
func Explain(src, mode string) (string, error) {
	switch mode {
	case "static", "dynamic", "details":
	default:
		return "", &Error{Code: CodeInvalidParameter, Detail: "explain mode must be one of static, dynamic or details"}
	}

	q, err := parse(src)
	if err != nil {
		return "", err
	}

	var sb strings.Builder

	sb.WriteString("Query:\n" + strings.TrimSpace(src) + "\n\n")
	fmt.Fprintf(&sb, "%-4s %-10s %-22s %s\n", "ID", "Out #1", "Name", "Arguments")

	for i, c := range q.clauses {
		out := "-"
		if i+1 < len(q.clauses) {
			out = fmt.Sprintf("#%d", i+1)
		}

		name, args := describeClause(c)
		fmt.Fprintf(&sb, "%-4d %-10s %-22s %s\n", i, out, name, args)
	}

	if mode == "details" {
		fmt.Fprintf(&sb, "\nWrite query: %t\nClauses: %d\n", q.write, len(q.clauses))
	}

	return sb.String(), nil
}

func describeClause(c clause) (string, string) {
	switch c := c.(type) {
	case matchClause:
		name := "PatternMatch"
		if c.optional {
			name = "OptionalPatternMatch"
		}

		return name, fmt.Sprintf("patterns=%d filter=%t", len(c.pats), c.where != nil)
	case createClause:
		return "Create", fmt.Sprintf("patterns=%d", len(c.pats))
	case mergeClause:
		return "Merge", fmt.Sprintf("onCreate=%d onMatch=%d", len(c.onCreate), len(c.onMatch))
	case setClause:
		return "Set", fmt.Sprintf("items=%d", len(c.items))
	case removeClause:
		return "Remove", fmt.Sprintf("items=%d", len(c.items))
	case deleteClause:
		return "Delete", fmt.Sprintf("exprs=%d detach=%t", len(c.exprs), c.detach)
	case withClause:
		return "Project", describeProjection(c.proj)
	case unwindClause:
		return "Unwind", "variable=" + c.v
	case returnClause:
		return "Return", describeProjection(c.proj)
	default:
		return fmt.Sprintf("%T", c), ""
	}
}

func describeProjection(p projection) string {
	return fmt.Sprintf("columns=%d distinct=%t order=%d skip=%t limit=%t",
		len(p.items), p.distinct, len(p.order), p.skip != nil, p.limit != nil)
}
