package sparql

import (
	"fmt"
	"strings"
)

// Explain renders the query's algebra as an indented tree.
func (q *Query) Explain() string {
	var sb strings.Builder

	forms := map[queryForm]string{
		formSelect:    "SELECT",
		formAsk:       "ASK",
		formConstruct: "CONSTRUCT",
		formDescribe:  "DESCRIBE",
	}
	fmt.Fprintf(&sb, "Form: %s\n", forms[q.Form])

	if len(q.From)+len(q.FromNamed) > 0 {
		fmt.Fprintf(&sb, "Dataset: FROM %d, FROM NAMED %d\n", len(q.From), len(q.FromNamed))
	} else {
		sb.WriteString("Dataset: union of all graphs\n")
	}

	if q.Form == formSelect {
		fmt.Fprintf(&sb, "Project: %s%s\n", map[bool]string{true: "DISTINCT ", false: ""}[q.Distinct], projText(q))
	}

	if len(q.GroupBy) > 0 {
		fmt.Fprintf(&sb, "GroupBy: %d expression(s)\n", len(q.GroupBy))
	}

	if len(q.Order) > 0 {
		fmt.Fprintf(&sb, "OrderBy: %d condition(s)\n", len(q.Order))
	}

	if q.Offset > 0 || q.Limit >= 0 {
		fmt.Fprintf(&sb, "Slice: offset=%d limit=%d\n", q.Offset, q.Limit)
	}

	sb.WriteString("Pattern:\n")
	explainGroup(&sb, q.Where, 1)

	return sb.String()
}

func projText(q *Query) string {
	if q.Star {
		return "*"
	}

	vars := make([]string, len(q.Proj))
	for i, p := range q.Proj {
		vars[i] = "?" + p.Var
	}

	return strings.Join(vars, " ")
}

func nodeText(n node) string {
	if n.isVar() {
		return "?" + n.Var
	}

	return n.Term.String()
}

func explainGroup(sb *strings.Builder, g *group, depth int) {
	pad := strings.Repeat("  ", depth)

	for _, el := range g.Elems {
		switch x := el.(type) {
		case *bgpEl:
			for _, tp := range x.Triples {
				p := nodeText(tp.P)
				if tp.Path != nil {
					p = "<path>"
				}

				fmt.Fprintf(sb, "%sTriplePattern(%s %s %s)\n", pad, nodeText(tp.S), p, nodeText(tp.O))
			}
		case *optionalEl:
			sb.WriteString(pad + "LeftJoin\n")
			explainGroup(sb, x.G, depth+1)
		case *unionEl:
			sb.WriteString(pad + "Union\n")

			for _, a := range x.Alts {
				explainGroup(sb, a, depth+1)
			}
		case *minusEl:
			sb.WriteString(pad + "Minus\n")
			explainGroup(sb, x.G, depth+1)
		case *filterEl:
			sb.WriteString(pad + "Filter\n")
		case *bindEl:
			fmt.Fprintf(sb, "%sExtend(?%s)\n", pad, x.Var)
		case *valuesEl:
			fmt.Fprintf(sb, "%sValues(%d row(s))\n", pad, len(x.V.Rows))
		case *graphEl:
			fmt.Fprintf(sb, "%sGraph(%s)\n", pad, nodeText(x.Name))
			explainGroup(sb, x.G, depth+1)
		case *subSelectEl:
			sb.WriteString(pad + "SubSelect\n")
			explainGroup(sb, x.Q.Where, depth+1)
		case *groupEl:
			sb.WriteString(pad + "Group\n")
			explainGroup(sb, x.G, depth+1)
		}
	}
}

// ProjectedVars lists the variables a SELECT produces.
func (q *Query) ProjectedVars() []string { return projectedVars(q) }

// SetDataset overrides the query's FROM / FROM NAMED clauses (SPARQL protocol default-graph-uri / named-graph-uri).
func (q *Query) SetDataset(from, named []Term) { q.From, q.FromNamed = from, named }

// SetUsing applies the protocol's using-graph-uri / using-named-graph-uri to update operations lacking USING.
func (u *Update) SetUsing(using, named []Term) {
	for i := range u.ops {
		op := &u.ops[i]
		if op.Kind == upModify && len(op.Using)+len(op.UsingNamed) == 0 && !op.With.Bound() {
			op.Using, op.UsingNamed = using, named
		}
	}
}

// IsAsk reports whether the query is an ASK.
func (q *Query) IsAsk() bool { return q.Form == formAsk }

// IsSelect reports whether the query is a SELECT.
func (q *Query) IsSelect() bool { return q.Form == formSelect }
