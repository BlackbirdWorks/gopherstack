package azuresearch

import (
	"sort"
	"strings"
)

const maxOrderClauses = 32

type orderClause struct {
	key  func(h hit) any
	desc bool
}

type orderBy struct{ clauses []orderClause }

func splitTopLevel(s string) []string {
	var (
		out   []string
		depth int
		cur   strings.Builder
		inStr bool
	)

	for _, r := range s {
		switch {
		case r == '\'':
			inStr = !inStr
		case !inStr && r == '(':
			depth++
		case !inStr && r == ')':
			depth--
		case !inStr && depth == 0 && r == ',':
			out = append(out, cur.String())
			cur.Reset()

			continue
		}

		cur.WriteRune(r)
	}

	return append(out, cur.String())
}

func compileOrderBy(expr string, c *compiled) (*orderBy, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil //nolint:nilnil // no ordering requested
	}

	parts := splitTopLevel(expr)
	if len(parts) > maxOrderClauses {
		return nil, badf("The $orderby parameter allows at most %d clauses.", maxOrderClauses)
	}

	ob := &orderBy{}

	for _, part := range parts {
		cl, err := orderClauseOf(strings.TrimSpace(part), c)
		if err != nil {
			return nil, err
		}

		ob.clauses = append(ob.clauses, cl)
	}

	return ob, nil
}

func orderClauseOf(part string, c *compiled) (orderClause, error) {
	desc := false

	switch {
	case strings.HasSuffix(part, " desc"):
		desc, part = true, strings.TrimSpace(strings.TrimSuffix(part, " desc"))
	case strings.HasSuffix(part, " asc"):
		part = strings.TrimSpace(strings.TrimSuffix(part, " asc"))
	}

	switch {
	case part == "search.score()":
		return orderClause{desc: desc, key: func(h hit) any { return h.score }}, nil
	case strings.HasPrefix(part, "geo.distance("):
		return geoOrder(part, desc, c)
	}

	fd, ok := c.byPath[part]
	if !ok || !fd.Sortable {
		return orderClause{}, badf("Invalid expression: The field '%s' cannot be used in $orderby because it is not sortable.", part)
	}

	segs := strings.Split(part, "/")

	return orderClause{desc: desc, key: func(h hit) any { return drill(h.doc.Fields, segs) }}, nil
}

func geoOrder(part string, desc bool, c *compiled) (orderClause, error) {
	fc := &filterCtx{comp: c}
	p := &fparser{ctx: fc, scope: map[string]*fieldDef{}}

	toks, err := tokenizeFilter(part)
	if err != nil {
		return orderClause{}, err
	}

	p.toks = toks

	op, err := p.parseOperand()
	if err != nil {
		return orderClause{}, err
	}

	return orderClause{desc: desc, key: func(h hit) any {
		return op(&fenv{d: h.doc, doc: h.doc.Fields, vars: map[string]any{}})
	}}, nil
}

func (o *orderBy) sortHits(hits []hit) {
	sort.SliceStable(hits, func(i, j int) bool {
		for _, cl := range o.clauses {
			a, b := cl.key(hits[i]), cl.key(hits[j])

			c := compareNullsFirst(a, b)
			if c == 0 {
				continue
			}

			if cl.desc {
				return c > 0
			}

			return c < 0
		}

		return false
	})
}

func compareNullsFirst(a, b any) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}

	c, _ := orderValues(a, b)

	return c
}

// validateSelect rejects unknown and non-retrievable fields.
func validateSelect(c *compiled, sel []string) error {
	for _, s := range sel {
		if s == "*" {
			continue
		}

		fd, ok := c.byPath[s]
		if !ok {
			return badf("Invalid expression: The field '%s' in the $select parameter is not present in the index.", s)
		}

		if !fd.Retrievable {
			return badf("Invalid expression: The field '%s' in the $select parameter is not retrievable.", s)
		}
	}

	return nil
}

// project builds the response document: retrievable fields only, in schema order, honouring $select.
func project(defs []*fieldDef, src map[string]any, sel []string, prefix string) *ordered {
	out := newOrdered()
	all := len(sel) == 0 || containsStr(sel, "*")

	for _, fd := range defs {
		if !fd.Retrievable && !fd.Key {
			continue
		}

		path := prefix + fd.Name
		if !all && !selected(sel, path) {
			continue
		}

		v, ok := src[fd.Name]
		if !ok {
			v = nil
		}

		if fd.Base == typeComplex && v != nil {
			v = projectComplex(fd, v, sel, all, path)
		}

		out.set(fd.Name, v)
	}

	return out
}

func projectComplex(fd *fieldDef, v any, sel []string, all bool, path string) any {
	sub := sel
	if !all && containsStr(sel, path) {
		sub = nil
	}

	switch t := v.(type) {
	case map[string]any:
		return project(fd.Children, t, sub, path+"/")
	case []any:
		out := make([]any, len(t))

		for i, el := range t {
			if m, ok := el.(map[string]any); ok {
				out[i] = project(fd.Children, m, sub, path+"/")
			}
		}

		return out
	}

	return v
}

func selected(sel []string, path string) bool {
	for _, s := range sel {
		if s == path || strings.HasPrefix(s, path+"/") || strings.HasPrefix(path, s+"/") {
			return true
		}
	}

	return false
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}

	return false
}
