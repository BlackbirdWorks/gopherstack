package cloudwatchlogs

import (
	"fmt"
	"sort"
	"strings"
)

// fieldExpr is one term of a fields/display command: an expression plus the
// output column name it materialises into.
type fieldExpr struct {
	node  exprNode
	alias string
}

// parseFieldsCommand parses "fields expr [as alias], ...". Plain field references
// pass through unchanged; arithmetic/function expressions are materialised into
// their alias.
func parseFieldsCommand(q *insightsQuery, rest string) error {
	var exprs []fieldExpr
	var outputs []string

	for _, term := range splitTopLevelCommas(rest) {
		if term == "" {
			continue
		}

		exprText, alias := splitAlias(term)

		node, err := parseExpression(exprText)
		if err != nil {
			return fmt.Errorf("invalid fields expression %q: %w", term, err)
		}

		if alias == "" {
			alias = strings.TrimSpace(exprText)
		}

		exprs = append(exprs, fieldExpr{node: node, alias: alias})
		outputs = append(outputs, alias)
	}

	if len(exprs) == 0 {
		return nil
	}

	q.stages = append(q.stages, &fieldsStage{exprs: exprs})
	q.outputFields = outputs

	return nil
}

// splitAlias splits "expr as name" into its expression and alias parts. When no
// "as" clause is present the alias is empty.
func splitAlias(term string) (string, string) {
	lower := strings.ToLower(term)
	idx := indexTopLevelWord(lower, "as")
	if idx < 0 {
		return strings.TrimSpace(term), ""
	}

	expr := strings.TrimSpace(term[:idx])
	alias := strings.TrimSpace(term[idx+len(" as "):])
	alias = strings.Trim(alias, "`\"'")

	return expr, alias
}

// indexTopLevelWord finds " word " at paren/quote depth zero, returning the index
// of the leading space, or -1 when not found.
func indexTopLevelWord(s, word string) int {
	target := " " + word + " "
	depth := 0
	inStr := byte(0)

	for i := 0; i+len(target) <= len(s); i++ {
		ch := s[i]
		switch {
		case inStr != 0:
			if ch == inStr {
				inStr = 0
			}
		case ch == '"' || ch == '\'' || ch == '/':
			inStr = ch
		case ch == '(' || ch == '[':
			depth++
		case ch == ')' || ch == ']':
			depth--
		case depth == 0 && s[i:i+len(target)] == target:
			return i
		}
	}

	return -1
}

// fieldsStage evaluates each field expression and materialises the result under
// its alias for downstream stages and projection.
type fieldsStage struct {
	exprs []fieldExpr
}

func (s *fieldsStage) apply(recs []*insightsRecord) []*insightsRecord {
	for _, rec := range recs {
		for _, fe := range s.exprs {
			// Skip pure pass-through of built-ins/JSON so resolve() stays lazy;
			// only materialise computed values (those whose alias differs from a
			// direct field name, or that are arithmetic/function nodes).
			if fn, isField := fe.node.(*fieldNode); isField && fe.alias == fn.name {
				continue
			}

			rec.setField(fe.alias, fe.node.eval(rec))
		}
	}

	return recs
}

// parseFilterCommand parses "filter <boolean-expression>".
func parseFilterCommand(q *insightsQuery, rest string) error {
	node, err := parseExpression(rest)
	if err != nil {
		return fmt.Errorf("invalid filter expression %q: %w", rest, err)
	}

	q.stages = append(q.stages, &filterStage{node: node})

	return nil
}

// filterStage retains records for which the boolean expression is truthy.
type filterStage struct {
	node exprNode
}

func (s *filterStage) apply(recs []*insightsRecord) []*insightsRecord {
	out := make([]*insightsRecord, 0, len(recs))
	for _, rec := range recs {
		if s.node.eval(rec).truthy() {
			out = append(out, rec)
		}
	}

	return out
}

// sortKey is one component of a multi-key sort.
type sortKey struct {
	field string
	desc  bool
}

// parseSortCommand parses "sort f1 [asc|desc], f2 [asc|desc], ...".
func parseSortCommand(q *insightsQuery, rest string) error {
	var keys []sortKey

	for _, term := range splitTopLevelCommas(rest) {
		fields := strings.Fields(term)
		if len(fields) == 0 {
			continue
		}

		key := sortKey{field: fields[0], desc: false}
		if len(fields) >= sortDirectionParts && strings.EqualFold(fields[1], "desc") {
			key.desc = true
		}

		keys = append(keys, key)
	}

	if len(keys) == 0 {
		return nil
	}

	q.stages = append(q.stages, &sortStage{keys: keys})

	return nil
}

// sortDirectionParts is the token count of a "field DIR" sort term.
const sortDirectionParts = 2

// sortStage performs a stable multi-key sort over the record set.
type sortStage struct {
	keys []sortKey
}

func (s *sortStage) apply(recs []*insightsRecord) []*insightsRecord {
	sort.SliceStable(recs, func(i, j int) bool {
		for _, k := range s.keys {
			cmp := compareValues(recs[i].resolve(k.field), recs[j].resolve(k.field))
			if cmp == 0 {
				continue
			}

			if k.desc {
				return cmp > 0
			}

			return cmp < 0
		}

		return false
	})

	return recs
}
