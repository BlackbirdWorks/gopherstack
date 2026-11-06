package athena

import (
	"strings"
)

const (
	writeDelete writeKind = iota + 100
	writeUpdate
	writeMerge
	writeOptimize
)

type mergeAction int

const (
	mergeDelete mergeAction = iota + 1
	mergeUpdate
	mergeInsert
)

type dmlAssign struct {
	col  string
	expr string
}

type mergeClause struct {
	cond    string
	insCols []string
	insVals []string
	sets    []dmlAssign
	action  mergeAction
	matched bool
}

// dmlSpec is a parsed DELETE, UPDATE, MERGE or OPTIMIZE; the SELECT that feeds it is built once the table's columns are known.
type dmlSpec struct {
	targetText  string
	targetAlias string
	where       string
	source      string
	on          string
	sets        []dmlAssign
	clauses     []mergeClause
}

// topIndex returns the index of the first token at or after from that is one
// of words outside parentheses and CASE blocks, or len(toks).
func topIndex(toks []token, from int, words ...string) int {
	depth, cases := 0, 0

	for i := from; i < len(toks); i++ {
		t := toks[i]

		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case t.isWord("case"):
			cases++
		case t.isWord("end") && cases > 0:
			cases--
		case depth == 0 && cases == 0 && t.kind == tkWord:
			for _, w := range words {
				if t.isWord(w) {
					return i
				}
			}
		}
	}

	return len(toks)
}

func topCommas(toks []token, from, to int) []int {
	var out []int

	depth, cases := 0, 0

	for i := from; i < to; i++ {
		switch t := toks[i]; {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case t.isWord("case"):
			cases++
		case t.isWord("end") && cases > 0:
			cases--
		case depth == 0 && cases == 0 && t.isPunct(","):
			out = append(out, i)
		}
	}

	return out
}

// splitAssigns reads `col = expr, ...` between significant indexes from and to.
func (c *stmtCursor) splitAssigns(from, to int) ([]dmlAssign, bool) {
	var out []dmlAssign

	start := from

	for _, comma := range append(topCommas(c.toks, from, to), to) {
		if comma-start < argsThree || !c.toks[start+1].isPunct("=") ||
			(c.toks[start].kind != tkWord && c.toks[start].kind != tkQuoted) {
			return nil, false
		}

		out = append(out, dmlAssign{col: c.toks[start].ident(), expr: c.text(start+2, comma)})
		start = comma + 1
	}

	return out, len(out) > 0
}

func (c *stmtCursor) optionalAlias(stop ...string) string {
	c.acceptWords("as")

	if c.done() || (c.toks[c.pos].kind != tkWord && c.toks[c.pos].kind != tkQuoted) {
		return ""
	}

	for _, w := range stop {
		if c.toks[c.pos].isWord(w) {
			return ""
		}
	}

	a := c.toks[c.pos].ident()
	c.pos++

	return a
}

func (c *stmtCursor) targetName() ([]string, string) {
	start := c.pos

	parts := c.qualifiedName()
	if parts == nil {
		return nil, ""
	}

	return parts, c.text(start, c.pos)
}

func (c *stmtCursor) parseDelete() (*writeStmt, bool) {
	parts, text := c.targetName()
	if parts == nil {
		return nil, false
	}

	w := &writeStmt{kind: writeDelete, target: parts, dml: &dmlSpec{targetText: text}}

	if c.acceptWords("where") {
		w.dml.where = c.text(c.pos, len(c.toks))
		c.pos = len(c.toks)
	}

	return w, c.done()
}

func (c *stmtCursor) parseUpdate() (*writeStmt, bool) {
	parts, text := c.targetName()
	if parts == nil || !c.acceptWords("set") {
		return nil, false
	}

	end := topIndex(c.toks, c.pos, "where")

	sets, ok := c.splitAssigns(c.pos, end)
	if !ok {
		return nil, false
	}

	w := &writeStmt{kind: writeUpdate, target: parts, dml: &dmlSpec{targetText: text, sets: sets}}
	c.pos = end

	if c.acceptWords("where") {
		w.dml.where = c.text(c.pos, len(c.toks))
		c.pos = len(c.toks)
	}

	return w, c.done()
}

func (c *stmtCursor) parseMerge() (*writeStmt, bool) {
	parts, text := c.targetName()
	if parts == nil {
		return nil, false
	}

	d := &dmlSpec{targetText: text}
	d.targetAlias = c.optionalAlias("using")

	if !c.acceptWords("using") {
		return nil, false
	}

	srcStart := c.pos

	if c.done() {
		return nil, false
	}

	if _, ok := c.group(); !ok && c.qualifiedName() == nil {
		return nil, false
	}

	srcEnd := c.pos
	alias := c.optionalAlias("on")
	d.source = c.text(srcStart, srcEnd)

	if alias != "" {
		d.source += " AS " + quoteIdent(alias)
	}

	if !c.acceptWords("on") {
		return nil, false
	}

	onEnd := topIndex(c.toks, c.pos, "when")
	d.on = c.text(c.pos, onEnd)
	c.pos = onEnd

	for c.acceptWords("when") {
		mc, ok := c.parseMergeClause()
		if !ok {
			return nil, false
		}

		d.clauses = append(d.clauses, mc)
	}

	return &writeStmt{kind: writeMerge, target: parts, dml: d}, len(d.clauses) > 0 && c.done()
}

func (c *stmtCursor) parseMergeClause() (mergeClause, bool) {
	var mc mergeClause

	switch {
	case c.acceptWords("not", "matched"):
	case c.acceptWords("matched"):
		mc.matched = true
	default:
		return mc, false
	}

	if c.acceptWords("and") {
		end := topIndex(c.toks, c.pos, "then")
		mc.cond = c.text(c.pos, end)
		c.pos = end
	}

	if !c.acceptWords("then") {
		return mc, false
	}

	end := topIndex(c.toks, c.pos, "when")

	switch {
	case mc.matched && c.acceptWords("delete"):
		mc.action = mergeDelete
	case mc.matched && c.acceptWords("update", "set"):
		sets, ok := c.splitAssigns(c.pos, end)
		if !ok {
			return mc, false
		}

		mc.action, mc.sets, c.pos = mergeUpdate, sets, end
	case !mc.matched && c.acceptWords("insert"):
		return c.parseMergeInsert(mc, end)
	default:
		return mc, false
	}

	return mc, c.pos == end
}

func (c *stmtCursor) parseMergeInsert(mc mergeClause, end int) (mergeClause, bool) {
	mc.action = mergeInsert

	if !c.done() && c.toks[c.pos].isPunct("(") {
		inner, ok := c.group()
		if !ok {
			return mc, false
		}

		for _, part := range splitCommas(inner) {
			sig := significant(part)
			if len(sig) != 1 {
				return mc, false
			}

			mc.insCols = append(mc.insCols, sig[0].ident())
		}
	}

	if !c.acceptWords("values") || c.done() {
		return mc, false
	}


	inner, ok := c.group()
	if !ok {
		return mc, false
	}

	for _, part := range splitCommas(inner) {
		mc.insVals = append(mc.insVals, strings.TrimSpace(joinTokens(part)))
	}

	return mc, c.pos == end
}

func (c *stmtCursor) parseOptimize() (*writeStmt, bool) {
	parts, text := c.targetName()
	if parts == nil || !c.acceptWords("rewrite", "data", "using", "bin_pack") {
		return nil, false
	}

	w := &writeStmt{kind: writeOptimize, target: parts, dml: &dmlSpec{targetText: text}}

	if c.acceptWords("where") {
		w.dml.where = c.text(c.pos, len(c.toks))
		c.pos = len(c.toks)
	}

	return w, c.done()
}
