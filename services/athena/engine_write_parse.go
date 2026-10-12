package athena

import (
	"strings"
)

type writeKind int

const (
	writeCTAS writeKind = iota + 1
	writeInsert
	writeUnload
)

// writeStmt is a parsed CTAS, INSERT INTO or UNLOAD statement.
type writeStmt struct {
	props       map[string][]string
	location    string
	body        string
	target      []string
	insertCols  []string
	kind        writeKind
	ifNotExists bool
	noData      bool
}

// stmtCursor walks a statement's significant tokens while keeping the
// original tokens (with whitespace) so a sub-query can be re-joined.
type stmtCursor struct {
	full []token
	idx  []int
	sigCursor
}

func newStmtCursor(sql string) (*stmtCursor, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}

	c := &stmtCursor{full: toks}

	for i, t := range toks {
		if t.kind != tkSpace {
			c.idx = append(c.idx, i)
			c.toks = append(c.toks, t)
		}
	}

	return c, nil
}

// text joins the original tokens from significant index from up to (excluding) to.
func (c *stmtCursor) text(from, to int) string {
	if from >= to || from >= len(c.idx) {
		return ""
	}

	end := len(c.full)
	if to < len(c.idx) {
		end = c.idx[to]
	}

	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(joinTokens(c.full[c.idx[from]:end])), ";"))
}

// parseWriteStmt recognises CREATE TABLE ... AS, INSERT INTO ... and UNLOAD;
// ok=false means the statement is something else.
func parseWriteStmt(sql string) (*writeStmt, bool) {
	c, err := newStmtCursor(sql)
	if err != nil {
		return nil, false
	}

	switch {
	case c.acceptWords("insert", "into"):
		return c.parseInsert()
	case c.acceptWords("unload"):
		return c.parseUnload()
	case c.acceptWords("create"):
		c.acceptWords("external")

		if !c.acceptWords("table") {
			return nil, false
		}

		return c.parseCTAS()
	default:
		return nil, false
	}
}

func (c *stmtCursor) parseCTAS() (*writeStmt, bool) {
	w := &writeStmt{kind: writeCTAS, props: map[string][]string{}}
	w.ifNotExists = c.acceptWords("if", "not", "exists")

	if w.target = c.qualifiedName(); w.target == nil {
		return nil, false
	}

	if c.acceptWords("with") {
		inner, ok := c.group()
		if !ok {
			return nil, false
		}

		w.props = parseWriteProps(inner)
	}

	if !c.acceptWords("as") {
		return nil, false
	}

	end := len(c.toks)

	switch {
	case endsWithWords(c.toks, "with", "no", "data"):
		w.noData = true
		end -= argsThree
	case endsWithWords(c.toks, "with", "data"):
		end -= argsTwo
	}

	w.body = c.text(c.pos, end)

	return w, w.body != ""
}

func endsWithWords(toks []token, ws ...string) bool {
	if len(toks) < len(ws) {
		return false
	}

	for i, w := range ws {
		if !toks[len(toks)-len(ws)+i].isWord(w) {
			return false
		}
	}

	return true
}

func (c *stmtCursor) parseInsert() (*writeStmt, bool) {
	w := &writeStmt{kind: writeInsert}

	if w.target = c.qualifiedName(); w.target == nil {
		return nil, false
	}

	if !c.done() && c.toks[c.pos].isPunct("(") {
		inner, ok := c.group()
		if !ok {
			return nil, false
		}

		for _, part := range splitCommas(inner) {
			sig := significant(part)
			if len(sig) != 1 {
				return nil, false
			}

			w.insertCols = append(w.insertCols, strings.ToLower(sig[0].ident()))
		}
	}

	if c.done() {
		return nil, false
	}

	if first := c.toks[c.pos]; !first.isWord("select") && !first.isWord("with") && !first.isWord("values") {
		return nil, false
	}

	w.body = c.text(c.pos, len(c.toks))

	return w, true
}

func (c *stmtCursor) parseUnload() (*writeStmt, bool) {
	w := &writeStmt{kind: writeUnload, props: map[string][]string{}}
	open := c.pos

	if _, ok := c.group(); !ok {
		return nil, false
	}

	w.body = c.text(open+1, c.pos-1)

	if !c.acceptWords("to") || c.done() || c.toks[c.pos].kind != tkString {
		return nil, false
	}

	w.location = c.stringLit()

	if c.acceptWords("with") {
		inner, ok := c.group()
		if !ok {
			return nil, false
		}

		w.props = parseWriteProps(inner)
	}

	return w, w.body != "" && c.done()
}

// parseWriteProps reads key = 'value' | number | ARRAY['a','b'] pairs.
func parseWriteProps(toks []token) map[string][]string {
	props := map[string][]string{}

	for _, part := range splitProps(toks) {
		sig := significant(part)
		if len(sig) < argsThree || !sig[1].isPunct("=") {
			continue
		}

		var vals []string

		for _, t := range sig[2:] {
			switch t.kind {
			case tkString:
				vals = append(vals, stringLiteral(t))
			case tkNumber, tkWord:
				if !t.isWord("array") {
					vals = append(vals, t.text)
				}
			case tkSpace, tkQuoted, tkPunct:
			}
		}

		props[strings.ToLower(sig[0].ident())] = vals
	}

	return props
}

func splitProps(toks []token) [][]token {
	var (
		parts [][]token
		cur   []token
		depth int
	)

	for _, t := range toks {
		switch {
		case t.isPunct("["):
			depth++
		case t.isPunct("]"):
			depth--
		}

		if depth == 0 && t.isPunct(",") {
			parts = append(parts, cur)
			cur = nil

			continue
		}

		cur = append(cur, t)
	}

	return append(parts, cur)
}
