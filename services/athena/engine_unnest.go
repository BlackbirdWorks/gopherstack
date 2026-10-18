package athena

import (
	"strconv"
	"strings"
)

// unnestCol is where an UNNEST output column lives in the json_each row.
type unnestCol struct {
	alias    string
	physical string
	ordinal  bool
}

const (
	jsonEachKey   = unnestColKey
	jsonEachValue = unnestColValue
)

func (x *translator) nextUnnestAlias() string {
	x.nUnnest++

	return "_unnest" + strconv.Itoa(x.nUnnest)
}

// unnestRef rewrites UNNEST(expr) [WITH ORDINALITY] [AS alias(cols)] at toks[j]
// onto json_each; arrays and maps are JSON text in the SQLite store.
func (x *translator) unnestRef(toks []token, j int, out []token) ([]token, int) {
	open := nextSig(toks, j)
	closeIdx := matchParen(toks, open)

	if closeIdx < 0 {
		x.err = &syntaxError{"line 1:1: mismatched input in UNNEST"}

		return out, j
	}

	inner := toks[open+1 : closeIdx]
	if hasTopLevelComma(inner) {
		x.err = &syntaxError{"line 1:1: UNNEST with more than one argument is not supported"}

		return out, closeIdx
	}

	arg := strings.TrimSpace(joinTokens(x.translate(inner)))
	end := closeIdx
	ordinality := false

	if n := nextSig(toks, end); n >= 0 && toks[n].isWord("with") {
		if o := nextSig(toks, n); o >= 0 && toks[o].isWord("ordinality") {
			ordinality, end = true, o
		}
	}

	alias, cols, end := x.unnestAlias(toks, end)
	x.registerUnnestCols(alias, cols, ordinality)

	res := unnestModuleName + "(" + arg + ") AS " + quoteIdent(alias)

	return append(out, token{kind: tkWord, text: res}), end
}

func (x *translator) unnestAlias(toks []token, end int) (string, []string, int) {
	alias := x.nextUnnestAlias()

	n := nextSig(toks, end)
	if n >= 0 && toks[n].isWord("as") {
		end = n
		n = nextSig(toks, end)
	}

	if n < 0 || !isAliasToken(toks[n]) {
		return alias, nil, end
	}

	alias, end = toks[n].ident(), n

	var cols []string

	if p := nextSig(toks, end); p >= 0 && toks[p].isPunct("(") {
		if c := matchParen(toks, p); c > 0 {
			for _, part := range splitCommas(toks[p+1 : c]) {
				if sig := significant(part); len(sig) == 1 {
					cols = append(cols, sig[0].ident())
				}
			}

			end = c
		}
	}

	return alias, cols, end
}

func (x *translator) registerUnnestCols(alias string, cols []string, ordinality bool) {
	if x.unnest == nil {
		x.unnest = map[string]unnestCol{}
	}

	phys := []string{jsonEachValue}
	if len(cols) == argsTwo+boolToInt(ordinality) && len(cols) >= argsTwo && (!ordinality || len(cols) == argsThree) {
		phys = []string{jsonEachKey, jsonEachValue}
	}

	if ordinality && len(phys) == 1 {
		phys = append(phys, jsonEachKey)
	}

	for i, c := range cols {
		if i >= len(phys) {
			break
		}

		x.unnest[c] = unnestCol{alias: alias, physical: phys[i], ordinal: ordinality && i == len(cols)-1}
	}

	x.unnestAliases = append(x.unnestAliases, alias)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}

	return 0
}

// rewriteUnnestRefs points bare references to UNNEST columns at json_each.
func (x *translator) rewriteUnnestRefs(toks []token) []token {
	if len(x.unnest) == 0 {
		return toks
	}

	out := make([]token, 0, len(toks))

	for i, t := range toks {
		if t.kind != tkWord && t.kind != tkQuoted {
			out = append(out, t)

			continue
		}

		col, ok := x.unnest[t.ident()]
		if !ok || strings.HasPrefix(t.text, unnestModuleName+"(") {
			out = append(out, t)

			continue
		}

		prev := prevSig(toks, i)
		afterDot := prev >= 0 && toks[prev].isPunct(".")

		switch {
		case afterDot:
			out = append(out, t)
		case nextIsDot(toks, i):
			out = append(out, t)
		default:
			out = append(out, token{kind: tkWord, text: col.sql()})
		}
	}

	return out
}

func nextIsDot(toks []token, i int) bool {
	n := nextSig(toks, i)

	return n >= 0 && toks[n].isPunct(".")
}

func (c unnestCol) sql() string {
	ref := quoteIdent(c.alias) + "." + quoteIdent(c.physical)
	if c.ordinal {
		return "(" + ref + " + 1)"
	}

	return ref
}

func hasTopLevelComma(toks []token) bool {
	depth := 0

	for _, t := range toks {
		switch {
		case t.isPunct("("), t.isPunct("["):
			depth++
		case t.isPunct(")"), t.isPunct("]"):
			depth--
		case depth == 0 && t.isPunct(","):
			return true
		}
	}

	return false
}

// isAliasToken reports whether t can be a table alias rather than the next clause.
func isAliasToken(t token) bool {
	if t.kind != tkWord && t.kind != tkQuoted {
		return false
	}

	switch t.ident() {
	case "cross", "left", "right", "full", "inner", kwJoin:
		return false
	default:
		return !isClauseEnder(t.ident())
	}
}
