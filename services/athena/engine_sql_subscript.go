package athena

import "strings"

func isOperandKeyword(lw string) bool {
	if isClauseEnder(lw) {
		return true
	}

	switch lw {
	case "in", "and", "or", "not", "then", "else", "when", "select", "by", "from",
		"as", "case", "between", "like", "is", "all", "distinct":
		return true
	default:
		return false
	}
}

func lastSig(out []token) int { return prevSig(out, len(out)) }

func matchBackParen(out []token, closeIdx int) int {
	depth := 0

	for i := closeIdx; i >= 0; i-- {
		switch {
		case out[i].isPunct(")"):
			depth++
		case out[i].isPunct("("):
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// primaryStart returns the index in out where the primary expression ending at
// the last significant token begins, or -1 when the tail is not an operand.
func primaryStart(out []token) int {
	end := lastSig(out)
	if end < 0 {
		return -1
	}

	switch t := out[end]; {
	case t.isPunct(")"):
		open := matchBackParen(out, end)
		if open < 0 {
			return -1
		}

		if p := prevSig(out, open); p >= 0 && out[p].kind == tkWord && !isOperandKeyword(strings.ToLower(out[p].text)) {
			return p
		}

		return open
	case t.kind == tkWord && !isOperandKeyword(strings.ToLower(t.text)), t.kind == tkQuoted,
		t.kind == tkString, t.kind == tkNumber:
		return chainStart(out, end)
	default:
		return -1
	}
}

// chainStart extends a name leftwards over a.b.c qualifiers.
func chainStart(out []token, end int) int {
	start := end

	for {
		d := prevSig(out, start)
		if d < 0 || !out[d].isPunct(".") {
			return start
		}

		p := prevSig(out, d)
		if p < 0 || (out[p].kind != tkWord && out[p].kind != tkQuoted) {
			return start
		}

		start = p
	}
}

func matchBracket(toks []token, open int) int {
	depth := 0

	for i := open; i < len(toks); i++ {
		switch {
		case toks[i].isPunct("["):
			depth++
		case toks[i].isPunct("]"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// subscript rewrites operand[idx] (1-based, Trino) into athena_subscript(operand, idx).
func (x *translator) subscript(toks []token, i int, out []token) ([]token, int) {
	closeIdx := matchBracket(toks, i)
	start := primaryStart(out)

	if closeIdx < 0 || start < 0 {
		x.err = &syntaxError{"line 1:1: mismatched input '['"}

		return out, i
	}

	idx := strings.TrimSpace(joinTokens(x.translate(toks[i+1 : closeIdx])))
	operand := strings.TrimSpace(joinTokens(out[start:]))
	res := token{kind: tkWord, text: "athena_subscript(" + operand + ", " + idx + ")"}

	return append(out[:start:start], res), closeIdx
}

func isStructChainToken(t token) bool { return t.kind == tkWord || t.kind == tkQuoted }

// structChain collects a.b.c starting at toks[j]; it returns the part tokens
// and the index of the last token consumed.
func structChain(toks []token, j int) ([]token, int) {
	parts := []token{toks[j]}
	end := j

	for end+2 < len(toks) && toks[end+1].isPunct(".") && isStructChainToken(toks[end+2]) {
		parts = append(parts, toks[end+2])
		end += 2
	}

	return parts, end
}

func (x *translator) structBase(parts []token) int {
	if x.qualifiers[parts[0].ident()] {
		return 1
	}

	return 0
}

// rewriteStructFields turns col.field (ROW columns of the referenced tables)
// into athena_field(col, 'field') chains.
func (x *translator) rewriteStructFields(toks []token) []token {
	if len(x.structCols) == 0 {
		return toks
	}

	out := make([]token, 0, len(toks))

	for j := 0; j < len(toks); j++ {
		t := toks[j]
		if !isStructChainToken(t) || (j > 0 && toks[j-1].isPunct(".")) {
			out = append(out, t)

			continue
		}

		parts, end := structChain(toks, j)
		base := x.structBase(parts)

		if base+1 >= len(parts) || !x.structCols[parts[base].ident()] {
			out = append(out, t)

			continue
		}

		out = append(out, structDeref(parts, base))
		j = end
	}

	return out
}

func structDeref(parts []token, base int) token {
	segs := make([]string, 0, base+1)
	for _, p := range parts[:base+1] {
		segs = append(segs, p.text)
	}

	expr := strings.Join(segs, ".")

	for _, f := range parts[base+1:] {
		expr = "athena_field(" + expr + ", '" + strings.ReplaceAll(f.ident(), "'", "''") + "')"
	}

	return token{kind: tkWord, text: expr}
}
