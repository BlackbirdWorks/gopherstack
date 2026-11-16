package athena

import "strings"

const twoTokens = 2

// rowOperand is a syntactic ROW operand: a ROW(...) or CAST-to-ROW call, or a column declared as ROW.
type rowOperand struct {
	start, end int
	row        bool
}

func (x *translator) isRowCall(name string) bool {
	n := strings.ToLower(name)

	return n == fnRowCall || n == fnRowCast
}

func (x *translator) chainIsRowCol(parts []token) bool {
	base := x.structBase(parts)

	return len(parts) == base+1 && x.structCols[parts[base].ident()]
}

// operandForward reads one operand starting at the first significant token at or after i.
func (x *translator) operandForward(toks []token, i int) (rowOperand, bool) {
	s := i
	for s < len(toks) && toks[s].kind == tkSpace {
		s++
	}

	if s >= len(toks) {
		return rowOperand{}, false
	}

	t := toks[s]

	if t.kind == tkNumber || t.kind == tkString {
		return rowOperand{start: s, end: s}, true
	}

	if t.kind != tkWord && t.kind != tkQuoted {
		return rowOperand{}, false
	}

	if n := nextSig(toks, s); n >= 0 && toks[n].isPunct("(") {
		end := matchParen(toks, n)
		if end < 0 {
			return rowOperand{}, false
		}

		return rowOperand{start: s, end: end, row: x.isRowCall(t.text)}, true
	}

	if strings.Contains(t.text, "(") {
		return rowOperand{start: s, end: s}, true
	}

	parts, end := structChain(toks, s)

	return rowOperand{start: s, end: end, row: x.chainIsRowCol(parts)}, true
}

// operandBackward reads the operand that ends at the last significant token of toks.
func (x *translator) operandBackward(toks []token) (rowOperand, bool) {
	j := len(toks) - 1
	for j >= 0 && toks[j].kind == tkSpace {
		j--
	}

	if j < 0 {
		return rowOperand{}, false
	}

	t := toks[j]

	switch {
	case t.isPunct(")"):
		return x.callBackward(toks, j)
	case t.kind == tkNumber || t.kind == tkString || (t.kind == tkWord && strings.Contains(t.text, "(")):
		return rowOperand{start: j, end: j}, true
	case t.kind == tkWord || t.kind == tkQuoted:
		start := j
		for start >= 2 && toks[start-1].isPunct(".") && isStructChainToken(toks[start-2]) {
			start -= 2
		}

		parts, _ := structChain(toks, start)

		return rowOperand{start: start, end: j, row: x.chainIsRowCol(parts)}, true
	default:
		return rowOperand{}, false
	}
}

func (x *translator) callBackward(toks []token, closeIdx int) (rowOperand, bool) {
	open := matchOpenBackward(toks, closeIdx)
	if open < 0 {
		return rowOperand{}, false
	}

	p := prevSig(toks, open)
	if p < 0 || (toks[p].kind != tkWord && toks[p].kind != tkQuoted) {
		return rowOperand{}, false
	}

	return rowOperand{start: p, end: closeIdx, row: x.isRowCall(toks[p].text)}, true
}

func matchOpenBackward(toks []token, closeIdx int) int {
	depth := 0

	for i := closeIdx; i >= 0; i-- {
		switch {
		case toks[i].isPunct(")"):
			depth++
		case toks[i].isPunct("("):
			if depth--; depth == 0 {
				return i
			}
		}
	}

	return -1
}

// comparator reports the SQL comparison operator that starts at toks[i] and how many tokens it spans.
func comparator(toks []token, i int) (string, int) {
	t := toks[i]
	next := func() string {
		if i+1 < len(toks) && toks[i+1].kind == tkPunct {
			return toks[i+1].text
		}

		return ""
	}

	switch {
	case t.isPunct("<") && next() == "=":
		return "<=", twoTokens
	case t.isPunct("<") && next() == ">":
		return "<>", twoTokens
	case t.isPunct(">") && next() == "=":
		return ">=", twoTokens
	case t.isPunct("!") && next() == "=":
		return "<>", twoTokens
	case t.isPunct("=") || t.isPunct("<") || t.isPunct(">"):
		return t.text, 1
	default:
		return "", 0
	}
}

// rewriteRowOps routes comparisons, ORDER BY keys and min/max over ROW operands through the
// registered athena_row_* functions, which compare field by field in declaration order.
func (x *translator) rewriteRowOps(toks []token) []token {
	out := make([]token, 0, len(toks))

	for i := 0; i < len(toks); i++ {
		next, handled := x.rewriteAt(toks, i, &out)
		if handled {
			i = next

			continue
		}

		out = append(out, toks[i])
	}

	return out
}

// rewriteAt handles the constructs that start at toks[i]; next is the last input index consumed.
func (x *translator) rewriteAt(toks []token, i int, out *[]token) (int, bool) {
	t := toks[i]

	if op, n := comparator(toks, i); n > 0 && !isArrow(t, *out) {
		if next, ok := x.rewriteComparison(toks, i, op, n, out); ok {
			return next, true
		}
	}

	if t.isWord("min") || t.isWord("max") {
		if fn, ok := x.extremeFunc(toks, i); ok {
			*out = append(*out, token{kind: tkWord, text: fn})

			return i, true
		}
	}

	if t.isWord("order") {
		if by := nextSig(toks, i); by >= 0 && toks[by].isWord("by") {
			end := orderByClauseEnd(toks, by+1)
			*out = append(*out, toks[i:by+1]...)
			*out = append(*out, x.rewriteOrderItems(toks[by+1:end])...)

			return end - 1, true
		}
	}

	return i, false
}

func isArrow(t token, out []token) bool {
	return t.isPunct(">") && len(out) > 0 && out[len(out)-1].isPunct("-")
}

func (x *translator) rewriteComparison(toks []token, i int, op string, n int, out *[]token) (int, bool) {
	left, lok := x.operandBackward(*out)
	if !lok {
		return 0, false
	}

	right, rok := x.operandForward(toks, i+n)
	if !rok || (!left.row && !right.row) {
		return 0, false
	}

	l := joinTokens((*out)[left.start : left.end+1])
	r := joinTokens(toks[right.start : right.end+1])

	*out = append((*out)[:left.start], token{
		kind: tkWord, text: fnRowCompare + "(" + l + ", " + r + ") " + op + " 0",
	})

	return right.end, true
}

func (x *translator) extremeFunc(toks []token, i int) (string, bool) {
	open := nextSig(toks, i)
	if open < 0 || !toks[open].isPunct("(") {
		return "", false
	}

	end := matchParen(toks, open)
	if end < 0 {
		return "", false
	}

	op, ok := x.operandForward(toks, open+1)
	if !ok || !op.row || nextSig(toks, op.end) != end {
		return "", false
	}

	if strings.EqualFold(toks[i].text, "min") {
		return fnRowMin, true
	}

	return fnRowMax, true
}

// rewriteOrderItems wraps ROW sort keys of an ORDER BY list in athena_row_sortkey.
func (x *translator) rewriteOrderItems(toks []token) []token {
	out := make([]token, 0, len(toks)+twoTokens)
	start, depth := 0, 0

	flush := func(stop int) {
		out = append(out, x.wrapOrderItem(toks[start:stop])...)
	}

	for i, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isPunct(","):
			flush(i)
			out = append(out, t)
			start = i + 1
		}
	}

	flush(len(toks))

	return out
}

func (x *translator) wrapOrderItem(item []token) []token {
	first := 0
	for first < len(item) && item[first].kind == tkSpace {
		first++
	}

	op, ok := x.operandForward(item, first)
	if !ok || !op.row {
		return item
	}

	for _, t := range item[op.end+1:] {
		if t.kind != tkSpace && !t.isWord("asc") && !t.isWord("desc") && !t.isWord("nulls") &&
			!t.isWord("first") && !t.isWord("last") {
			return item
		}
	}

	out := make([]token, 0, len(item)+1)
	out = append(out, item[:op.start]...)
	out = append(out, token{kind: tkWord, text: fnRowKey + "(" + joinTokens(item[op.start:op.end+1]) + ")"})

	return append(out, item[op.end+1:]...)
}
