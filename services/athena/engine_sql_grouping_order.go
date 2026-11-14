package athena

import (
	"strconv"
	"strings"
)

const (
	hiddenGroupPrefix = "__g"
	hiddenOrderPrefix = "__o"
)

type orderEntry struct {
	suffix string
	expr   []token
	// col is the select-list position this key names, or -1 when it needs a hidden column.
	col int
	// ordinal keys (ORDER BY 2) pass through unchanged.
	ordinal bool
}

func splitOrderSuffix(item []token) ([]token, string) {
	toks := trimSpaceTokens(item)

	var idx []int

	for i, t := range toks {
		if t.kind != tkSpace {
			idx = append(idx, i)
		}
	}

	n, cut := len(idx), len(toks)

	if n >= argsThree && toks[idx[n-2]].isWord("nulls") &&
		(toks[idx[n-1]].isWord("first") || toks[idx[n-1]].isWord("last")) {
		cut, n = idx[n-argsTwo], n-argsTwo
	}

	if n >= argsTwo && (toks[idx[n-1]].isWord("asc") || toks[idx[n-1]].isWord("desc")) {
		cut = idx[n-1]
	}

	return trimSpaceTokens(toks[:cut]), " " + strings.TrimSpace(joinTokens(toks[cut:]))
}

// outputName is the name a select item has in the result: its alias or its bare column name.
func outputName(item []token) string {
	expr, alias := splitAlias(item)
	if alias != "" {
		return alias
	}

	sig := significant(expr)
	if len(sig) == 1 && (sig[0].kind == tkWord || sig[0].kind == tkQuoted) {
		return sig[0].ident()
	}

	if len(sig) == argsThree && sig[1].isPunct(".") {
		return sig[2].ident()
	}

	return ""
}

func (b *groupingBlock) classifyOrder(items [][]token) ([]orderEntry, bool) {
	names := make([]string, len(b.items))
	for i, it := range b.items {
		names[i] = outputName(it)
	}

	entries := make([]orderEntry, len(items))
	hidden := false

	for i, it := range items {
		expr, suffix := splitOrderSuffix(it)
		e := orderEntry{expr: expr, suffix: suffix, col: -1}
		sig := significant(expr)

		switch {
		case len(sig) == 1 && sig[0].kind == tkNumber:
			e.ordinal = true
		case len(sig) == 1 && (sig[0].kind == tkWord || sig[0].kind == tkQuoted):
			for j, n := range names {
				if n != "" && n == sig[0].ident() {
					e.col = j

					break
				}
			}
		}

		hidden = hidden || (!e.ordinal && e.col < 0)
		entries[i] = e
	}

	return entries, hidden
}

// prepareOrder returns the outer select list and tail to use. When every ORDER BY key is an
// output column or ordinal the tail is unchanged and outer is empty; otherwise each other key
// becomes a hidden select item evaluated per branch and outer projects the visible columns.
func (b *groupingBlock) prepareOrder() (string, []token, error) {
	tail := b.tail

	ord := depthZeroWord(tail, 0, "order")
	if ord < 0 || slicesHasStar(b.items) {
		return "", tail, nil
	}

	by := nextSig(tail, ord)
	if by < 0 || !tail[by].isWord("by") {
		return "", tail, nil
	}

	end := firstAtOrEnd(tail, by+1, kwLimit, kwOffset, kwFetch)

	entries, hidden := b.classifyOrder(splitArgs(tail[by+1 : end]))
	if !hidden {
		return "", tail, nil
	}

	outer, err := b.hideOrderKeys(entries)
	if err != nil {
		return "", nil, err
	}

	keys := make([]string, len(entries))

	for i, e := range entries {
		keys[i] = orderKeySQL(e, hiddenBefore(entries, i))
	}

	nt := make([]token, 0, len(tail))
	nt = append(nt, tail[:ord]...)
	nt = append(nt, token{kind: tkWord, text: "ORDER BY " + strings.Join(keys, ", ") + " "})
	nt = append(nt, tail[end:]...)

	return outer, nt, nil
}

func slicesHasStar(items [][]token) bool {
	for _, it := range items {
		if sig := significant(it); len(sig) == 1 && sig[0].isPunct("*") {
			return true
		}
	}

	return false
}

func hiddenBefore(entries []orderEntry, i int) int {
	n := 0

	for _, e := range entries[:i] {
		if !e.ordinal && e.col < 0 {
			n++
		}
	}

	return n
}

func orderKeySQL(e orderEntry, hiddenIdx int) string {
	switch {
	case e.ordinal:
		return joinTokens(e.expr) + e.suffix
	case e.col >= 0:
		return quoteIdent(hiddenGroupPrefix+strconv.Itoa(e.col)) + e.suffix
	default:
		return quoteIdent(hiddenOrderPrefix+strconv.Itoa(hiddenIdx)) + e.suffix
	}
}

func visibleName(item []token) string {
	if n := outputName(item); n != "" {
		return n
	}

	expr, _ := splitAlias(item)

	return strings.TrimSpace(joinTokens(expr))
}

// hideOrderKeys renames the visible items to __gN, appends hidden __oN items, and returns the
// projection restoring the original names.
func (b *groupingBlock) hideOrderKeys(entries []orderEntry) (string, error) {
	outer := make([]string, len(b.items))
	items := make([][]token, 0, len(b.items)+len(entries))

	for i, it := range b.items {
		expr, _ := splitAlias(it)
		outer[i] = quoteIdent(hiddenGroupPrefix+strconv.Itoa(i)) + " AS " + quoteIdent(visibleName(it))

		alias := quoteIdent(hiddenGroupPrefix + strconv.Itoa(i))

		toks, err := tokenize(strings.TrimSpace(joinTokens(expr)) + " AS " + alias)
		if err != nil {
			return "", err
		}

		items = append(items, toks)
	}

	n := 0

	for _, e := range entries {
		if e.ordinal || e.col >= 0 {
			continue
		}

		alias := quoteIdent(hiddenOrderPrefix + strconv.Itoa(n))

		toks, err := tokenize(strings.TrimSpace(joinTokens(e.expr)) + " AS " + alias)
		if err != nil {
			return "", err
		}

		items = append(items, toks)
		n++
	}

	b.items = items

	return strings.Join(outer, ", "), nil
}

// setOperatorSpans returns the [start,end) token spans of each depth-zero UNION/INTERSECT/EXCEPT [ALL|DISTINCT].
func setOperatorSpans(toks []token, from int) [][2]int {
	var spans [][2]int

	for i := from; ; {
		op := depthZeroWord(toks, i, "union", kwIntersect, kwExcept)
		if op < 0 {
			return spans
		}

		end := op + 1
		if n := nextSig(toks, op); n >= 0 && (toks[n].isWord("all") || toks[n].isWord("distinct")) {
			end = n + 1
		}

		spans = append(spans, [2]int{op, end})
		i = end
	}
}

// expandSetOperation expands grouping extensions inside each operand of a set operation.
func expandSetOperation(toks []token) ([]token, bool, error) {
	sel := depthZeroWord(toks, 0, "select")
	if sel < 0 {
		return nil, false, nil
	}

	spans := setOperatorSpans(toks, sel)
	if len(spans) == 0 {
		return nil, false, nil
	}

	var sb strings.Builder

	sb.WriteString(joinTokens(toks[:sel]))

	changedAny := false
	prev := sel

	for i := 0; i <= len(spans); i++ {
		stop := len(toks)
		if i < len(spans) {
			stop = spans[i][0]
		}

		seg, tail := toks[prev:stop], []token(nil)
		if i == len(spans) {
			cut := firstAtOrEnd(seg, 0, "order", kwLimit, kwOffset, kwFetch)
			seg, tail = seg[:cut], seg[cut:]
		}

		out, changed, err := expandGroupingBlock(seg)
		if err != nil {
			return nil, false, err
		}

		changedAny = changedAny || changed

		sb.WriteString(joinTokens(out))
		sb.WriteString(joinTokens(tail))

		if i < len(spans) {
			sb.WriteString(joinTokens(toks[spans[i][0]:spans[i][1]]))
			prev = spans[i][1]
		}
	}

	if !changedAny {
		return nil, false, nil
	}

	out, err := tokenize(sb.String())

	return out, true, err
}
