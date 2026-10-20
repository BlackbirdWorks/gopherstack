package athena

import "strings"

func isOrderByEnd(t token) bool {
	if t.kind != tkWord {
		return false
	}

	lw := strings.ToLower(t.text)

	return isClauseEnder(lw) || lw == "rows" || lw == "range" || lw == "groups"
}

// orderByClauseEnd returns the index one past the ORDER BY clause that starts at from.
func orderByClauseEnd(toks []token, from int) int {
	depth := 0

	for i := from; i < len(toks); i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			depth--
			if depth < 0 {
				return i
			}
		case depth == 0 && (isOrderByEnd(toks[i]) || toks[i].isPunct(";")):
			return i
		}
	}

	return len(toks)
}

func itemNeedsNullsLast(item []token) bool {
	sig := significant(item)
	if len(sig) == 0 || sig[len(sig)-1].isWord("desc") {
		return false
	}

	for _, t := range sig {
		if t.isWord("nulls") {
			return false
		}
	}

	return true
}

// nullsLastInserts returns the token indexes before which " NULLS LAST" belongs.
func nullsLastInserts(toks []token, from, end int) []int {
	var out []int

	start, depth := from, 0

	flush := func(stop int) {
		if !itemNeedsNullsLast(toks[start:stop]) {
			return
		}

		last := prevSig(toks[:stop], stop)
		out = append(out, last+1)
	}

	for i := from; i < end; i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			depth--
		case depth == 0 && toks[i].isPunct(","):
			flush(i)

			start = i + 1
		}
	}

	flush(end)

	return out
}

// addNullsLast gives ORDER BY items Trino's default NULLS LAST (SQLite sorts NULLs first ascending).
func addNullsLast(toks []token) []token {
	insert := map[int]bool{}

	for i := range toks {
		if !toks[i].isWord("order") {
			continue
		}

		by := nextSig(toks, i)
		if by < 0 || !toks[by].isWord("by") {
			continue
		}

		for _, at := range nullsLastInserts(toks, by+1, orderByClauseEnd(toks, by+1)) {
			insert[at] = true
		}
	}

	if len(insert) == 0 {
		return toks
	}

	out := make([]token, 0, len(toks)+len(insert)*argsTwo)

	for i, t := range toks {
		if insert[i] {
			out = append(out, token{kind: tkWord, text: " NULLS LAST"})
		}

		out = append(out, t)
	}

	if insert[len(toks)] {
		out = append(out, token{kind: tkWord, text: " NULLS LAST"})
	}

	return out
}
