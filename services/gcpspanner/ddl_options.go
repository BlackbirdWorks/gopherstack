package gcpspanner

import (
	"slices"
	"strings"
)

const (
	optLocalityGroup   = "locality_group"
	optColumnarPolicy  = "columnar_policy"
	optCommitTimestamp = "allow_commit_timestamp"
	optDefaultLeader   = "default_leader"

	kwPrimary     = "PRIMARY"
	kwDelete      = "DELETE"
	kindSequence  = "SEQUENCE"
	kindModel     = "MODEL"
	kindTableFunc = "TABLE FUNCTION"

	optPairTokens = 3
	hoursPerDay   = 24
)

type optPair struct{ key, val string }

func (o optPair) null() bool { return strings.EqualFold(o.val, "null") }

// topLevelClause finds `word (...)` at parenthesis depth zero and returns the keyword, "(" and ")" indexes.
func topLevelClause(toks []ddlTok, word string) (int, int, int, bool) {
	depth := 0

	for i, t := range toks {
		switch t.text {
		case "(", "[":
			depth++
		case ")", "]":
			depth--
		}

		if depth == 0 && t.is(word) && i+1 < len(toks) && toks[i+1].text == "(" {
			if end := matchParen(toks, i+1); end >= 0 {
				return i, i + 1, end, true
			}
		}
	}

	return 0, 0, 0, false
}

// parsePairs reads `k = v, ...` between toks[open] and toks[closeIdx].
func parsePairs(text string, toks []ddlTok, open, closeIdx int) ([]optPair, error) {
	var out []optPair

	for _, it := range splitTop(toks, open, closeIdx) {
		if len(it) < optPairTokens || !it[0].word || it[1].text != "=" {
			return nil, errInvalid("Syntax error: expected option = value.")
		}

		out = append(out, optPair{key: foldKey(identName(it[0])), val: text[it[2].start:it[len(it)-1].end]})
	}

	return out, nil
}

func mergePairs(old, upd []optPair) []optPair {
	out := slices.Clone(old)

	for _, u := range upd {
		i := slices.IndexFunc(out, func(o optPair) bool { return o.key == u.key })

		switch {
		case i >= 0 && u.null():
			out = slices.Delete(out, i, i+1)
		case i >= 0:
			out[i] = u
		case !u.null():
			out = append(out, u)
		}
	}

	return out
}

func renderPairs(word string, pairs []optPair) string {
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.key + " = " + p.val
	}

	return word + " (" + strings.Join(parts, ", ") + ")"
}

// applyOptions merges upd into the OPTIONS clause of text (a column, an index or a table tail). A clause that
// ends up empty is removed together with its separating comma; a new one is appended after sep.
func applyOptions(text string, postgres bool, upd []optPair, sep string) (string, error) {
	return applyClausePairs(text, postgres, "OPTIONS", upd, sep)
}

func applyClausePairs(text string, postgres bool, word string, upd []optPair, sep string) (string, error) {
	toks, err := tokenizeDDL(text, postgres)
	if err != nil {
		return "", err
	}

	kw, open, closeIdx, ok := topLevelClause(toks, word)
	if !ok {
		merged := mergePairs(nil, upd)
		if len(merged) == 0 {
			return text, nil
		}

		return strings.TrimRight(text, " \n\t") + sep + renderPairs(word, merged), nil
	}

	old, err := parsePairs(text, toks, open, closeIdx)
	if err != nil {
		return "", err
	}

	merged := mergePairs(old, upd)
	if len(merged) > 0 {
		return text[:toks[kw].start] + renderPairs(word, merged) + text[toks[closeIdx].end:], nil
	}

	start := toks[kw].start
	if kw > 0 && toks[kw-1].text == "," {
		start = toks[kw-1].start
	}

	return strings.TrimRight(text[:start], " \n\t") + text[toks[closeIdx].end:], nil
}

// pairsAfter reads a parenthesised option list at act[at] of stmt and returns the position after it.
func pairsAfter(stmt string, act []ddlTok, at int) ([]optPair, int, error) {
	if at >= len(act) || act[at].text != "(" {
		return nil, 0, errInvalid("Syntax error: expected '(' with the options.")
	}

	end := matchParen(act, at)
	if end < 0 {
		return nil, 0, errInvalid("Syntax error: unbalanced parentheses.")
	}

	pairs, err := parsePairs(stmt, act, at, end)
	if err != nil {
		return nil, 0, err
	}

	return pairs, end + 1, nil
}

func checkOptionKeys(pairs []optPair, allowed map[string]func(optPair) error, what string) error {
	if allowed == nil {
		return nil
	}

	for _, p := range pairs {
		check, ok := allowed[p.key]
		if !ok {
			return errInvalid("Unrecognized %s option: %s.", what, p.key)
		}

		if check == nil || p.null() {
			continue
		}

		if err := check(p); err != nil {
			return err
		}
	}

	return nil
}

func quotedString(p optPair) error {
	if len(p.val) < 2 || (p.val[0] != '\'' && p.val[0] != '"') {
		return errInvalid("Option %s must be a string literal.", p.key)
	}

	return nil
}

func boolValue(p optPair) error {
	if !strings.EqualFold(p.val, "true") && !strings.EqualFold(p.val, "false") {
		return errInvalid("Option %s must be true, false or null.", p.key)
	}

	return nil
}
