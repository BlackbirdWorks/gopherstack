package ocidns

import (
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// evalSteerExpr evaluates one caseCondition or answerCondition: attribute, ==, != or in, and a literal or list.
// Unparsable expressions and attributes without a value (no geo or ASN data) never match.
func evalSteerExpr(expr string, answer map[string]any, q steerQuery) bool {
	if len(expr) > maxSteerExprLen {
		return false
	}

	toks, ok := steerTokens(expr)
	if !ok || len(toks) < 3 {
		return false
	}

	attr, op, rest := toks[0], toks[1], toks[2:]

	val, known := steerAttr(attr, answer, q)
	if !known {
		return false
	}

	return steerCompare(attr, val, op, rest)
}

func steerCompare(attr, val, op string, rest []string) bool {
	switch op {
	case steerOpEq, steerOpNe:
		lit, ok := steerLiteral(rest)
		if !ok || len(lit) != 1 {
			return false
		}

		return steerMatch(attr, val, lit[0]) == (op == steerOpEq)
	case steerOpIn:
		list, ok := steerList(rest)

		return ok && slices.ContainsFunc(list, func(l steerLit) bool { return steerMatch(attr, val, l) })
	}

	return false
}

type steerLit struct {
	kind string
	val  string
}

func steerAttr(attr string, a map[string]any, q steerQuery) (string, bool) {
	switch strings.ToLower(attr) {
	case steerAttrAddress:
		return q.client.String(), q.client.IsValid()
	case "answer.name", "answer.pool", "answer.rtype", "answer.rdata":
		if a == nil {
			return "", false
		}

		return strField(a, strings.TrimPrefix(strings.ToLower(attr), "answer.")), true
	case "answer.isdisabled":
		if a == nil {
			return "", false
		}

		dis, _ := a["isDisabled"].(bool)

		return strconv.FormatBool(dis), true
	}

	return "", false
}

func steerMatch(attr, val string, l steerLit) bool {
	if strings.EqualFold(attr, steerAttrAddress) {
		if l.kind != "subnet" && l.kind != "str" {
			return false
		}

		ip, err := netip.ParseAddr(val)
		if err != nil {
			return false
		}

		if pfx, perr := netip.ParsePrefix(l.val); perr == nil {
			return pfx.Contains(ip)
		}

		lip, lerr := netip.ParseAddr(l.val)

		return lerr == nil && lip == ip
	}

	return val == l.val
}

func steerLiteral(toks []string) ([]steerLit, bool) {
	switch {
	case len(toks) == 1:
		return []steerLit{steerScalar(toks[0])}, true
	case len(toks) == 2 && toks[0] != "(":
		return []steerLit{{kind: strings.ToLower(toks[0]), val: unquoteSteer(toks[1])}}, true
	}

	return nil, false
}

func steerScalar(t string) steerLit {
	if strings.HasPrefix(t, "'") {
		return steerLit{kind: "str", val: unquoteSteer(t)}
	}

	return steerLit{kind: "raw", val: t}
}

func unquoteSteer(t string) string {
	return strings.Trim(t, "'")
}

// steerList parses ( [type] 'x' , ... ).
func steerList(toks []string) ([]steerLit, bool) {
	if len(toks) < 3 || toks[0] != "(" || toks[len(toks)-1] != ")" {
		return nil, false
	}

	var out []steerLit

	cur := []string{}

	flush := func() bool {
		lit, ok := steerLiteral(cur)
		cur = cur[:0]

		if ok {
			out = append(out, lit...)
		}

		return ok
	}

	for _, t := range toks[1 : len(toks)-1] {
		if t == "," {
			if !flush() || len(out) > maxSteerListLen {
				return nil, false
			}

			continue
		}

		cur = append(cur, t)
	}

	if !flush() || len(out) > maxSteerListLen {
		return nil, false
	}

	return out, true
}

func steerTokens(s string) ([]string, bool) {
	var toks []string

	for i := 0; i < len(s); {
		tok, n, ok := steerNext(s, i)
		if !ok {
			return nil, false
		}

		if tok != "" {
			toks = append(toks, tok)
		}

		i = n
	}

	return toks, true
}

// steerNext reads the token at s[i] ("" for white space) and returns the index after it.
func steerNext(s string, i int) (string, int, bool) {
	switch c := s[i]; c {
	case ' ', '\t':
		return "", i + 1, true
	case '(', ')', ',':
		return string(c), i + 1, true
	case '\'':
		end := strings.IndexByte(s[i+1:], '\'')
		if end < 0 {
			return "", 0, false
		}

		return s[i : i+end+steerOpLen], i + end + steerOpLen, true
	case '=', '!':
		if i+1 >= len(s) || s[i+1] != '=' {
			return "", 0, false
		}

		return s[i : i+steerOpLen], i + steerOpLen, true
	}

	j := i
	for j < len(s) && !strings.ContainsRune(" \t(),'=!", rune(s[j])) {
		j++
	}

	return s[i:j], j, true
}
