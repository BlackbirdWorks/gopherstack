package cloudwatchlogs

import (
	"strconv"
	"strings"
)

// spaceEllipsis is the token used inside a "[...]" pattern to match a variable
// run of whitespace-delimited fields.
const spaceEllipsis = "..."

// spaceTerm is one positional term in a space-delimited "[...]" pattern.
type spaceTerm struct {
	op       string
	value    string
	num      float64
	ellipsis bool
	hasCond  bool
	isNum    bool
}

// compileSpaceFilterPattern compiles a CloudWatch Logs space-delimited metric
// filter pattern ("[ip, user, method=GET, status=4*]") into a message matcher.
// The message is split on whitespace into positional fields; each non-ellipsis
// term optionally constrains its field. A single "..." term may appear at the
// start or end to allow a variable number of leading/trailing fields.
func compileSpaceFilterPattern(pattern string) func(string) bool {
	body := strings.TrimSpace(pattern)
	body = strings.TrimPrefix(body, "[")
	body = strings.TrimSuffix(body, "]")

	terms := parseSpaceTerms(body)

	return func(message string) bool {
		return matchSpaceTerms(terms, strings.Fields(message))
	}
}

// parseSpaceTerms parses the comma-separated terms of a space pattern.
func parseSpaceTerms(body string) []spaceTerm {
	raws := strings.Split(body, ",")
	terms := make([]spaceTerm, 0, len(raws))
	for _, raw := range raws {
		terms = append(terms, parseSpaceTerm(strings.TrimSpace(raw)))
	}

	return terms
}

// parseSpaceTerm parses a single space-pattern term.
func parseSpaceTerm(raw string) spaceTerm {
	if raw == spaceEllipsis {
		return spaceTerm{op: "", value: "", ellipsis: true, hasCond: false, num: 0, isNum: false}
	}

	op, idx := findSpaceOp(raw)
	if idx < 0 {
		return spaceTerm{op: "", value: "", ellipsis: false, hasCond: false, num: 0, isNum: false}
	}

	value := strings.Trim(strings.TrimSpace(raw[idx+len(op):]), `"'`)
	term := spaceTerm{op: op, value: value, ellipsis: false, hasCond: true, num: 0, isNum: false}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		term.num = f
		term.isNum = true
	}

	return term
}

// findSpaceOp locates the comparison operator in a term, returning it and its
// byte offset (-1 when the term is a bare field binding).
func findSpaceOp(raw string) (string, int) {
	for _, op := range []string{"!=", "<=", ">=", "=", "<", ">"} {
		if idx := strings.Index(raw, op); idx >= 0 {
			return op, idx
		}
	}

	return "", -1
}

// matchSpaceTerms aligns the parsed terms against the message's fields.
func matchSpaceTerms(terms []spaceTerm, fields []string) bool {
	switch ellipsisPosition(terms) {
	case ellipsisNone:
		return matchFixed(terms, fields)
	case ellipsisLead:
		return matchTrailing(terms[1:], fields)
	case ellipsisTail:
		return matchLeading(terms[:len(terms)-1], fields)
	default:
		return false
	}
}

// ellipsis position classifications.
const (
	ellipsisNone = iota
	ellipsisLead
	ellipsisTail
	ellipsisOther
)

// ellipsisPosition reports where (if anywhere) a single ellipsis appears.
func ellipsisPosition(terms []spaceTerm) int {
	count, first := 0, -1
	for i, t := range terms {
		if t.ellipsis {
			count++
			if first < 0 {
				first = i
			}
		}
	}

	switch {
	case count == 0:
		return ellipsisNone
	case count != 1:
		return ellipsisOther
	case first == 0:
		return ellipsisLead
	case first == len(terms)-1:
		return ellipsisTail
	default:
		return ellipsisOther
	}
}

// matchFixed requires an exact positional alignment of terms and fields.
func matchFixed(terms []spaceTerm, fields []string) bool {
	if len(terms) != len(fields) {
		return false
	}

	for i, t := range terms {
		if !matchSpaceField(t, fields[i]) {
			return false
		}
	}

	return true
}

// matchLeading anchors terms at the start, allowing extra trailing fields.
func matchLeading(terms []spaceTerm, fields []string) bool {
	if len(fields) < len(terms) {
		return false
	}

	for i, t := range terms {
		if !matchSpaceField(t, fields[i]) {
			return false
		}
	}

	return true
}

// matchTrailing anchors terms at the end, allowing extra leading fields.
func matchTrailing(terms []spaceTerm, fields []string) bool {
	if len(fields) < len(terms) {
		return false
	}

	offset := len(fields) - len(terms)
	for i, t := range terms {
		if !matchSpaceField(t, fields[offset+i]) {
			return false
		}
	}

	return true
}

// matchSpaceField applies one term's optional condition to a field value.
func matchSpaceField(t spaceTerm, field string) bool {
	if !t.hasCond {
		return true
	}

	switch t.op {
	case "=":
		return spaceValueEquals(t.value, field)
	case "!=":
		return !spaceValueEquals(t.value, field)
	default:
		return spaceNumericCompare(t, field)
	}
}

// spaceValueEquals compares a field against a value, honouring "*" wildcards.
func spaceValueEquals(value, field string) bool {
	if strings.Contains(value, "*") {
		return wildcardMatch(value, field)
	}

	return value == field
}

// spaceNumericCompare implements <, <=, >, >= on numeric fields.
func spaceNumericCompare(t spaceTerm, field string) bool {
	f, err := strconv.ParseFloat(field, 64)
	if err != nil || !t.isNum {
		return false
	}

	switch t.op {
	case "<":
		return f < t.num
	case "<=":
		return f <= t.num
	case ">":
		return f > t.num
	case ">=":
		return f >= t.num
	default:
		return false
	}
}
