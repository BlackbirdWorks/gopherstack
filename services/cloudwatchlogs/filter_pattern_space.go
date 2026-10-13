package cloudwatchlogs

import (
	"strconv"
	"strings"
)

// spaceEllipsis is the token used inside a "[...]" pattern to match a variable
// run of whitespace-delimited fields.
const spaceEllipsis = "..."

// spaceCond is one comparison on a field.
type spaceCond struct {
	op    string
	value string
	num   float64
	isNum bool
}

// spaceTerm is one positional term in a space-delimited "[...]" pattern.
// alts is a disjunction of conjunctions ("a = 1 && b > 2 || c = 3"); empty means unconstrained.
type spaceTerm struct {
	// name is the field's bare identifier (e.g. "size" in "size>100"), used to
	// resolve "$size" references. Empty for the ellipsis term.
	name     string
	alts     [][]spaceCond
	ellipsis bool
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
		return spaceTerm{ellipsis: true}
	}

	var term spaceTerm
	for orPart := range strings.SplitSeq(raw, "||") {
		var conj []spaceCond
		for andPart := range strings.SplitSeq(orPart, "&&") {
			andPart = strings.TrimSpace(andPart)
			op, idx := findSpaceOp(andPart)
			if idx < 0 {
				if term.name == "" {
					term.name = andPart
				}

				continue
			}

			if term.name == "" {
				term.name = strings.TrimSpace(andPart[:idx])
			}

			value := strings.Trim(strings.TrimSpace(andPart[idx+len(op):]), `"'`)
			c := spaceCond{op: op, value: value}
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				c.num = f
				c.isNum = true
			}

			conj = append(conj, c)
		}

		if len(conj) > 0 {
			term.alts = append(term.alts, conj)
		}
	}

	return term
}

// findSpaceOp locates the comparison operator in a term, returning it and its
// byte offset (-1 when the term is a bare field binding).
func findSpaceOp(raw string) (string, int) {
	best, bestIdx := "", -1
	for _, op := range []string{"!=", "<=", ">=", "=", "<", ">"} {
		idx := strings.Index(raw, op)
		if idx >= 0 && (bestIdx < 0 || idx < bestIdx) {
			best, bestIdx = op, idx
		}
	}

	return best, bestIdx
}

// matchSpaceTerms aligns the parsed terms against the message's fields; a single
// "..." absorbs any run of fields between the anchored prefix and suffix.
func matchSpaceTerms(terms []spaceTerm, fields []string) bool {
	e := ellipsisIndex(terms)
	if e == ellipsisMany {
		return false
	}

	if e == ellipsisNone {
		if len(terms) != len(fields) {
			return false
		}

		return matchSpaceRange(terms, fields)
	}

	prefix, suffix := terms[:e], terms[e+1:]
	if len(fields) < len(prefix)+len(suffix) {
		return false
	}

	return matchSpaceRange(prefix, fields[:len(prefix)]) &&
		matchSpaceRange(suffix, fields[len(fields)-len(suffix):])
}

const (
	ellipsisNone = -1
	ellipsisMany = -2
)

// ellipsisIndex returns the index of the single ellipsis, ellipsisNone, or ellipsisMany.
func ellipsisIndex(terms []spaceTerm) int {
	idx := ellipsisNone
	for i, t := range terms {
		if !t.ellipsis {
			continue
		}

		if idx != ellipsisNone {
			return ellipsisMany
		}

		idx = i
	}

	return idx
}

// matchSpaceRange matches terms pairwise against equally many fields.
func matchSpaceRange(terms []spaceTerm, fields []string) bool {
	for i, t := range terms {
		if !matchSpaceField(t, fields[i]) {
			return false
		}
	}

	return true
}

// matchSpaceField applies one term's optional condition to a field value.
func matchSpaceField(t spaceTerm, field string) bool {
	if len(t.alts) == 0 {
		return true
	}

	for _, conj := range t.alts {
		ok := true
		for _, c := range conj {
			if !matchSpaceCond(c, field) {
				ok = false

				break
			}
		}

		if ok {
			return true
		}
	}

	return false
}

func matchSpaceCond(c spaceCond, field string) bool {
	switch c.op {
	case "=":
		return spaceValueEquals(c.value, field)
	case "!=":
		return !spaceValueEquals(c.value, field)
	default:
		return spaceNumericCompare(c, field)
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
func spaceNumericCompare(t spaceCond, field string) bool {
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

// compileSpaceFilterPatternExtract returns a field extractor for a space-delimited pattern,
// keyed by the field's bare name (fieldRef has its leading "$" already stripped by the
// caller, e.g. "size" for a MetricValue of "$size"). Returns the raw field string, or false
// if no field of that name is defined or the message doesn't have enough fields to align it.
func compileSpaceFilterPatternExtract(pattern string) func(message, fieldRef string) (string, bool) {
	body := strings.TrimSpace(pattern)
	body = strings.TrimPrefix(body, "[")
	body = strings.TrimSuffix(body, "]")
	terms := parseSpaceTerms(body)

	return func(message, fieldRef string) (string, bool) {
		fields := strings.Fields(message)

		idx, ok := spaceFieldIndex(terms, len(fields), fieldRef)
		if !ok {
			return "", false
		}

		return fields[idx], true
	}
}

// spacePatternFieldNames returns the "$"-prefixed named fields declared in a space-delimited
// pattern (skipping the ellipsis and any unnamed term), in declaration order.
func spacePatternFieldNames(pattern string) []string {
	body := strings.TrimSpace(pattern)
	body = strings.TrimPrefix(body, "[")
	body = strings.TrimSuffix(body, "]")

	var out []string
	for _, t := range parseSpaceTerms(body) {
		if t.ellipsis || t.name == "" {
			continue
		}

		out = append(out, "$"+t.name)
	}

	return out
}

// spaceFieldIndex returns the message-field index bound to name, using the same
// ellipsis-alignment rules as matchSpaceTerms.
func spaceFieldIndex(terms []spaceTerm, numFields int, name string) (int, bool) {
	e := ellipsisIndex(terms)
	if e == ellipsisMany {
		return 0, false
	}

	if e == ellipsisNone {
		if numFields != len(terms) {
			return 0, false
		}

		return namedTermIndex(terms, name, func(i int) int { return i })
	}

	if numFields < len(terms)-1 {
		return 0, false
	}

	return namedTermIndex(terms, name, func(i int) int {
		if i < e {
			return i
		}

		return numFields - (len(terms) - i)
	})
}

// namedTermIndex finds name among terms and maps its term index to a message-field index.
func namedTermIndex(terms []spaceTerm, name string, pos func(int) int) (int, bool) {
	for i, t := range terms {
		if !t.ellipsis && t.name == name {
			return pos(i), true
		}
	}

	return 0, false
}
