package timestreamquery

import "strings"

// funcPrefix namespaces every SQLite function this package registers: the driver registry is process-wide and
// shared with every other engine, so a plain name would be callable (or shadow a built-in) everywhere.
const funcPrefix = "ts_"

// plainFuncs holds the documented Timestream names registered under funcPrefix; written only during registration.
//
//nolint:gochecknoglobals // filled once at init, read-only afterwards
var plainFuncs = map[string]struct{}{}

// engineFunc returns the registered SQLite name for a Timestream function and records the plain name.
func engineFunc(name string) string {
	if strings.HasPrefix(name, funcPrefix) {
		return name
	}

	plainFuncs[name] = struct{}{}

	return funcPrefix + name
}

// qualifyEngineFuncs renames calls to Timestream-only functions in engine SQL to their prefixed registrations,
// leaving string literals, quoted identifiers, comments and member access alone.
func qualifyEngineFuncs(q string) string {
	var b strings.Builder

	for i := 0; i < len(q); {
		switch c := q[i]; {
		case c == '\'' || c == '"' || c == '`':
			j := closingQuote(q, i)
			b.WriteString(q[i:j])
			i = j
		case skipComment(q, i) != i:
			j := skipComment(q, i)
			b.WriteString(q[i:j])
			i = j
		case isIdentStart(c):
			j := i + 1
			for j < len(q) && isIdentPart(q[j]) {
				j++
			}

			word := q[i:j]
			if _, ok := plainFuncs[strings.ToLower(word)]; ok && calledAt(q, i, j) {
				b.WriteString(funcPrefix)
			}

			b.WriteString(word)
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}

	return b.String()
}

// calledAt reports whether q[start:end] is a bare function name: followed by "(" and not a member of something.
func calledAt(q string, start, end int) bool {
	if start > 0 && q[start-1] == '.' {
		return false
	}

	for end < len(q) && isSpaceByte(q[end]) {
		end++
	}

	return end < len(q) && q[end] == '('
}
