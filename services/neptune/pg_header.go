package neptune

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type pgKind int

const (
	colProp pgKind = iota
	colID
	colLabel
	colFrom
	colTo
	colType
)

const (
	cardSingle = "single"
	cardSet    = "set"
	typString  = "string"
)

var (
	errPGHeader = errors.New("invalid header")
	errPGValue  = errors.New("invalid value")
)

type pgColumn struct {
	name   string
	typ    string
	card   string
	kind   pgKind
	array  bool
	idProp bool
}

type pgHeader struct {
	cols   []pgColumn
	isEdge bool
}

var gremlinTypes = map[string]bool{
	"bool": true, "boolean": true, "byte": true, "short": true, "int": true, "long": true, "float": true,
	"double": true, "string": true, "date": true, "datetime": true,
}

var cypherTypes = map[string]bool{
	"bool": true, "boolean": true, "byte": true, "short": true, "int": true, "long": true, "float": true,
	"double": true, "string": true, "datetime": true, "char": true, "date": true, "localdate": true,
	"localdatetime": true, "duration": true, "point": true,
}

func pgHeaderErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errPGHeader, fmt.Sprintf(format, a...))
}

func parsePGHeader(fields []csvField, cypher, userEdgeIDs bool) (*pgHeader, error) {
	h := &pgHeader{}
	seen := map[pgKind]bool{}

	for _, f := range fields {
		var (
			c   pgColumn
			err error
		)

		if cypher {
			c, err = parseCypherColumn(strings.TrimSpace(f.val))
		} else {
			c, err = parseGremlinColumn(strings.TrimSpace(f.val))
		}

		if err != nil {
			return nil, err
		}

		if c.kind != colProp {
			if seen[c.kind] {
				return nil, pgHeaderErr("system column %q appears more than once", f.val)
			}

			seen[c.kind] = true
		}

		h.cols = append(h.cols, c)
	}

	h.isEdge = seen[colFrom] || seen[colTo]

	return h, h.validate(seen, cypher, userEdgeIDs)
}

func (h *pgHeader) validate(seen map[pgKind]bool, cypher, userEdgeIDs bool) error {
	switch {
	case !h.isEdge && !seen[colID]:
		return pgHeaderErr("vertex file requires an ID column")
	case h.isEdge && !(seen[colFrom] && seen[colTo]):
		return pgHeaderErr("edge file requires both from and to columns")
	case h.isEdge && !cypher && !seen[colID]:
		return pgHeaderErr("edge file requires a ~id column")
	case h.isEdge && cypher && userEdgeIDs && !seen[colID]:
		return pgHeaderErr("relationship file requires an :ID column when userProvidedEdgeIds is true")
	case h.isEdge && cypher && !userEdgeIDs && seen[colID]:
		return pgHeaderErr("relationship file must not have an :ID column when userProvidedEdgeIds is false")
	case !h.isEdge && (seen[colType]):
		return pgHeaderErr("vertex file cannot have a relationship type column")
	case h.isEdge && cypher && seen[colLabel]:
		return pgHeaderErr("edge file cannot have a node label column")
	}

	if h.isEdge {
		for _, c := range h.cols {
			if c.kind == colProp && (c.array || c.card == cardSet) {
				return pgHeaderErr("edge property %q must be single-valued", c.name)
			}
		}
	}

	return nil
}

func parseGremlinColumn(s string) (pgColumn, error) {
	if strings.HasPrefix(s, "~") {
		switch s {
		case "~id":
			return pgColumn{kind: colID}, nil
		case "~label":
			return pgColumn{kind: colLabel}, nil
		case "~from":
			return pgColumn{kind: colFrom}, nil
		case "~to":
			return pgColumn{kind: colTo}, nil
		}

		return pgColumn{}, pgHeaderErr("unknown system column %q", s)
	}

	name, rest := s, ""

	if i := lastUnescapedColon(s); i >= 0 {
		name, rest = s[:i], s[i+1:]
	}

	name = strings.ReplaceAll(name, `\:`, ":")
	if name == "" {
		return pgColumn{}, pgHeaderErr("empty property name in %q", s)
	}

	c := pgColumn{name: name, typ: typString}
	if rest == "" {
		return c, nil
	}

	if strings.HasSuffix(rest, "[]") {
		c.array, rest = true, strings.TrimSuffix(rest, "[]")
	}

	if strings.HasSuffix(rest, ")") {
		i := strings.LastIndex(rest, "(")
		if i < 0 {
			return pgColumn{}, pgHeaderErr("malformed cardinality in %q", s)
		}

		c.card = strings.ToLower(rest[i+1 : len(rest)-1])
		rest = rest[:i]

		if c.card != cardSingle && c.card != cardSet {
			return pgColumn{}, pgHeaderErr("cardinality of %q must be single or set", s)
		}

		if c.card == cardSingle && c.array {
			return pgColumn{}, pgHeaderErr("single cardinality with an array type in %q", s)
		}
	}

	c.typ = strings.ToLower(rest)
	if !gremlinTypes[c.typ] {
		return pgColumn{}, pgHeaderErr("unsupported data type %q in %q", rest, s)
	}

	return c, nil
}

func parseCypherColumn(s string) (pgColumn, error) {
	i := lastUnescapedColon(s)
	if i < 0 {
		if s == "" {
			return pgColumn{}, pgHeaderErr("empty column name")
		}

		return pgColumn{name: s, typ: typString}, nil
	}

	name, rest := strings.ReplaceAll(s[:i], `\:`, ":"), s[i+1:]

	if rest == "ID" || strings.HasPrefix(rest, "ID(") {
		return pgColumn{kind: colID, name: name, idProp: name != "", typ: typString}, nil
	}

	if name == "" {
		switch {
		case rest == "LABEL":
			return pgColumn{kind: colLabel}, nil
		case rest == "TYPE":
			return pgColumn{kind: colType}, nil
		case rest == "START_ID" || strings.HasPrefix(rest, "START_ID("):
			return pgColumn{kind: colFrom}, nil
		case rest == "END_ID" || strings.HasPrefix(rest, "END_ID("):
			return pgColumn{kind: colTo}, nil
		}

		return pgColumn{}, pgHeaderErr("unknown system column %q", s)
	}

	if strings.HasSuffix(rest, "[]") {
		return pgColumn{}, pgHeaderErr("openCypher property %q cannot be an array", name)
	}

	typ := strings.ToLower(rest)
	if !cypherTypes[typ] {
		return pgColumn{}, pgHeaderErr("unsupported data type %q in %q", rest, s)
	}

	return pgColumn{name: name, typ: typ}, nil
}

var dateLayouts = []string{"2006-01-02", "2006-01-02T15:04", "2006-01-02T15:04:05", "2006-01-02T15:04:05Z07:00"}

// convertPGValue parses s as the Neptune CSV type typ. Dates are kept as UTC ISO-8601 strings because the
// property store has no temporal value; the auto-cast openCypher types are stored verbatim.
func convertPGValue(typ, s string, cypher bool) (any, error) {
	switch typ {
	case "bool", "boolean":
		return strings.EqualFold(s, "true"), nil
	case "byte":
		return parsePGInt(s, 8)
	case "short":
		return parsePGInt(s, 16)
	case "int":
		return parsePGInt(s, 32)
	case "long":
		return parsePGInt(s, 64)
	case "float":
		return parsePGFloat(s, 32)
	case "double":
		return parsePGFloat(s, 64)
	case "date", "datetime":
		if cypher && typ == "date" {
			return s, nil
		}

		for _, l := range dateLayouts {
			if t, err := time.Parse(l, s); err == nil {
				return t.UTC().Format("2006-01-02T15:04:05Z"), nil
			}
		}

		return nil, fmt.Errorf("%w: %q is not an ISO-8601 date", errPGValue, s)
	default:
		return s, nil
	}
}

func parsePGInt(s string, bits int) (any, error) {
	n, err := strconv.ParseInt(s, 10, bits)
	if err != nil {
		return nil, fmt.Errorf("%w: %q is not a %d-bit integer", errPGValue, s, bits)
	}

	return n, nil
}

func parsePGFloat(s string, bits int) (any, error) {
	if strings.EqualFold(strings.TrimLeft(s, "+-"), "inf") {
		return nil, fmt.Errorf("%w: INF is not supported, use Infinity", errPGValue)
	}

	f, err := strconv.ParseFloat(s, bits)
	if err != nil && !math.IsInf(f, 0) {
		return nil, fmt.Errorf("%w: %q is not a %d-bit float", errPGValue, s, bits)
	}

	return f, nil
}
