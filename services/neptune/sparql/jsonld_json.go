package sparql

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
)

const (
	jsonLDDirection  = "@direction"
	ldJSONType       = "@json"
	ldMaxJSONDepth   = 512
	ldESMaxExp       = 21
	ldESMinExp       = -6
	ldFirstPrintable = 0x20
)

// rdfJSONDatatype is the rdf:JSON datatype IRI of JSON-LD 1.1 @json literals.
const rdfJSONDatatype = NSRDF + "JSON"

// ldCanonicalJSON serialises v per RFC 8785 (JSON Canonicalization Scheme).
func ldCanonicalJSON(v any) (string, error) {
	var sb strings.Builder

	if err := ldCanonWrite(&sb, v, 0); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func ldCanonWrite(sb *strings.Builder, v any, depth int) error {
	if depth > ldMaxJSONDepth {
		return fmt.Errorf("%w: @json value nested too deeply", errJSONLD)
	}

	switch t := v.(type) {
	case ldNull, nil:
		sb.WriteString("null")
	case bool:
		sb.WriteString(strconv.FormatBool(t))
	case string:
		ldCanonString(sb, t)
	case ldNum:
		f, err := strconv.ParseFloat(string(t), 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return fmt.Errorf("%w: @json number %q is not an IEEE 754 double", errJSONLD, string(t))
		}

		sb.WriteString(ldESNumber(f))
	case []any:
		sb.WriteByte('[')

		for i, e := range t {
			if i > 0 {
				sb.WriteByte(',')
			}

			if err := ldCanonWrite(sb, e, depth+1); err != nil {
				return err
			}
		}

		sb.WriteByte(']')
	case map[string]any:
		return ldCanonObject(sb, t, depth)
	default:
		return fmt.Errorf("%w: unsupported @json value", errJSONLD)
	}

	return nil
}

func ldCanonObject(sb *strings.Builder, m map[string]any, depth int) error {
	type kv struct {
		key string
		u16 []uint16
	}

	keys := make([]kv, 0, len(m))
	for k := range m {
		keys = append(keys, kv{key: k, u16: utf16.Encode([]rune(k))})
	}

	sort.Slice(keys, func(i, j int) bool { return slices.Compare(keys[i].u16, keys[j].u16) < 0 })
	sb.WriteByte('{')

	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}

		ldCanonString(sb, k.key)
		sb.WriteByte(':')

		if err := ldCanonWrite(sb, m[k.key], depth+1); err != nil {
			return err
		}
	}

	sb.WriteByte('}')

	return nil
}

func ldCanonString(sb *strings.Builder, s string) {
	const hexDigits = "0123456789abcdef"

	sb.WriteByte('"')

	for _, r := range s {
		switch {
		case r == '"':
			sb.WriteString(`\"`)
		case r == '\\':
			sb.WriteString(`\\`)
		case r == '\b':
			sb.WriteString(`\b`)
		case r == '\t':
			sb.WriteString(`\t`)
		case r == '\n':
			sb.WriteString(`\n`)
		case r == '\f':
			sb.WriteString(`\f`)
		case r == '\r':
			sb.WriteString(`\r`)
		case r < ldFirstPrintable:
			sb.WriteString(`\u00`)
			sb.WriteByte(hexDigits[r>>4])
			sb.WriteByte(hexDigits[r&0xF])
		default:
			sb.WriteRune(r)
		}
	}

	sb.WriteByte('"')
}

// ldESNumber formats f as ECMAScript Number::toString (RFC 8785 section 3.2.2.3).
func ldESNumber(f float64) string {
	if f == 0 {
		return "0"
	}

	var sb strings.Builder

	if f < 0 {
		sb.WriteByte('-')

		f = -f
	}

	mant, expStr, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, 64), "e")
	digits := strings.Replace(mant, ".", "", 1)
	exp, _ := strconv.Atoi(expStr)
	k, n := len(digits), exp+1

	switch {
	case k <= n && n <= ldESMaxExp:
		sb.WriteString(digits)
		sb.WriteString(strings.Repeat("0", n-k))
	case 0 < n && n <= ldESMaxExp:
		sb.WriteString(digits[:n])
		sb.WriteByte('.')
		sb.WriteString(digits[n:])
	case ldESMinExp < n && n <= 0:
		sb.WriteString("0.")
		sb.WriteString(strings.Repeat("0", -n))
		sb.WriteString(digits)
	default:
		sb.WriteByte(digits[0])

		if k > 1 {
			sb.WriteByte('.')
			sb.WriteString(digits[1:])
		}

		sb.WriteByte('e')

		if n-1 >= 0 {
			sb.WriteByte('+')
		}

		sb.WriteString(strconv.Itoa(n - 1))
	}

	return sb.String()
}

func ldJSONLiteral(v any) (Term, error) {
	s, err := ldCanonicalJSON(v)
	if err != nil {
		return Term{}, err
	}

	return Typed(s, rdfJSONDatatype), nil
}

// ldCheckDirection validates a value object's @direction; toRdf without rdfDirection drops it.
func ldCheckDirection(m map[string]any) error {
	d, ok := m[jsonLDDirection]
	if !ok {
		return nil
	}

	switch s := d.(type) {
	case ldNull:
		return nil
	case string:
		if s == "ltr" || s == "rtl" {
			return nil
		}
	}

	return fmt.Errorf("%w: invalid base direction", errJSONLD)
}
