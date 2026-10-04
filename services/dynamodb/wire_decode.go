package dynamodb

import (
	"bytes"
	"encoding/base64"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	wireTableName     = "TableName"
	maxDecodeDepth    = 64
	asciiPrintable    = 0x20
	escapeLen         = 2
	listCapHint       = 4
	decimalBase       = 10
	maxInt32Magnitude = 1 << 31
	supplementaryBase = 0x10000
)

// wireReader is a strict single-pass JSON cursor. Every method returns false on anything
// unusual so callers fall back to the reflection-based decoder, which owns error semantics.
type wireReader struct {
	b []byte
	i int
}

func (r *wireReader) skipWS() {
	for r.i < len(r.b) {
		switch r.b[r.i] {
		case ' ', '\t', '\n', '\r':
			r.i++
		default:
			return
		}
	}
}

func (r *wireReader) peek() byte {
	r.skipWS()

	if r.i >= len(r.b) {
		return 0
	}

	return r.b[r.i]
}

func (r *wireReader) consume(c byte) bool {
	if r.peek() != c {
		return false
	}

	r.i++

	return true
}

func (r *wireReader) atEnd() bool {
	r.skipWS()

	return r.i == len(r.b)
}

func (r *wireReader) literal(lit string) bool {
	if len(r.b)-r.i < len(lit) || string(r.b[r.i:r.i+len(lit)]) != lit {
		return false
	}

	r.i += len(lit)

	return true
}

func (r *wireReader) boolean() (bool, bool) {
	switch r.peek() {
	case 't':
		return true, r.literal("true")
	case 'f':
		return false, r.literal("false")
	default:
		return false, false
	}
}

// rawString returns the string's raw bytes between quotes (aliasing the input) and whether
// it contains escapes. ok is false for control characters, bad UTF-8, or an unterminated string.
func (r *wireReader) rawString() ([]byte, bool, bool) {
	if !r.consume('"') {
		return nil, false, false
	}

	start := r.i
	escaped, nonASCII := false, false

	for r.i < len(r.b) {
		c := r.b[r.i]

		switch {
		case c == '"':
			span := r.b[start:r.i]
			r.i++

			if nonASCII && !utf8.Valid(span) {
				return nil, false, false
			}

			return span, escaped, true
		case c == '\\':
			escaped = true
			r.i += escapeLen
		case c < asciiPrintable:
			return nil, false, false
		case c >= utf8.RuneSelf:
			nonASCII = true
			r.i++
		default:
			r.i += r.skipPlain()
		}
	}

	return nil, false, false
}

// skipPlain advances over a run of plain ASCII bytes and returns its length (at least 1).
func (r *wireReader) skipPlain() int {
	n := 1

	for r.i+n < len(r.b) {
		c := r.b[r.i+n]
		if c == '"' || c == '\\' || c < asciiPrintable || c >= utf8.RuneSelf {
			break
		}

		n++
	}

	return n
}

// str decodes a JSON string into a freshly allocated Go string.
func (r *wireReader) str() (string, bool) {
	span, escaped, ok := r.rawString()
	if !ok {
		return "", false
	}

	if !escaped {
		return string(span), true
	}

	return unescapeJSON(span)
}

func unescapeJSON(span []byte) (string, bool) {
	out := make([]byte, 0, len(span))

	for i := 0; i < len(span); i++ {
		c := span[i]
		if c != '\\' {
			out = append(out, c)

			continue
		}

		i++
		if i >= len(span) {
			return "", false
		}

		switch span[i] {
		case '"', '\\', '/':
			out = append(out, span[i])
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'u':
			r, n, ok := decodeUnicodeEscape(span[i+1:])
			if !ok {
				return "", false
			}

			out = utf8.AppendRune(out, r)
			i += n
		default:
			return "", false
		}
	}

	return string(out), true
}

const (
	hexQuad       = 4
	surrogateLow  = 0xDC00
	surrogateHigh = 0xD800
	surrogateEnd  = 0xE000
	escapeAdvance = 6
)

// decodeUnicodeEscape decodes XXXX (and a following \uXXXX low surrogate when needed).
// n is the number of bytes consumed after the 'u'.
func decodeUnicodeEscape(s []byte) (rune, int, bool) {
	hi, ok := parseHex4(s)
	if !ok {
		return 0, 0, false
	}

	switch {
	case hi >= surrogateHigh && hi < surrogateLow:
		if len(s) < hexQuad+escapeAdvance || s[hexQuad] != '\\' || s[hexQuad+1] != 'u' {
			return 0, 0, false
		}

		lo, okLo := parseHex4(s[hexQuad+2:])
		if !okLo || lo < surrogateLow || lo >= surrogateEnd {
			return 0, 0, false
		}

		return (hi-surrogateHigh)<<10 | (lo - surrogateLow) + supplementaryBase, hexQuad + escapeAdvance, true
	case hi >= surrogateLow && hi < surrogateEnd:
		return 0, 0, false
	default:
		return hi, hexQuad, true
	}
}

func parseHex4(s []byte) (rune, bool) {
	if len(s) < hexQuad {
		return 0, false
	}

	var v rune

	for _, c := range s[:hexQuad] {
		v <<= 4

		switch {
		case c >= '0' && c <= '9':
			v |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			v |= rune(c-'a') + decimalBase
		case c >= 'A' && c <= 'F':
			v |= rune(c-'A') + decimalBase
		default:
			return 0, false
		}
	}

	return v, true
}

// int32Val parses a plain base-10 integer that fits int32.
func (r *wireReader) int32Val() (int32, bool) {
	r.skipWS()

	neg := r.i < len(r.b) && r.b[r.i] == '-'
	if neg {
		r.i++
	}

	v, ok := r.digits()
	if !ok || (r.i < len(r.b) && (r.b[r.i] == '.' || r.b[r.i] == 'e' || r.b[r.i] == 'E')) {
		return 0, false
	}

	if neg {
		v = -v
	}

	if v > math.MaxInt32 || v < math.MinInt32 {
		return 0, false
	}

	return int32(v), true
}

// digits reads an unsigned integer without leading zeros, bailing once it exceeds int32 range.
func (r *wireReader) digits() (int64, bool) {
	start := r.i

	var v int64

	for r.i < len(r.b) && r.b[r.i] >= '0' && r.b[r.i] <= '9' {
		v = v*decimalBase + int64(r.b[r.i]-'0')
		r.i++

		if v > maxInt32Magnitude {
			return 0, false
		}
	}

	n := r.i - start
	if n == 0 || (n > 1 && r.b[start] == '0') {
		return 0, false
	}

	return v, true
}

// members iterates the members of a JSON object; fn must consume the value for key.
func (r *wireReader) members(fn func(key []byte, escaped bool) bool) bool {
	if !r.consume('{') {
		return false
	}

	if r.consume('}') {
		return true
	}

	for {
		span, escaped, ok := r.rawString()
		if !ok || !r.consume(':') || !fn(span, escaped) {
			return false
		}

		if r.consume('}') {
			return true
		}

		if !r.consume(',') {
			return false
		}
	}
}

// elements iterates a JSON array; fn must consume one element.
func (r *wireReader) elements(fn func() bool) bool {
	if !r.consume('[') {
		return false
	}

	if r.consume(']') {
		return true
	}

	for {
		if !fn() {
			return false
		}

		if r.consume(']') {
			return true
		}

		if !r.consume(',') {
			return false
		}
	}
}

func (r *wireReader) stringList() ([]string, bool) {
	out := []string{}

	ok := r.elements(func() bool {
		s, good := r.str()
		out = append(out, s)

		return good
	})

	return out, ok
}

func (r *wireReader) stringMap() (map[string]string, bool) {
	out := map[string]string{}

	ok := r.members(func(key []byte, escaped bool) bool {
		k, good := r.keyString(key, escaped)
		if !good {
			return false
		}

		v, good := r.str()
		out[k] = v

		return good
	})

	return out, ok
}

func (r *wireReader) keyString(span []byte, escaped bool) (string, bool) {
	if !escaped {
		return string(span), true
	}

	return unescapeJSON(span)
}

// attrMap decodes a map of attribute name to wire AttributeValue into SDK values.
func (r *wireReader) attrMap(depth int) (map[string]types.AttributeValue, bool) {
	if depth > maxDecodeDepth {
		return nil, false
	}

	out := make(map[string]types.AttributeValue)

	ok := r.members(func(key []byte, escaped bool) bool {
		k, good := r.keyString(key, escaped)
		if !good {
			return false
		}

		av, good := r.attrValue(depth + 1)
		out[k] = av

		return good
	})

	return out, ok
}

// attrValue decodes exactly one single-key wire AttributeValue object.
func (r *wireReader) attrValue(depth int) (types.AttributeValue, bool) {
	if depth > maxDecodeDepth || !r.consume('{') {
		return nil, false
	}

	key, escaped, ok := r.rawString()
	if !ok || escaped || !r.consume(':') {
		return nil, false
	}

	av, ok := r.typedValue(string(key), depth)
	if !ok || !r.consume('}') {
		return nil, false
	}

	return av, true
}

func (r *wireReader) typedValue(typeKey string, depth int) (types.AttributeValue, bool) {
	switch typeKey {
	case "S":
		s, ok := r.str()

		return &types.AttributeValueMemberS{Value: s}, ok
	case "N":
		s, ok := r.str()

		return &types.AttributeValueMemberN{Value: s}, ok
	case typeBOOL:
		b, ok := r.boolean()

		return &types.AttributeValueMemberBOOL{Value: b}, ok
	case typeNULL:
		b, ok := r.boolean()

		return &types.AttributeValueMemberNULL{Value: b}, ok
	case "B":
		b, ok := r.binary()

		return &types.AttributeValueMemberB{Value: b}, ok
	case "M":
		m, ok := r.attrMap(depth)

		return &types.AttributeValueMemberM{Value: m}, ok
	case "L":
		return r.listValue(depth)
	case "SS", "NS":
		return r.setValue(typeKey)
	case "BS":
		return r.binarySetValue()
	default:
		return nil, false
	}
}

func (r *wireReader) binary() ([]byte, bool) {
	s, ok := r.str()
	if !ok {
		return nil, false
	}

	b, err := base64.StdEncoding.DecodeString(s)

	return b, err == nil
}

func (r *wireReader) listValue(depth int) (types.AttributeValue, bool) {
	out := make([]types.AttributeValue, 0, listCapHint)

	ok := r.elements(func() bool {
		av, good := r.attrValue(depth + 1)
		out = append(out, av)

		return good
	})

	return &types.AttributeValueMemberL{Value: out}, ok
}

func (r *wireReader) setValue(typeKey string) (types.AttributeValue, bool) {
	var out []string

	ok := r.elements(func() bool {
		s, good := r.str()
		out = append(out, s)

		return good
	})

	if typeKey == "SS" {
		return &types.AttributeValueMemberSS{Value: out}, ok
	}

	return &types.AttributeValueMemberNS{Value: out}, ok
}

func (r *wireReader) binarySetValue() (types.AttributeValue, bool) {
	var out [][]byte

	ok := r.elements(func() bool {
		b, good := r.binary()
		out = append(out, b)

		return good
	})

	return &types.AttributeValueMemberBS{Value: out}, ok
}

// skipString advances past a string using vectorised searches; it does not validate contents.
func (r *wireReader) skipString() bool {
	if !r.consume('"') {
		return false
	}

	for {
		q := bytes.IndexByte(r.b[r.i:], '"')
		if q < 0 {
			return false
		}

		span := r.b[r.i : r.i+q]
		bs := bytes.IndexByte(span, '\\')

		if bs < 0 {
			r.i += q + 1

			return true
		}

		r.i += bs + escapeLen
		if r.i > len(r.b) {
			return false
		}
	}
}

// skipValue advances past one JSON value without building it.
func (r *wireReader) skipValue(depth int) bool {
	if depth > maxDecodeDepth {
		return false
	}

	switch r.peek() {
	case '"':
		return r.skipString()
	case '{':
		return r.members(func(_ []byte, _ bool) bool { return r.skipValue(depth + 1) })
	case '[':
		return r.elements(func() bool { return r.skipValue(depth + 1) })
	case 't':
		return r.literal("true")
	case 'f':
		return r.literal("false")
	case 'n':
		return r.literal("null")
	default:
		return r.skipNumber()
	}
}

func (r *wireReader) skipNumber() bool {
	start := r.i

	for r.i < len(r.b) {
		c := r.b[r.i]
		if (c < '0' || c > '9') && c != '-' && c != '+' && c != '.' && c != 'e' && c != 'E' {
			break
		}

		r.i++
	}

	return r.i > start
}

// topLevelStrings returns the top-level TableName and BackupArn string members of a JSON
// object. ok is false when the body needs the reflection decoder's exact semantics.
func topLevelStrings(body []byte) (string, string, bool) {
	r := wireReader{b: body}

	var tableName, backupArn string

	ok := r.members(func(key []byte, escaped bool) bool {
		if escaped {
			return false
		}

		switch string(key) {
		case wireTableName:
			s, good := r.str()
			tableName = s

			return good
		case "BackupArn":
			s, good := r.str()
			backupArn = s

			return good
		default:
			return !looksLikeResourceKey(key) && r.skipValue(0)
		}
	})

	return tableName, backupArn, ok && r.atEnd()
}

// looksLikeResourceKey flags case-variant spellings that encoding/json would still match.
func looksLikeResourceKey(key []byte) bool {
	return strings.EqualFold(string(key), wireTableName) || strings.EqualFold(string(key), "BackupArn")
}
