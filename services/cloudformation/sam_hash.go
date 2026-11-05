package cloudformation

import (
	"bytes"
	"crypto/sha1" //nolint:gosec // SAM's LogicalIdGenerator contract is SHA-1; not a security use
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	samDigestLen   = 40
	hexDigits      = "0123456789abcdef"
	utf16BMPLimit  = 0xFFFF
	utf16Supp      = 0x10000
	utf16HighBase  = 0xD800
	utf16LowBase   = 0xDC00
	utf16SurrMask  = 0x3FF
	utf16SurrShift = 10
	samHashRegion  = "us-east-1"
)

// samDigest is SAM's LogicalIdGenerator digest: SHA-1 over a string as-is, or
// over the compact sorted-key ASCII JSON of anything else.
func samDigest(data any) string {
	var raw string
	if s, ok := data.(string); ok {
		raw = s
	} else {
		raw = pyJSON(data)
	}
	sum := sha1.Sum([]byte(raw)) //nolint:gosec // see import

	return hex.EncodeToString(sum[:])
}

// samLogicalID is prefix + the first samHashLen digest characters. An empty
// data object yields the bare prefix, as in SAM.
func samLogicalID(prefix string, data any) string {
	if samEmpty(data) {
		return prefix
	}

	return prefix + samDigest(data)[:samHashLen]
}

func samEmpty(data any) bool {
	switch t := data.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	}

	return false
}

// pyJSON matches Python's json.dumps(separators=(",", ":"), sort_keys=True).
func pyJSON(v any) string {
	var b bytes.Buffer
	pyJSONValue(&b, v)

	return b.String()
}

func pyJSONValue(b *bytes.Buffer, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(t))
	case string:
		pyJSONString(b, t)
	case float64:
		b.WriteString(pyJSONNumber(t))
	case int:
		b.WriteString(strconv.Itoa(t))
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			pyJSONValue(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		b.WriteByte('{')
		for i, k := range slices.Sorted(maps.Keys(t)) {
			if i > 0 {
				b.WriteByte(',')
			}
			pyJSONString(b, k)
			b.WriteByte(':')
			pyJSONValue(b, t[k])
		}
		b.WriteByte('}')
	default:
		fmt.Fprintf(b, "%v", t)
	}
}

func pyJSONNumber(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}

	return strconv.FormatFloat(f, 'g', -1, 64)
}

func pyJSONString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r < ' ' || r == 0x7f:
			pyUnicodeEscape(b, r)
		case r >= utf8.RuneSelf:
			if r > utf16BMPLimit {
				r -= utf16Supp
				pyUnicodeEscape(b, utf16HighBase+(r>>utf16SurrShift))
				pyUnicodeEscape(b, utf16LowBase+(r&utf16SurrMask))
			} else {
				pyUnicodeEscape(b, r)
			}
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}

func pyUnicodeEscape(b *bytes.Buffer, r rune) {
	b.WriteString(`\u`)
	for shift := 12; shift >= 0; shift -= 4 {
		b.WriteByte(hexDigits[(r>>uint(shift))&0xF])
	}
}

// samPartitionLiteral renders the partition-neutral "${AWS::Partition}" the
// way SAM hashes it: the literal "aws" partition.
func samPartitionLiteral(v any) any {
	switch t := v.(type) {
	case string:
		return strings.ReplaceAll(t, "arn:${AWS::Partition}:", "arn:"+arn.PartitionForRegion(samHashRegion)+":")
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = samPartitionLiteral(e)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = samPartitionLiteral(e)
		}

		return out
	}

	return v
}
