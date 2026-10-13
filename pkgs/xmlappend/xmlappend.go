// Package xmlappend provides allocation-free XML text/attribute escaping and
// element helpers, byte-identical to encoding/xml's output, for hot response
// encoders.
package xmlappend

import (
	"encoding/xml"
	"unicode/utf8"
)

// Escaped appends s to buf escaped exactly as encoding/xml escapes character
// data and attribute values.
func Escaped(buf []byte, s string) []byte {
	if needsSlowPath(s) {
		var w sliceWriter
		_ = xml.EscapeText(&w, []byte(s))

		return append(buf, w...)
	}

	start := 0

	for i := range len(s) {
		esc := escapeSeq(s[i])
		if esc == "" {
			continue
		}

		buf = append(buf, s[start:i]...)
		buf = append(buf, esc...)
		start = i + 1
	}

	return append(buf, s[start:]...)
}

// needsSlowPath reports whether s holds bytes (non-ASCII, or control
// characters encoding/xml rewrites) the single-byte table cannot handle.
func needsSlowPath(s string) bool {
	for i := range len(s) {
		if c := s[i]; c >= utf8.RuneSelf || (c < ' ' && c != '\t' && c != '\n' && c != '\r') {
			return true
		}
	}

	return false
}

func escapeSeq(c byte) string {
	switch c {
	case '"':
		return "&#34;"
	case '\'':
		return "&#39;"
	case '&':
		return "&amp;"
	case '<':
		return "&lt;"
	case '>':
		return "&gt;"
	case '\t':
		return "&#x9;"
	case '\n':
		return "&#xA;"
	case '\r':
		return "&#xD;"
	default:
		return ""
	}
}

// Elem appends <name>escaped(val)</name> to buf.
func Elem(buf []byte, name, val string) []byte {
	buf = append(buf, '<')
	buf = append(buf, name...)
	buf = append(buf, '>')
	buf = Escaped(buf, val)
	buf = append(buf, '<', '/')
	buf = append(buf, name...)

	return append(buf, '>')
}

type sliceWriter []byte

func (w *sliceWriter) Write(p []byte) (int, error) {
	*w = append(*w, p...)

	return len(p), nil
}
