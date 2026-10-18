package apptrace

import (
	"bytes"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Redacted replaces every secret value.
const Redacted = "[REDACTED]"

const sensitiveKeyRe = `[A-Za-z0-9_.\-]*` +
	`(?:password|passwd|secret|token|privatekey|apikey|credential|plaintext|signature)` +
	`[A-Za-z0-9_.\-]*`

type redactor struct {
	jsonRe *regexp.Regexp
	formRe *regexp.Regexp
	xmlRe  *regexp.Regexp

	jsonCode *regexp.Regexp
	xmlCode  *regexp.Regexp
}

func newRedactor() *redactor {
	return &redactor{
		jsonRe: regexp.MustCompile(`(?i)("(` + sensitiveKeyRe + `)"\s*:\s*)"(?:[^"\\]|\\.)*(?:"|$)`),
		formRe: regexp.MustCompile(`(?i)((?:^|[&?;])(` + sensitiveKeyRe + `)=)[^&;]*`),
		xmlRe:  regexp.MustCompile(`(?i)(<(` + sensitiveKeyRe + `)>)[^<]*`),

		jsonCode: regexp.MustCompile(`"(?:__type|Code|code)"\s*:\s*"([^"]+)"`),
		xmlCode:  regexp.MustCompile(`<Code>([^<]+)</Code>`),
	}
}

// benignKey reports token-named fields that are counters or paging cursors.
func benignKey(key string) bool {
	k := strings.ToLower(key)

	for _, b := range [...]string{
		"nexttoken", "continuationtoken", "pagetoken", "marker", "maxtokens",
		"inputtokens", "outputtokens", "totaltokens", "tokencount",
	} {
		if strings.Contains(k, b) {
			return true
		}
	}

	return false
}

// replaceKeyed redacts the value part of each match whose key (group 2) is
// not benign; group 1 is the kept prefix.
func replaceKeyed(re *regexp.Regexp, s, repl string) string {
	idx := re.FindAllStringSubmatchIndex(s, -1)
	if len(idx) == 0 {
		return s
	}

	var b strings.Builder

	last := 0

	for _, m := range idx {
		if benignKey(s[m[4]:m[5]]) {
			continue
		}

		b.WriteString(s[last:m[3]])
		b.WriteString(repl)

		last = m[1]
	}

	b.WriteString(s[last:])

	return b.String()
}

// sensitiveHeader reports whether a header value must never be stored.
func sensitiveHeader(name string) bool {
	n := strings.ToLower(name)

	for _, frag := range [...]string{
		"authorization", "token", "secret", "password", "cookie", "signature",
		"credential", "api-key", "customer-key",
	} {
		if strings.Contains(n, frag) {
			return true
		}
	}

	return false
}

func (r *redactor) headers(h http.Header) map[string]string {
	out := make(map[string]string, len(h))

	for k, v := range h {
		if len(v) == 0 {
			continue
		}

		if sensitiveHeader(k) {
			out[k] = Redacted

			continue
		}

		out[k] = strings.Join(v, ", ")
	}

	return out
}

func (r *redactor) query(raw string) string {
	return replaceKeyed(r.formRe, raw, Redacted)
}

// trimPartialRune drops an incomplete UTF-8 sequence left by truncation.
func trimPartialRune(b []byte) []byte {
	for i := 0; i < utf8.UTFMax && len(b) > 0; i++ {
		if utf8.Valid(b) {
			return b
		}

		b = b[:len(b)-1]
	}

	return b
}

// body renders a captured body as redacted text; binary payloads collapse to
// a size note.
func (r *redactor) body(b []byte, total int64, encoded bool) string {
	if len(b) == 0 {
		return ""
	}

	if encoded {
		return "[compressed body, " + strconv.FormatInt(total, 10) + " bytes]"
	}

	b = trimPartialRune(b)
	if !utf8.Valid(b) || bytes.IndexByte(b, 0) >= 0 {
		return "[binary body, " + strconv.FormatInt(total, 10) + " bytes]"
	}

	s := string(b)
	s = replaceKeyed(r.jsonRe, s, `"`+Redacted+`"`)
	s = replaceKeyed(r.formRe, s, Redacted)

	return replaceKeyed(r.xmlRe, s, Redacted)
}
