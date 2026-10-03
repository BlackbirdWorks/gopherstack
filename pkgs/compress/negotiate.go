package compress

import (
	"strconv"
	"strings"
)

// Encoding is a supported content coding token.
type Encoding string

// Supported content codings.
const (
	Zstd   Encoding = "zstd"
	Brotli Encoding = "br"
	Gzip   Encoding = "gzip"
)

const acceptEncoding = "Accept-Encoding"

const numEncodings = 3

// preference is the server's order when client weights tie.
func preference() [numEncodings]Encoding { return [numEncodings]Encoding{Zstd, Brotli, Gzip} }

type prefs struct {
	qs       [numEncodings]float64
	seen     [numEncodings]bool
	star     float64
	identity float64
}

func parsePrefs(values []string) prefs {
	p := prefs{star: -1, identity: -1}

	for _, v := range values {
		for part := range strings.SplitSeq(v, ",") {
			name, q, ok := parseCoding(part)
			if !ok {
				continue
			}

			switch name {
			case "*":
				p.star = q
			case "identity":
				p.identity = q
			default:
				if i := indexOf(name); i >= 0 {
					p.qs[i], p.seen[i] = q, true
				}
			}
		}
	}

	return p
}

// Negotiate picks the response coding for an Accept-Encoding header per RFC 9110
// section 12.5.3; enabled filters the server-supported set. Returns "" for identity.
func Negotiate(values []string, enabled func(Encoding) bool) Encoding {
	if len(values) == 0 {
		return ""
	}

	p := parsePrefs(values)
	best, bestQ := Encoding(""), 0.0

	for i, enc := range preference() {
		if !enabled(enc) {
			continue
		}

		q := p.qs[i]
		if !p.seen[i] {
			q = max(p.star, 0)
		}

		if q > bestQ {
			best, bestQ = enc, q
		}
	}

	if best == "" || p.identity > bestQ {
		return ""
	}

	return best
}

func indexOf(name string) int {
	if name == "x-gzip" {
		name = string(Gzip)
	}

	for i, enc := range preference() {
		if string(enc) == name {
			return i
		}
	}

	return -1
}

func parseCoding(part string) (string, float64, bool) {
	name, params, _ := strings.Cut(part, ";")

	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "", 0, false
	}

	q := 1.0

	for p := range strings.SplitSeq(params, ";") {
		k, val, found := strings.Cut(p, "=")
		if !found || !strings.EqualFold(strings.TrimSpace(k), "q") {
			continue
		}

		f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil {
			return "", 0, false
		}

		q = min(max(f, 0), 1)
	}

	return name, q, true
}
