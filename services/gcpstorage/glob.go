package gcpstorage

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

const maxGlobBytes = 1024

// compileGlob translates a Cloud Storage matchGlob pattern
// (docs.cloud.google.com/storage/docs/json_api/v1/objects/list) into an anchored regexp.
func compileGlob(pat string) (*regexp.Regexp, *APIError) {
	if len(pat) > maxGlobBytes {
		return nil, errInvalid("matchGlob must be at most 1024 bytes.")
	}

	g := &globParser{src: pat}

	expr, apiErr := g.sequence(0)
	if apiErr != nil {
		return nil, apiErr
	}

	re, err := regexp.Compile(`(?s)^` + expr + `$`)
	if err != nil {
		return nil, errInvalid("Invalid matchGlob.")
	}

	return re, nil
}

type globParser struct {
	src string
	pos int
}

func errGlob() *APIError { return errInvalid("Invalid matchGlob pattern.") }

// sequence parses until end of input or, inside braces (depth > 0), a ',' or '}' left for the caller.
func (g *globParser) sequence(depth int) (string, *APIError) {
	var sb strings.Builder

	for g.pos < len(g.src) {
		c := g.src[g.pos]

		if depth > 0 && (c == ',' || c == '}') {
			return sb.String(), nil
		}

		part, apiErr := g.atom(c, depth)
		if apiErr != nil {
			return "", apiErr
		}

		sb.WriteString(part)
	}

	if depth > 0 {
		return "", errGlob()
	}

	return sb.String(), nil
}

func (g *globParser) atom(c byte, depth int) (string, *APIError) {
	switch c {
	case '?':
		g.pos++

		return `[^/]`, nil
	case '*':
		return g.star(depth)
	case '[':
		return g.class()
	case '{':
		return g.alternation(depth)
	case '}', ']':
		return "", errGlob()
	case '\\':
		if g.pos+1 >= len(g.src) {
			return "", errGlob()
		}

		r, size := utf8.DecodeRuneInString(g.src[g.pos+1:])
		g.pos += 1 + size

		return regexp.QuoteMeta(string(r)), nil
	case '/':
		if depth > 0 {
			return "", errInvalid("matchGlob brace expansions cannot contain '/'.")
		}

		g.pos++

		return "/", nil
	default:
		r, size := utf8.DecodeRuneInString(g.src[g.pos:])
		g.pos += size

		return regexp.QuoteMeta(string(r)), nil
	}
}

func (g *globParser) star(depth int) (string, *APIError) {
	if strings.HasPrefix(g.src[g.pos:], "**") {
		if depth > 0 {
			return "", errInvalid("matchGlob brace expansions cannot contain '**'.")
		}

		g.pos += 2
		if g.pos < len(g.src) && g.src[g.pos] == '/' {
			g.pos++

			return `(?:.*/)?`, nil
		}

		return `.*`, nil
	}

	g.pos++

	return `[^/]*`, nil
}

func (g *globParser) class() (string, *APIError) {
	i := g.pos + 1
	neg := false

	if i < len(g.src) && (g.src[i] == '!' || g.src[i] == '^') {
		neg = true
		i++
	}

	var sb strings.Builder

	for ; i < len(g.src) && g.src[i] != ']'; i++ {
		c := g.src[i]
		if c == '\\' {
			i++
			if i >= len(g.src) {
				return "", errGlob()
			}

			c = g.src[i]
		}

		if strings.IndexByte(`\]^[`, c) >= 0 {
			sb.WriteByte('\\')
		}

		sb.WriteByte(c)
	}

	if i >= len(g.src) || sb.Len() == 0 {
		return "", errGlob()
	}

	g.pos = i + 1

	if neg {
		return `[^/` + sb.String() + `]`, nil
	}

	return `[` + sb.String() + `]`, nil
}

func (g *globParser) alternation(depth int) (string, *APIError) {
	g.pos++

	var alts []string

	for {
		alt, apiErr := g.sequence(depth + 1)
		if apiErr != nil {
			return "", apiErr
		}

		alts = append(alts, alt)

		if g.pos >= len(g.src) {
			return "", errGlob()
		}

		c := g.src[g.pos]
		g.pos++

		if c == '}' {
			break
		}
	}

	return `(?:` + strings.Join(alts, "|") + `)`, nil
}
