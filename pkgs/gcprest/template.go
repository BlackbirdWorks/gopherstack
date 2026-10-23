// Package gcprest serves the REST/JSON surface of a gRPC-only Google Cloud emulator by transcoding
// HTTP requests, driven by the google.api.http annotations on the service protos, into in-process
// calls on the registered gRPC implementation.
package gcprest

import (
	"errors"
	"fmt"
	"strings"
)

var errBadTemplate = errors.New("gcprest: invalid path template")

type elemKind uint8

const (
	elemLiteral elemKind = iota
	elemStar
	elemDoubleStar
)

type elem struct {
	lit  string
	kind elemKind
}

type variable struct {
	field string
	start int
	end   int
}

type binding struct {
	field string
	value string
}

// template is a compiled google.api.http path template.
type template struct {
	verb  string
	elems []elem
	vars  []variable
}

func parseTemplate(s string) (*template, error) {
	if !strings.HasPrefix(s, "/") {
		return nil, fmt.Errorf("%w: %q must start with /", errBadTemplate, s)
	}

	t := &template{}

	if i := strings.LastIndexByte(s, ':'); i > strings.LastIndexByte(s, '/') && i > strings.LastIndexByte(s, '}') {
		t.verb = s[i+1:]
		s = s[:i]
	}

	for rest := s[1:]; rest != ""; {
		var err error

		if rest, err = t.consume(rest); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// consume parses one segment or variable from rest and returns the remainder after its trailing slash.
func (t *template) consume(rest string) (string, error) {
	if rest[0] == '{' {
		end := strings.IndexByte(rest, '}')
		if end < 0 {
			return "", fmt.Errorf("%w: unterminated variable", errBadTemplate)
		}

		t.addVariable(rest[1:end])

		return strings.TrimPrefix(rest[end+1:], "/"), nil
	}

	seg, remainder, _ := strings.Cut(rest, "/")
	t.elems = append(t.elems, newElem(seg))

	return remainder, nil
}

func (t *template) addVariable(spec string) {
	field, pattern, hasPattern := strings.Cut(spec, "=")
	if !hasPattern {
		pattern = "*"
	}

	v := variable{field: field, start: len(t.elems)}

	for seg := range strings.SplitSeq(pattern, "/") {
		t.elems = append(t.elems, newElem(seg))
	}

	v.end = len(t.elems)
	t.vars = append(t.vars, v)
}

func newElem(seg string) elem {
	switch seg {
	case "*":
		return elem{kind: elemStar}
	case "**":
		return elem{kind: elemDoubleStar}
	default:
		return elem{kind: elemLiteral, lit: seg}
	}
}

type matcher struct {
	t    *template
	segs []string
	pos  []int
}

// match reports whether segs (already stripped of the verb) satisfy the template and returns the variable bindings.
func (t *template) match(segs []string) ([]binding, bool) {
	m := &matcher{t: t, segs: segs, pos: make([]int, len(t.elems)+1)}
	if !m.walk(0, 0) {
		return nil, false
	}

	out := make([]binding, 0, len(t.vars))
	for _, v := range t.vars {
		out = append(out, binding{field: v.field, value: strings.Join(segs[m.pos[v.start]:m.pos[v.end]], "/")})
	}

	return out, true
}

func (m *matcher) walk(ei, si int) bool {
	m.pos[ei] = si

	if ei == len(m.t.elems) {
		return si == len(m.segs)
	}

	e := m.t.elems[ei]

	switch e.kind {
	case elemLiteral:
		return si < len(m.segs) && m.segs[si] == e.lit && m.walk(ei+1, si+1)
	case elemStar:
		return si < len(m.segs) && m.segs[si] != "" && m.walk(ei+1, si+1)
	default:
		for k := si; k <= len(m.segs); k++ {
			if m.walk(ei+1, k) {
				return true
			}
		}

		return false
	}
}
