package azurearm

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var errBadNameFilter = errors.New("azurearm: unsupported $filter")

// parseNameFilter compiles the OData $filter of an event subscription list. Only
// the 'name' property is filterable, with contains(), eq, ne, not, and and or.
func parseNameFilter(src string) (func(string) bool, error) {
	if strings.TrimSpace(src) == "" {
		return func(string) bool { return true }, nil
	}

	toks, err := lexFilter(src)
	if err != nil {
		return nil, err
	}

	p := &filterParser{toks: toks}

	fn, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.pos != len(p.toks) {
		return nil, fmt.Errorf("%w: unexpected %q", errBadNameFilter, p.toks[p.pos].text)
	}

	return fn, nil
}

type filterTok struct {
	text  string
	quote bool
}

func lexFilter(src string) ([]filterTok, error) {
	var toks []filterTok

	for i := 0; i < len(src); {
		switch c := src[i]; c {
		case ' ', '\t':
			i++
		case '(', ')', ',':
			toks = append(toks, filterTok{text: string(c)})
			i++
		case '\'':
			text, next, ok := lexQuoted(src, i+1)
			if !ok {
				return nil, fmt.Errorf("%w: unterminated string", errBadNameFilter)
			}

			toks, i = append(toks, filterTok{text: text, quote: true}), next
		default:
			start := i
			for i < len(src) && !strings.ContainsRune(" \t(),'", rune(src[i])) {
				i++
			}

			toks = append(toks, filterTok{text: src[start:i]})
		}
	}

	return toks, nil
}

// lexQuoted reads a string literal body starting after its opening quote (” is an escaped quote).
func lexQuoted(src string, i int) (string, int, bool) {
	var sb strings.Builder

	for i < len(src) {
		switch {
		case src[i] == '\'' && i+1 < len(src) && src[i+1] == '\'':
			sb.WriteByte('\'')

			i += 2
		case src[i] == '\'':
			return sb.String(), i + 1, true
		default:
			sb.WriteByte(src[i])

			i++
		}
	}

	return "", i, false
}

type filterParser struct {
	toks []filterTok
	pos  int
}

func (p *filterParser) peekKeyword(kw string) bool {
	return p.pos < len(p.toks) && !p.toks[p.pos].quote && strings.EqualFold(p.toks[p.pos].text, kw)
}

func (p *filterParser) expect(text string) error {
	if p.pos >= len(p.toks) || p.toks[p.pos].quote || !strings.EqualFold(p.toks[p.pos].text, text) {
		return fmt.Errorf("%w: expected %q", errBadNameFilter, text)
	}

	p.pos++

	return nil
}

func (p *filterParser) parseOr() (func(string) bool, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.peekKeyword("or") {
		p.pos++

		right, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(n string) bool { return l(n) || r(n) }
	}

	return left, nil
}

func (p *filterParser) parseAnd() (func(string) bool, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.peekKeyword("and") {
		p.pos++

		right, rerr := p.parseUnary()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(n string) bool { return l(n) && r(n) }
	}

	return left, nil
}

func (p *filterParser) parseUnary() (func(string) bool, error) {
	switch {
	case p.peekKeyword("not"):
		p.pos++

		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return func(n string) bool { return !inner(n) }, nil
	case p.pos < len(p.toks) && p.toks[p.pos].text == "(" && !p.toks[p.pos].quote:
		p.pos++

		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		return inner, p.expect(")")
	case p.peekKeyword("contains"):
		return p.parseContains()
	}

	return p.parseComparison()
}

func (p *filterParser) nameOperand() error {
	if p.pos >= len(p.toks) || p.toks[p.pos].quote || !strings.EqualFold(p.toks[p.pos].text, "name") {
		return fmt.Errorf("%w: only the 'name' property can be filtered", errBadNameFilter)
	}

	p.pos++

	return nil
}

func (p *filterParser) literal() (string, error) {
	if p.pos >= len(p.toks) || !p.toks[p.pos].quote {
		return "", fmt.Errorf("%w: expected a quoted string", errBadNameFilter)
	}

	v := p.toks[p.pos].text
	p.pos++

	return v, nil
}

func (p *filterParser) parseContains() (func(string) bool, error) {
	p.pos++

	if err := p.expect("("); err != nil {
		return nil, err
	}

	if err := p.nameOperand(); err != nil {
		return nil, err
	}

	if err := p.expect(","); err != nil {
		return nil, err
	}

	lit, err := p.literal()
	if err != nil {
		return nil, err
	}

	if err = p.expect(")"); err != nil {
		return nil, err
	}

	lit = strs.Fold(lit)

	return func(n string) bool { return strings.Contains(strs.Fold(n), lit) }, nil
}

func (p *filterParser) parseComparison() (func(string) bool, error) {
	if err := p.nameOperand(); err != nil {
		return nil, err
	}

	var negate bool

	switch {
	case p.peekKeyword("eq"):
	case p.peekKeyword("ne"):
		negate = true
	default:
		return nil, fmt.Errorf("%w: expected eq or ne", errBadNameFilter)
	}

	p.pos++

	lit, err := p.literal()
	if err != nil {
		return nil, err
	}

	return func(n string) bool { return strs.Equal(n, lit) != negate }, nil
}
