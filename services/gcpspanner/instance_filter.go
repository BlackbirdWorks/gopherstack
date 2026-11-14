package gcpspanner

import (
	"slices"
	"strings"
	"unicode"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
)

type instPred func(*instancepb.Instance) bool

type instTok struct {
	text   string
	quoted bool
}

func readInstanceQuoted(rs []rune, i int) (instTok, int, error) {
	j := i + 1
	for j < len(rs) && rs[j] != '"' {
		j++
	}

	if j >= len(rs) {
		return instTok{}, 0, errInvalid("Invalid filter: unterminated quoted string.")
	}

	return instTok{text: string(rs[i+1 : j]), quoted: true}, j + 1, nil
}

func readInstanceWord(rs []rune, i int) (instTok, int, error) {
	j := i
	for j < len(rs) && !unicode.IsSpace(rs[j]) && !strings.ContainsRune("():=!\"", rs[j]) {
		j++
	}

	if j == i {
		return instTok{}, 0, errInvalid("Invalid filter: unexpected %q.", string(rs[i]))
	}

	return instTok{text: string(rs[i:j])}, j, nil
}

func nextInstanceToken(rs []rune, i int) (instTok, int, error) {
	r := rs[i]

	switch {
	case r == '!' && i+1 < len(rs) && rs[i+1] == '=':
		return instTok{text: "!="}, i + len("!="), nil
	case strings.ContainsRune("():=", r), r == '-' && i+1 < len(rs) && !unicode.IsSpace(rs[i+1]):
		return instTok{text: string(r)}, i + 1, nil
	case r == '"':
		return readInstanceQuoted(rs, i)
	default:
		return readInstanceWord(rs, i)
	}
}

func tokenizeInstanceFilter(s string) ([]instTok, error) {
	var toks []instTok

	rs := []rune(s)

	for i := 0; i < len(rs); {
		if unicode.IsSpace(rs[i]) {
			i++

			continue
		}

		tok, next, err := nextInstanceToken(rs, i)
		if err != nil {
			return nil, err
		}

		toks = append(toks, tok)
		i = next
	}

	return toks, nil
}

type instParser struct {
	toks []instTok
	pos  int
}

func (p *instParser) peek() (instTok, bool) {
	if p.pos >= len(p.toks) {
		return instTok{}, false
	}

	return p.toks[p.pos], true
}

func (p *instParser) is(text string) bool {
	t, ok := p.peek()

	return ok && !t.quoted && t.text == text
}

func (p *instParser) keyword(kw string) bool {
	if p.is(kw) {
		p.pos++

		return true
	}

	return false
}

// parseInstanceFilter accepts the grammar of ListInstancesRequest.filter: terms are
// implicitly ANDed; AND, OR, NOT and parentheses are also accepted, OR binding tighter.
func parseInstanceFilter(s string) (instPred, error) {
	toks, err := tokenizeInstanceFilter(s)
	if err != nil {
		return nil, err
	}

	p := &instParser{toks: toks}

	pred, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	if t, more := p.peek(); more {
		return nil, errInvalid("Invalid filter: unexpected %q.", t.text)
	}

	return pred, nil
}

func (p *instParser) parseAnd() (instPred, error) {
	preds := []instPred{}

	for {
		next, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		preds = append(preds, next)

		p.keyword("AND")

		if _, more := p.peek(); !more || p.is(")") {
			break
		}
	}

	return func(in *instancepb.Instance) bool {
		return !slices.ContainsFunc(preds, func(f instPred) bool { return !f(in) })
	}, nil
}

func (p *instParser) parseOr() (instPred, error) {
	first, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	preds := []instPred{first}

	for p.keyword("OR") {
		next, nerr := p.parseTerm()
		if nerr != nil {
			return nil, nerr
		}

		preds = append(preds, next)
	}

	return func(in *instancepb.Instance) bool {
		return slices.ContainsFunc(preds, func(f instPred) bool { return f(in) })
	}, nil
}

func (p *instParser) parseTerm() (instPred, error) {
	switch {
	case p.keyword("NOT"), p.keyword("-"):
		inner, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		return func(in *instancepb.Instance) bool { return !inner(in) }, nil
	case p.keyword("("):
		inner, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		if !p.keyword(")") {
			return nil, errInvalid("Invalid filter: missing closing parenthesis.")
		}

		return inner, nil
	}

	return p.parseComparison()
}

func (p *instParser) parseComparison() (instPred, error) {
	if p.pos+3 > len(p.toks) {
		return nil, errInvalid("Invalid filter: expected field, operator and value.")
	}

	field, op, val := p.toks[p.pos], p.toks[p.pos+1], p.toks[p.pos+2]
	p.pos += 3

	if field.quoted || op.quoted || (op.text != ":" && op.text != "=" && op.text != "!=") {
		return nil, errInvalid("Invalid filter: expected field, operator and value.")
	}

	get, err := instanceFieldGetter(strings.ToLower(field.text))
	if err != nil {
		return nil, err
	}

	want := strings.ToLower(val.text)
	present := !val.quoted && val.text == "*"

	return func(in *instancepb.Instance) bool {
		got, ok := get(in)
		got = strings.ToLower(got)

		switch {
		case op.text == "!=":
			return !ok || got != want
		case !ok:
			return false
		case present:
			return true
		case op.text == "=":
			return got == want
		default:
			return strings.Contains(got, want)
		}
	}, nil
}

func instanceFieldGetter(field string) (func(*instancepb.Instance) (string, bool), error) {
	switch {
	case field == "name":
		return func(in *instancepb.Instance) (string, bool) { return in.GetName(), true }, nil
	case field == "display_name":
		return func(in *instancepb.Instance) (string, bool) { return in.GetDisplayName(), true }, nil
	case strings.HasPrefix(field, "labels.") && len(field) > len("labels."):
		key := strings.TrimPrefix(field, "labels.")

		return func(in *instancepb.Instance) (string, bool) {
			for k, v := range in.GetLabels() {
				if strings.EqualFold(k, key) {
					return v, true
				}
			}

			return "", false
		}, nil
	default:
		return nil, errInvalid("Invalid filter: unsupported field %q.", field)
	}
}
