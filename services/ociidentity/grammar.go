package ociidentity

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// ErrGrammar marks a policy statement or matching rule that violates the documented grammar.
var ErrGrammar = errors.New("syntax error")

//nolint:gochecknoglobals // immutable verb list
var policyVerbs = []string{"inspect", "read", "use", "manage"}

type tokKind int

const (
	tokWord tokKind = iota
	tokQuoted
	tokSym
)

type token struct {
	text string
	kind tokKind
}

func (t token) is(s string) bool { return t.kind != tokQuoted && strings.EqualFold(t.text, s) }

func isSymRune(r rune) bool { return strings.ContainsRune("{}(),=", r) }

// lex splits s into words, single-quoted strings and the symbols { } ( ) , = !=.
func lex(s string) ([]token, error) {
	var toks []token

	rs := []rune(s)

	for i := 0; i < len(rs); {
		r := rs[i]

		switch {
		case unicode.IsSpace(r):
			i++
		case r == '\'':
			j := i + 1
			for j < len(rs) && rs[j] != '\'' {
				j++
			}

			if j >= len(rs) {
				return nil, fmt.Errorf("%w: unterminated quoted string", ErrGrammar)
			}

			toks = append(toks, token{text: string(rs[i+1 : j]), kind: tokQuoted})
			i = j + 1
		case r == '!' && i+1 < len(rs) && rs[i+1] == '=':
			toks = append(toks, token{text: "!=", kind: tokSym})
			i += 2
		case isSymRune(r):
			toks = append(toks, token{text: string(r), kind: tokSym})
			i++
		default:
			j := i
			for j < len(rs) && !unicode.IsSpace(rs[j]) && !isSymRune(rs[j]) && rs[j] != '\'' && rs[j] != '!' {
				j++
			}

			if j == i {
				return nil, fmt.Errorf("%w: unexpected character %q", ErrGrammar, string(r))
			}

			toks = append(toks, token{text: string(rs[i:j]), kind: tokWord})
			i = j
		}
	}

	return toks, nil
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) done() bool { return p.pos >= len(p.toks) }

func (p *parser) peek() token {
	if p.done() {
		return token{}
	}

	return p.toks[p.pos]
}

func (p *parser) peekIs(s string) bool { return !p.done() && p.peek().is(s) }

func (p *parser) next() (token, error) {
	if p.done() {
		return token{}, fmt.Errorf("%w: unexpected end of input", ErrGrammar)
	}

	t := p.toks[p.pos]
	p.pos++

	return t, nil
}

func (p *parser) expect(s string) error {
	t, err := p.next()
	if err != nil {
		return fmt.Errorf("%w (expected %q)", err, s)
	}

	if !t.is(s) {
		return fmt.Errorf("%w: expected %q but found %q", ErrGrammar, s, t.text)
	}

	return nil
}

func (p *parser) word(what string) (token, error) {
	t, err := p.next()
	if err != nil {
		return t, fmt.Errorf("%w (expected %s)", err, what)
	}

	if t.kind == tokSym {
		return t, fmt.Errorf("%w: expected %s but found %q", ErrGrammar, what, t.text)
	}

	return t, nil
}

func (p *parser) acceptWord(s string) bool {
	if p.peekIs(s) {
		p.pos++

		return true
	}

	return false
}

// commaList parses one or more words separated by commas.
func (p *parser) commaList(what string) error {
	for {
		if _, err := p.word(what); err != nil {
			return err
		}

		if !p.acceptSym(",") {
			return nil
		}
	}
}

func (p *parser) acceptSym(s string) bool {
	if !p.done() && p.peek().kind == tokSym && p.peek().text == s {
		p.pos++

		return true
	}

	return false
}

func (p *parser) expectSym(s string) error {
	if !p.acceptSym(s) {
		return fmt.Errorf("%w: expected %q", ErrGrammar, s)
	}

	return nil
}

// condition parses `variable op value` or `any|all { condition, ... }`.
func (p *parser) condition() error {
	if p.peekIs("any") || p.peekIs("all") {
		p.pos++

		return p.conditionGroup()
	}

	v, err := p.word("a variable")
	if err != nil {
		return err
	}

	if !strings.Contains(v.text, ".") {
		return fmt.Errorf("%w: %q is not a variable", ErrGrammar, v.text)
	}

	return p.operatorAndValue()
}

func (p *parser) conditionGroup() error {
	if err := p.expectSym("{"); err != nil {
		return err
	}

	for {
		if err := p.condition(); err != nil {
			return err
		}

		if p.acceptSym(",") {
			continue
		}

		return p.expectSym("}")
	}
}

func (p *parser) operatorAndValue() error {
	switch {
	case p.acceptSym("="), p.acceptSym("!="):
		return p.value()
	case p.acceptWord("not"):
		if err := p.expect("in"); err != nil {
			return err
		}

		return p.valueList()
	case p.acceptWord("in"):
		return p.valueList()
	case p.acceptWord("before"), p.acceptWord("after"):
		return p.value()
	case p.acceptWord("between"):
		if err := p.value(); err != nil {
			return err
		}

		if err := p.expect("and"); err != nil {
			return err
		}

		return p.value()
	}

	return fmt.Errorf("%w: expected a comparison operator", ErrGrammar)
}

func (p *parser) value() error {
	t, err := p.next()
	if err != nil {
		return fmt.Errorf("%w (expected a value)", err)
	}

	if t.kind == tokSym {
		return fmt.Errorf("%w: expected a value but found %q", ErrGrammar, t.text)
	}

	return nil
}

func (p *parser) valueList() error {
	if err := p.expectSym("("); err != nil {
		return err
	}

	for {
		if err := p.value(); err != nil {
			return err
		}

		if p.acceptSym(",") {
			continue
		}

		return p.expectSym(")")
	}
}

// ValidateMatchingRule checks a dynamic group rule: `ANY|ALL {condition, ...}`.
func ValidateMatchingRule(rule string) error {
	toks, err := lex(rule)
	if err != nil {
		return err
	}

	p := &parser{toks: toks}

	if !p.peekIs("any") && !p.peekIs("all") {
		return fmt.Errorf("%w: a rule must start with ANY or ALL", ErrGrammar)
	}

	p.pos++

	if err = p.conditionGroup(); err != nil {
		return err
	}

	if !p.done() {
		return fmt.Errorf("%w: unexpected %q after the rule", ErrGrammar, p.peek().text)
	}

	return nil
}

func (p *parser) subject() error {
	switch {
	case p.acceptWord("any-user"), p.acceptWord("any-group"):
		return nil
	case p.acceptWord("service"):
		return p.commaList("a service name")
	case p.acceptWord("group"), p.acceptWord("dynamic-group"):
		p.acceptWord("id")

		return p.commaList("a group name or OCID")
	}

	return fmt.Errorf("%w: expected a subject (group, dynamic-group, any-group, any-user or service)", ErrGrammar)
}

func (p *parser) verbAndResource() error {
	if err := p.expect("to"); err != nil {
		return err
	}

	if p.acceptSym("{") {
		if err := p.commaList("a permission"); err != nil {
			return err
		}

		if err := p.expectSym("}"); err != nil {
			return err
		}
	} else {
		v, err := p.word("a verb")
		if err != nil {
			return err
		}

		if !containsFold(policyVerbs, v.text) {
			return fmt.Errorf("%w: %q is not a verb (inspect, read, use or manage)", ErrGrammar, v.text)
		}
	}

	if _, err := p.word("a resource-type"); err != nil {
		return err
	}

	return p.expect("in")
}

func containsFold(list []string, s string) bool {
	for _, e := range list {
		if strings.EqualFold(e, s) {
			return true
		}
	}

	return false
}

func (p *parser) location() error {
	if p.acceptWord("tenancy") {
		return nil
	}

	if err := p.expect("compartment"); err != nil {
		return fmt.Errorf("%w: expected tenancy or compartment", ErrGrammar)
	}

	p.acceptWord("id")

	_, err := p.word("a compartment name or OCID")

	return err
}

func (p *parser) optionalWhere() error {
	if p.acceptWord("where") {
		return p.condition()
	}

	return nil
}

func (p *parser) finish() error {
	if !p.done() {
		return fmt.Errorf("%w: unexpected %q", ErrGrammar, p.peek().text)
	}

	return nil
}

// ValidateStatement checks one policy statement against the documented grammar
// (Allow, Deny, Define, Endorse and Admit).
func ValidateStatement(stmt string) error {
	toks, err := lex(stmt)
	if err != nil {
		return err
	}

	p := &parser{toks: toks}

	head, err := p.word("a statement keyword")
	if err != nil {
		return err
	}

	switch strings.ToLower(head.text) {
	case "allow", "deny":
		err = p.allow()
	case "define":
		err = p.define()
	case "endorse":
		err = p.endorse()
	case "admit":
		err = p.admit()
	default:
		err = fmt.Errorf("%w: statements start with Allow, Deny, Define, Endorse or Admit", ErrGrammar)
	}

	if err != nil {
		return err
	}

	return p.finish()
}

func (p *parser) allow() error {
	if err := p.subject(); err != nil {
		return err
	}

	if err := p.verbAndResource(); err != nil {
		return err
	}

	if err := p.location(); err != nil {
		return err
	}

	return p.optionalWhere()
}

func (p *parser) define() error {
	kind, err := p.word("a resource kind")
	if err != nil {
		return err
	}

	if !containsFold([]string{"tenancy", "group", "dynamic-group", "user", "compartment"}, kind.text) {
		return fmt.Errorf("%w: cannot define %q", ErrGrammar, kind.text)
	}

	if _, err = p.word("an alias"); err != nil {
		return err
	}

	if err = p.expect("as"); err != nil {
		return err
	}

	_, err = p.word("an OCID")

	return err
}

func (p *parser) endorse() error {
	if err := p.subject(); err != nil {
		return err
	}

	if err := p.verbAndResource(); err != nil {
		return err
	}

	if !p.acceptWord("any-tenancy") {
		if err := p.expect("tenancy"); err != nil {
			return err
		}

		if _, err := p.word("a tenancy alias"); err != nil {
			return err
		}
	}

	return p.optionalWhere()
}

func (p *parser) admit() error {
	if err := p.subject(); err != nil {
		return err
	}

	if err := p.expect("of"); err != nil {
		return err
	}

	if err := p.expect("tenancy"); err != nil {
		return err
	}

	if _, err := p.word("a tenancy alias"); err != nil {
		return err
	}

	if err := p.verbAndResource(); err != nil {
		return err
	}

	if err := p.location(); err != nil {
		return err
	}

	return p.optionalWhere()
}
