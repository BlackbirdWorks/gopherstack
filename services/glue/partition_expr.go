package glue

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// Sentinel parse errors for partition expression parsing.
var (
	errExprUnexpectedToken     = errors.New("unexpected token")
	errExprExpectedCloseParen  = errors.New("expected closing parenthesis")
	errExprExpectedColumn      = errors.New("expected column name")
	errExprExpectedOperator    = errors.New("expected operator after column")
	errExprExpectedIN          = errors.New("expected IN after NOT")
	errExprExpectedString      = errors.New("expected string literal after operator")
	errExprExpectedOpenParen   = errors.New("expected ( after IN")
	errExprUnterminatedIN      = errors.New("unterminated IN list")
	errExprExpectedCommaInList = errors.New("expected , in IN list")
	errExprExpectedStringInIN  = errors.New("expected string in IN list")
)

// partitionExpr is a compiled predicate for filtering partitions by their values.
type partitionExpr interface {
	eval(keyNames, values []string) bool
}

// exprAnd evaluates left AND right.
type exprAnd struct{ left, right partitionExpr }

func (e *exprAnd) eval(k, v []string) bool { return e.left.eval(k, v) && e.right.eval(k, v) }

// exprOr evaluates left OR right.
type exprOr struct{ left, right partitionExpr }

func (e *exprOr) eval(k, v []string) bool { return e.left.eval(k, v) || e.right.eval(k, v) }

// exprNot negates its child.
type exprNot struct{ child partitionExpr }

func (e *exprNot) eval(k, v []string) bool { return !e.child.eval(k, v) }

// exprCmp compares a partition column to a literal using op.
type exprCmp struct {
	col string
	op  string
	val string
}

func (e *exprCmp) eval(keyNames, values []string) bool {
	idx := -1

	for i, n := range keyNames {
		if strings.EqualFold(n, e.col) {
			idx = i

			break
		}
	}

	if idx < 0 || idx >= len(values) {
		return false
	}

	pv := values[idx]

	switch e.op {
	case "=":
		return pv == e.val
	case "<>", "!=":
		return pv != e.val
	case ">":
		return pv > e.val
	case ">=":
		return pv >= e.val
	case "<":
		return pv < e.val
	case "<=":
		return pv <= e.val
	case "LIKE":
		return likeMatch(e.val, pv)
	default:
		return false
	}
}

// exprIn checks whether the partition column value is in a set.
type exprIn struct {
	col    string
	values []string
	negate bool
}

func (e *exprIn) eval(keyNames, values []string) bool {
	idx := -1

	for i, n := range keyNames {
		if strings.EqualFold(n, e.col) {
			idx = i

			break
		}
	}

	if idx < 0 || idx >= len(values) {
		return e.negate
	}

	pv := values[idx]

	if slices.Contains(e.values, pv) {
		return !e.negate
	}

	return e.negate
}

// likeMatch implements SQL LIKE: % matches any sequence, _ matches one char.
func likeMatch(pattern, s string) bool {
	var sb strings.Builder

	sb.WriteString("^")

	for _, ch := range pattern {
		switch ch {
		case '%':
			sb.WriteString(".*")
		case '_':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}

	sb.WriteString("$")

	matched, err := regexp.MatchString(sb.String(), s)

	return err == nil && matched
}

// parsePartitionExpr parses a SQL WHERE expression for partition filtering.
func parsePartitionExpr(expr string) (partitionExpr, error) {
	p := &exprParser{tokens: tokenize(expr)}

	result, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if !p.done() {
		return nil, fmt.Errorf("%w: %q", errExprUnexpectedToken, p.peek())
	}

	return result, nil
}

// tableNameRegexp converts a Glue table Expression (which is a regex) to a compiled regexp.
func tableNameRegexp(expr string) (*regexp.Regexp, error) {
	return regexp.Compile(expr)
}

// token kinds.
const (
	tokWord   = "WORD"
	tokString = "STRING"
	tokLParen = "LPAREN"
	tokRParen = "RPAREN"
	tokComma  = "COMMA"
	tokOp     = "OP"
)

type token struct {
	kind string
	val  string
}

//nolint:gocognit,cyclop,funlen // tokenize is a straightforward character-class scanner
func tokenize(s string) []token {
	var tokens []token
	i := 0

	for i < len(s) {
		ch := rune(s[i])

		if unicode.IsSpace(ch) {
			i++

			continue
		}

		if ch == '(' {
			tokens = append(tokens, token{tokLParen, "("})
			i++

			continue
		}

		if ch == ')' {
			tokens = append(tokens, token{tokRParen, ")"})
			i++

			continue
		}

		if ch == ',' {
			tokens = append(tokens, token{tokComma, ","})
			i++

			continue
		}

		// String literal
		if ch == '\'' {
			j := i + 1

			var sb strings.Builder

			for j < len(s) {
				if s[j] == '\'' {
					if j+1 < len(s) && s[j+1] == '\'' {
						sb.WriteByte('\'')
						j += 2

						continue
					}

					break
				}

				sb.WriteByte(s[j])
				j++
			}

			tokens = append(tokens, token{tokString, sb.String()})
			i = j + 1

			continue
		}

		// Operators: >=, <=, <>, !=, >, <, =
		if ch == '>' || ch == '<' || ch == '=' || ch == '!' {
			op := string(ch)

			if i+1 < len(s) && (s[i+1] == '=' || s[i+1] == '>') {
				op += string(s[i+1])
				i++
			}

			tokens = append(tokens, token{tokOp, op})
			i++

			continue
		}

		// Word (identifier or keyword)
		if unicode.IsLetter(ch) || ch == '_' {
			j := i

			for j < len(s) && (unicode.IsLetter(rune(s[j])) || unicode.IsDigit(rune(s[j])) || s[j] == '_') {
				j++
			}

			tokens = append(tokens, token{tokWord, s[i:j]})
			i = j

			continue
		}

		// skip unknown
		i++
	}

	return tokens
}

type exprParser struct {
	tokens []token
	pos    int
}

func (p *exprParser) peek() string {
	if p.pos >= len(p.tokens) {
		return ""
	}

	return p.tokens[p.pos].val
}

func (p *exprParser) peekKind() string {
	if p.pos >= len(p.tokens) {
		return ""
	}

	return p.tokens[p.pos].kind
}

func (p *exprParser) consume() token {
	t := p.tokens[p.pos]
	p.pos++

	return t
}

func (p *exprParser) done() bool { return p.pos >= len(p.tokens) }

func (p *exprParser) parseOr() (partitionExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for !p.done() && strings.EqualFold(p.peek(), "OR") {
		p.consume()

		var right partitionExpr

		right, err = p.parseAnd()
		if err != nil {
			return nil, err
		}

		left = &exprOr{left, right}
	}

	return left, nil
}

func (p *exprParser) parseAnd() (partitionExpr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for !p.done() && strings.EqualFold(p.peek(), "AND") {
		p.consume()

		var right partitionExpr

		right, err = p.parseUnary()
		if err != nil {
			return nil, err
		}

		left = &exprAnd{left, right}
	}

	return left, nil
}

func (p *exprParser) parseUnary() (partitionExpr, error) {
	if !p.done() && strings.EqualFold(p.peek(), "NOT") {
		p.consume()

		child, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}

		return &exprNot{child}, nil
	}

	return p.parsePrimary()
}

//nolint:cyclop // parser primary expression handler — complexity matches grammar rules
func (p *exprParser) parsePrimary() (partitionExpr, error) {
	if !p.done() && p.peekKind() == tokLParen {
		p.consume() // (

		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if p.done() || p.peekKind() != tokRParen {
			return nil, errExprExpectedCloseParen
		}

		p.consume() // )

		return inner, nil
	}

	if p.done() || p.peekKind() != tokWord {
		return nil, fmt.Errorf("%w, got %q", errExprExpectedColumn, p.peek())
	}

	col := p.consume().val

	if p.done() {
		return nil, fmt.Errorf("%w %q", errExprExpectedOperator, col)
	}

	// Check for NOT IN or IN
	if strings.EqualFold(p.peek(), "IN") {
		return p.parseIn(col, false)
	}

	if strings.EqualFold(p.peek(), "NOT") {
		p.consume() // NOT

		if p.done() || !strings.EqualFold(p.peek(), "IN") {
			return nil, errExprExpectedIN
		}

		return p.parseIn(col, true)
	}

	if p.done() || (p.peekKind() != tokOp && !strings.EqualFold(p.peek(), "LIKE")) {
		return nil, fmt.Errorf("%w %q, got %q", errExprExpectedOperator, col, p.peek())
	}

	op := strings.ToUpper(p.consume().val)

	if p.done() || p.peekKind() != tokString {
		return nil, fmt.Errorf("%w, got %q", errExprExpectedString, p.peek())
	}

	val := p.consume().val

	return &exprCmp{col: col, op: op, val: val}, nil
}

func (p *exprParser) parseIn(col string, negate bool) (partitionExpr, error) {
	p.consume() // IN

	if p.done() || p.peekKind() != tokLParen {
		return nil, errExprExpectedOpenParen
	}

	p.consume() // (

	var values []string

	for {
		if p.done() {
			return nil, errExprUnterminatedIN
		}

		if p.peekKind() == tokRParen {
			p.consume()

			break
		}

		if len(values) > 0 {
			if p.peekKind() != tokComma {
				return nil, fmt.Errorf("%w, got %q", errExprExpectedCommaInList, p.peek())
			}

			p.consume()
		}

		if p.done() || p.peekKind() != tokString {
			return nil, fmt.Errorf("%w, got %q", errExprExpectedStringInIN, p.peek())
		}

		values = append(values, p.consume().val)
	}

	return &exprIn{col: col, values: values, negate: negate}, nil
}
