package azurearm

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type logicExprKind int

const (
	exprLit logicExprKind = iota
	exprCall
	exprIndex
	exprProp
)

type logicExpr struct {
	lit  any
	base *logicExpr
	idx  *logicExpr
	name string
	args []*logicExpr
	kind logicExprKind
	safe bool
}

func exprErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrLogicExpr, fmt.Sprintf(format, a...))
}

type exprParser struct {
	s   string
	pos int
}

func parseLogicExpr(s string) (*logicExpr, error) {
	p := &exprParser{s: s}

	n, err := p.parsePostfix()
	if err != nil {
		return nil, err
	}

	p.skipSpace()

	if p.pos != len(p.s) {
		return nil, exprErr("unexpected character %q at position %d in %q", p.s[p.pos], p.pos, s)
	}

	return n, nil
}

func (p *exprParser) skipSpace() {
	for p.pos < len(p.s) && (p.s[p.pos] == ' ' || p.s[p.pos] == '\t' || p.s[p.pos] == '\n' || p.s[p.pos] == '\r') {
		p.pos++
	}
}

func (p *exprParser) peek() byte {
	if p.pos < len(p.s) {
		return p.s[p.pos]
	}

	return 0
}

func (p *exprParser) parsePostfix() (*logicExpr, error) {
	n, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for {
		p.skipSpace()

		next, ok, perr := p.parseAccessor(n)
		if perr != nil {
			return nil, perr
		}

		if !ok {
			return n, nil
		}

		n = next
	}
}

func (p *exprParser) parseAccessor(base *logicExpr) (*logicExpr, bool, error) {
	safe := false
	save := p.pos

	if p.peek() == '?' {
		safe = true
		p.pos++
	}

	switch p.peek() {
	case '[':
		p.pos++

		idx, err := p.parsePostfix()
		if err != nil {
			return nil, false, err
		}

		p.skipSpace()

		if p.peek() != ']' {
			return nil, false, exprErr("expected ']' at position %d", p.pos)
		}

		p.pos++

		return &logicExpr{kind: exprIndex, base: base, idx: idx, safe: safe}, true, nil
	case '.':
		p.pos++
		start := p.pos

		for p.pos < len(p.s) && isIdentByte(p.s[p.pos]) {
			p.pos++
		}

		if start == p.pos {
			return nil, false, exprErr("expected property name at position %d", start)
		}

		return &logicExpr{kind: exprProp, base: base, name: p.s[start:p.pos], safe: safe}, true, nil
	}

	p.pos = save

	return nil, false, nil
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func (p *exprParser) parsePrimary() (*logicExpr, error) {
	p.skipSpace()

	c := p.peek()

	switch {
	case c == '\'':
		return p.parseString()
	case c == '-' || c == '.' || c >= '0' && c <= '9':
		return p.parseNumber()
	case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		return p.parseIdent()
	}

	return nil, exprErr("unexpected %q at position %d in %q", string(c), p.pos, p.s)
}

func (p *exprParser) parseString() (*logicExpr, error) {
	p.pos++

	var sb strings.Builder

	for p.pos < len(p.s) {
		c := p.s[p.pos]

		if c == '\'' {
			if p.pos+1 < len(p.s) && p.s[p.pos+1] == '\'' {
				sb.WriteByte('\'')

				p.pos += 2

				continue
			}

			p.pos++

			return &logicExpr{kind: exprLit, lit: sb.String()}, nil
		}

		sb.WriteByte(c)

		p.pos++
	}

	return nil, exprErr("unterminated string literal in %q", p.s)
}

func (p *exprParser) parseNumber() (*logicExpr, error) {
	start := p.pos
	if p.peek() == '-' {
		p.pos++
	}

	for p.pos < len(p.s) && (p.s[p.pos] >= '0' && p.s[p.pos] <= '9' || p.s[p.pos] == '.' ||
		p.s[p.pos] == 'e' || p.s[p.pos] == 'E') {
		p.pos++
	}

	txt := p.s[start:p.pos]

	if i, err := strconv.ParseInt(txt, 10, 64); err == nil {
		return &logicExpr{kind: exprLit, lit: i}, nil
	}

	f, err := strconv.ParseFloat(txt, 64)
	if err != nil {
		return nil, exprErr("invalid number %q", txt)
	}

	return &logicExpr{kind: exprLit, lit: f}, nil
}

func (p *exprParser) parseIdent() (*logicExpr, error) {
	start := p.pos

	for p.pos < len(p.s) && isIdentByte(p.s[p.pos]) {
		p.pos++
	}

	name := p.s[start:p.pos]

	p.skipSpace()

	if p.peek() != '(' {
		switch name {
		case "true":
			return &logicExpr{kind: exprLit, lit: true}, nil
		case logicLitFalse:
			return &logicExpr{kind: exprLit, lit: false}, nil
		case "null":
			return &logicExpr{kind: exprLit, lit: nil}, nil
		}

		return nil, exprErr("unknown identifier %q in %q", name, p.s)
	}

	p.pos++

	call := &logicExpr{kind: exprCall, name: name}

	p.skipSpace()

	if p.peek() == ')' {
		p.pos++

		return call, nil
	}

	for {
		arg, err := p.parsePostfix()
		if err != nil {
			return nil, err
		}

		call.args = append(call.args, arg)

		p.skipSpace()

		switch p.peek() {
		case ',':
			p.pos++
		case ')':
			p.pos++

			return call, nil
		default:
			return nil, exprErr("expected ',' or ')' at position %d in %q", p.pos, p.s)
		}
	}
}

// normLogic maps decoded JSON numbers to int64 when integral so arithmetic stays exact.
func normLogic(v any) any {
	switch t := v.(type) {
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
			return int64(t)
		}

		return t
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}

		f, _ := t.Float64()

		return f
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = normLogic(x)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = normLogic(x)
		}

		return out
	}

	return v
}

// logicString renders a value the way @{...} interpolation and string() do.
func logicString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "True"
		}

		return "False"
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	}

	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}

	return string(b)
}

// evalLogicTemplate resolves a string literal from a definition: a leading '@' makes it an expression, '@{...}'
// interpolates, '@@' escapes.
func evalLogicTemplate(s string, c *logicEvalCtx) (any, error) {
	if !strings.Contains(s, "@") {
		return s, nil
	}

	if strings.HasPrefix(s, "@@") {
		return evalLogicInterpolation(s, c)
	}

	if strings.HasPrefix(s, "@") && !strings.HasPrefix(s, "@{") {
		n, err := parseLogicExpr(s[1:])
		if err != nil {
			return nil, err
		}

		return c.eval(n)
	}

	return evalLogicInterpolation(s, c)
}

func evalLogicInterpolation(s string, c *logicEvalCtx) (any, error) {
	var sb strings.Builder

	for i := 0; i < len(s); i++ {
		if s[i] != '@' {
			sb.WriteByte(s[i])

			continue
		}

		if i+1 < len(s) && s[i+1] == '@' {
			sb.WriteByte('@')

			i++

			continue
		}

		if i+1 < len(s) && s[i+1] == '{' {
			end := matchingBrace(s, i+1)
			if end < 0 {
				return nil, exprErr("unterminated '@{' in %q", s)
			}

			n, err := parseLogicExpr(s[i+2 : end])
			if err != nil {
				return nil, err
			}

			v, err := c.eval(n)
			if err != nil {
				return nil, err
			}

			sb.WriteString(logicString(v))

			i = end

			continue
		}

		sb.WriteByte('@')
	}

	return sb.String(), nil
}

func matchingBrace(s string, open int) int {
	depth, inStr := 0, false

	for i := open; i < len(s); i++ {
		switch {
		case s[i] == '\'':
			inStr = !inStr
		case inStr:
		case s[i] == '{':
			depth++
		case s[i] == '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// evalLogicValue resolves every template string nested in a definition fragment.
func evalLogicValue(v any, c *logicEvalCtx) (any, error) {
	switch t := v.(type) {
	case string:
		return evalLogicTemplate(t, c)
	case map[string]any:
		out := make(map[string]any, len(t))

		for k, x := range t {
			r, err := evalLogicValue(x, c)
			if err != nil {
				return nil, err
			}

			out[k] = r
		}

		return out, nil
	case []any:
		out := make([]any, len(t))

		for i, x := range t {
			r, err := evalLogicValue(x, c)
			if err != nil {
				return nil, err
			}

			out[i] = r
		}

		return out, nil
	}

	return normLogic(v), nil
}
