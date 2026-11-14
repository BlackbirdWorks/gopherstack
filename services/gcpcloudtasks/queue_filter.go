package gcpcloudtasks

import (
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type queuePred func(*cloudtaskspb.Queue) bool

type filterToken struct {
	text   string
	quoted bool
}

// tokenizeQueueFilter splits a Logging-query-language filter into words, quoted strings,
// parentheses and comparison operators.
func tokenizeQueueFilter(s string) ([]filterToken, error) {
	var out []filterToken

	rs := []rune(s)

	for i := 0; i < len(rs); {
		var (
			tok filterToken
			err error
		)

		c := rs[i]

		switch {
		case c == ' ' || c == '\t' || c == '\n':
			i++

			continue
		case c == '(' || c == ')':
			tok, i = filterToken{text: string(c)}, i+1
		case c == '"':
			tok, i, err = readQuotedToken(rs, i)
		case strings.ContainsRune("=!<>:~", c):
			tok, i = readOperatorToken(rs, i)
		default:
			tok, i = readWordToken(rs, i)
		}

		if err != nil {
			return nil, err
		}

		out = append(out, tok)
	}

	return out, nil
}

func readQuotedToken(rs []rune, i int) (filterToken, int, error) {
	var sb strings.Builder

	i++

	for i < len(rs) && rs[i] != '"' {
		if rs[i] == '\\' && i+1 < len(rs) {
			i++
		}

		sb.WriteRune(rs[i])
		i++
	}

	if i >= len(rs) {
		return filterToken{}, i, errInvalid("Unterminated quoted string in filter.")
	}

	return filterToken{text: sb.String(), quoted: true}, i + 1, nil
}

func readOperatorToken(rs []rune, i int) (filterToken, int) {
	c, j := rs[i], i+1

	switch {
	case j < len(rs) && rs[j] == '=' && c != ':' && c != '~':
		j++
	case c == '!' && j < len(rs) && rs[j] == '~':
		j++
	}

	return filterToken{text: string(rs[i:j])}, j
}

func readWordToken(rs []rune, i int) (filterToken, int) {
	j := i
	for j < len(rs) && !strings.ContainsRune(" \t\n()\"=!<>:~", rs[j]) {
		j++
	}

	return filterToken{text: string(rs[i:j])}, j
}

type queueFilterParser struct {
	toks []filterToken
	pos  int
}

func (p *queueFilterParser) peek() (filterToken, bool) {
	if p.pos >= len(p.toks) {
		return filterToken{}, false
	}

	return p.toks[p.pos], true
}

func (p *queueFilterParser) keyword(kw string) bool {
	if t, ok := p.peek(); ok && !t.quoted && t.text == kw {
		p.pos++

		return true
	}

	return false
}

// parseQueueFilter compiles a Logging-query-language filter over Queue fields: NOT binds
// tighter than OR, OR tighter than AND, and adjacent terms are an implicit AND
// (docs.cloud.google.com/logging/docs/view/logging-query-language).
func parseQueueFilter(f string) (queuePred, error) {
	toks, err := tokenizeQueueFilter(f)
	if err != nil {
		return nil, err
	}

	if len(toks) == 0 {
		return func(*cloudtaskspb.Queue) bool { return true }, nil
	}

	p := &queueFilterParser{toks: toks}

	pred, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	if p.pos < len(toks) {
		return nil, errInvalid("Unexpected %q in filter.", toks[p.pos].text)
	}

	return pred, nil
}

func (p *queueFilterParser) parseAnd() (queuePred, error) {
	left, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	for {
		if t, ok := p.peek(); !ok || (!t.quoted && t.text == ")") {
			return left, nil
		}

		p.keyword("AND")

		right, rerr := p.parseOr()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(q *cloudtaskspb.Queue) bool { return l(q) && r(q) }
	}
}

func (p *queueFilterParser) parseOr() (queuePred, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.keyword("OR") {
		right, rerr := p.parseNot()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(q *cloudtaskspb.Queue) bool { return l(q) || r(q) }
	}

	return left, nil
}

func (p *queueFilterParser) parseNot() (queuePred, error) {
	if p.keyword("NOT") {
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return func(q *cloudtaskspb.Queue) bool { return !inner(q) }, nil
	}

	if t, ok := p.peek(); ok && !t.quoted && strings.HasPrefix(t.text, "-") && len(t.text) > 1 {
		p.toks[p.pos].text = t.text[1:]

		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return func(q *cloudtaskspb.Queue) bool { return !inner(q) }, nil
	}

	return p.parsePrimary()
}

func (p *queueFilterParser) parsePrimary() (queuePred, error) {
	t, ok := p.peek()
	if !ok {
		return nil, errInvalid("Unexpected end of filter.")
	}

	if !t.quoted && t.text == "(" {
		p.pos++

		inner, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		if c, cok := p.peek(); !cok || c.text != ")" {
			return nil, errInvalid("Missing closing parenthesis in filter.")
		}

		p.pos++

		return inner, nil
	}

	return p.parseComparison()
}

func (p *queueFilterParser) parseComparison() (queuePred, error) {
	field := p.toks[p.pos]
	p.pos++

	op, ok := p.peek()
	if !ok || op.quoted || !strings.ContainsAny(op.text[:1], "=!<>:~") {
		return nil, errInvalid("Filter term %q needs a comparison operator.", field.text)
	}

	p.pos++

	val, ok := p.peek()
	if !ok || (!val.quoted && (val.text == "(" || val.text == ")")) {
		return nil, errInvalid("Filter term %q needs a value.", field.text)
	}

	p.pos++

	return compileQueueComparison(snakeCase(field.text), op.text, val.text)
}

func snakeCase(path string) string {
	var sb strings.Builder

	for i, r := range path {
		if r >= 'A' && r <= 'Z' && i > 0 && path[i-1] != '.' {
			sb.WriteByte('_')
		}

		sb.WriteString(strings.ToLower(string(r)))
	}

	return sb.String()
}

func compileQueueComparison(path, op, want string) (queuePred, error) {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=", ":":
	default:
		return nil, errInvalid("Unsupported filter operator %q.", op)
	}

	rf, err := resolveQueueField(new(cloudtaskspb.Queue).ProtoReflect(), path)
	if err != nil {
		return nil, err
	}

	if (rf.desc.Message() != nil || rf.desc.IsList() || rf.desc.IsMap()) && (op != ":" || want != "*") {
		return nil, errInvalid("Filter field %q only supports :*.", path)
	}

	return func(q *cloudtaskspb.Queue) bool {
		fd, rerr := resolveQueueField(q.ProtoReflect(), path)
		if rerr != nil {
			return false
		}

		return compareQueueField(fd, op, want)
	}, nil
}

type resolvedField struct {
	desc protoreflect.FieldDescriptor
	val  protoreflect.Value
	has  bool
}

func resolveQueueField(m protoreflect.Message, path string) (resolvedField, error) {
	parts := strings.Split(path, ".")

	for i, name := range parts {
		fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			return resolvedField{}, errInvalid("Unknown filter field %q.", path)
		}

		last := i == len(parts)-1
		if last {
			return resolvedField{desc: fd, val: m.Get(fd), has: m.Has(fd)}, nil
		}

		if fd.Message() == nil || fd.IsList() || fd.IsMap() {
			return resolvedField{}, errInvalid("Filter field %q is not a message.", path)
		}

		m = m.Get(fd).Message()
	}

	return resolvedField{}, errInvalid("Empty filter field.")
}

func compareQueueField(f resolvedField, op, want string) bool {
	if op == ":" && want == "*" {
		return f.has
	}

	if f.desc.IsList() || f.desc.IsMap() || f.desc.Message() != nil {
		return false
	}

	have, num, isNum := scalarOf(f.desc, f.val)

	if op == ":" {
		return strings.Contains(strings.ToLower(have), strings.ToLower(want))
	}

	if isNum {
		if w, err := strconv.ParseFloat(want, 64); err == nil {
			return orderedCompare(op, compareFloat(num, w))
		}
	}

	return orderedCompare(op, strings.Compare(strings.ToLower(have), strings.ToLower(want)))
}

func scalarOf(fd protoreflect.FieldDescriptor, v protoreflect.Value) (string, float64, bool) {
	switch fd.Kind() {
	case protoreflect.EnumKind:
		if ev := fd.Enum().Values().ByNumber(v.Enum()); ev != nil {
			return string(ev.Name()), 0, false
		}

		return strconv.Itoa(int(v.Enum())), 0, false
	case protoreflect.BoolKind:
		return strconv.FormatBool(v.Bool()), 0, false
	case protoreflect.StringKind:
		return v.String(), 0, false
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return fmt.Sprint(v.Float()), v.Float(), true
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10), float64(v.Int()), true
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10), float64(v.Uint()), true
	default:
		return "", 0, false
	}
}

func compareFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func orderedCompare(op string, c int) bool {
	switch op {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	default:
		return false
	}
}
