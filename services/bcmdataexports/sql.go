package bcmdataexports

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

var errNotNumeric = errors.New("not numeric")

type tokKind int

const (
	tokIdent tokKind = iota
	tokQuoted
	tokString
	tokNumber
	tokOp
	tokEOF
)

type token struct {
	text string
	kind tokKind
}

type selectItem struct {
	col   string
	alias string
}

type literal struct {
	text  string
	isNum bool
}

type expr interface {
	eval(get func(col string) (any, bool)) (bool, error)
}

type boolExpr struct {
	l, r expr
	and  bool
}

type cmpExpr struct {
	col string
	op  string
	lit literal
}

type query struct {
	where expr
	table string
	items []selectItem
	limit int
	star  bool
}

func tokenize(s string) ([]token, error) {
	var toks []token

	rs := []rune(s)

	for i := 0; i < len(rs); {
		r := rs[i]

		switch {
		case unicode.IsSpace(r):
			i++
		case r == '\'' || r == '"':
			text, next, ok := readQuoted(rs, i)
			if !ok {
				return nil, fail(ErrValidation, "unterminated quoted text in query statement")
			}

			kind := tokString
			if r == '"' {
				kind = tokQuoted
			}

			toks = append(toks, token{kind: kind, text: text})
			i = next
		case unicode.IsLetter(r) || r == '_':
			j := scanWhile(rs, i, isIdentRune)
			toks = append(toks, token{kind: tokIdent, text: string(rs[i:j])})
			i = j
		case startsNumber(rs, i):
			j := scanWhile(rs, i+1, func(c rune) bool { return unicode.IsDigit(c) || c == '.' })
			toks = append(toks, token{kind: tokNumber, text: string(rs[i:j])})
			i = j
		case strings.ContainsRune("<>!=,*();", r):
			tk, next := lexOp(rs, i)
			toks = append(toks, tk)
			i = next
		default:
			return nil, fail(ErrValidation, "unexpected character %q in query statement", r)
		}
	}

	return append(toks, token{kind: tokEOF}), nil
}

func startsNumber(rs []rune, i int) bool {
	return unicode.IsDigit(rs[i]) || (rs[i] == '-' && i+1 < len(rs) && unicode.IsDigit(rs[i+1]))
}

func lexOp(rs []rune, i int) (token, int) {
	j := i + 1
	if strings.ContainsRune("<>!=", rs[i]) && j < len(rs) && strings.ContainsRune("=>", rs[j]) {
		j++
	}

	return token{kind: tokOp, text: string(rs[i:j])}, j
}

func isIdentRune(c rune) bool {
	return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_' || c == '.'
}

func scanWhile(rs []rune, from int, pred func(rune) bool) int {
	j := from
	for j < len(rs) && pred(rs[j]) {
		j++
	}

	return j
}

// readQuoted reads the quoted text opening at rs[i]; a doubled quote is an escaped quote.
func readQuoted(rs []rune, i int) (string, int, bool) {
	quote := rs[i]

	var sb strings.Builder

	for j := i + 1; j < len(rs); j++ {
		if rs[j] != quote {
			sb.WriteRune(rs[j])

			continue
		}

		if j+1 < len(rs) && rs[j+1] == quote {
			sb.WriteRune(quote)

			j++

			continue
		}

		return sb.String(), j + 1, true
	}

	return "", 0, false
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}

	return t
}

func (p *parser) keyword(kw string) bool {
	if t := p.peek(); t.kind == tokIdent && strings.EqualFold(t.text, kw) {
		p.pos++

		return true
	}

	return false
}

func (p *parser) op(o string) bool {
	if t := p.peek(); t.kind == tokOp && t.text == o {
		p.pos++

		return true
	}

	return false
}

func parseQuery(sql string) (*query, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}

	p := &parser{toks: toks}
	q := &query{limit: -1}

	if !p.keyword("SELECT") {
		return nil, fail(ErrValidation, "query statement must start with SELECT")
	}

	if err = p.parseItems(q); err != nil {
		return nil, err
	}

	if !p.keyword("FROM") {
		return nil, fail(ErrValidation, "query statement is missing FROM")
	}

	t := p.next()
	if t.kind != tokIdent && t.kind != tokQuoted {
		return nil, fail(ErrValidation, "query statement is missing a table name")
	}

	q.table = strings.ToUpper(t.text)

	if p.keyword("WHERE") {
		if q.where, err = p.parseOr(); err != nil {
			return nil, err
		}
	}

	if p.keyword("LIMIT") {
		n, nerr := strconv.Atoi(p.next().text)
		if nerr != nil || n < 0 {
			return nil, fail(ErrValidation, "LIMIT must be a non-negative integer")
		}

		q.limit = n
	}

	p.op(";")

	if p.peek().kind != tokEOF {
		return nil, fail(ErrValidation, "unsupported query statement syntax near %q", p.peek().text)
	}

	return q, nil
}

func (p *parser) parseItems(q *query) error {
	if p.op("*") {
		q.star = true

		return nil
	}

	for {
		t := p.next()
		if t.kind != tokIdent && t.kind != tokQuoted {
			return fail(ErrValidation, "expected a column name in the SELECT list")
		}

		item := selectItem{col: strings.ToLower(t.text)}
		if p.keyword("AS") {
			a := p.next()
			if a.kind != tokIdent && a.kind != tokQuoted {
				return fail(ErrValidation, "expected an alias after AS")
			}

			item.alias = a.text
		}

		q.items = append(q.items, item)

		if !p.op(",") {
			return nil
		}
	}
}

func (p *parser) parseOr() (expr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.keyword("OR") {
		r, rerr := p.parseAnd()
		if rerr != nil {
			return nil, rerr
		}

		l = &boolExpr{l: l, r: r}
	}

	return l, nil
}

func (p *parser) parseAnd() (expr, error) {
	l, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for p.keyword("AND") {
		r, rerr := p.parseTerm()
		if rerr != nil {
			return nil, rerr
		}

		l = &boolExpr{l: l, r: r, and: true}
	}

	return l, nil
}

func (p *parser) parseTerm() (expr, error) {
	if p.op("(") {
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if !p.op(")") {
			return nil, fail(ErrValidation, "missing closing parenthesis in query statement")
		}

		return e, nil
	}

	c := p.next()
	if c.kind != tokIdent && c.kind != tokQuoted {
		return nil, fail(ErrValidation, "expected a column name in WHERE")
	}

	var op string

	if t := p.next(); t.kind == tokOp && slices.Contains([]string{"=", "!=", "<>", "<", ">", "<=", ">="}, t.text) {
		op = t.text
	} else if t.kind == tokIdent && strings.EqualFold(t.text, "LIKE") {
		op = "LIKE"
	} else {
		return nil, fail(ErrValidation, "unsupported operator %q in WHERE", t.text)
	}

	v := p.next()
	if v.kind != tokString && v.kind != tokNumber {
		return nil, fail(ErrValidation, "expected a literal value in WHERE")
	}

	return &cmpExpr{col: strings.ToLower(c.text), op: op, lit: literal{text: v.text, isNum: v.kind == tokNumber}}, nil
}

func (b *boolExpr) eval(get func(string) (any, bool)) (bool, error) {
	l, err := b.l.eval(get)
	if err != nil {
		return false, err
	}

	if b.and && !l || !b.and && l {
		return l, nil
	}

	return b.r.eval(get)
}

func (c *cmpExpr) eval(get func(string) (any, bool)) (bool, error) {
	v, ok := get(c.col)
	if !ok {
		return false, fail(ErrValidation, "column %q does not exist in the table", c.col)
	}

	if c.op == "LIKE" {
		return likeMatch(fmt.Sprint(orEmpty(v)), c.lit.text), nil
	}

	if lf, lerr := toFloat(v); lerr == nil && c.lit.isNum {
		rf, _ := strconv.ParseFloat(c.lit.text, 64)

		return compareOrdered(c.op, cmpFloat(lf, rf)), nil
	}

	return compareOrdered(c.op, strings.Compare(fmt.Sprint(orEmpty(v)), c.lit.text)), nil
}

func orEmpty(v any) any {
	if v == nil {
		return ""
	}

	return v
}

func toFloat(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case string:
		return strconv.ParseFloat(n, 64)
	default:
		return 0, errNotNumeric
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareOrdered(op string, c int) bool {
	switch op {
	case "=":
		return c == 0
	case "!=", "<>":
		return c != 0
	case "<":
		return c < 0
	case ">":
		return c > 0
	case "<=":
		return c <= 0
	default:
		return c >= 0
	}
}

func likeMatch(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}

	parts := strings.Split(pattern, "%")
	if len(parts) == 1 {
		return s == pattern
	}

	if !strings.HasPrefix(s, parts[0]) {
		return false
	}

	s = s[len(parts[0]):]

	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(s, mid)
		if i < 0 {
			return false
		}

		s = s[i+len(mid):]
	}

	return strings.HasSuffix(s, parts[len(parts)-1])
}

// result is a projected, filtered view of a table.
type result struct {
	cols []columnDef
	rows [][]any
}

// run evaluates q over the full table columns and rows.
func (q *query) run(all []columnDef, rows [][]any) (*result, error) {
	idx := make(map[string]int, len(all))
	for i, c := range all {
		idx[strings.ToLower(c.Name)] = i
	}

	picks, out, err := q.project(all, idx)
	if err != nil {
		return nil, err
	}

	if q.where != nil {
		if err = validateWhere(q.where, idx); err != nil {
			return nil, err
		}
	}

	res := &result{cols: out}

	for _, row := range rows {
		if q.limit >= 0 && len(res.rows) >= q.limit {
			break
		}

		if q.where != nil {
			ok, werr := q.where.eval(func(col string) (any, bool) { return row[idx[col]], true })
			if werr != nil {
				return nil, werr
			}

			if !ok {
				continue
			}
		}

		pr := make([]any, len(picks))
		for j, i := range picks {
			pr[j] = row[i]
		}

		res.rows = append(res.rows, pr)
	}

	return res, nil
}

func (q *query) project(all []columnDef, idx map[string]int) ([]int, []columnDef, error) {
	var picks []int

	var out []columnDef

	if q.star {
		for i, c := range all {
			picks = append(picks, i)
			out = append(out, c)
		}
	}

	for _, it := range q.items {
		i, ok := idx[it.col]
		if !ok {
			return nil, nil, fail(ErrValidation, "column %q does not exist in table %s", it.col, q.table)
		}

		c := all[i]
		if it.alias != "" {
			c.Name = it.alias
		}

		picks = append(picks, i)
		out = append(out, c)
	}

	return picks, out, nil
}

func validateWhere(e expr, idx map[string]int) error {
	switch n := e.(type) {
	case *boolExpr:
		if err := validateWhere(n.l, idx); err != nil {
			return err
		}

		return validateWhere(n.r, idx)
	case *cmpExpr:
		if _, ok := idx[n.col]; !ok {
			return fail(ErrValidation, "column %q does not exist in the table", n.col)
		}
	}

	return nil
}
