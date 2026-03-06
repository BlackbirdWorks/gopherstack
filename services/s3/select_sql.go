package s3

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

// sqlTokenType identifies the kind of a SQL token.
type sqlTokenType int

const (
	tokEOF sqlTokenType = iota
	tokIdent
	tokString
	tokNumber
	tokStar
	tokComma
	tokDot
	tokLParen
	tokRParen
	tokEq  // =
	tokNeq // != or <>
	tokLt
	tokLte
	tokGt
	tokGte
)

// Sentinel errors for the SQL tokeniser and parser.
var (
	errUnterminatedString  = errors.New("unterminated string literal")
	errUnterminatedIdent   = errors.New("unterminated quoted identifier")
	errExpectedLIKE        = errors.New("expected LIKE after NOT")
	errExpectedInListToken = errors.New("expected , or ) in IN list")
	errUnexpectedChar      = errors.New("unexpected character")
	errUnexpectedToken     = errors.New("unexpected token")
	errExpectedKeyword     = errors.New("expected keyword")
	errExpectedTokenType   = errors.New("expected token type")
	errUnknownOperator     = errors.New("unknown operator")
)

// sqlNullType is the internal representation of SQL NULL.
// Using a named type avoids (nil, nil) returns from eval functions.
type sqlNullType struct{}

// sqlNullValue is the single sentinel value representing SQL NULL.
var sqlNullValue any = sqlNullType{} //nolint:gochecknoglobals // package-level singleton for SQL NULL representation

// sqlToken is a single token produced by the SQL tokeniser.
type sqlToken struct {
	val string
	typ sqlTokenType
}

// sqlTokeniser produces tokens from a SQL string.
type sqlTokeniser struct {
	src string
	pos int
}

func newSQLTokeniser(src string) *sqlTokeniser {
	return &sqlTokeniser{src: src}
}

func (t *sqlTokeniser) peek() (sqlToken, error) {
	saved := t.pos
	tok, err := t.next()
	t.pos = saved

	return tok, err
}

// next returns the next token.
func (t *sqlTokeniser) next() (sqlToken, error) { //nolint:cyclop // tokeniser switch inherently broad
	t.skipWhitespace()

	if t.pos >= len(t.src) {
		return sqlToken{typ: tokEOF}, nil
	}

	ch := t.src[t.pos]

	switch {
	case ch == '\'':
		return t.readString()
	case ch == '"':
		return t.readQuotedIdent()
	case ch == '*':
		t.pos++

		return sqlToken{typ: tokStar, val: "*"}, nil
	case ch == ',':
		t.pos++

		return sqlToken{typ: tokComma, val: ","}, nil
	case ch == '.':
		t.pos++

		return sqlToken{typ: tokDot, val: "."}, nil
	case ch == '(':
		t.pos++

		return sqlToken{typ: tokLParen, val: "("}, nil
	case ch == ')':
		t.pos++

		return sqlToken{typ: tokRParen, val: ")"}, nil
	case ch == '=':
		t.pos++

		return sqlToken{typ: tokEq, val: "="}, nil
	case unicode.IsDigit(rune(ch)) || (ch == '-' && t.pos+1 < len(t.src) && unicode.IsDigit(rune(t.src[t.pos+1]))):
		return t.readNumber()
	case unicode.IsLetter(rune(ch)) || ch == '_':
		return t.readIdent()
	}

	return t.readOperator(ch)
}

func (t *sqlTokeniser) skipWhitespace() {
	for t.pos < len(t.src) && unicode.IsSpace(rune(t.src[t.pos])) {
		t.pos++
	}
}

func (t *sqlTokeniser) readOperator(ch byte) (sqlToken, error) {
	switch {
	case ch == '!' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '=':
		t.pos += 2

		return sqlToken{typ: tokNeq, val: "!="}, nil
	case ch == '<' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '>':
		t.pos += 2

		return sqlToken{typ: tokNeq, val: "<>"}, nil
	case ch == '<' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '=':
		t.pos += 2

		return sqlToken{typ: tokLte, val: "<="}, nil
	case ch == '>' && t.pos+1 < len(t.src) && t.src[t.pos+1] == '=':
		t.pos += 2

		return sqlToken{typ: tokGte, val: ">="}, nil
	case ch == '<':
		t.pos++

		return sqlToken{typ: tokLt, val: "<"}, nil
	case ch == '>':
		t.pos++

		return sqlToken{typ: tokGt, val: ">"}, nil
	}

	return sqlToken{}, fmt.Errorf("unexpected character %q at position %d: %w", ch, t.pos, errUnexpectedChar)
}

func (t *sqlTokeniser) readString() (sqlToken, error) {
	t.pos++ // skip opening quote
	start := t.pos

	for t.pos < len(t.src) && t.src[t.pos] != '\'' {
		t.pos++
	}

	if t.pos >= len(t.src) {
		return sqlToken{}, errUnterminatedString
	}

	val := t.src[start:t.pos]
	t.pos++ // skip closing quote

	return sqlToken{typ: tokString, val: val}, nil
}

func (t *sqlTokeniser) readQuotedIdent() (sqlToken, error) {
	t.pos++ // skip opening quote
	start := t.pos

	for t.pos < len(t.src) && t.src[t.pos] != '"' {
		t.pos++
	}

	if t.pos >= len(t.src) {
		return sqlToken{}, errUnterminatedIdent
	}

	val := t.src[start:t.pos]
	t.pos++ // skip closing quote

	return sqlToken{typ: tokIdent, val: val}, nil
}

func (t *sqlTokeniser) readNumber() (sqlToken, error) {
	start := t.pos

	if t.pos < len(t.src) && t.src[t.pos] == '-' {
		t.pos++
	}

	for t.pos < len(t.src) && (unicode.IsDigit(rune(t.src[t.pos])) || t.src[t.pos] == '.') {
		t.pos++
	}

	return sqlToken{typ: tokNumber, val: t.src[start:t.pos]}, nil
}

func (t *sqlTokeniser) readIdent() (sqlToken, error) {
	start := t.pos

	for t.pos < len(t.src) {
		ch := t.src[t.pos]
		if !unicode.IsLetter(rune(ch)) && !unicode.IsDigit(rune(ch)) && ch != '_' {
			break
		}

		t.pos++
	}

	return sqlToken{typ: tokIdent, val: t.src[start:t.pos]}, nil
}

// sqlQuery is the parsed result of a SELECT statement.
type sqlQuery struct {
	condition  sqlExpr
	tableAlias string
	columns    []selectColumn
	limit      int
	selectAll  bool
}

// selectColumn represents a single column projection.
type selectColumn struct {
	expr  sqlExpr
	alias string
}

// sqlExpr is a node in the SQL expression AST.
type sqlExpr interface {
	eval(row sqlRow) (any, error)
}

// sqlRow provides access to a row's field values by name.
type sqlRow interface {
	field(name string) (string, bool)
}

// sqlParser builds a sqlQuery from a SQL string.
type sqlParser struct {
	tok *sqlTokeniser
}

func newSQLParser(src string) *sqlParser {
	return &sqlParser{tok: newSQLTokeniser(src)}
}

// parseSQL parses a SELECT statement and returns a sqlQuery.
func parseSQL(src string) (*sqlQuery, error) {
	return newSQLParser(src).parse()
}

func (p *sqlParser) parse() (*sqlQuery, error) { //nolint:cyclop // SQL parsing inherently has many branches
	q := &sqlQuery{}

	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}

	cols, selectAll, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}

	q.columns = cols
	q.selectAll = selectAll

	if fromErr := p.expectKeyword("FROM"); fromErr != nil {
		return nil, fromErr
	}

	// consume table name (s3object or similar)
	if _, err = p.tok.next(); err != nil {
		return nil, err
	}

	// optional alias
	if tok, peekErr := p.tok.peek(); peekErr == nil && tok.typ == tokIdent && !isKeyword(tok.val) {
		if _, peekErr = p.tok.next(); peekErr != nil {
			return nil, peekErr
		}

		q.tableAlias = tok.val
	}

	// optional WHERE
	if tok, peekErr := p.tok.peek(); peekErr == nil && tok.typ == tokIdent && strings.EqualFold(tok.val, "WHERE") {
		if _, peekErr = p.tok.next(); peekErr != nil {
			return nil, peekErr
		}

		if q.condition, err = p.parseOr(); err != nil {
			return nil, err
		}
	}

	// optional LIMIT
	if tok, peekErr := p.tok.peek(); peekErr == nil && tok.typ == tokIdent && strings.EqualFold(tok.val, "LIMIT") {
		if _, peekErr = p.tok.next(); peekErr != nil {
			return nil, peekErr
		}

		if q.limit, err = p.parseLimit(); err != nil {
			return nil, err
		}
	}

	return q, nil
}

func (p *sqlParser) parseLimit() (int, error) {
	numTok, err := p.tok.next()
	if err != nil {
		return 0, err
	}

	n, convErr := strconv.Atoi(numTok.val)
	if convErr != nil {
		return 0, fmt.Errorf("LIMIT value must be an integer: %w", convErr)
	}

	return n, nil
}

func (p *sqlParser) parseSelectList() ([]selectColumn, bool, error) {
	tok, err := p.tok.peek()
	if err != nil {
		return nil, false, err
	}

	if tok.typ == tokStar {
		if _, err = p.tok.next(); err != nil {
			return nil, false, err
		}

		return nil, true, nil
	}

	var cols []selectColumn

	for {
		col, colErr := p.parseSelectColumn()
		if colErr != nil {
			return nil, false, colErr
		}

		cols = append(cols, col)

		next, peekErr := p.tok.peek()
		if peekErr != nil {
			return nil, false, peekErr
		}

		if next.typ != tokComma {
			break
		}

		if _, err = p.tok.next(); err != nil { // consume comma
			return nil, false, err
		}
	}

	return cols, false, nil
}

func (p *sqlParser) parseSelectColumn() (selectColumn, error) {
	expr, err := p.parseExprAtom()
	if err != nil {
		return selectColumn{}, err
	}

	col := selectColumn{expr: expr}

	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return selectColumn{}, peekErr
	}

	if tok.typ == tokIdent && strings.EqualFold(tok.val, "AS") {
		if _, err = p.tok.next(); err != nil {
			return selectColumn{}, err
		}

		aliasTok, aliasErr := p.tok.next()
		if aliasErr != nil {
			return selectColumn{}, aliasErr
		}

		col.alias = aliasTok.val
	}

	return col, nil
}

// parseOr handles OR expressions.
func (p *sqlParser) parseOr() (sqlExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for {
		tok, peekErr := p.tok.peek()
		if peekErr != nil {
			return left, peekErr
		}

		if tok.typ != tokIdent || !strings.EqualFold(tok.val, "OR") {
			break
		}

		if _, err = p.tok.next(); err != nil {
			return nil, err
		}

		right, rightErr := p.parseAnd()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &sqlBinaryExpr{op: "OR", left: left, right: right}
	}

	return left, nil
}

// parseAnd handles AND expressions.
func (p *sqlParser) parseAnd() (sqlExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for {
		tok, peekErr := p.tok.peek()
		if peekErr != nil {
			return left, peekErr
		}

		if tok.typ != tokIdent || !strings.EqualFold(tok.val, "AND") {
			break
		}

		if _, err = p.tok.next(); err != nil {
			return nil, err
		}

		right, rightErr := p.parseNot()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &sqlBinaryExpr{op: "AND", left: left, right: right}
	}

	return left, nil
}

// parseNot handles NOT expressions.
func (p *sqlParser) parseNot() (sqlExpr, error) {
	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return nil, peekErr
	}

	if tok.typ == tokIdent && strings.EqualFold(tok.val, "NOT") {
		if _, err := p.tok.next(); err != nil {
			return nil, err
		}

		inner, innerErr := p.parseComparison()
		if innerErr != nil {
			return nil, innerErr
		}

		return &sqlNotExpr{inner: inner}, nil
	}

	return p.parseComparison()
}

// parseComparison handles comparison operators and IS NULL / IS NOT NULL / LIKE / BETWEEN / IN.
func (p *sqlParser) parseComparison() (sqlExpr, error) {
	left, err := p.parseExprAtom()
	if err != nil {
		return nil, err
	}

	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return left, peekErr
	}

	switch {
	case isComparisonOp(tok.typ):
		return p.parseComparisonOp(left, tok)

	case tok.typ == tokIdent && strings.EqualFold(tok.val, "IS"):
		return p.parseIsNull(left)

	case tok.typ == tokIdent && strings.EqualFold(tok.val, "LIKE"):
		return p.parseLike(left, false)

	case tok.typ == tokIdent && strings.EqualFold(tok.val, "NOT"):
		return p.parseNotLike(left)

	case tok.typ == tokIdent && strings.EqualFold(tok.val, "BETWEEN"):
		return p.parseBetween(left)

	case tok.typ == tokIdent && strings.EqualFold(tok.val, "IN"):
		return p.parseIn(left)

	default:
		return left, nil
	}
}

func isComparisonOp(t sqlTokenType) bool {
	switch t {
	case tokEq, tokNeq, tokLt, tokLte, tokGt, tokGte:
		return true
	default:
		return false
	}
}

func (p *sqlParser) parseComparisonOp(left sqlExpr, tok sqlToken) (sqlExpr, error) {
	if _, err := p.tok.next(); err != nil {
		return nil, err
	}

	right, err := p.parseExprAtom()
	if err != nil {
		return nil, err
	}

	return &sqlBinaryExpr{op: tok.val, left: left, right: right}, nil
}

func (p *sqlParser) parseIsNull(left sqlExpr) (sqlExpr, error) {
	if _, err := p.tok.next(); err != nil { // consume IS
		return nil, err
	}

	next, err := p.tok.peek()
	if err != nil {
		return nil, err
	}

	if next.typ == tokIdent && strings.EqualFold(next.val, "NOT") {
		if _, err = p.tok.next(); err != nil { // consume NOT
			return nil, err
		}

		if err = p.expectKeyword("NULL"); err != nil {
			return nil, err
		}

		return &sqlIsNullExpr{inner: left, notNull: true}, nil
	}

	if err = p.expectKeyword("NULL"); err != nil {
		return nil, err
	}

	return &sqlIsNullExpr{inner: left, notNull: false}, nil
}

func (p *sqlParser) parseLike(left sqlExpr, negated bool) (sqlExpr, error) {
	if _, err := p.tok.next(); err != nil { // consume LIKE
		return nil, err
	}

	pattern, err := p.tok.next()
	if err != nil {
		return nil, err
	}

	expr := sqlExpr(&sqlLikeExpr{left: left, pattern: pattern.val})

	if negated {
		expr = &sqlNotExpr{inner: expr}
	}

	return expr, nil
}

func (p *sqlParser) parseNotLike(left sqlExpr) (sqlExpr, error) {
	if _, err := p.tok.next(); err != nil { // consume NOT
		return nil, err
	}

	next, err := p.tok.peek()
	if err != nil {
		return nil, err
	}

	if next.typ != tokIdent || !strings.EqualFold(next.val, "LIKE") {
		return nil, errExpectedLIKE
	}

	return p.parseLike(left, true)
}

func (p *sqlParser) parseBetween(left sqlExpr) (sqlExpr, error) {
	if _, err := p.tok.next(); err != nil { // consume BETWEEN
		return nil, err
	}

	low, err := p.parseExprAtom()
	if err != nil {
		return nil, err
	}

	if err = p.expectKeyword("AND"); err != nil {
		return nil, err
	}

	high, err := p.parseExprAtom()
	if err != nil {
		return nil, err
	}

	return &sqlBetweenExpr{val: left, low: low, high: high}, nil
}

func (p *sqlParser) parseIn(left sqlExpr) (sqlExpr, error) {
	if _, err := p.tok.next(); err != nil { // consume IN
		return nil, err
	}

	items, err := p.parseInList()
	if err != nil {
		return nil, err
	}

	return &sqlInExpr{val: left, items: items}, nil
}

func (p *sqlParser) parseInList() ([]sqlExpr, error) {
	if err := p.expectToken(tokLParen); err != nil {
		return nil, err
	}

	var items []sqlExpr

	for {
		item, err := p.parseExprAtom()
		if err != nil {
			return nil, err
		}

		items = append(items, item)

		tok, peekErr := p.tok.peek()
		if peekErr != nil {
			return nil, peekErr
		}

		if tok.typ == tokRParen {
			if _, err = p.tok.next(); err != nil {
				return nil, err
			}

			break
		}

		if tok.typ != tokComma {
			return nil, errExpectedInListToken
		}

		if _, err = p.tok.next(); err != nil {
			return nil, err
		}
	}

	return items, nil
}

// parseExprAtom parses a primary expression: literal, column reference, or function call.
func (p *sqlParser) parseExprAtom() (sqlExpr, error) {
	tok, err := p.tok.next()
	if err != nil {
		return nil, err
	}

	switch tok.typ {
	case tokString:
		return &sqlLiteral{val: tok.val}, nil

	case tokNumber:
		return &sqlLiteral{val: tok.val, numeric: true}, nil

	case tokIdent:
		return p.parseIdentExpr(tok.val)

	case tokLParen:
		return p.parseParenExpr()

	case tokStar:
		return &sqlStarExpr{}, nil

	default:
		return nil, fmt.Errorf("unexpected token %q: %w", tok.val, errUnexpectedToken)
	}
}

func (p *sqlParser) parseIdentExpr(name string) (sqlExpr, error) {
	upper := strings.ToUpper(name)

	switch upper {
	case "NULL":
		return &sqlLiteral{null: true}, nil
	case "TRUE":
		return &sqlLiteral{val: "true"}, nil
	case "FALSE":
		return &sqlLiteral{val: "false"}, nil
	case "CAST":
		return p.parseCast()
	default:
		return p.parseColumnRef(name)
	}
}

func (p *sqlParser) parseParenExpr() (sqlExpr, error) {
	inner, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if err = p.expectToken(tokRParen); err != nil {
		return nil, err
	}

	return inner, nil
}

func (p *sqlParser) parseColumnRef(name string) (sqlExpr, error) {
	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return &sqlColumnRef{name: name}, peekErr
	}

	// alias.column — consume the dot and the column name
	if tok.typ != tokDot {
		return &sqlColumnRef{name: name}, nil
	}

	if _, err := p.tok.next(); err != nil { // consume dot
		return nil, err
	}

	colTok, colErr := p.tok.next()
	if colErr != nil {
		return nil, colErr
	}

	if colTok.typ == tokStar {
		return &sqlStarExpr{}, nil
	}

	return &sqlColumnRef{name: colTok.val}, nil
}

func (p *sqlParser) parseCast() (sqlExpr, error) {
	if err := p.expectToken(tokLParen); err != nil {
		return nil, err
	}

	inner, err := p.parseExprAtom()
	if err != nil {
		return nil, err
	}

	if err = p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	typTok, typErr := p.tok.next()
	if typErr != nil {
		return nil, typErr
	}

	if err = p.expectToken(tokRParen); err != nil {
		return nil, err
	}

	return &sqlCastExpr{inner: inner, castType: strings.ToUpper(typTok.val)}, nil
}

func (p *sqlParser) expectKeyword(kw string) error {
	tok, err := p.tok.next()
	if err != nil {
		return err
	}

	if tok.typ != tokIdent || !strings.EqualFold(tok.val, kw) {
		return fmt.Errorf("expected keyword %q, got %q: %w", kw, tok.val, errExpectedKeyword)
	}

	return nil
}

func (p *sqlParser) expectToken(t sqlTokenType) error {
	tok, err := p.tok.next()
	if err != nil {
		return err
	}

	if tok.typ != t {
		return fmt.Errorf("expected token type %d, got %q: %w", t, tok.val, errExpectedTokenType)
	}

	return nil
}

func isKeyword(s string) bool {
	switch strings.ToUpper(s) {
	case "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "LIKE", "IS", "NULL",
		"LIMIT", "AS", "CAST", "BETWEEN", "IN", "TRUE", "FALSE":
		return true
	default:
		return false
	}
}

// ---- AST node types ----

// sqlLiteral represents a constant value.
type sqlLiteral struct {
	val     string
	numeric bool
	null    bool
}

func (l *sqlLiteral) eval(_ sqlRow) (any, error) {
	if l.null {
		return sqlNullValue, nil
	}

	return l.val, nil
}

// sqlColumnRef is a reference to a column by name or positional _N.
type sqlColumnRef struct {
	name string
}

func (c *sqlColumnRef) eval(row sqlRow) (any, error) {
	// Support positional _1, _2, etc. for string rows.
	if val, found := evalPositionalRef(c.name, row); found {
		return val, nil
	}

	val, _ := row.field(c.name)

	return val, nil
}

// evalPositionalRef handles positional column references like _1, _2, etc.
// Returns (value, true) if the name is a valid positional reference for a stringRow, otherwise (_, false).
func evalPositionalRef(name string, row sqlRow) (string, bool) {
	if len(name) < 2 || name[0] != '_' {
		return "", false
	}

	sr, ok := row.(*stringRow)
	if !ok {
		return "", false
	}

	idx, err := strconv.Atoi(name[1:])
	if err != nil || idx < 1 {
		return "", false
	}

	keys := sortedKeys(sr.data)
	if idx-1 >= len(keys) {
		return "", true // out-of-range positional: return empty (found=true to skip field lookup)
	}

	return sr.data[keys[idx-1]], true
}

// sqlStarExpr represents SELECT *.
type sqlStarExpr struct{}

func (s *sqlStarExpr) eval(_ sqlRow) (any, error) {
	return "*", nil
}

// sqlBinaryExpr handles comparison and logical operators.
type sqlBinaryExpr struct {
	left, right sqlExpr
	op          string
}

func (b *sqlBinaryExpr) eval(row sqlRow) (any, error) {
	left, leftErr := b.left.eval(row)
	if leftErr != nil {
		return nil, leftErr
	}

	switch strings.ToUpper(b.op) {
	case "AND":
		if !isTruthy(left) {
			return false, nil
		}

		right, rightErr := b.right.eval(row)
		if rightErr != nil {
			return nil, rightErr
		}

		return isTruthy(right), nil

	case "OR":
		if isTruthy(left) {
			return true, nil
		}

		right, rightErr := b.right.eval(row)
		if rightErr != nil {
			return nil, rightErr
		}

		return isTruthy(right), nil
	}

	right, rightErr := b.right.eval(row)
	if rightErr != nil {
		return nil, rightErr
	}

	return compareSQLValues(b.op, left, right)
}

// sqlNotExpr negates a boolean expression.
type sqlNotExpr struct {
	inner sqlExpr
}

func (n *sqlNotExpr) eval(row sqlRow) (any, error) {
	val, err := n.inner.eval(row)
	if err != nil {
		return nil, err
	}

	return !isTruthy(val), nil
}

// sqlIsNullExpr checks for NULL.
type sqlIsNullExpr struct {
	inner   sqlExpr
	notNull bool
}

func (n *sqlIsNullExpr) eval(row sqlRow) (any, error) {
	val, err := n.inner.eval(row)
	if err != nil {
		return nil, err
	}

	_, isNullType := val.(sqlNullType)
	isNull := val == nil || val == "" || isNullType

	if n.notNull {
		return !isNull, nil
	}

	return isNull, nil
}

// sqlLikeExpr implements SQL LIKE pattern matching.
type sqlLikeExpr struct {
	left    sqlExpr
	pattern string
}

func (l *sqlLikeExpr) eval(row sqlRow) (any, error) {
	val, err := l.left.eval(row)
	if err != nil {
		return nil, err
	}

	return sqlLikeMatch(l.pattern, fmt.Sprintf("%v", val)), nil
}

// sqlCastExpr casts a value to a target type.
type sqlCastExpr struct {
	inner    sqlExpr
	castType string
}

func (c *sqlCastExpr) eval(row sqlRow) (any, error) {
	val, err := c.inner.eval(row)
	if err != nil {
		return nil, err
	}

	return castSQLValue(fmt.Sprintf("%v", val), c.castType)
}

func castSQLValue(s, castType string) (string, error) {
	switch castType {
	case "INT", "INTEGER", "BIGINT", "SMALLINT":
		n, convErr := strconv.ParseFloat(s, 64)
		if convErr != nil {
			return "", convErr
		}

		return strconv.FormatInt(int64(n), 10), nil

	case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL":
		return s, nil

	case "STRING", "CHAR", "VARCHAR", "TEXT":
		return s, nil

	case "BOOL", "BOOLEAN":
		switch strings.ToLower(s) {
		case "true", "1", "yes":
			return "true", nil
		default:
			return "false", nil
		}

	case "TIMESTAMP":
		return s, nil

	default:
		return s, nil
	}
}

// sqlBetweenExpr handles BETWEEN ... AND ...
type sqlBetweenExpr struct {
	val, low, high sqlExpr
}

func (b *sqlBetweenExpr) eval(row sqlRow) (any, error) {
	val, err := b.val.eval(row)
	if err != nil {
		return nil, err
	}

	low, err := b.low.eval(row)
	if err != nil {
		return nil, err
	}

	high, err := b.high.eval(row)
	if err != nil {
		return nil, err
	}

	ge, err := compareSQLValues(">=", val, low)
	if err != nil {
		return nil, err
	}

	le, err := compareSQLValues("<=", val, high)
	if err != nil {
		return nil, err
	}

	return isTruthy(ge) && isTruthy(le), nil
}

// sqlInExpr handles IN (...)
type sqlInExpr struct {
	val   sqlExpr
	items []sqlExpr
}

func (n *sqlInExpr) eval(row sqlRow) (any, error) {
	val, err := n.val.eval(row)
	if err != nil {
		return nil, err
	}

	for _, item := range n.items {
		itemVal, itemErr := item.eval(row)
		if itemErr != nil {
			return nil, itemErr
		}

		eq, eqErr := compareSQLValues("=", val, itemVal)
		if eqErr != nil {
			return nil, eqErr
		}

		if isTruthy(eq) {
			return true, nil
		}
	}

	return false, nil
}

// ---- helpers ----

// isTruthy converts a value to boolean.
func isTruthy(v any) bool {
	if v == nil {
		return false
	}

	switch val := v.(type) {
	case bool:
		return val
	case string:
		switch strings.ToLower(val) {
		case "", "false", "0":
			return false
		default:
			return true
		}
	case float64:
		return val != 0
	case int64:
		return val != 0
	case sqlNullType:
		return false
	default:
		return false
	}
}

// compareSQLValues compares two values with a SQL operator.
// Both numeric and string comparisons are supported.
func compareSQLValues(op string, left, right any) (bool, error) {
	if left == nil || right == nil {
		return false, nil
	}

	// SQL NULL comparisons always return false.
	if _, ok := left.(sqlNullType); ok {
		return false, nil
	}

	if _, ok := right.(sqlNullType); ok {
		return false, nil
	}

	ls := fmt.Sprintf("%v", left)
	rs := fmt.Sprintf("%v", right)

	// Try numeric comparison first.
	ln, leftNumErr := strconv.ParseFloat(ls, 64)
	rn, rightNumErr := strconv.ParseFloat(rs, 64)

	if leftNumErr == nil && rightNumErr == nil {
		return compareFloat(op, ln, rn)
	}

	return compareString(op, ls, rs)
}

func compareFloat(op string, l, r float64) (bool, error) {
	const eps = 1e-12

	switch op {
	case "=":
		return math.Abs(l-r) < eps, nil
	case "!=", "<>":
		return math.Abs(l-r) >= eps, nil
	case "<":
		return l < r, nil
	case "<=":
		return l <= r, nil
	case ">":
		return l > r, nil
	case ">=":
		return l >= r, nil
	default:
		return false, fmt.Errorf("unknown operator %q: %w", op, errUnknownOperator)
	}
}

func compareString(op string, l, r string) (bool, error) {
	cmp := strings.Compare(l, r)

	switch op {
	case "=":
		return cmp == 0, nil
	case "!=", "<>":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unknown operator %q: %w", op, errUnknownOperator)
	}
}

// sqlLikeMatch matches a string against an SQL LIKE pattern.
// % matches any sequence of characters, _ matches a single character.
func sqlLikeMatch(pattern, s string) bool {
	return likeMatch(pattern, s)
}

// likeMatch performs SQL LIKE pattern matching in linear time.
// % matches any sequence of characters, _ matches any single character.
func likeMatch(pattern, s string) bool {
	pLen := len(pattern)
	sLen := len(s)

	p := 0
	i := 0
	starIdx := -1
	match := 0

	for i < sLen {
		if p < pLen && (pattern[p] == s[i] || pattern[p] == '_') {
			i++
			p++

			continue
		}

		if p < pLen && pattern[p] == '%' {
			starIdx = p
			match = i
			p++

			continue
		}

		if starIdx != -1 {
			p = starIdx + 1
			match++
			i = match

			continue
		}

		return false
	}

	for p < pLen && pattern[p] == '%' {
		p++
	}

	return p == pLen
}

// evalQuery applies a parsed sqlQuery to a set of rows.
// Each row is a map of column name → string value.
func evalQuery(q *sqlQuery, rows []map[string]string) ([]map[string]string, error) {
	var result []map[string]string

	for _, rawRow := range rows {
		row := &stringRow{data: rawRow}

		if q.condition != nil {
			val, err := q.condition.eval(row)
			if err != nil {
				return nil, err
			}

			if !isTruthy(val) {
				continue
			}
		}

		projected, err := projectStringRow(q, row, rawRow)
		if err != nil {
			return nil, err
		}

		result = append(result, projected)

		if q.limit > 0 && len(result) >= q.limit {
			break
		}
	}

	return result, nil
}

func projectStringRow(q *sqlQuery, row sqlRow, rawRow map[string]string) (map[string]string, error) {
	if q.selectAll {
		return rawRow, nil
	}

	projected := make(map[string]string, len(q.columns))

	for i, col := range q.columns {
		val, err := col.expr.eval(row)
		if err != nil {
			return nil, err
		}

		name := columnName(col, i)
		projected[name] = fmt.Sprintf("%v", val)
	}

	return projected, nil
}

// evalQueryJSON applies a parsed sqlQuery to JSON rows (map[string]any).
func evalQueryJSON(q *sqlQuery, rows []map[string]any) ([]map[string]any, error) {
	var result []map[string]any

	for _, rawRow := range rows {
		row := &jsonRow{data: rawRow}

		if q.condition != nil {
			val, err := q.condition.eval(row)
			if err != nil {
				return nil, err
			}

			if !isTruthy(val) {
				continue
			}
		}

		projected, err := projectJSONRow(q, row, rawRow)
		if err != nil {
			return nil, err
		}

		result = append(result, projected)

		if q.limit > 0 && len(result) >= q.limit {
			break
		}
	}

	return result, nil
}

func projectJSONRow(q *sqlQuery, row sqlRow, rawRow map[string]any) (map[string]any, error) {
	if q.selectAll {
		return rawRow, nil
	}

	projected := make(map[string]any, len(q.columns))

	for i, col := range q.columns {
		val, err := col.expr.eval(row)
		if err != nil {
			return nil, err
		}

		name := columnName(col, i)
		projected[name] = val
	}

	return projected, nil
}

func columnName(col selectColumn, idx int) string {
	if col.alias != "" {
		return col.alias
	}

	if ref, ok := col.expr.(*sqlColumnRef); ok {
		return ref.name
	}

	return fmt.Sprintf("_col%d", idx+1)
}

// stringRow implements sqlRow for CSV rows (string values).
type stringRow struct {
	data map[string]string
}

func (r *stringRow) field(name string) (string, bool) {
	for k, v := range r.data {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}

	return "", false
}

// jsonRow implements sqlRow for JSON rows (any values).
type jsonRow struct {
	data map[string]any
}

func (r *jsonRow) field(name string) (string, bool) {
	for k, v := range r.data {
		if strings.EqualFold(k, name) {
			return fmt.Sprintf("%v", v), true
		}
	}

	return "", false
}

// sortedKeys returns sorted keys of a map for deterministic positional access.
// Keys of the form "_N" (positional CSV columns) are sorted numerically so that
// _1, _2, ..., _9, _10 comes before _2 in the positional ordering rather than
// the lexicographic _1, _10, _2 ordering.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, func(a, b string) int {
		ai, aok := positionalIndex(a)
		bi, bok := positionalIndex(b)

		if aok && bok {
			return ai - bi
		}

		if aok {
			return -1
		}

		if bok {
			return 1
		}

		return strings.Compare(a, b)
	})

	return keys
}

// positionalIndex returns the numeric index for positional CSV column keys like
// "_1", "_2", "_10". Returns (0, false) for non-positional keys.
func positionalIndex(s string) (int, bool) {
	if len(s) < 2 || s[0] != '_' {
		return 0, false
	}

	n, err := strconv.Atoi(s[1:])
	if err != nil || n < 1 {
		return 0, false
	}

	return n, true
}
