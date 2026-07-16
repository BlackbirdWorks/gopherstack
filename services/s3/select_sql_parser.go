package s3

import (
	"fmt"
	"strconv"
	"strings"
)

// sqlQuery is the parsed result of a SELECT statement.
type sqlQuery struct {
	condition  sqlExpr
	tableAlias string
	columns    []selectColumn
	orderBy    []sqlOrderByClause
	limit      int
	selectAll  bool
}

// selectColumn represents a single column projection.
type selectColumn struct {
	expr  sqlExpr
	alias string
}

// sqlOrderByClause is a single ORDER BY sort key.
type sqlOrderByClause struct {
	expr sqlExpr
	desc bool
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

func (p *sqlParser) parse() (*sqlQuery, error) {
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

	if err = p.parseFromClause(q); err != nil {
		return nil, err
	}

	if err = p.parseWhereLimit(q); err != nil {
		return nil, err
	}

	return q, nil
}

// parseFromClause consumes the FROM clause, table name, and optional alias.
func (p *sqlParser) parseFromClause(q *sqlQuery) error {
	if err := p.expectKeyword("FROM"); err != nil {
		return err
	}

	// consume table name (s3object or similar)
	if _, err := p.tok.next(); err != nil {
		return err
	}

	// optional alias
	tok, peekErr := p.tok.peek()
	if peekErr == nil && tok.typ == tokIdent && !isKeyword(tok.val) {
		if _, peekErr = p.tok.next(); peekErr != nil {
			return peekErr
		}

		q.tableAlias = tok.val
	}

	return nil
}

// parseWhereLimit consumes the optional WHERE, ORDER BY, and LIMIT clauses.
func (p *sqlParser) parseWhereLimit(q *sqlQuery) error {
	if err := p.parseWhereClause(q); err != nil {
		return err
	}

	if err := p.parseOrderByClause(q); err != nil {
		return err
	}

	return p.parseLimitClause(q)
}

func (p *sqlParser) parseWhereClause(q *sqlQuery) error {
	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return peekErr
	}

	if tok.typ != tokIdent || !strings.EqualFold(tok.val, "WHERE") {
		return nil
	}

	if _, err := p.tok.next(); err != nil {
		return err
	}

	var err error
	q.condition, err = p.parseOr()

	return err
}

func (p *sqlParser) parseOrderByClause(q *sqlQuery) error {
	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return peekErr
	}

	if tok.typ != tokIdent || !strings.EqualFold(tok.val, "ORDER") {
		return nil
	}

	if _, err := p.tok.next(); err != nil {
		return err
	}

	if err := p.expectKeyword("BY"); err != nil {
		return err
	}

	orderBy, err := p.parseOrderByList()
	if err != nil {
		return err
	}

	q.orderBy = orderBy

	return nil
}

func (p *sqlParser) parseLimitClause(q *sqlQuery) error {
	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return peekErr
	}

	if tok.typ != tokIdent || !strings.EqualFold(tok.val, "LIMIT") {
		return nil
	}

	if _, err := p.tok.next(); err != nil {
		return err
	}

	var err error
	q.limit, err = p.parseLimit()

	return err
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

		left = &sqlBinaryExpr{op: sqlOpAND, left: left, right: right}
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
	return t == tokEq || t == tokNeq || t == tokLt || t == tokLte || t == tokGt || t == tokGte
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
		return &sqlLiteral{val: sqlValTrue}, nil
	case "FALSE":
		return &sqlLiteral{val: sqlValFalse}, nil
	case "CAST":
		return p.parseCast()
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return p.parseAggFunc(sqlAggFuncName(upper))
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
		"LIMIT", "AS", "CAST", "BETWEEN", "IN", "TRUE", "FALSE",
		"ORDER", "BY", "ASC", "DESC":
		return true
	default:
		return false
	}
}

// parseAggFunc parses an aggregate function call: FN(expr) or COUNT(*).
func (p *sqlParser) parseAggFunc(fn sqlAggFuncName) (sqlExpr, error) {
	if err := p.expectToken(tokLParen); err != nil {
		return nil, err
	}

	next, peekErr := p.tok.peek()
	if peekErr != nil {
		return nil, peekErr
	}

	var arg sqlExpr

	if next.typ == tokStar {
		if _, err := p.tok.next(); err != nil {
			return nil, err
		}
	} else {
		var err error

		arg, err = p.parseExprAtom()
		if err != nil {
			return nil, err
		}
	}

	if err := p.expectToken(tokRParen); err != nil {
		return nil, err
	}

	return &sqlAggExpr{fn: fn, arg: arg}, nil
}

// hasAggregates reports whether any SELECT column uses an aggregate function.
func (q *sqlQuery) hasAggregates() bool {
	for _, col := range q.columns {
		if _, ok := col.expr.(*sqlAggExpr); ok {
			return true
		}
	}

	return false
}

// parseOrderByList parses one or more ORDER BY sort keys.
func (p *sqlParser) parseOrderByList() ([]sqlOrderByClause, error) {
	var clauses []sqlOrderByClause

	for {
		expr, err := p.parseExprAtom()
		if err != nil {
			return nil, err
		}

		clause := sqlOrderByClause{expr: expr}

		if err = p.parseOrderByDirection(&clause); err != nil {
			return nil, err
		}

		clauses = append(clauses, clause)

		tok, peekErr := p.tok.peek()
		if peekErr != nil {
			return nil, peekErr
		}

		if tok.typ != tokComma {
			break
		}

		if _, err = p.tok.next(); err != nil {
			return nil, err
		}
	}

	return clauses, nil
}

func (p *sqlParser) parseOrderByDirection(clause *sqlOrderByClause) error {
	tok, peekErr := p.tok.peek()
	if peekErr != nil {
		return peekErr
	}

	if tok.typ != tokIdent {
		return nil
	}

	switch strings.ToUpper(tok.val) {
	case "ASC":
		if _, err := p.tok.next(); err != nil {
			return err
		}
	case "DESC":
		if _, err := p.tok.next(); err != nil {
			return err
		}

		clause.desc = true
	}

	return nil
}
