package cosmosdb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// sql_parser.go is a recursive-descent parser for Cosmos DB SQL.
//
//	query      := SELECT [DISTINCT] [TOP operand] ( '*' | [VALUE] item (',' item)* )
//	              [FROM source (JOIN source)*] [WHERE expr] [GROUP BY expr (',' expr)*]
//	              [ORDER BY expr [ASC|DESC] (',' ...)*] [OFFSET operand LIMIT operand]
//	source     := ident [AS] [ident]            (top-level container, first source only)
//	            | ident IN expr | expr [AS] ident
//	item       := expr [AS ident]
//	expr       := ternary ('?' expr ':' expr) over ?? OR AND NOT, comparison, IN, BETWEEN, LIKE, IS NULL,
//	              ||, + -, * / %, unary - +, postfix '.' '[ ]', calls, literals, subqueries.
//
// Every identifier must resolve to a FROM/JOIN/subquery binding (sql_resolve.go).

// maxQueryDepth bounds recursion so adversarial queries fail with a parse error.
const maxQueryDepth = 100

type sqlParser struct {
	lx          *sqlLexer
	agg         *bool
	cur         sqlToken
	depth       int
	anonSources int
}

func newSQLParser(s string) *sqlParser {
	p := &sqlParser{lx: newSQLLexer(s)}
	p.advance()

	return p
}

func (p *sqlParser) advance() { p.cur = p.lx.next() }

func (p *sqlParser) peek() sqlToken {
	lx := *p.lx

	return lx.next()
}

func (p *sqlParser) errorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrQueryParse, fmt.Sprintf(format, args...))
}

func (p *sqlParser) expect(typ sqlTokenType, what string) (sqlToken, error) {
	if p.cur.typ != typ {
		return sqlToken{}, p.errorf("expected %s, got %q", what, p.cur.lit)
	}

	tok := p.cur
	p.advance()

	return tok, nil
}

func (p *sqlParser) enter() error {
	p.depth++
	if p.depth > maxQueryDepth {
		return ErrQueryTooDeep
	}

	return nil
}

func (p *sqlParser) leave() { p.depth-- }

// parseQuery parses and resolves a complete query. It never panics.
func parseQuery(query string) (*sqlSelectStmt, error) {
	p := newSQLParser(query)

	stmt, err := p.parseSelectStmt(false)
	if err != nil {
		return nil, err
	}

	if p.cur.typ == sqlTokError {
		return nil, p.errorf("%s", p.cur.lit)
	}

	if p.cur.typ != sqlTokEOF {
		return nil, p.errorf("unexpected trailing input %q", p.cur.lit)
	}

	if resolveErr := resolveStmt(stmt, nil); resolveErr != nil {
		return nil, resolveErr
	}

	return stmt, nil
}

func (p *sqlParser) parseSelectStmt(sub bool) (*sqlSelectStmt, error) {
	if _, err := p.expect(sqlTokSelect, "SELECT"); err != nil {
		return nil, err
	}

	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	stmt := &sqlSelectStmt{}

	prevAgg := p.agg
	p.agg = &stmt.hasAgg

	defer func() { p.agg = prevAgg }()

	if err := p.parseSelectHead(stmt); err != nil {
		return nil, err
	}

	if p.cur.typ == sqlTokFrom {
		p.advance()

		if err := p.parseFromClause(stmt, sub); err != nil {
			return nil, err
		}
	} else if stmt.isStar {
		return nil, p.errorf("SELECT * is only valid when a FROM clause names a source")
	}

	if err := p.parseClauses(stmt); err != nil {
		return nil, err
	}

	nameSelectItems(stmt)

	return stmt, nil
}

func (p *sqlParser) parseSelectHead(stmt *sqlSelectStmt) error {
	for {
		switch p.cur.typ {
		case sqlTokDistinct:
			stmt.distinct = true

			p.advance()

			continue
		case sqlTokTop:
			p.advance()

			top, err := p.parseCountOperand("TOP")
			if err != nil {
				return err
			}

			stmt.top = top

			continue
		default:
		}

		break
	}

	return p.parseSelectList(stmt)
}

func (p *sqlParser) parseSelectList(stmt *sqlSelectStmt) error {
	if p.cur.typ == sqlTokStar {
		p.advance()

		stmt.isStar = true

		return nil
	}

	if p.cur.typ == sqlTokValue {
		p.advance()

		stmt.selectValue = true

		x, err := p.parseExpr()
		if err != nil {
			return err
		}

		stmt.items = []sqlSelectItem{{expr: x}}

		return nil
	}

	for {
		x, err := p.parseExpr()
		if err != nil {
			return err
		}

		item := sqlSelectItem{expr: x, name: deriveName(x)}

		if p.cur.typ == sqlTokAs {
			p.advance()

			tok, aliasErr := p.expectName("alias identifier")
			if aliasErr != nil {
				return aliasErr
			}

			item.name = tok.lit
		}

		stmt.items = append(stmt.items, item)

		if p.cur.typ != sqlTokComma {
			return nil
		}

		p.advance()
	}
}

func (p *sqlParser) expectName(what string) (sqlToken, error) {
	if !isWordToken(p.cur.typ) {
		return sqlToken{}, p.errorf("expected %s, got %q", what, p.cur.lit)
	}

	tok := p.cur
	p.advance()

	return tok, nil
}

func deriveName(x sqlExpr) string {
	switch n := x.(type) {
	case *exprProp:
		return n.key
	case *exprVar:
		return n.name
	default:
		return ""
	}
}

// nameSelectItems gives unnamed projections Cosmos's positional "$1", "$2" names.
func nameSelectItems(stmt *sqlSelectStmt) {
	if stmt.selectValue {
		return
	}

	n := 0

	for i := range stmt.items {
		if stmt.items[i].name == "" {
			n++
			stmt.items[i].name = "$" + strconv.Itoa(n)
		}
	}
}

func (p *sqlParser) parseCountOperand(what string) (sqlExpr, error) {
	switch p.cur.typ {
	case sqlTokNumber:
		n, err := strconv.Atoi(p.cur.lit)
		if err != nil || n < 0 {
			return nil, p.errorf("invalid %s value %q", what, p.cur.lit)
		}

		p.advance()

		return &exprLit{v: json.Number(strconv.Itoa(n))}, nil
	case sqlTokParam:
		x := &exprParam{name: p.cur.lit}

		p.advance()

		return x, nil
	default:
		return nil, p.errorf("expected %s value, got %q", what, p.cur.lit)
	}
}

func (p *sqlParser) parseFromClause(stmt *sqlSelectStmt, sub bool) error {
	var (
		first sqlSource
		err   error
	)

	if sub {
		first, err = p.parseSubSource()
	} else {
		first, err = p.parseContainerSource()
	}

	if err != nil {
		return err
	}

	stmt.sources = append(stmt.sources, first)

	for p.cur.typ == sqlTokJoin {
		p.advance()

		src, srcErr := p.parseSubSource()
		if srcErr != nil {
			return srcErr
		}

		stmt.sources = append(stmt.sources, src)
	}

	return nil
}

// parseContainerSource parses the top-level "container [[AS] alias]" source.
func (p *sqlParser) parseContainerSource() (sqlSource, error) {
	first, err := p.expectName("FROM source identifier")
	if err != nil {
		return sqlSource{}, err
	}

	alias := first.lit

	if p.cur.typ == sqlTokAs {
		p.advance()
	}

	if p.cur.typ == sqlTokIdent {
		alias = p.cur.lit

		p.advance()
	}

	return sqlSource{alias: alias}, nil
}

// parseSubSource parses "alias IN expr" or "expr [AS] alias".
func (p *sqlParser) parseSubSource() (sqlSource, error) {
	if p.cur.typ == sqlTokIdent && p.peek().typ == sqlTokIn {
		alias := p.cur.lit

		p.advance()
		p.advance()

		x, err := p.parsePostfix()
		if err != nil {
			return sqlSource{}, err
		}

		return sqlSource{src: x, alias: alias}, nil
	}

	x, err := p.parsePostfix()
	if err != nil {
		return sqlSource{}, err
	}

	alias := deriveName(x)

	if sq, isSub := x.(*exprSubquery); isSub && sq.kind == subScalar {
		sq.kind = subArray
		p.anonSources++
		alias = "$sub" + strconv.Itoa(p.anonSources)
	}

	switch p.cur.typ {
	case sqlTokAs:
		p.advance()

		tok, nameErr := p.expectName("source alias")
		if nameErr != nil {
			return sqlSource{}, nameErr
		}

		alias = tok.lit
	case sqlTokIdent:
		alias = p.cur.lit

		p.advance()
	default:
	}

	if alias == "" {
		return sqlSource{}, p.errorf("source requires an alias")
	}

	return sqlSource{src: x, alias: alias}, nil
}

func (p *sqlParser) parseClauses(stmt *sqlSelectStmt) error {
	if p.cur.typ == sqlTokWhere {
		p.advance()

		where, err := p.parseExpr()
		if err != nil {
			return err
		}

		stmt.where = where
	}

	if p.cur.typ == sqlTokGroup {
		if err := p.parseGroupBy(stmt); err != nil {
			return err
		}
	}

	if p.cur.typ == sqlTokOrder {
		if err := p.parseOrderBy(stmt); err != nil {
			return err
		}
	}

	if p.cur.typ == sqlTokOffset {
		return p.parseOffsetLimit(stmt)
	}

	return nil
}

func (p *sqlParser) parseGroupBy(stmt *sqlSelectStmt) error {
	p.advance()

	if _, err := p.expect(sqlTokBy, "BY"); err != nil {
		return err
	}

	for {
		x, err := p.parseExpr()
		if err != nil {
			return err
		}

		stmt.groupBy = append(stmt.groupBy, x)

		if p.cur.typ != sqlTokComma {
			return nil
		}

		p.advance()
	}
}

// msgOrderByWithGroupBy is the service's text for the unsupported combination (the gocosmos known-issues list quotes
// it from the gateway).
const msgOrderByWithGroupBy = "'ORDER BY' is not supported in presence of GROUP BY."

func (p *sqlParser) parseOrderBy(stmt *sqlSelectStmt) error {
	if len(stmt.groupBy) > 0 {
		return p.errorf("%s", msgOrderByWithGroupBy)
	}

	p.advance()

	if _, err := p.expect(sqlTokBy, "BY"); err != nil {
		return err
	}

	if p.cur.typ == sqlTokIdent && strings.EqualFold(p.cur.lit, "RANK") && p.peek().typ == sqlTokIdent {
		return p.parseOrderByRank(stmt)
	}

	for {
		x, err := p.parseExpr()
		if err != nil {
			return err
		}

		key := sqlOrderKey{expr: x}

		switch p.cur.typ {
		case sqlTokAsc:
			p.advance()
		case sqlTokDesc:
			key.desc = true

			p.advance()
		default:
		}

		stmt.orderBy = append(stmt.orderBy, key)

		if p.cur.typ != sqlTokComma {
			return nil
		}

		p.advance()
	}
}

// parseOrderByRank parses `ORDER BY RANK FullTextScore(...)` or `RANK RRF(...)`, which stand alone.
func (p *sqlParser) parseOrderByRank(stmt *sqlSelectStmt) error {
	p.advance()

	x, err := p.parseExpr()
	if err != nil {
		return err
	}

	if err = validateRankExpr(x); err != nil {
		return err
	}

	if p.cur.typ == sqlTokComma {
		return p.errorf("ORDER BY RANK cannot be combined with other sort keys")
	}

	stmt.rank = true
	stmt.orderBy = append(stmt.orderBy, sqlOrderKey{expr: x, desc: true})

	return nil
}

func (p *sqlParser) parseOffsetLimit(stmt *sqlSelectStmt) error {
	p.advance()

	offset, err := p.parseCountOperand("OFFSET")
	if err != nil {
		return err
	}

	if _, expectErr := p.expect(sqlTokLimit, "LIMIT"); expectErr != nil {
		return expectErr
	}

	limit, err := p.parseCountOperand("LIMIT")
	if err != nil {
		return err
	}

	stmt.offset, stmt.limit = offset, limit

	return nil
}

// --- expressions ---

func (p *sqlParser) parseExpr() (sqlExpr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	cond, err := p.parseCoalesce()
	if err != nil {
		return nil, err
	}

	if p.cur.typ != sqlTokQuestion {
		return cond, nil
	}

	p.advance()

	a, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if _, expectErr := p.expect(sqlTokColon, "':'"); expectErr != nil {
		return nil, expectErr
	}

	b, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	return &exprTernary{cond: cond, a: a, b: b}, nil
}

func (p *sqlParser) parseCoalesce() (sqlExpr, error) {
	left, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokCoalesce {
		p.advance()

		right, rightErr := p.parseOr()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &exprBinary{op: sqlTokCoalesce, l: left, r: right}
	}

	return left, nil
}

func (p *sqlParser) parseOr() (sqlExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokOr {
		p.advance()

		right, rightErr := p.parseAnd()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &exprBinary{op: sqlTokOr, l: left, r: right}
	}

	return left, nil
}

func (p *sqlParser) parseAnd() (sqlExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokAnd {
		p.advance()

		right, rightErr := p.parseNot()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &exprBinary{op: sqlTokAnd, l: left, r: right}
	}

	return left, nil
}

func (p *sqlParser) parseNot() (sqlExpr, error) {
	if p.cur.typ != sqlTokNot {
		return p.parsePredicate()
	}

	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	p.advance()

	inner, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	return &exprUnary{op: sqlTokNot, x: inner}, nil
}

func isSQLCompareOp(t sqlTokenType) bool {
	switch t {
	case sqlTokEq, sqlTokNeq, sqlTokLt, sqlTokLte, sqlTokGt, sqlTokGte:
		return true
	default:
		return false
	}
}

func (p *sqlParser) parsePredicate() (sqlExpr, error) {
	left, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	switch {
	case isSQLCompareOp(p.cur.typ):
		op := p.cur.typ

		p.advance()

		right, rightErr := p.parseConcat()
		if rightErr != nil {
			return nil, rightErr
		}

		return &exprBinary{op: op, l: left, r: right}, nil
	case p.cur.typ == sqlTokIs:
		return p.parseIsNull(left)
	case p.cur.typ == sqlTokNot:
		p.advance()

		return p.parseNegatablePredicate(left, true)
	default:
		return p.parseNegatablePredicate(left, false)
	}
}

func (p *sqlParser) parseIsNull(left sqlExpr) (sqlExpr, error) {
	p.advance()

	negate := false
	if p.cur.typ == sqlTokNot {
		negate = true

		p.advance()
	}

	if _, err := p.expect(sqlTokNull, "NULL"); err != nil {
		return nil, err
	}

	return &exprIsNull{x: left, negate: negate}, nil
}

// parseNegatablePredicate handles [NOT] IN / BETWEEN / LIKE; with no such keyword it returns left (or errors
// if a NOT was already consumed).
func (p *sqlParser) parseNegatablePredicate(left sqlExpr, negate bool) (sqlExpr, error) {
	switch p.cur.typ {
	case sqlTokIn:
		p.advance()

		return p.parseInList(left, negate)
	case sqlTokBetween:
		p.advance()

		return p.parseBetween(left, negate)
	case sqlTokLike:
		p.advance()

		return p.parseLike(left, negate)
	default:
		if negate {
			return nil, p.errorf("expected IN, BETWEEN or LIKE after NOT, got %q", p.cur.lit)
		}

		return left, nil
	}
}

func (p *sqlParser) parseInList(left sqlExpr, negate bool) (sqlExpr, error) {
	if _, err := p.expect(sqlTokLParen, "'('"); err != nil {
		return nil, err
	}

	node := &exprIn{x: left, negate: negate}

	for {
		item, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		node.list = append(node.list, item)

		if p.cur.typ != sqlTokComma {
			break
		}

		p.advance()
	}

	if _, err := p.expect(sqlTokRParen, "')'"); err != nil {
		return nil, err
	}

	return node, nil
}

func (p *sqlParser) parseBetween(left sqlExpr, negate bool) (sqlExpr, error) {
	lo, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	if _, expectErr := p.expect(sqlTokAnd, "AND"); expectErr != nil {
		return nil, expectErr
	}

	hi, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	return &exprBetween{x: left, lo: lo, hi: hi, negate: negate}, nil
}

func (p *sqlParser) parseLike(left sqlExpr, negate bool) (sqlExpr, error) {
	pat, err := p.parseConcat()
	if err != nil {
		return nil, err
	}

	node := &exprLike{x: left, pattern: pat, negate: negate}

	if p.cur.typ == sqlTokEscape {
		p.advance()

		tok, escErr := p.expect(sqlTokString, "escape character")
		if escErr != nil {
			return nil, escErr
		}

		node.escape = tok.lit
	}

	return node, nil
}

func (p *sqlParser) parseConcat() (sqlExpr, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokConcat {
		p.advance()

		right, rightErr := p.parseAdditive()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &exprBinary{op: sqlTokConcat, l: left, r: right}
	}

	return left, nil
}

func (p *sqlParser) parseAdditive() (sqlExpr, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokPlus || p.cur.typ == sqlTokMinus {
		op := p.cur.typ

		p.advance()

		right, rightErr := p.parseMultiplicative()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &exprBinary{op: op, l: left, r: right}
	}

	return left, nil
}

func (p *sqlParser) parseMultiplicative() (sqlExpr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokStar || p.cur.typ == sqlTokSlash || p.cur.typ == sqlTokPercent {
		op := p.cur.typ

		p.advance()

		right, rightErr := p.parseUnary()
		if rightErr != nil {
			return nil, rightErr
		}

		left = &exprBinary{op: op, l: left, r: right}
	}

	return left, nil
}

func (p *sqlParser) parseUnary() (sqlExpr, error) {
	if p.cur.typ != sqlTokMinus && p.cur.typ != sqlTokPlus {
		return p.parsePostfix()
	}

	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	op := p.cur.typ

	p.advance()

	if op == sqlTokMinus && p.cur.typ == sqlTokNumber {
		lit := "-" + p.cur.lit

		p.advance()

		return &exprLit{v: json.Number(lit)}, nil
	}

	inner, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	return &exprUnary{op: op, x: inner}, nil
}

func (p *sqlParser) parsePostfix() (sqlExpr, error) {
	x, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for {
		switch p.cur.typ {
		case sqlTokDot:
			p.advance()

			tok, nameErr := p.expectName("identifier after '.'")
			if nameErr != nil {
				return nil, nameErr
			}

			x = &exprProp{base: x, key: tok.lit}
		case sqlTokLBracket:
			p.advance()

			idx, idxErr := p.parseExpr()
			if idxErr != nil {
				return nil, idxErr
			}

			if _, expectErr := p.expect(sqlTokRBracket, "']'"); expectErr != nil {
				return nil, expectErr
			}

			x = &exprIndex{base: x, idx: idx}
		default:
			return x, nil
		}
	}
}

func (p *sqlParser) parsePrimary() (sqlExpr, error) {
	tok := p.cur

	switch tok.typ {
	case sqlTokNumber:
		p.advance()

		return &exprLit{v: json.Number(tok.lit)}, nil
	case sqlTokString:
		p.advance()

		return &exprLit{v: tok.lit}, nil
	case sqlTokTrue, sqlTokFalse:
		p.advance()

		return &exprLit{v: tok.typ == sqlTokTrue}, nil
	case sqlTokNull:
		p.advance()

		return &exprLit{v: nil}, nil
	case sqlTokUndefined:
		p.advance()

		return &exprUndefined{}, nil
	case sqlTokParam:
		p.advance()

		return &exprParam{name: tok.lit}, nil
	case sqlTokLParen:
		return p.parseParen()
	case sqlTokLBracket:
		return p.parseArrayLiteral()
	case sqlTokLBrace:
		return p.parseObjectLiteral()
	case sqlTokIdent:
		return p.parseIdentOrCall()
	case sqlTokError:
		return nil, p.errorf("%s", tok.lit)
	default:
		return nil, p.errorf("unexpected token %q", tok.lit)
	}
}

func (p *sqlParser) parseParen() (sqlExpr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	p.advance()

	if p.cur.typ == sqlTokSelect {
		sub, err := p.parseSelectStmt(true)
		if err != nil {
			return nil, err
		}

		if _, expectErr := p.expect(sqlTokRParen, "')'"); expectErr != nil {
			return nil, expectErr
		}

		return &exprSubquery{stmt: sub, kind: subScalar}, nil
	}

	x, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if p.cur.typ != sqlTokRParen {
		return nil, p.errorf("expected ) got %q", p.cur.lit)
	}

	p.advance()

	return x, nil
}

func (p *sqlParser) parseArrayLiteral() (sqlExpr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	p.advance()

	node := &exprArrayLit{}

	for p.cur.typ != sqlTokRBracket {
		item, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		node.items = append(node.items, item)

		if p.cur.typ != sqlTokComma {
			break
		}

		p.advance()
	}

	if _, err := p.expect(sqlTokRBracket, "']'"); err != nil {
		return nil, err
	}

	return node, nil
}

func (p *sqlParser) parseObjectLiteral() (sqlExpr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	p.advance()

	node := &exprObjectLit{}

	for p.cur.typ != sqlTokRBrace {
		if p.cur.typ != sqlTokString && !isWordToken(p.cur.typ) {
			return nil, p.errorf("expected object key, got %q", p.cur.lit)
		}

		key := p.cur.lit

		p.advance()

		if _, err := p.expect(sqlTokColon, "':'"); err != nil {
			return nil, err
		}

		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		node.keys = append(node.keys, key)
		node.vals = append(node.vals, val)

		if p.cur.typ != sqlTokComma {
			break
		}

		p.advance()
	}

	if _, err := p.expect(sqlTokRBrace, "'}'"); err != nil {
		return nil, err
	}

	return node, nil
}

func (p *sqlParser) parseIdentOrCall() (sqlExpr, error) {
	name := p.cur.lit

	p.advance()

	if strings.EqualFold(name, udfNamespace) && p.cur.typ == sqlTokDot && p.udfCallAhead() {
		return p.parseUDFCall()
	}

	if p.cur.typ != sqlTokLParen {
		return &exprVar{name: name}, nil
	}

	upper := strings.ToUpper(name)

	if (upper == "EXISTS" || upper == "ARRAY") && p.peek().typ == sqlTokSelect {
		return p.parseSubqueryCall(upper)
	}

	return p.parseCall(upper)
}

func (p *sqlParser) parseSubqueryCall(upper string) (sqlExpr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	p.advance()

	sub, err := p.parseSelectStmt(true)
	if err != nil {
		return nil, err
	}

	if _, expectErr := p.expect(sqlTokRParen, "')'"); expectErr != nil {
		return nil, expectErr
	}

	kind := subArray
	if upper == "EXISTS" {
		kind = subExists
	}

	return &exprSubquery{stmt: sub, kind: kind}, nil
}

// udfCallAhead reports whether the tokens after the current "." are `name (`.
func (p *sqlParser) udfCallAhead() bool {
	lx := *p.lx
	name, paren := lx.next(), lx.next()

	return name.typ == sqlTokIdent && paren.typ == sqlTokLParen
}

// parseUDFCall parses `udf.name(args)`; the current token is the ".".
func (p *sqlParser) parseUDFCall() (sqlExpr, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	p.advance()

	name := p.cur.lit

	p.advance()
	p.advance()

	call := &exprCall{name: name, kind: fnUDF}

	if err := p.parseCallArgs(call); err != nil {
		return nil, err
	}

	return call, nil
}

func (p *sqlParser) parseCall(upper string) (sqlExpr, error) {
	kind := builtinKind(upper)
	if kind == fnUnknown {
		return nil, p.errorf("unknown function %q", upper)
	}

	if err := p.enter(); err != nil {
		return nil, err
	}

	defer p.leave()

	p.advance()

	call := &exprCall{name: upper, kind: kind}

	if err := p.parseCallArgs(call); err != nil {
		return nil, err
	}

	lo, hi := builtinArity(upper)
	if len(call.args) < lo || (hi >= 0 && len(call.args) > hi) {
		return nil, p.errorf("wrong number of arguments for %s", upper)
	}

	if kind == fnAggregate {
		*p.agg = true
	}

	return call, nil
}

// parseCallArgs parses a comma-separated argument list up to and including the closing ")".
func (p *sqlParser) parseCallArgs(call *exprCall) error {
	for p.cur.typ != sqlTokRParen {
		arg, err := p.parseExpr()
		if err != nil {
			return err
		}

		call.args = append(call.args, arg)

		if p.cur.typ != sqlTokComma {
			break
		}

		p.advance()
	}

	_, err := p.expect(sqlTokRParen, "')'")

	return err
}
