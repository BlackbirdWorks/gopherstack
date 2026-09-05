package cosmosdb

import (
	"fmt"
	"strconv"
)

// sql_parser.go implements a hand-written recursive-descent parser for the
// Cosmos DB SQL query subset this emulator supports, over sql_tokenizer.go's
// tokens. sql_exec.go holds the executor. Modeled on
// services/azuretable/odata_filter.go's identical
// lexer->parser->AST->evaluator shape and services/s3's
// select_sql_parser.go, gopherstack's two existing SQL/filter-parsing
// precedents.
//
// Supported grammar:
//
//	query      := SELECT [TOP number] selectList FROM ident [ident]
//	              [WHERE expr] [ORDER BY orderList] [OFFSET number LIMIT number]
//	selectList := '*' | projection (',' projection)*
//	projection := path [AS ident]
//	path       := ident ('.' ident)*
//	orderList  := orderItem (',' orderItem)*
//	orderItem  := path [ASC|DESC]
//	expr       := orExpr
//	orExpr     := andExpr ('OR' andExpr)*
//	andExpr    := unary ('AND' unary)*
//	unary      := 'NOT' unary | primary
//	primary    := '(' expr ')' | operand 'IS' ['NOT'] 'NULL' | comparison
//	comparison := operand ('='|'!='|'<>'|'<'|'<='|'>'|'>=') operand
//	operand    := path | '@param' | 'string' | number | true | false | null
//
// The first identifier after FROM is the source/container alias (e.g. "c"
// in "FROM c"); an optional second identifier re-aliases it (e.g. "r" in
// "FROM root r"). Every path in selectList/WHERE/ORDER BY must start with
// whichever alias is in effect -- that leading segment is stripped before
// evaluation (see sql_exec.go's resolvePath), since this emulator only ever
// queries a single, already-known container (no JOINs).

// sqlNode is a node in a parsed WHERE expression tree.
type sqlNode interface{ isSQLNode() }

type sqlAndNode struct{ left, right sqlNode }

func (*sqlAndNode) isSQLNode() {}

type sqlOrNode struct{ left, right sqlNode }

func (*sqlOrNode) isSQLNode() {}

type sqlNotNode struct{ expr sqlNode }

func (*sqlNotNode) isSQLNode() {}

type sqlCmpNode struct {
	left, right sqlOperand
	op          sqlTokenType
}

func (*sqlCmpNode) isSQLNode() {}

type sqlIsNullNode struct {
	operand sqlOperand
	negate  bool
}

func (*sqlIsNullNode) isSQLNode() {}

// sqlOperandKind discriminates sqlOperand's active field set.
type sqlOperandKind int

const (
	sqlOperandPath sqlOperandKind = iota
	sqlOperandParam
	sqlOperandString
	sqlOperandNumber
	sqlOperandBool
	sqlOperandNull
)

// sqlOperand is either a document-field path reference, a query-parameter
// reference, or a typed literal value.
type sqlOperand struct {
	paramName string
	strVal    string
	numLit    string
	path      []string
	kind      sqlOperandKind
	boolVal   bool
}

// sqlProjection is one SELECT list item: the (alias-stripped) path to
// project, and the output key to emit it under.
type sqlProjection struct {
	alias string
	path  []string
}

// sqlOrderKey is one ORDER BY item.
type sqlOrderKey struct {
	path []string
	desc bool
}

// sqlSelectStmt is a fully parsed Cosmos SQL query.
type sqlSelectStmt struct {
	where       sqlNode
	fromAlias   string
	projections []sqlProjection
	orderBy     []sqlOrderKey
	top         int
	offset      int
	limit       int
	isStar      bool
	hasTop      bool
	hasOffset   bool
	hasLimit    bool
}

// maxQueryDepth bounds recursive-descent recursion so a deeply nested (or
// adversarial) query fails with a parse error instead of overflowing the
// stack, mirroring services/azuretable's maxFilterDepth.
const maxQueryDepth = 100

// sqlParser is a recursive-descent parser over a Cosmos SQL query string.
type sqlParser struct {
	lx  *sqlLexer
	cur sqlToken
}

func newSQLParser(s string) *sqlParser {
	p := &sqlParser{lx: newSQLLexer(s)}
	p.advance()

	return p
}

func (p *sqlParser) advance() { p.cur = p.lx.next() }

func (p *sqlParser) expect(typ sqlTokenType, what string) (sqlToken, error) {
	if p.cur.typ != typ {
		return sqlToken{}, fmt.Errorf("%w: expected %s, got %q", ErrQueryParse, what, p.cur.lit)
	}

	tok := p.cur
	p.advance()

	return tok, nil
}

// ParseQuery parses a complete Cosmos SQL query string into a sqlSelectStmt.
// Returns ErrQueryParse (wrapped with detail) on any malformed input,
// ErrQueryTooDeep if WHERE nesting exceeds maxQueryDepth, and never panics.
func ParseQuery(query string) (*sqlSelectStmt, error) {
	p := newSQLParser(query)

	stmt, err := p.parseSelectStmt()
	if err != nil {
		return nil, err
	}

	if p.cur.typ == sqlTokError {
		return nil, fmt.Errorf("%w: %s", ErrQueryParse, p.cur.lit)
	}

	if p.cur.typ != sqlTokEOF {
		return nil, fmt.Errorf("%w: unexpected trailing input %q", ErrQueryParse, p.cur.lit)
	}

	return stmt, nil
}

func (p *sqlParser) parseSelectStmt() (*sqlSelectStmt, error) {
	if _, err := p.expect(sqlTokSelect, "SELECT"); err != nil {
		return nil, err
	}

	stmt := &sqlSelectStmt{}

	if p.cur.typ == sqlTokTop {
		p.advance()

		n, err := p.parseIntLiteral("TOP")
		if err != nil {
			return nil, err
		}

		stmt.hasTop, stmt.top = true, n
	}

	if err := p.parseSelectList(stmt); err != nil {
		return nil, err
	}

	if _, err := p.expect(sqlTokFrom, "FROM"); err != nil {
		return nil, err
	}

	if err := p.parseFromClause(stmt); err != nil {
		return nil, err
	}

	if p.cur.typ == sqlTokWhere {
		p.advance()

		where, err := p.parseOr(0)
		if err != nil {
			return nil, err
		}

		stmt.where = where
	}

	if p.cur.typ == sqlTokOrder {
		if err := p.parseOrderBy(stmt); err != nil {
			return nil, err
		}
	}

	if p.cur.typ == sqlTokOffset {
		if err := p.parseOffsetLimit(stmt); err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

func (p *sqlParser) parseIntLiteral(what string) (int, error) {
	tok, err := p.expect(sqlTokNumber, what+" value")
	if err != nil {
		return 0, err
	}

	n, convErr := strconv.Atoi(tok.lit)
	if convErr != nil || n < 0 {
		return 0, fmt.Errorf("%w: invalid %s value %q", ErrQueryParse, what, tok.lit)
	}

	return n, nil
}

func (p *sqlParser) parseSelectList(stmt *sqlSelectStmt) error {
	if p.cur.typ == sqlTokStar {
		p.advance()

		stmt.isStar = true

		return nil
	}

	for {
		proj, err := p.parseProjection()
		if err != nil {
			return err
		}

		stmt.projections = append(stmt.projections, proj)

		if p.cur.typ != sqlTokComma {
			return nil
		}

		p.advance()
	}
}

func (p *sqlParser) parseProjection() (sqlProjection, error) {
	path, err := p.parsePath()
	if err != nil {
		return sqlProjection{}, err
	}

	alias := ""
	if len(path) > 0 {
		alias = path[len(path)-1]
	}

	if p.cur.typ == sqlTokAs {
		p.advance()

		tok, aliasErr := p.expect(sqlTokIdent, "alias identifier")
		if aliasErr != nil {
			return sqlProjection{}, aliasErr
		}

		alias = tok.lit
	}

	return sqlProjection{path: path, alias: alias}, nil
}

// parsePath parses a dotted identifier chain (e.g. "c.a.b") and strips its
// leading alias segment -- callers only ever want the path relative to the
// query's single FROM source (see this file's top doc comment).
func (p *sqlParser) parsePath() ([]string, error) {
	first, err := p.expect(sqlTokIdent, "identifier")
	if err != nil {
		return nil, err
	}

	segments := []string{first.lit}

	for p.cur.typ == sqlTokDot {
		p.advance()

		tok, segErr := p.expect(sqlTokIdent, "identifier after '.'")
		if segErr != nil {
			return nil, segErr
		}

		segments = append(segments, tok.lit)
	}

	// Strip the leading alias segment: "c.a.b" -> ["a","b"]; a bare alias
	// reference ("c" alone, e.g. "SELECT c FROM c" or ORDER BY c) becomes an
	// empty path, meaning "the whole row".
	return segments[1:], nil
}

func (p *sqlParser) parseFromClause(stmt *sqlSelectStmt) error {
	first, err := p.expect(sqlTokIdent, "FROM source identifier")
	if err != nil {
		return err
	}

	stmt.fromAlias = first.lit

	if p.cur.typ == sqlTokIdent {
		stmt.fromAlias = p.cur.lit
		p.advance()
	}

	return nil
}

func (p *sqlParser) parseOrderBy(stmt *sqlSelectStmt) error {
	p.advance()

	if _, err := p.expect(sqlTokBy, "BY"); err != nil {
		return err
	}

	for {
		path, err := p.parsePath()
		if err != nil {
			return err
		}

		desc := false

		switch p.cur.typ {
		case sqlTokAsc:
			p.advance()
		case sqlTokDesc:
			desc = true

			p.advance()
		default:
		}

		stmt.orderBy = append(stmt.orderBy, sqlOrderKey{path: path, desc: desc})

		if p.cur.typ != sqlTokComma {
			return nil
		}

		p.advance()
	}
}

func (p *sqlParser) parseOffsetLimit(stmt *sqlSelectStmt) error {
	p.advance()

	offset, err := p.parseIntLiteral("OFFSET")
	if err != nil {
		return err
	}

	if _, expectErr := p.expect(sqlTokLimit, "LIMIT"); expectErr != nil {
		return expectErr
	}

	limit, err := p.parseIntLiteral("LIMIT")
	if err != nil {
		return err
	}

	stmt.hasOffset, stmt.offset = true, offset
	stmt.hasLimit, stmt.limit = true, limit

	return nil
}

func checkQueryDepth(depth int) error {
	if depth > maxQueryDepth {
		return ErrQueryTooDeep
	}

	return nil
}

// parseOr/parseAnd/parseUnary/parsePrimary form one precedence-climbing
// layer of recursive descent per grammar rule each, not one nesting level
// each -- depth is threaded through unchanged across same-level calls and
// incremented only where genuine nesting happens (parseUnary's NOT branch,
// parsePrimary's parenthesized branch), exactly mirroring
// services/azuretable/odata_filter.go's identical, review-corrected
// depth-counting discipline (see that file's doc comment for the bug this
// avoids).
func (p *sqlParser) parseOr(depth int) (sqlNode, error) {
	if err := checkQueryDepth(depth); err != nil {
		return nil, err
	}

	left, err := p.parseAnd(depth)
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokOr {
		p.advance()

		right, andErr := p.parseAnd(depth)
		if andErr != nil {
			return nil, andErr
		}

		left = &sqlOrNode{left: left, right: right}
	}

	return left, nil
}

func (p *sqlParser) parseAnd(depth int) (sqlNode, error) {
	if err := checkQueryDepth(depth); err != nil {
		return nil, err
	}

	left, err := p.parseUnary(depth)
	if err != nil {
		return nil, err
	}

	for p.cur.typ == sqlTokAnd {
		p.advance()

		right, unaryErr := p.parseUnary(depth)
		if unaryErr != nil {
			return nil, unaryErr
		}

		left = &sqlAndNode{left: left, right: right}
	}

	return left, nil
}

func (p *sqlParser) parseUnary(depth int) (sqlNode, error) {
	if err := checkQueryDepth(depth); err != nil {
		return nil, err
	}

	if p.cur.typ == sqlTokNot {
		p.advance()

		inner, err := p.parseUnary(depth + 1)
		if err != nil {
			return nil, err
		}

		return &sqlNotNode{expr: inner}, nil
	}

	return p.parsePrimary(depth)
}

func (p *sqlParser) parsePrimary(depth int) (sqlNode, error) {
	if err := checkQueryDepth(depth); err != nil {
		return nil, err
	}

	if p.cur.typ == sqlTokLParen {
		p.advance()

		node, err := p.parseOr(depth + 1)
		if err != nil {
			return nil, err
		}

		if p.cur.typ != sqlTokRParen {
			return nil, fmt.Errorf("%w: expected ) got %q", ErrQueryParse, p.cur.lit)
		}

		p.advance()

		return node, nil
	}

	return p.parseComparisonOrIsNull()
}

func (p *sqlParser) parseComparisonOrIsNull() (sqlNode, error) {
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	if p.cur.typ == sqlTokIs {
		p.advance()

		negate := false
		if p.cur.typ == sqlTokNot {
			negate = true

			p.advance()
		}

		if _, expectErr := p.expect(sqlTokNull, "NULL"); expectErr != nil {
			return nil, expectErr
		}

		return &sqlIsNullNode{operand: left, negate: negate}, nil
	}

	if !isSQLCompareOp(p.cur.typ) {
		return nil, fmt.Errorf("%w: expected comparison operator or IS NULL, got %q", ErrQueryParse, p.cur.lit)
	}

	op := p.cur.typ
	p.advance()

	right, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	return &sqlCmpNode{left: left, right: right, op: op}, nil
}

func isSQLCompareOp(t sqlTokenType) bool {
	switch t {
	case sqlTokEq, sqlTokNeq, sqlTokLt, sqlTokLte, sqlTokGt, sqlTokGte:
		return true
	default:
		return false
	}
}

func (p *sqlParser) parseOperand() (sqlOperand, error) {
	tok := p.cur

	switch tok.typ {
	case sqlTokIdent:
		path, err := p.parsePath()
		if err != nil {
			return sqlOperand{}, err
		}

		return sqlOperand{kind: sqlOperandPath, path: path}, nil
	case sqlTokParam:
		p.advance()

		return sqlOperand{kind: sqlOperandParam, paramName: tok.lit}, nil
	case sqlTokString:
		p.advance()

		return sqlOperand{kind: sqlOperandString, strVal: tok.lit}, nil
	case sqlTokNumber:
		p.advance()

		return sqlOperand{kind: sqlOperandNumber, numLit: tok.lit}, nil
	case sqlTokTrue, sqlTokFalse:
		p.advance()

		return sqlOperand{kind: sqlOperandBool, boolVal: tok.typ == sqlTokTrue}, nil
	case sqlTokNull:
		p.advance()

		return sqlOperand{kind: sqlOperandNull}, nil
	case sqlTokError:
		return sqlOperand{}, fmt.Errorf("%w: %s", ErrQueryParse, tok.lit)
	default:
		return sqlOperand{}, fmt.Errorf("%w: unexpected token %q", ErrQueryParse, tok.lit)
	}
}
