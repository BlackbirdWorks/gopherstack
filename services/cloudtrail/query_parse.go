package cloudtrail

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// maxSQLErrorPreviewTokens bounds how many leftover tokens a "unsupported
// construct near ..." error message quotes.
const maxSQLErrorPreviewTokens = 6

const havingItemName = "_having"

// selectItemKind identifies what a single SELECT list entry projects.
type selectItemKind int

const (
	itemColumn selectItemKind = iota
	itemCountStar
	itemCount
	itemSum
	itemAvg
	itemMin
	itemMax
)

func aggFuncKind(name string) (selectItemKind, bool) {
	switch strings.ToUpper(name) {
	case "SUM":
		return itemSum, true
	case "AVG":
		return itemAvg, true
	case "MIN":
		return itemMin, true
	case "MAX":
		return itemMax, true
	default:
		return 0, false
	}
}

func (k selectItemKind) isAggregate() bool { return k >= itemCountStar }

// selectItem is one resolved SELECT list entry: a bare column, or a COUNT
// aggregate. column is the lowercased source column to read from a row
// (eventToRow's keys are lowercase); outName is the result column name --
// the explicit AS alias when given, else the column's own (original-case)
// name for a bare column, or a Trino-style positional "_col<N>" for an
// unaliased aggregate (Trino/Presto name unaliased computed expressions
// "_col0", "_col1", ... by SELECT-list position; CloudTrail Lake "supports
// all valid Trino SQL", per query-limitations.html, and its own SQL
// reference does not itself document a different convention, so this is
// inferred from the underlying engine, not SDK/AWS-doc-verified -- see
// PARITY.md).
type selectItem struct {
	column  string
	outName string
	kind    selectItemKind
	aliased bool
}

// parsedLakeQuery is a successfully parsed statement in the supported
// CloudTrail Lake SQL subset (see query_exec.go's file doc comment).
type parsedLakeQuery struct {
	where    whereExpr
	having   *havingCond
	from     fromSource
	items    []selectItem
	joins    []joinClause
	groupBy  []string
	orderBy  []orderTerm
	limit    int
	hasAgg   bool
	distinct bool
}

// fromSource is one FROM/JOIN relation: an event data store or a derived-table subquery.
type fromSource struct {
	sub   *queryNode
	store string
	alias string
}

// prefix is the qualifier columns of this source may be addressed with ("alias.col").
func (f fromSource) prefix() string {
	if f.alias != "" {
		return strings.ToLower(f.alias)
	}

	return strings.ToLower(f.store)
}

// joinCond is one equality of an ON clause.
type joinCond struct{ left, right string }

type joinClause struct {
	kind string
	src  fromSource
	on   []joinCond
}

// queryNode is a query expression: a plain SELECT, or a UNION/INTERSECT/EXCEPT of two query expressions
// with an optional trailing ORDER BY/LIMIT applying to the combined result.
type queryNode struct {
	sel     *parsedLakeQuery
	left    *queryNode
	right   *queryNode
	op      string
	orderBy []orderTerm
	limit   int
	all     bool
}

// havingCond is a HAVING <aggregate | select alias> <op> <literal> predicate.
type havingCond struct {
	alias string
	op    string
	value string
	item  selectItem
}

// orderTerm is one ORDER BY key: a SELECT-list alias/column or a source column.
type orderTerm struct {
	name string
	desc bool
}

// lakeParser is a small hand-written recursive-descent parser over a flat
// token stream (see query_lex.go). Each grammar rule below is its own
// method so no single function's branching complexity needs a suppression.
type lakeParser struct {
	toks []sqlToken
	pos  int
}

func (p *lakeParser) peek() sqlToken {
	if p.pos >= len(p.toks) {
		return sqlToken{kind: sqlTokEOF}
	}

	return p.toks[p.pos]
}

func (p *lakeParser) advance() sqlToken {
	t := p.peek()
	p.pos++

	return t
}

func (p *lakeParser) atKeyword(kw string) bool {
	t := p.peek()

	return t.kind == sqlTokIdent && strings.EqualFold(t.text, kw)
}

func (p *lakeParser) eatKeyword(kw string) bool {
	if !p.atKeyword(kw) {
		return false
	}

	p.pos++

	return true
}

func (p *lakeParser) atPunct(punct string) bool {
	t := p.peek()

	return t.kind == sqlTokPunct && t.text == punct
}

func (p *lakeParser) eatPunct(punct string) bool {
	if !p.atPunct(punct) {
		return false
	}

	p.pos++

	return true
}

func (p *lakeParser) openParenNext() bool {
	i := p.pos + 1
	if i >= len(p.toks) {
		return false
	}

	return p.toks[i].kind == sqlTokPunct && p.toks[i].text == "("
}

func (p *lakeParser) remainingPreview() string {
	end := min(len(p.toks), p.pos+maxSQLErrorPreviewTokens)

	parts := make([]string, 0, end-p.pos)
	for _, t := range p.toks[p.pos:end] {
		parts = append(parts, t.text)
	}

	return strings.Join(parts, " ")
}

// lakeReservedWords are the identifiers this grammar treats structurally --
// used to tell a bare FROM-clause alias ("FROM eds1 edsA") apart from a
// keyword that must not be silently consumed as one.
var lakeReservedWords = map[string]struct{}{ //nolint:gochecknoglobals // static lookup table
	"WHERE": {}, "GROUP": {}, "LIMIT": {}, "AND": {}, "OR": {}, "NOT": {},
	"LIKE": {}, "IN": {}, "AS": {}, "BY": {}, "JOIN": {}, "INNER": {},
	"LEFT": {}, "RIGHT": {}, setOpUnion: {}, setOpExcept: {}, setOpIntersect: {}, "ORDER": {}, "HAVING": {},
	"ON": {}, "OUTER": {}, "FULL": {}, "CROSS": {}, "SELECT": {}, "ALL": {}, "DISTINCT": {},
}

func isLakeKeyword(text string) bool {
	_, ok := lakeReservedWords[strings.ToUpper(text)]

	return ok
}

// parseLakeQuery attempts to parse stmt against the supported CloudTrail
// Lake SQL subset. The second return is "" on success, or a human-readable
// reason on failure -- callers surface this as DescribeQuery/
// GetQueryResults' ErrorMessage on a FAILED query (DescribeQueryOutput.
// ErrorMessage: "The error message returned if a query failed", cloudtrail@
// v1.58.4 api_op_DescribeQuery.go:69; QueryStatus has a FAILED value,
// types/enums.go:384) -- a query outside what this emulator can execute is
// a genuine query failure, not a silent empty result set.
func parseLakeQuery(stmt string) (*queryNode, string) {
	toks, ok := tokenizeLakeSQL(stmt)
	if !ok {
		return nil, "unable to parse query: unsupported character or unterminated string literal"
	}

	p := &lakeParser{toks: toks}

	node, errMsg := p.parseQuery()
	if errMsg != "" {
		return nil, errMsg
	}

	if p.pos != len(p.toks) {
		return nil, fmt.Sprintf("unsupported SQL construct near %q", p.remainingPreview())
	}

	return node, ""
}

// parseQuery parses set-operation expressions followed by the ORDER BY/LIMIT that applies to the whole result.
func (p *lakeParser) parseQuery() (*queryNode, string) {
	node, errMsg := p.parseSetExpr()
	if errMsg != "" {
		return nil, errMsg
	}

	orderBy, errMsg := p.parseOptionalOrderBy()
	if errMsg != "" {
		return nil, errMsg
	}

	limit, errMsg := p.parseOptionalLimit()
	if errMsg != "" {
		return nil, errMsg
	}

	if node.sel == nil {
		node.orderBy, node.limit = orderBy, limit

		return node, ""
	}

	if (len(orderBy) > 0 || limit > 0) && (len(node.sel.orderBy) > 0 || node.sel.limit > 0) {
		return nil, "ORDER BY/LIMIT applied twice to the same query"
	}

	if len(orderBy) > 0 || limit > 0 {
		node.sel.orderBy, node.sel.limit = orderBy, limit
	}

	if validErr := validateOrderAndDistinct(
		node.sel.items, node.sel.hasAgg, node.sel.distinct, node.sel.orderBy,
	); validErr != "" {
		return nil, validErr
	}

	return node, ""
}

// parseSetExpr parses UNION/EXCEPT chains; INTERSECT binds tighter, as in standard SQL.
func (p *lakeParser) parseSetExpr() (*queryNode, string) {
	left, errMsg := p.parseIntersectExpr()
	if errMsg != "" {
		return nil, errMsg
	}

	for p.atKeyword(setOpUnion) || p.atKeyword(setOpExcept) {
		left, errMsg = p.parseSetTail(left, p.parseIntersectExpr)
		if errMsg != "" {
			return nil, errMsg
		}
	}

	return left, ""
}

func (p *lakeParser) parseIntersectExpr() (*queryNode, string) {
	left, errMsg := p.parseQueryPrimary()
	if errMsg != "" {
		return nil, errMsg
	}

	for p.atKeyword(setOpIntersect) {
		left, errMsg = p.parseSetTail(left, p.parseQueryPrimary)
		if errMsg != "" {
			return nil, errMsg
		}
	}

	return left, ""
}

func (p *lakeParser) parseSetTail(
	left *queryNode, operand func() (*queryNode, string),
) (*queryNode, string) {
	op := strings.ToUpper(p.advance().text)
	all := false

	switch {
	case p.eatKeyword("ALL"):
		all = true
	case p.eatKeyword("DISTINCT"):
	}

	if all && op != setOpUnion {
		return nil, op + " ALL is not supported by this emulator"
	}

	right, errMsg := operand()
	if errMsg != "" {
		return nil, errMsg
	}

	return &queryNode{op: op, all: all, left: left, right: right}, ""
}

func (p *lakeParser) parseQueryPrimary() (*queryNode, string) {
	if p.eatPunct("(") {
		node, errMsg := p.parseQuery()
		if errMsg != "" {
			return nil, errMsg
		}

		if !p.eatPunct(")") {
			return nil, "expected ) to close ("
		}

		return node, ""
	}

	sel, errMsg := p.parseSelectCore()
	if errMsg != "" {
		return nil, errMsg
	}

	return &queryNode{sel: &sel}, ""
}

func (p *lakeParser) parseSelectCore() (parsedLakeQuery, string) {
	if !p.eatKeyword("SELECT") {
		return parsedLakeQuery{}, "expected SELECT"
	}

	distinct := p.eatKeyword("DISTINCT")

	items, hasAgg, errMsg := p.parseSelectList()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	if !p.eatKeyword("FROM") {
		return parsedLakeQuery{}, "expected FROM"
	}

	from, joins, errMsg := p.parseFromClause()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	stripSourceQualifiers(items, from, joins)

	where, errMsg := p.parseOptionalWhere()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	groupBy, errMsg := p.parseOptionalGroupBy()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	having, errMsg := p.parseOptionalHaving()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	if havingErr := validateHaving(having, items, hasAgg); havingErr != "" {
		return parsedLakeQuery{}, havingErr
	}

	if validErr := validateAggregateColumns(items, hasAgg, groupBy); validErr != "" {
		return parsedLakeQuery{}, validErr
	}

	if validErr := validateOrderAndDistinct(items, hasAgg, distinct, nil); validErr != "" {
		return parsedLakeQuery{}, validErr
	}

	return parsedLakeQuery{
		items: items, where: where, groupBy: groupBy, having: having,
		hasAgg: hasAgg, distinct: distinct, from: from, joins: joins,
	}, ""
}

// stripSourceQualifiers drops a leading "<source>." from the default output name of an unaliased
// column, so SELECT a.eventName is named eventName as in Trino.
func stripSourceQualifiers(items []selectItem, from fromSource, joins []joinClause) {
	prefixes := make([]string, 0, 1+len(joins))
	prefixes = append(prefixes, from.prefix())

	for _, j := range joins {
		prefixes = append(prefixes, j.src.prefix())
	}

	for i := range items {
		if items[i].kind != itemColumn || items[i].aliased {
			continue
		}

		for _, pre := range prefixes {
			if pre != "" && len(items[i].outName) > len(pre)+1 &&
				strings.EqualFold(items[i].outName[:len(pre)+1], pre+".") {
				items[i].outName = items[i].outName[len(pre)+1:]

				break
			}
		}
	}
}

// parseFromClause parses the FROM relation and any JOINs.
func (p *lakeParser) parseFromClause() (fromSource, []joinClause, string) {
	src, errMsg := p.parseFromSource()
	if errMsg != "" {
		return fromSource{}, nil, errMsg
	}

	var joins []joinClause

	for {
		kind, kindErr := p.parseJoinKind()
		if kindErr != "" {
			return fromSource{}, nil, kindErr
		}

		if kind == "" {
			break
		}

		jc, tailErr := p.parseJoinTail(kind)
		if tailErr != "" {
			return fromSource{}, nil, tailErr
		}

		joins = append(joins, jc)
	}

	if p.atPunct(",") {
		return fromSource{}, nil, "multi-table FROM (comma-separated event data stores) is not supported by this emulator"
	}

	return src, joins, ""
}

func (p *lakeParser) parseFromSource() (fromSource, string) {
	var src fromSource

	if p.eatPunct("(") {
		sub, errMsg := p.parseQuery()
		if errMsg != "" {
			return fromSource{}, errMsg
		}

		if !p.eatPunct(")") {
			return fromSource{}, "expected ) to close the subquery"
		}

		src.sub = sub
	} else {
		t := p.advance()
		if t.kind != sqlTokIdent {
			return fromSource{}, "expected an event data store identifier or subquery after FROM"
		}

		src.store = t.text
	}

	switch {
	case p.eatKeyword("AS"):
		t := p.advance()
		if t.kind != sqlTokIdent {
			return fromSource{}, "expected an alias identifier after AS"
		}

		src.alias = t.text
	case p.peek().kind == sqlTokIdent && !isLakeKeyword(p.peek().text):
		src.alias = p.advance().text
	}

	return src, ""
}

const (
	setOpUnion     = "UNION"
	setOpExcept    = "EXCEPT"
	setOpIntersect = "INTERSECT"

	joinInner = "INNER"
	joinLeft  = "LEFT"
	joinRight = "RIGHT"
)

// parseJoinKind consumes a join keyword sequence, returning INNER, LEFT or RIGHT, or "" when none follows.
func (p *lakeParser) parseJoinKind() (string, string) {
	switch {
	case p.eatKeyword("JOIN"):
		return joinInner, ""
	case p.eatKeyword("INNER"):
		return joinInner, p.expectJoin()
	case p.eatKeyword("LEFT"):
		p.eatKeyword("OUTER")

		return joinLeft, p.expectJoin()
	case p.eatKeyword("RIGHT"):
		p.eatKeyword("OUTER")

		return joinRight, p.expectJoin()
	case p.atKeyword("FULL") || p.atKeyword("CROSS"):
		return "", strings.ToUpper(p.peek().text) + " JOIN is not supported by this emulator"
	}

	return "", ""
}

func (p *lakeParser) expectJoin() string {
	if !p.eatKeyword("JOIN") {
		return "expected JOIN"
	}

	return ""
}

const joinOnUnsupported = "JOIN ... ON supports only column = column equalities joined by AND"

func (p *lakeParser) parseJoinTail(kind string) (joinClause, string) {
	src, errMsg := p.parseFromSource()
	if errMsg != "" {
		return joinClause{}, errMsg
	}

	if !p.eatKeyword("ON") {
		return joinClause{}, "expected ON after JOIN"
	}

	jc := joinClause{kind: kind, src: src}

	for {
		l := p.advance()
		if l.kind != sqlTokIdent || !p.eatPunct("=") {
			return joinClause{}, joinOnUnsupported
		}

		r := p.advance()
		if r.kind != sqlTokIdent {
			return joinClause{}, joinOnUnsupported
		}

		jc.on = append(jc.on, joinCond{left: strings.ToLower(l.text), right: strings.ToLower(r.text)})

		if !p.eatKeyword("AND") {
			break
		}
	}

	return jc, ""
}

func (p *lakeParser) parseSelectList() ([]selectItem, bool, string) {
	if p.atPunct("*") {
		p.pos++

		return nil, false, ""
	}

	var items []selectItem

	hasAgg := false

	for {
		item, errMsg := p.parseSelectItem(len(items))
		if errMsg != "" {
			return nil, false, errMsg
		}

		if item.kind.isAggregate() {
			hasAgg = true
		}

		items = append(items, item)

		if !p.eatPunct(",") {
			break
		}
	}

	return items, hasAgg, ""
}

func (p *lakeParser) parseSelectItem(idx int) (selectItem, string) {
	if p.atKeyword("COUNT") && p.openParenNext() {
		return p.parseCountItem(idx)
	}

	if kind, ok := aggFuncKind(p.peek().text); ok && p.openParenNext() {
		return p.parseColumnAggItem(idx, kind)
	}

	t := p.advance()
	if t.kind != sqlTokIdent {
		return selectItem{}, "expected a column name, COUNT(...), or * in the SELECT list"
	}

	item := selectItem{kind: itemColumn, column: strings.ToLower(t.text), outName: t.text}

	alias, errMsg := p.parseOptionalAlias()
	if errMsg != "" {
		return selectItem{}, errMsg
	}

	if alias != "" {
		item.outName = alias
		item.aliased = true
	}

	return item, ""
}

func (p *lakeParser) parseColumnAggItem(idx int, kind selectItemKind) (selectItem, string) {
	name := strings.ToUpper(p.peek().text)
	p.pos += 2

	col := p.advance()
	if col.kind != sqlTokIdent {
		return selectItem{}, fmt.Sprintf("expected a column name inside %s(...)", name)
	}

	if !p.eatPunct(")") {
		return selectItem{}, fmt.Sprintf("expected ) to close %s(", name)
	}

	item := selectItem{kind: kind, column: strings.ToLower(col.text), outName: fmt.Sprintf("_col%d", idx)}

	alias, errMsg := p.parseOptionalAlias()
	if errMsg != "" {
		return selectItem{}, errMsg
	}

	if alias != "" {
		item.outName = alias
		item.aliased = true
	}

	return item, ""
}

func (p *lakeParser) parseCountItem(idx int) (selectItem, string) {
	p.pos += 2 // "COUNT" "("

	item := selectItem{kind: itemCountStar, outName: fmt.Sprintf("_col%d", idx)}

	switch {
	case p.eatPunct("*"):
	default:
		t := p.advance()
		if t.kind != sqlTokIdent {
			return selectItem{}, "expected * or a column name inside COUNT(...)"
		}

		item.kind = itemCount
		item.column = strings.ToLower(t.text)
	}

	if !p.eatPunct(")") {
		return selectItem{}, "expected ) to close COUNT("
	}

	alias, errMsg := p.parseOptionalAlias()
	if errMsg != "" {
		return selectItem{}, errMsg
	}

	if alias != "" {
		item.outName = alias
		item.aliased = true
	}

	return item, ""
}

func (p *lakeParser) parseOptionalAlias() (string, string) {
	if !p.eatKeyword("AS") {
		return "", ""
	}

	t := p.advance()
	if t.kind != sqlTokIdent {
		return "", "expected an alias identifier after AS"
	}

	return t.text, ""
}

func (p *lakeParser) parseOptionalWhere() (whereExpr, string) {
	if !p.eatKeyword("WHERE") {
		return nil, ""
	}

	return p.parseOrExpr()
}

func (p *lakeParser) parseOrExpr() (whereExpr, string) {
	left, errMsg := p.parseAndExpr()
	if errMsg != "" {
		return nil, errMsg
	}

	var right whereExpr

	for p.eatKeyword("OR") {
		right, errMsg = p.parseAndExpr()
		if errMsg != "" {
			return nil, errMsg
		}

		left = orNode{left: left, right: right}
	}

	return left, ""
}

func (p *lakeParser) parseAndExpr() (whereExpr, string) {
	left, errMsg := p.parseUnaryExpr()
	if errMsg != "" {
		return nil, errMsg
	}

	var right whereExpr

	for p.eatKeyword("AND") {
		right, errMsg = p.parseUnaryExpr()
		if errMsg != "" {
			return nil, errMsg
		}

		left = andNode{left: left, right: right}
	}

	return left, ""
}

func (p *lakeParser) parseUnaryExpr() (whereExpr, string) {
	if p.eatKeyword("NOT") {
		inner, errMsg := p.parseUnaryExpr()
		if errMsg != "" {
			return nil, errMsg
		}

		return notNode{inner: inner}, ""
	}

	return p.parsePrimaryExpr()
}

func (p *lakeParser) parsePrimaryExpr() (whereExpr, string) {
	if p.atKeyword("EXISTS") && p.openParenNext() {
		p.pos++

		sub, errMsg := p.parseSubquery()
		if errMsg != "" {
			return nil, errMsg
		}

		return existsNode{sub: sub}, ""
	}

	if p.eatPunct("(") {
		expr, errMsg := p.parseOrExpr()
		if errMsg != "" {
			return nil, errMsg
		}

		if !p.eatPunct(")") {
			return nil, "expected ) to close ("
		}

		return expr, ""
	}

	return p.parseCondition()
}

func (p *lakeParser) parseCondition() (whereExpr, string) {
	col := p.advance()
	if col.kind != sqlTokIdent {
		return nil, "expected a column name in the WHERE clause"
	}

	column := strings.ToLower(col.text)
	negate := p.eatKeyword("NOT")

	switch {
	case p.eatKeyword("LIKE"):
		return p.parseLikeCondition(column, negate)
	case p.eatKeyword("IN"):
		return p.parseInCondition(column, negate)
	case negate:
		return nil, "expected LIKE or IN after NOT"
	default:
		return p.parseComparison(column)
	}
}

func (p *lakeParser) parseComparison(column string) (whereExpr, string) {
	op := p.advance()
	if op.kind != sqlTokPunct || (op.text != "=" && op.text != "!=" && op.text != "<>") {
		return nil, fmt.Sprintf("expected =, !=, <>, LIKE, or IN after column %q", column)
	}

	if p.atPunct("(") && p.peekKeywordNext("SELECT") {
		sub, errMsg := p.parseSubquery()
		if errMsg != "" {
			return nil, errMsg
		}

		return cmpSubNode{column: column, sub: sub, negate: op.text != "="}, ""
	}

	val, errMsg := p.parseValue()
	if errMsg != "" {
		return nil, errMsg
	}

	return cmpNode{column: column, value: val, negate: op.text != "="}, ""
}

func (p *lakeParser) parseLikeCondition(column string, negate bool) (whereExpr, string) {
	val, errMsg := p.parseValue()
	if errMsg != "" {
		return nil, errMsg
	}

	re, reErr := likePatternToRegexp(val)
	if reErr != nil {
		return nil, fmt.Sprintf("invalid LIKE pattern %q: %v", val, reErr)
	}

	return likeNode{column: column, pattern: re, negate: negate}, ""
}

// parseSubquery parses "( query )" starting at the opening parenthesis.
func (p *lakeParser) parseSubquery() (*queryNode, string) {
	if !p.eatPunct("(") {
		return nil, "expected ( to open the subquery"
	}

	sub, errMsg := p.parseQuery()
	if errMsg != "" {
		return nil, errMsg
	}

	if !p.eatPunct(")") {
		return nil, "expected ) to close the subquery"
	}

	return sub, ""
}

func (p *lakeParser) peekKeywordNext(kw string) bool {
	i := p.pos + 1

	return i < len(p.toks) && p.toks[i].kind == sqlTokIdent && strings.EqualFold(p.toks[i].text, kw)
}

func (p *lakeParser) parseInCondition(column string, negate bool) (whereExpr, string) {
	if p.atPunct("(") && p.peekKeywordNext("SELECT") {
		sub, errMsg := p.parseSubquery()
		if errMsg != "" {
			return nil, errMsg
		}

		return inSubNode{column: column, sub: sub, negate: negate}, ""
	}

	if !p.eatPunct("(") {
		return nil, "expected ( after IN"
	}

	values := map[string]struct{}{}

	for {
		val, errMsg := p.parseValue()
		if errMsg != "" {
			return nil, errMsg
		}

		values[val] = struct{}{}

		if !p.eatPunct(",") {
			break
		}
	}

	if !p.eatPunct(")") {
		return nil, "expected ) to close IN ("
	}

	return inNode{column: column, values: values, negate: negate}, ""
}

func (p *lakeParser) parseValue() (string, string) {
	t := p.advance()
	if t.kind == sqlTokString || t.kind == sqlTokIdent || t.kind == sqlTokNumber {
		return t.text, ""
	}

	return "", "expected a value (string, identifier, or number)"
}

func (p *lakeParser) parseOptionalGroupBy() ([]string, string) {
	if !p.eatKeyword("GROUP") {
		return nil, ""
	}

	if !p.eatKeyword("BY") {
		return nil, "expected BY after GROUP"
	}

	var cols []string

	for {
		t := p.advance()
		if t.kind != sqlTokIdent {
			return nil, "expected a column name in GROUP BY"
		}

		cols = append(cols, strings.ToLower(t.text))

		if !p.eatPunct(",") {
			break
		}
	}

	return cols, ""
}

func (p *lakeParser) parseOptionalHaving() (*havingCond, string) {
	if !p.eatKeyword("HAVING") {
		return nil, ""
	}

	h := &havingCond{}

	_, isAgg := aggFuncKind(p.peek().text)
	if (isAgg || p.atKeyword("COUNT")) && p.openParenNext() {
		item, errMsg := p.parseSelectItem(0)
		if errMsg != "" {
			return nil, errMsg
		}

		item.outName = havingItemName
		h.item = item
	} else {
		t := p.advance()
		if t.kind != sqlTokIdent {
			return nil, "expected an aggregate or SELECT-list alias after HAVING"
		}

		h.alias = t.text
	}

	op := p.advance()
	if op.kind != sqlTokPunct || !slices.Contains([]string{"=", "!=", "<>", "<", "<=", ">", ">="}, op.text) {
		return nil, "expected a comparison operator in HAVING"
	}

	val, errMsg := p.parseValue()
	if errMsg != "" {
		return nil, errMsg
	}

	h.op, h.value = op.text, val

	return h, ""
}

func (p *lakeParser) parseOptionalOrderBy() ([]orderTerm, string) {
	if !p.eatKeyword("ORDER") {
		return nil, ""
	}

	if !p.eatKeyword("BY") {
		return nil, "expected BY after ORDER"
	}

	var terms []orderTerm

	for {
		t := p.advance()
		if t.kind != sqlTokIdent {
			return nil, "expected a column name or alias in ORDER BY"
		}

		term := orderTerm{name: t.text}

		switch {
		case p.eatKeyword("DESC"):
			term.desc = true
		default:
			p.eatKeyword("ASC")
		}

		terms = append(terms, term)

		if !p.eatPunct(",") {
			break
		}
	}

	return terms, ""
}

// validateOrderAndDistinct rejects DISTINCT on aggregate queries and ORDER BY
// keys an aggregate query's output does not carry.
func validateOrderAndDistinct(items []selectItem, hasAgg, distinct bool, orderBy []orderTerm) string {
	if distinct && hasAgg {
		return "SELECT DISTINCT combined with an aggregate function is not supported by this emulator"
	}

	if !hasAgg {
		return ""
	}

	for _, term := range orderBy {
		if !slices.ContainsFunc(items, func(it selectItem) bool { return itemMatchesOrder(it, term.name) }) {
			return fmt.Sprintf("ORDER BY %q must name a SELECT-list column or alias in an aggregate query", term.name)
		}
	}

	return ""
}

func itemMatchesOrder(it selectItem, name string) bool {
	return strings.EqualFold(it.outName, name) || (it.kind == itemColumn && it.column == strings.ToLower(name))
}

func (p *lakeParser) parseOptionalLimit() (int, string) {
	if !p.eatKeyword("LIMIT") {
		return 0, ""
	}

	t := p.advance()
	if t.kind != sqlTokNumber {
		return 0, "expected a number after LIMIT"
	}

	n, convErr := strconv.Atoi(t.text)
	if convErr != nil || n <= 0 {
		return 0, "invalid LIMIT value"
	}

	return n, ""
}

// validateAggregateColumns enforces standard SQL's rule that a plain column
// in an aggregate query's SELECT list must also be a GROUP BY key (real
// Trino rejects this at parse time rather than picking an arbitrary row's
// value), and that GROUP BY only appears alongside an aggregate -- this
// emulator does not model plain "GROUP BY without an aggregate" dedup.
func validateAggregateColumns(items []selectItem, hasAgg bool, groupBy []string) string {
	if !hasAgg {
		if len(groupBy) > 0 {
			return "GROUP BY without an aggregate function in the SELECT list is not supported by this emulator"
		}

		return ""
	}

	for _, item := range items {
		if item.kind != itemColumn {
			continue
		}

		if !slices.Contains(groupBy, item.column) {
			return fmt.Sprintf(
				"column %q must appear in the GROUP BY clause or be used inside an aggregate function",
				item.column,
			)
		}
	}

	return ""
}

func validateHaving(h *havingCond, items []selectItem, hasAgg bool) string {
	if h == nil {
		return ""
	}

	if !hasAgg {
		return "HAVING requires an aggregate function in the SELECT list"
	}

	if h.alias != "" &&
		!slices.ContainsFunc(items, func(it selectItem) bool { return strings.EqualFold(it.outName, h.alias) }) {
		return fmt.Sprintf("HAVING %q must name a SELECT-list alias or be an aggregate", h.alias)
	}

	return ""
}
