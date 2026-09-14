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

// selectItemKind identifies what a single SELECT list entry projects.
type selectItemKind int

const (
	itemColumn selectItemKind = iota
	itemCountStar
	itemCount
)

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
}

// parsedLakeQuery is a successfully parsed statement in the supported
// CloudTrail Lake SQL subset (see query_exec.go's file doc comment).
type parsedLakeQuery struct {
	items   []selectItem // nil means "SELECT *"
	where   whereExpr    // nil means no WHERE (match everything)
	groupBy []string     // lowercased GROUP BY column names
	limit   int          // 0 means "use defaultQueryRowLimit"
	hasAgg  bool
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

func (p *lakeParser) peekPunctAt(offset int, punct string) bool {
	i := p.pos + offset
	if i >= len(p.toks) {
		return false
	}

	return p.toks[i].kind == sqlTokPunct && p.toks[i].text == punct
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
	"LEFT": {}, "RIGHT": {}, "UNION": {}, "EXCEPT": {}, "INTERSECT": {},
}

func isLakeKeyword(text string) bool {
	_, ok := lakeReservedWords[strings.ToUpper(text)]

	return ok
}

// multiTableKeywords are the real CloudTrail Lake join/set operators this
// emulator does not implement (docs.aws.amazon.com/awscloudtrail/latest/
// userguide/query-limitations.html#query-aggregates-condition-operators'
// "Supported join operators": JOIN/UNION/EXCEPT/INTERSECT, plus INNER/LEFT/
// RIGHT as the JOIN qualifiers CloudTrail's own doc lists separately).
//
//nolint:gochecknoglobals // static lookup table
var multiTableKeywords = []string{"JOIN", "INNER", "LEFT", "RIGHT", "UNION", "EXCEPT", "INTERSECT"}

// parseLakeQuery attempts to parse stmt against the supported CloudTrail
// Lake SQL subset. The second return is "" on success, or a human-readable
// reason on failure -- callers surface this as DescribeQuery/
// GetQueryResults' ErrorMessage on a FAILED query (DescribeQueryOutput.
// ErrorMessage: "The error message returned if a query failed", cloudtrail@
// v1.58.4 api_op_DescribeQuery.go:69; QueryStatus has a FAILED value,
// types/enums.go:384) -- a query outside what this emulator can execute is
// a genuine query failure, not a silent empty result set.
func parseLakeQuery(stmt string) (parsedLakeQuery, string) {
	toks, ok := tokenizeLakeSQL(stmt)
	if !ok {
		return parsedLakeQuery{}, "unable to parse query: unsupported character or unterminated string literal"
	}

	p := &lakeParser{toks: toks}

	pq, errMsg := p.parseSelectStatement()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	if p.pos != len(p.toks) {
		return parsedLakeQuery{}, fmt.Sprintf("unsupported SQL construct near %q", p.remainingPreview())
	}

	return pq, ""
}

func (p *lakeParser) parseSelectStatement() (parsedLakeQuery, string) {
	if !p.eatKeyword("SELECT") {
		return parsedLakeQuery{}, "expected SELECT"
	}

	items, hasAgg, errMsg := p.parseSelectList()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	if !p.eatKeyword("FROM") {
		return parsedLakeQuery{}, "expected FROM"
	}

	if fromErr := p.parseFromTarget(); fromErr != "" {
		return parsedLakeQuery{}, fromErr
	}

	where, errMsg := p.parseOptionalWhere()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	groupBy, errMsg := p.parseOptionalGroupBy()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	limit, errMsg := p.parseOptionalLimit()
	if errMsg != "" {
		return parsedLakeQuery{}, errMsg
	}

	if validErr := validateAggregateColumns(items, hasAgg, groupBy); validErr != "" {
		return parsedLakeQuery{}, validErr
	}

	return parsedLakeQuery{items: items, where: where, groupBy: groupBy, limit: limit, hasAgg: hasAgg}, ""
}

// parseFromTarget consumes the FROM clause's single event-data-store
// identifier and an optional alias, then rejects anything that looks like a
// multi-event-data-store construct: CloudTrail Lake genuinely supports
// joins/set operations across event data stores (see multiTableKeywords),
// which this emulator does not implement -- disclosed in PARITY.md, not
// silently ignored.
func (p *lakeParser) parseFromTarget() string {
	if p.advance().kind != sqlTokIdent {
		return "expected an event data store identifier after FROM"
	}

	switch {
	case p.eatKeyword("AS"):
		if p.advance().kind != sqlTokIdent {
			return "expected an alias identifier after AS"
		}
	case p.peek().kind == sqlTokIdent && !isLakeKeyword(p.peek().text):
		p.pos++ // bare alias, no AS
	}

	for _, kw := range multiTableKeywords {
		if p.atKeyword(kw) {
			return fmt.Sprintf(
				"multi-event-data-store queries (JOIN/UNION/EXCEPT/INTERSECT) are not supported by this "+
					"emulator -- see docs.aws.amazon.com/awscloudtrail/latest/userguide/query-limitations.html"+
					"#query-aggregates-condition-operators; found %q after the FROM target",
				kw,
			)
		}
	}

	if p.atPunct(",") {
		return "multi-table FROM (comma-separated event data stores) is not supported by this emulator"
	}

	return ""
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

		if item.kind == itemCount || item.kind == itemCountStar {
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
	if p.atKeyword("COUNT") && p.peekPunctAt(1, "(") {
		return p.parseCountItem(idx)
	}

	if isUnsupportedAggregateFunc(p.peek().text) && p.peekPunctAt(1, "(") {
		return selectItem{}, fmt.Sprintf(
			"aggregate function %s is not supported by this emulator (only COUNT is implemented) -- see PARITY.md",
			strings.ToUpper(p.peek().text),
		)
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

func isUnsupportedAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
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

func (p *lakeParser) parseInCondition(column string, negate bool) (whereExpr, string) {
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
			return "GROUP BY without an aggregate function (COUNT) in the SELECT list is not supported by this emulator"
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
