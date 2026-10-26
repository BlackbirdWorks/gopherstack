package gcpdatastore

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type gqlResult struct {
	query *datastorepb.Query
	agg   *datastorepb.AggregationQuery
}

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString
	tokBinding
	tokPunct
)

type token struct {
	text string
	kind tokKind
}

type gqlParser struct {
	g        *datastorepb.GqlQuery
	toks     []token
	pos      int
	literals bool
}

type lexer struct {
	rs   []rune
	toks []token
	i    int
}

func tokenizeGQL(s string) ([]token, error) {
	l := &lexer{rs: []rune(s)}

	for l.i < len(l.rs) {
		if err := l.step(); err != nil {
			return nil, err
		}
	}

	return l.toks, nil
}

func isIdentRune(c rune) bool { return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_' }

func (l *lexer) step() error {
	c := l.rs[l.i]

	switch {
	case unicode.IsSpace(c):
		l.i++
	case c == '\'' || c == '"' || c == '`':
		return l.quoted(c)
	case c == '@' || c == ':':
		return l.binding()
	case unicode.IsDigit(c) || (c == '-' && l.i+1 < len(l.rs) && unicode.IsDigit(l.rs[l.i+1])):
		l.number()
	case unicode.IsLetter(c) || c == '_':
		l.ident()
	default:
		return l.punct(c)
	}

	return nil
}

func (l *lexer) quoted(q rune) error {
	text, next, err := scanQuoted(l.rs, l.i)
	if err != nil {
		return err
	}

	kind := tokString
	if q != '\'' {
		kind = tokIdent
	}

	l.toks = append(l.toks, token{kind: kind, text: text})
	l.i = next

	return nil
}

func (l *lexer) binding() error {
	j := l.i + 1
	for j < len(l.rs) && isIdentRune(l.rs[j]) {
		j++
	}

	if j == l.i+1 {
		return errInvalid("GQL syntax error: empty binding name.")
	}

	l.toks = append(l.toks, token{kind: tokBinding, text: string(l.rs[l.i+1 : j])})
	l.i = j

	return nil
}

func isExponentSign(rs []rune, j int) bool {
	return (rs[j] == '+' || rs[j] == '-') && (rs[j-1] == 'e' || rs[j-1] == 'E')
}

func (l *lexer) number() {
	j := l.i + 1
	for j < len(l.rs) && (unicode.IsDigit(l.rs[j]) || strings.ContainsRune(".eE", l.rs[j]) || isExponentSign(l.rs, j)) {
		j++
	}

	l.toks = append(l.toks, token{kind: tokNumber, text: string(l.rs[l.i:j])})
	l.i = j
}

func (l *lexer) ident() {
	j := l.i + 1
	for j < len(l.rs) && (isIdentRune(l.rs[j]) || l.rs[j] == '.') {
		j++
	}

	l.toks = append(l.toks, token{kind: tokIdent, text: string(l.rs[l.i:j])})
	l.i = j
}

func (l *lexer) punct(c rune) error {
	if l.i+1 < len(l.rs) {
		switch two := string(l.rs[l.i : l.i+2]); two {
		case "<=", ">=", "!=", "<>":
			l.toks = append(l.toks, token{kind: tokPunct, text: two})
			l.i += 2

			return nil
		}
	}

	if !strings.ContainsRune("(),*=<>;", c) {
		return errInvalid("GQL syntax error: unexpected character %q.", string(c))
	}

	l.toks = append(l.toks, token{kind: tokPunct, text: string(c)})
	l.i++

	return nil
}

func scanQuoted(rs []rune, start int) (string, int, error) {
	q := rs[start]

	var sb strings.Builder

	for i := start + 1; i < len(rs); i++ {
		switch {
		case rs[i] == '\\' && i+1 < len(rs):
			i++
			sb.WriteRune(rs[i])
		case rs[i] == q && i+1 < len(rs) && rs[i+1] == q:
			sb.WriteRune(q)

			i++
		case rs[i] == q:
			return sb.String(), i + 1, nil
		default:
			sb.WriteRune(rs[i])
		}
	}

	return "", 0, errInvalid("GQL syntax error: unterminated quoted text.")
}

func parseGQL(g *datastorepb.GqlQuery) (*gqlResult, error) {
	toks, err := tokenizeGQL(g.GetQueryString())
	if err != nil {
		return nil, err
	}

	p := &gqlParser{g: g, toks: toks}

	res, err := p.statement()
	if err != nil {
		return nil, err
	}

	p.accept(";")

	if p.peek().kind != tokEOF {
		return nil, errInvalid("GQL syntax error: unexpected %q.", p.peek().text)
	}

	if p.literals && !g.GetAllowLiterals() {
		return nil, errInvalid("The GQL query contains literals but allow_literals is false; bind the values instead.")
	}

	return res, nil
}

func (p *gqlParser) peek() token {
	if p.pos >= len(p.toks) {
		return token{kind: tokEOF}
	}

	return p.toks[p.pos]
}

func (p *gqlParser) next() token {
	t := p.peek()
	if p.pos < len(p.toks) {
		p.pos++
	}

	return t
}

func (p *gqlParser) isKw(kw string) bool {
	t := p.peek()

	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}

func (p *gqlParser) kw(kw string) bool {
	if p.isKw(kw) {
		p.pos++

		return true
	}

	return false
}

func (p *gqlParser) accept(punct string) bool {
	if t := p.peek(); t.kind == tokPunct && t.text == punct {
		p.pos++

		return true
	}

	return false
}

func (p *gqlParser) expect(punct string) error {
	if !p.accept(punct) {
		return errInvalid("GQL syntax error: expected %q but found %q.", punct, p.peek().text)
	}

	return nil
}

func (p *gqlParser) expectKw(kw string) error {
	if !p.kw(kw) {
		return errInvalid("GQL syntax error: expected %s but found %q.", kw, p.peek().text)
	}

	return nil
}

func (p *gqlParser) statement() (*gqlResult, error) {
	if p.kw("AGGREGATE") {
		return p.aggregate()
	}

	if err := p.expectKw("SELECT"); err != nil {
		return nil, err
	}

	q := &datastorepb.Query{}

	aggs, err := p.selectList(q)
	if err != nil {
		return nil, err
	}

	if err = p.queryTail(q); err != nil {
		return nil, err
	}

	if len(aggs) > 0 {
		return &gqlResult{query: q, agg: &datastorepb.AggregationQuery{
			QueryType:    &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q},
			Aggregations: aggs,
		}}, nil
	}

	return &gqlResult{query: q}, nil
}

func (p *gqlParser) aggregate() (*gqlResult, error) {
	var aggs []*datastorepb.AggregationQuery_Aggregation

	for {
		a, ok, err := p.aggFunc()
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errInvalid("GQL syntax error: expected an aggregation but found %q.", p.peek().text)
		}

		aggs = append(aggs, a)

		if !p.accept(",") {
			break
		}
	}

	if err := p.expectKw("OVER"); err != nil {
		return nil, err
	}

	if err := p.expect("("); err != nil {
		return nil, err
	}

	if err := p.expectKw("SELECT"); err != nil {
		return nil, err
	}

	q := &datastorepb.Query{}

	if more, err := p.selectList(q); err != nil {
		return nil, err
	} else if len(more) > 0 {
		return nil, errInvalid("GQL syntax error: aggregations are not allowed in the OVER query.")
	}

	if err := p.queryTail(q); err != nil {
		return nil, err
	}

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return &gqlResult{query: q, agg: &datastorepb.AggregationQuery{
		QueryType:    &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q},
		Aggregations: aggs,
	}}, nil
}

func (p *gqlParser) aggFunc() (*datastorepb.AggregationQuery_Aggregation, bool, error) {
	t := p.peek()
	if t.kind != tokIdent || p.pos+1 >= len(p.toks) || p.toks[p.pos+1].text != "(" {
		return nil, false, nil
	}

	name := strings.ToUpper(t.text)
	if name != "COUNT" && name != "COUNT_UP_TO" && name != "SUM" && name != "AVG" {
		return nil, false, nil
	}

	p.pos += 2

	a := &datastorepb.AggregationQuery_Aggregation{}
	if err := p.aggOperator(name, a); err != nil {
		return nil, false, err
	}

	if err := p.expect(")"); err != nil {
		return nil, false, err
	}

	if p.kw("AS") {
		alias := p.next()
		if alias.kind != tokIdent {
			return nil, false, errInvalid("GQL syntax error: expected an alias after AS.")
		}

		a.Alias = alias.text
	}

	return a, true, nil
}

func (p *gqlParser) aggOperator(name string, a *datastorepb.AggregationQuery_Aggregation) error {
	switch name {
	case "COUNT":
		a.Operator = &datastorepb.AggregationQuery_Aggregation_Count_{
			Count: &datastorepb.AggregationQuery_Aggregation_Count{},
		}

		return p.expect("*")
	case "COUNT_UP_TO":
		v, err := p.value()
		if err != nil {
			return err
		}

		a.Operator = &datastorepb.AggregationQuery_Aggregation_Count_{
			Count: &datastorepb.AggregationQuery_Aggregation_Count{
				UpTo: wrapperspb.Int64(v.GetIntegerValue()),
			},
		}

		return nil
	}

	prop := p.next()
	if prop.kind != tokIdent {
		return errInvalid("GQL syntax error: expected a property name in %s.", name)
	}

	ref := &datastorepb.PropertyReference{Name: prop.text}
	if name == "SUM" {
		a.Operator = &datastorepb.AggregationQuery_Aggregation_Sum_{
			Sum: &datastorepb.AggregationQuery_Aggregation_Sum{Property: ref},
		}
	} else {
		a.Operator = &datastorepb.AggregationQuery_Aggregation_Avg_{
			Avg: &datastorepb.AggregationQuery_Aggregation_Avg{Property: ref},
		}
	}

	return nil
}

// selectList parses the projection after SELECT; aggregation shorthand is returned separately.
func (p *gqlParser) selectList(q *datastorepb.Query) ([]*datastorepb.AggregationQuery_Aggregation, error) {
	if p.accept("*") {
		return nil, nil
	}

	distinct := p.kw("DISTINCT")

	var on []*datastorepb.PropertyReference

	if distinct && p.kw("ON") {
		var err error
		if on, err = p.distinctOn(); err != nil {
			return nil, err
		}
	}

	aggs, props, err := p.selectItems()
	if err != nil {
		return nil, err
	}

	for _, pr := range props {
		q.Projection = append(q.Projection, &datastorepb.Projection{Property: pr})
	}

	switch {
	case on != nil:
		q.DistinctOn = on
	case distinct:
		q.DistinctOn = props
	}

	return aggs, nil
}

func (p *gqlParser) distinctOn() ([]*datastorepb.PropertyReference, error) {
	if err := p.expect("("); err != nil {
		return nil, err
	}

	var on []*datastorepb.PropertyReference

	for {
		t := p.next()
		if t.kind != tokIdent {
			return nil, errInvalid("GQL syntax error: expected a property name in DISTINCT ON.")
		}

		on = append(on, &datastorepb.PropertyReference{Name: t.text})

		if !p.accept(",") {
			break
		}
	}

	return on, p.expect(")")
}

func (p *gqlParser) selectItems() (
	[]*datastorepb.AggregationQuery_Aggregation, []*datastorepb.PropertyReference, error,
) {
	var (
		aggs  []*datastorepb.AggregationQuery_Aggregation
		props []*datastorepb.PropertyReference
	)

	for {
		a, ok, err := p.aggFunc()
		if err != nil {
			return nil, nil, err
		}

		if ok {
			aggs = append(aggs, a)
		} else {
			t := p.next()
			if t.kind != tokIdent {
				return nil, nil, errInvalid("GQL syntax error: expected a property name but found %q.", t.text)
			}

			props = append(props, &datastorepb.PropertyReference{Name: t.text})
		}

		if !p.accept(",") {
			break
		}
	}

	if len(aggs) > 0 && len(props) > 0 {
		return nil, nil, errInvalid("GQL syntax error: aggregations cannot be mixed with properties.")
	}

	return aggs, props, nil
}

func (p *gqlParser) queryTail(q *datastorepb.Query) error {
	if p.kw("FROM") {
		t := p.next()
		if t.kind != tokIdent {
			return errInvalid("GQL syntax error: expected a kind after FROM.")
		}

		q.Kind = []*datastorepb.KindExpression{{Name: t.text}}
	}

	if p.kw("WHERE") {
		f, err := p.orExpr()
		if err != nil {
			return err
		}

		q.Filter = f
	}

	if p.kw("ORDER") {
		if err := p.expectKw("BY"); err != nil {
			return err
		}

		if err := p.orderBy(q); err != nil {
			return err
		}
	}

	return p.limitOffset(q)
}

func (p *gqlParser) orderBy(q *datastorepb.Query) error {
	for {
		t := p.next()
		if t.kind != tokIdent {
			return errInvalid("GQL syntax error: expected a property name in ORDER BY.")
		}

		dir := datastorepb.PropertyOrder_ASCENDING

		if p.kw("DESC") {
			dir = datastorepb.PropertyOrder_DESCENDING
		} else {
			p.kw("ASC")
		}

		q.Order = append(q.Order, &datastorepb.PropertyOrder{
			Property: &datastorepb.PropertyReference{Name: t.text}, Direction: dir,
		})

		if !p.accept(",") {
			return nil
		}
	}
}

func (p *gqlParser) intArg() (int64, error) {
	v, err := p.value()
	if err != nil {
		return 0, err
	}

	if _, ok := v.GetValueType().(*datastorepb.Value_IntegerValue); !ok {
		return 0, errInvalid("GQL: LIMIT and OFFSET require integers.")
	}

	return v.GetIntegerValue(), nil
}

func (p *gqlParser) limitClause(q *datastorepb.Query) error {
	n, err := p.intArg()
	if err != nil {
		return err
	}

	if p.accept(",") {
		q.Offset = clampInt32(int(n))

		if n, err = p.intArg(); err != nil {
			return err
		}
	}

	q.Limit = wrapperspb.Int32(clampInt32(int(n)))

	return nil
}

func (p *gqlParser) offsetClause(q *datastorepb.Query) error {
	n, err := p.intArg()
	q.Offset = clampInt32(int(n))

	return err
}

func (p *gqlParser) limitOffset(q *datastorepb.Query) error {
	for {
		var err error

		switch {
		case p.kw("LIMIT"):
			err = p.limitClause(q)
		case p.kw("OFFSET"):
			err = p.offsetClause(q)
		case p.kw("START"):
			q.StartCursor, err = p.cursorArg()
		case p.kw("END"):
			q.EndCursor, err = p.cursorArg()
		default:
			return nil
		}

		if err != nil {
			return err
		}
	}
}

func (p *gqlParser) cursorArg() ([]byte, error) {
	if err := p.expectKw("CURSOR"); err != nil {
		return nil, err
	}

	t := p.next()
	if t.kind != tokBinding {
		return nil, errInvalid("GQL: cursors must be passed as bindings.")
	}

	bp, err := p.binding(t.text)
	if err != nil {
		return nil, err
	}

	if c, ok := bp.GetParameterType().(*datastorepb.GqlQueryParameter_Cursor); ok {
		return c.Cursor, nil
	}

	return nil, errInvalid("GQL: binding %q is not a cursor.", t.text)
}

func (p *gqlParser) binding(name string) (*datastorepb.GqlQueryParameter, error) {
	if n, err := strconv.Atoi(name); err == nil {
		if n < 1 || n > len(p.g.GetPositionalBindings()) {
			return nil, errInvalid("GQL: positional binding %d is not provided.", n)
		}

		return p.g.GetPositionalBindings()[n-1], nil
	}

	bp, ok := p.g.GetNamedBindings()[name]
	if !ok {
		return nil, errInvalid("GQL: named binding %q is not provided.", name)
	}

	return bp, nil
}

func (p *gqlParser) orExpr() (*datastorepb.Filter, error) {
	return p.compose(datastorepb.CompositeFilter_OR, "OR", p.andExpr)
}

func (p *gqlParser) andExpr() (*datastorepb.Filter, error) {
	return p.compose(datastorepb.CompositeFilter_AND, "AND", p.term)
}

func (p *gqlParser) compose(
	op datastorepb.CompositeFilter_Operator, kw string, sub func() (*datastorepb.Filter, error),
) (*datastorepb.Filter, error) {
	first, err := sub()
	if err != nil {
		return nil, err
	}

	filters := []*datastorepb.Filter{first}

	for p.kw(kw) {
		f, ferr := sub()
		if ferr != nil {
			return nil, ferr
		}

		filters = append(filters, f)
	}

	if len(filters) == 1 {
		return first, nil
	}

	return &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{
		CompositeFilter: &datastorepb.CompositeFilter{Op: op, Filters: filters},
	}}, nil
}

func (p *gqlParser) term() (*datastorepb.Filter, error) {
	if p.accept("(") {
		f, err := p.orExpr()
		if err != nil {
			return nil, err
		}

		return f, p.expect(")")
	}

	prop := p.next()
	if prop.kind != tokIdent {
		return nil, errInvalid("GQL syntax error: expected a property name but found %q.", prop.text)
	}

	op, val, err := p.condition()
	if err != nil {
		return nil, err
	}

	return &datastorepb.Filter{
		FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
			Property: &datastorepb.PropertyReference{Name: prop.text}, Op: op, Value: val,
		}},
	}, nil
}

func gqlOperator(s string) (datastorepb.PropertyFilter_Operator, bool) {
	switch s {
	case "=":
		return datastorepb.PropertyFilter_EQUAL, true
	case "<":
		return datastorepb.PropertyFilter_LESS_THAN, true
	case "<=":
		return datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL, true
	case ">":
		return datastorepb.PropertyFilter_GREATER_THAN, true
	case ">=":
		return datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, true
	case "!=", "<>":
		return datastorepb.PropertyFilter_NOT_EQUAL, true
	default:
		return 0, false
	}
}

func (p *gqlParser) condition() (datastorepb.PropertyFilter_Operator, *datastorepb.Value, error) {
	t := p.peek()
	if op, ok := gqlOperator(t.text); ok && t.kind == tokPunct {
		p.pos++
		v, err := p.value()

		return op, v, err
	}

	switch {
	case p.kw("IN"):
		v, err := p.listValue()

		return datastorepb.PropertyFilter_IN, v, err
	case p.kw("NOT"):
		if err := p.expectKw("IN"); err != nil {
			return 0, nil, err
		}

		v, err := p.listValue()

		return datastorepb.PropertyFilter_NOT_IN, v, err
	case p.kw("CONTAINS"):
		v, err := p.value()

		return datastorepb.PropertyFilter_EQUAL, v, err
	case p.kw("IS"):
		if err := p.expectKw("NULL"); err != nil {
			return 0, nil, err
		}

		return datastorepb.PropertyFilter_EQUAL, nullValue(), nil
	case p.kw("HAS"):
		if err := p.expectKw("ANCESTOR"); err != nil {
			return 0, nil, err
		}

		v, err := p.value()

		return datastorepb.PropertyFilter_HAS_ANCESTOR, v, err
	}

	return 0, nil, errInvalid("GQL syntax error: expected a comparison operator but found %q.", t.text)
}

func (p *gqlParser) listValue() (*datastorepb.Value, error) {
	p.kw("ARRAY")

	if t := p.peek(); t.kind == tokBinding {
		return p.value()
	}

	if err := p.expect("("); err != nil {
		return nil, err
	}

	var vals []*datastorepb.Value

	for {
		v, err := p.value()
		if err != nil {
			return nil, err
		}

		vals = append(vals, v)

		if !p.accept(",") {
			break
		}
	}

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return &datastorepb.Value{
		ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: vals}},
	}, nil
}

func (p *gqlParser) value() (*datastorepb.Value, error) {
	t := p.next()

	switch t.kind {
	case tokBinding:
		bp, err := p.binding(t.text)
		if err != nil {
			return nil, err
		}

		v, ok := bp.GetParameterType().(*datastorepb.GqlQueryParameter_Value)
		if !ok || v.Value == nil {
			return nil, errInvalid("GQL: binding %q has no value.", t.text)
		}

		return v.Value, nil
	case tokNumber:
		p.literals = true

		return numberLiteral(t.text)
	case tokString:
		p.literals = true

		return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: t.text}}, nil
	case tokIdent:
		p.literals = true

		return p.identValue(t.text)
	default:
		return nil, errInvalid("GQL syntax error: expected a value but found %q.", t.text)
	}
}

func numberLiteral(s string) (*datastorepb.Value, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return intValue(n), nil
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errInvalid("GQL: invalid number %q.", s)
	}

	return doubleValue(f), nil
}

func (p *gqlParser) identValue(word string) (*datastorepb.Value, error) {
	switch strings.ToUpper(word) {
	case "TRUE", "FALSE":
		return &datastorepb.Value{
			ValueType: &datastorepb.Value_BooleanValue{BooleanValue: strings.EqualFold(word, "true")},
		}, nil
	case "NULL":
		return nullValue(), nil
	case "BLOB":
		s, err := p.stringArg()
		if err != nil {
			return nil, err
		}

		raw, derr := base64.URLEncoding.DecodeString(s)
		if derr != nil {
			if raw, derr = base64.StdEncoding.DecodeString(s); derr != nil {
				return nil, errInvalid("GQL: invalid BLOB literal.")
			}
		}

		return &datastorepb.Value{ValueType: &datastorepb.Value_BlobValue{BlobValue: raw}}, nil
	case "DATETIME":
		s, err := p.stringArg()
		if err != nil {
			return nil, err
		}

		ts, perr := time.Parse(time.RFC3339Nano, s)
		if perr != nil {
			return nil, errInvalid("GQL: invalid DATETIME literal %q.", s)
		}

		return timestampValue(timestamppb.New(ts)), nil
	case "KEY":
		return p.keyLiteral()
	default:
		return nil, errInvalid("GQL syntax error: unexpected %q.", word)
	}
}

func (p *gqlParser) stringArg() (string, error) {
	if err := p.expect("("); err != nil {
		return "", err
	}

	t := p.next()
	if t.kind != tokString {
		return "", errInvalid("GQL syntax error: expected a string literal.")
	}

	return t.text, p.expect(")")
}

func (p *gqlParser) keyLiteral() (*datastorepb.Value, error) {
	if err := p.expect("("); err != nil {
		return nil, err
	}

	key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{}}

	var parts []token

	for {
		part, isPart, err := p.keyArg(key)
		if err != nil {
			return nil, err
		}

		if isPart {
			parts = append(parts, part)
		}

		if !p.accept(",") {
			break
		}
	}

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	if len(parts) == 0 || len(parts)%2 != 0 {
		return nil, errInvalid("GQL: KEY needs (kind, id-or-name) pairs.")
	}

	for i := 0; i < len(parts); i += 2 {
		el, err := pathElement(parts[i], parts[i+1])
		if err != nil {
			return nil, err
		}

		key.Path = append(key.Path, el)
	}

	return keyValue(key), nil
}

// keyArg parses one KEY(...) argument: a PROJECT/NAMESPACE setting (applied to key) or a path token.
func (p *gqlParser) keyArg(key *datastorepb.Key) (token, bool, error) {
	if p.isKw("PROJECT") || p.isKw("NAMESPACE") {
		which := strings.ToUpper(p.next().text)

		s, err := p.stringArg()
		if err != nil {
			return token{}, false, err
		}

		if which == "PROJECT" {
			key.PartitionId.ProjectId = s
		} else {
			key.PartitionId.NamespaceId = s
		}

		return token{}, false, nil
	}

	t := p.next()
	if t.kind == tokBinding {
		bt, err := p.bindingToken(t.text)

		return bt, true, err
	}

	return t, true, nil
}

func pathElement(kind, id token) (*datastorepb.Key_PathElement, error) {
	el := &datastorepb.Key_PathElement{Kind: kind.text}

	if id.kind != tokNumber {
		el.IdType = &datastorepb.Key_PathElement_Name{Name: id.text}

		return el, nil
	}

	n, err := strconv.ParseInt(id.text, 10, 64)
	if err != nil {
		return nil, errInvalid("GQL: invalid key id %q.", id.text)
	}

	el.IdType = &datastorepb.Key_PathElement_Id{Id: n}

	return el, nil
}

func (p *gqlParser) bindingToken(name string) (token, error) {
	bp, err := p.binding(name)
	if err != nil {
		return token{}, err
	}

	if v, ok := bp.GetParameterType().(*datastorepb.GqlQueryParameter_Value); ok {
		switch vt := v.Value.GetValueType().(type) {
		case *datastorepb.Value_IntegerValue:
			return token{kind: tokNumber, text: strconv.FormatInt(vt.IntegerValue, 10)}, nil
		case *datastorepb.Value_StringValue:
			return token{kind: tokString, text: vt.StringValue}, nil
		}
	}

	return token{}, errInvalid("GQL: binding %q cannot be used in a KEY.", name)
}
