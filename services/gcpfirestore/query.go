package gcpfirestore

import (
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
)

type orderTerm struct {
	path []string
	name bool
	desc bool
}

func (t orderTerm) value(d *firestorepb.Document) (*firestorepb.Value, bool) {
	if t.name {
		return refVal(d.GetName()), true
	}

	return getPath(d.GetFields(), t.path)
}

type compiledQuery struct {
	q      *firestorepb.StructuredQuery
	parent docPath
	orders []orderTerm
	scope  scope
	limit  int
	offset int
	hasLim bool
}

func isInequality(op firestorepb.StructuredQuery_FieldFilter_Operator) bool {
	switch op {
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN, firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
		firestorepb.StructuredQuery_FieldFilter_GREATER_THAN, firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
		firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL, firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		return true
	case firestorepb.StructuredQuery_FieldFilter_OPERATOR_UNSPECIFIED, firestorepb.StructuredQuery_FieldFilter_EQUAL,
		firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, firestorepb.StructuredQuery_FieldFilter_IN,
		firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		return false
	}

	return false
}

func collectInequality(f *firestorepb.StructuredQuery_Filter, out *[]string) {
	switch ft := f.GetFilterType().(type) {
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		for _, c := range ft.CompositeFilter.GetFilters() {
			collectInequality(c, out)
		}
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		if isInequality(ft.FieldFilter.GetOp()) {
			*out = append(*out, ft.FieldFilter.GetField().GetFieldPath())
		}
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		op := ft.UnaryFilter.GetOp()
		if op == firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN ||
			op == firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL {
			*out = append(*out, ft.UnaryFilter.GetField().GetFieldPath())
		}
	}
}

func makeTerm(fieldPath string, desc bool) (orderTerm, error) {
	if fieldPath == nameField {
		return orderTerm{name: true, desc: desc}, nil
	}

	p, err := parseFieldPath(fieldPath)
	if err != nil {
		return orderTerm{}, err
	}

	return orderTerm{path: p, desc: desc}, nil
}

func compileQuery(parent string, q *firestorepb.StructuredQuery) (*compiledQuery, error) {
	pp, perr := parseParent(parent)
	if perr != nil {
		return nil, perr
	}

	if len(q.GetFrom()) != 1 {
		return nil, errInvalid("Exactly one collection selector is required in 'from'.")
	}

	from := q.GetFrom()[0]
	cq := &compiledQuery{
		q:      q,
		parent: pp,
		scope:  scope{parent: pp.String(), collectionID: from.GetCollectionId(), descendants: from.GetAllDescendants()},
		offset: int(max(q.GetOffset(), 0)),
	}

	if from.GetCollectionId() == "" && !from.GetAllDescendants() {
		return nil, errInvalid("collection_id is required for a non-collection-group query.")
	}

	if q.GetLimit() != nil {
		cq.hasLim, cq.limit = true, int(q.GetLimit().GetValue())
	}

	if oerr := cq.compileOrders(); oerr != nil {
		return nil, oerr
	}

	return cq, nil
}

func (cq *compiledQuery) compileOrders() error {
	for _, o := range cq.q.GetOrderBy() {
		t, err := makeTerm(o.GetField().GetFieldPath(), o.GetDirection() == firestorepb.StructuredQuery_DESCENDING)
		if err != nil {
			return err
		}

		cq.orders = append(cq.orders, t)
	}

	var ineq []string
	collectInequality(cq.q.GetWhere(), &ineq)
	slices.Sort(ineq)

	for _, f := range slices.Compact(ineq) {
		if f == nameField || slices.ContainsFunc(cq.orders, func(t orderTerm) bool {
			return !t.name && strings.Join(t.path, ".") == f
		}) {
			continue
		}

		t, err := makeTerm(f, false)
		if err != nil {
			return err
		}

		cq.orders = append(cq.orders, t)
	}

	if !slices.ContainsFunc(cq.orders, func(t orderTerm) bool { return t.name }) {
		desc := len(cq.orders) > 0 && cq.orders[len(cq.orders)-1].desc
		cq.orders = append(cq.orders, orderTerm{name: true, desc: desc})
	}

	return nil
}

func (cq *compiledQuery) fieldValue(d *firestorepb.Document, fieldPath string) (*firestorepb.Value, bool) {
	if fieldPath == nameField {
		return refVal(d.GetName()), true
	}

	p, err := parseFieldPath(fieldPath)
	if err != nil {
		return nil, false
	}

	return getPath(d.GetFields(), p)
}

func (cq *compiledQuery) matchFilter(f *firestorepb.StructuredQuery_Filter, d *firestorepb.Document) bool {
	switch ft := f.GetFilterType().(type) {
	case nil:
		return true
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		isOr := ft.CompositeFilter.GetOp() == firestorepb.StructuredQuery_CompositeFilter_OR

		for _, c := range ft.CompositeFilter.GetFilters() {
			m := cq.matchFilter(c, d)
			if isOr && m {
				return true
			}

			if !isOr && !m {
				return false
			}
		}

		return !isOr
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		v, ok := cq.fieldValue(d, ft.FieldFilter.GetField().GetFieldPath())

		return ok && matchField(ft.FieldFilter.GetOp(), v, ft.FieldFilter.GetValue())
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		v, ok := cq.fieldValue(d, ft.UnaryFilter.GetField().GetFieldPath())

		return matchUnary(ft.UnaryFilter.GetOp(), v, ok)
	}

	return false
}

func matchUnary(op firestorepb.StructuredQuery_UnaryFilter_Operator, v *firestorepb.Value, ok bool) bool {
	if !ok {
		return false
	}

	switch op {
	case firestorepb.StructuredQuery_UnaryFilter_IS_NAN:
		return isNaN(v)
	case firestorepb.StructuredQuery_UnaryFilter_IS_NULL:
		return valueRank(v) == rankNull
	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
		return !isNaN(v)
	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
		return valueRank(v) != rankNull
	case firestorepb.StructuredQuery_UnaryFilter_OPERATOR_UNSPECIFIED:
		return false
	}

	return false
}

func matchField(op firestorepb.StructuredQuery_FieldFilter_Operator, v, operand *firestorepb.Value) bool {
	switch op {
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		return valuesEqual(v, operand)
	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return !valuesEqual(v, operand)
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN, firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
		firestorepb.StructuredQuery_FieldFilter_GREATER_THAN, firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return matchRange(op, v, operand)
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		return containsEqual(arrayOf(v), operand)
	case firestorepb.StructuredQuery_FieldFilter_IN:
		return containsEqual(arrayOf(operand), v)
	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		return !containsEqual(arrayOf(operand), v)
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		return slices.ContainsFunc(arrayOf(v), func(e *firestorepb.Value) bool {
			return containsEqual(arrayOf(operand), e)
		})
	case firestorepb.StructuredQuery_FieldFilter_OPERATOR_UNSPECIFIED:
		return false
	}

	return false
}

func containsEqual(vs []*firestorepb.Value, v *firestorepb.Value) bool {
	return slices.ContainsFunc(vs, func(e *firestorepb.Value) bool { return valuesEqual(e, v) })
}

func matchRange(op firestorepb.StructuredQuery_FieldFilter_Operator, v, operand *firestorepb.Value) bool {
	if valueRank(v) != valueRank(operand) || isNaN(v) || isNaN(operand) {
		return false
	}

	c := compareValues(v, operand)

	switch op {
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN:
		return c < 0
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return c <= 0
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN:
		return c > 0
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL, firestorepb.StructuredQuery_FieldFilter_EQUAL,
		firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL, firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS,
		firestorepb.StructuredQuery_FieldFilter_IN, firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY,
		firestorepb.StructuredQuery_FieldFilter_NOT_IN, firestorepb.StructuredQuery_FieldFilter_OPERATOR_UNSPECIFIED:
	}

	return c >= 0
}

func (cq *compiledQuery) compare(a, b *firestorepb.Document) int {
	for _, t := range cq.orders {
		av, _ := t.value(a)
		bv, _ := t.value(b)

		c := compareValues(av, bv)
		if t.desc {
			c = -c
		}

		if c != 0 {
			return c
		}
	}

	return 0
}

// cursorCmp compares a document with a cursor over the cursor's prefix of the order terms.
func (cq *compiledQuery) cursorCmp(d *firestorepb.Document, c *firestorepb.Cursor) int {
	root := cq.parent.root() + "/"

	for i, cv := range c.GetValues() {
		dv, _ := cq.orders[i].value(d)

		if cq.orders[i].name && cv.GetReferenceValue() != "" &&
			!strings.HasPrefix(cv.GetReferenceValue(), "projects/") {
			cv = refVal(root + cv.GetReferenceValue())
		}

		r := compareValues(dv, cv)
		if cq.orders[i].desc {
			r = -r
		}

		if r != 0 {
			return r
		}
	}

	return 0
}

func (cq *compiledQuery) validateCursors() error {
	for _, c := range []*firestorepb.Cursor{cq.q.GetStartAt(), cq.q.GetEndAt()} {
		if c != nil && len(c.GetValues()) > len(cq.orders) {
			return errInvalid(
				"Too many cursor values specified. The specified values must match the orderBy clauses of the query.",
			)
		}
	}

	return nil
}

func (cq *compiledQuery) inRange(d *firestorepb.Document) bool {
	if s := cq.q.GetStartAt(); s != nil {
		c := cq.cursorCmp(d, s)
		if (s.GetBefore() && c < 0) || (!s.GetBefore() && c <= 0) {
			return false
		}
	}

	if e := cq.q.GetEndAt(); e != nil {
		c := cq.cursorCmp(d, e)
		if (e.GetBefore() && c >= 0) || (!e.GetBefore() && c > 0) {
			return false
		}
	}

	return true
}

// filtered returns the sorted documents matching where, orderBy fields and cursors, before offset/limit.
func (b *InMemoryBackend) filtered(cq *compiledQuery, rv *readView) ([]*firestorepb.Document, error) {
	if err := cq.validateCursors(); err != nil {
		return nil, err
	}

	rv.noteScope(cq.scope)

	var out []*firestorepb.Document

	for name, rec := range b.docs {
		if !matchesCollection(name, cq.scope.parent, cq.scope.collectionID, cq.scope.descendants) {
			continue
		}

		d := rec.at(rv.at)
		if d == nil || !cq.hasOrderFields(d) || !cq.matchFilter(cq.q.GetWhere(), d) || !cq.inRange(d) {
			continue
		}

		out = append(out, d)
	}

	slices.SortFunc(out, cq.compare)

	return out, nil
}

func (cq *compiledQuery) hasOrderFields(d *firestorepb.Document) bool {
	for _, t := range cq.orders {
		if _, ok := t.value(d); !ok {
			return false
		}
	}

	return true
}

// page applies offset and limit and reports how many documents the offset skipped.
func (cq *compiledQuery) page(docs []*firestorepb.Document) ([]*firestorepb.Document, int) {
	skipped := min(cq.offset, len(docs))
	docs = docs[skipped:]

	if cq.hasLim && cq.limit < len(docs) {
		docs = docs[:cq.limit]
	}

	return docs, skipped
}

func (b *InMemoryBackend) runQuery(
	parent string, q *firestorepb.StructuredQuery, rv *readView,
) ([]*firestorepb.Document, int, error) {
	cq, err := compileQuery(parent, q)
	if err != nil {
		return nil, 0, err
	}

	docs, err := b.filtered(cq, rv)
	if err != nil {
		return nil, 0, err
	}

	page, skipped := cq.page(docs)

	return page, skipped, nil
}

// project applies a select clause; a clause with no fields returns names only.
func project(d *firestorepb.Document, sel *firestorepb.StructuredQuery_Projection) *firestorepb.Document {
	if sel == nil {
		return d
	}

	paths := make([]string, 0, len(sel.GetFields()))
	for _, f := range sel.GetFields() {
		paths = append(paths, f.GetFieldPath())
	}

	return maskDoc(d, paths)
}

func maskDoc(d *firestorepb.Document, paths []string) *firestorepb.Document {
	fields := map[string]*firestorepb.Value{}
	out := &firestorepb.Document{
		Name: d.GetName(), CreateTime: d.GetCreateTime(), UpdateTime: d.GetUpdateTime(), Fields: fields,
	}

	for _, p := range paths {
		if p == nameField {
			continue
		}

		path, err := parseFieldPath(p)
		if err != nil {
			continue
		}

		if v, ok := getPath(d.GetFields(), path); ok {
			setPath(fields, path, v)
		}
	}

	return out
}

func (b *InMemoryBackend) aggregate(
	parent string, agg *firestorepb.StructuredAggregationQuery, rv *readView,
) (map[string]*firestorepb.Value, error) {
	base := agg.GetStructuredQuery()
	if base == nil {
		return nil, errInvalid("structured_query is required.")
	}

	if len(agg.GetAggregations()) == 0 {
		return nil, errInvalid("At least one aggregation is required.")
	}

	docs, _, err := b.runQuery(parent, base, rv)
	if err != nil {
		return nil, err
	}

	out := make(map[string]*firestorepb.Value, len(agg.GetAggregations()))

	for i, a := range agg.GetAggregations() {
		alias := a.GetAlias()
		if alias == "" {
			alias = "field_" + strconv.Itoa(i+1)
		}

		v, aerr := evalAggregation(a, docs)
		if aerr != nil {
			return nil, aerr
		}

		out[alias] = v
	}

	return out, nil
}

func evalAggregation(
	a *firestorepb.StructuredAggregationQuery_Aggregation,
	docs []*firestorepb.Document,
) (*firestorepb.Value, error) {
	switch op := a.GetOperator().(type) {
	case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
		n := int64(len(docs))
		if up := op.Count.GetUpTo(); up != nil {
			n = min(n, up.GetValue())
		}

		return intVal(n), nil
	case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
		return sumOrAvg(docs, op.Sum.GetField().GetFieldPath(), false)
	case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
		return sumOrAvg(docs, op.Avg.GetField().GetFieldPath(), true)
	}

	return nil, errInvalid("Aggregation has no operator.")
}

func sumOrAvg(docs []*firestorepb.Document, fieldPath string, avg bool) (*firestorepb.Value, error) {
	path, err := parseFieldPath(fieldPath)
	if err != nil {
		return nil, err
	}

	var (
		isum    int64
		fsum    float64
		count   int64
		useF    bool
		overrun bool
	)

	for _, d := range docs {
		v, ok := getPath(d.GetFields(), path)
		if !ok || !isNumber(v) {
			continue
		}

		count++

		if i, isInt := v.GetValueType().(*firestorepb.Value_IntegerValue); isInt {
			next := isum + i.IntegerValue
			if (i.IntegerValue > 0 && next < isum) || (i.IntegerValue < 0 && next > isum) {
				overrun = true
			}

			isum = next
			fsum += float64(i.IntegerValue)

			continue
		}

		useF = true
		fsum += v.GetDoubleValue()
	}

	switch {
	case avg && count == 0:
		return nullVal(), nil
	case avg:
		return doubleVal(fsum / float64(count)), nil
	case useF || overrun:
		return doubleVal(fsum), nil
	}

	return intVal(isum), nil
}
