package gcpdatastore

import (
	"slices"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

const (
	maxInValues    = 30
	maxNotInValues = 10
	kindProperty   = "__kind__"
	nsProperty     = "__namespace__"
	propProperty   = "__property__"
)

type order struct {
	name string
	desc bool
}

// queryPlan is a validated, normalised Query.
type queryPlan struct {
	ancestor  *datastorepb.Key
	filter    *datastorepb.Filter
	q         *datastorepb.Query
	eqProps   map[string]struct{}
	part      partition
	kind      string
	distinct  []string
	proj      []string
	explicit  []order
	orders    []order
	ineq      []string
	offset    int
	limit     int
	nonKey    bool
	hasLimit  bool
	keysOnly  bool
	unbounded bool
}

func buildPlan(part partition, q *datastorepb.Query) (*queryPlan, error) {
	if q.GetFindNearest() != nil {
		return nil, errUnimplemented("Vector search (find_nearest) is not supported.")
	}

	p := &queryPlan{q: q, part: part, eqProps: map[string]struct{}{}, offset: int(q.GetOffset())}

	if len(q.GetKind()) > 1 {
		return nil, errInvalid("Only one kind may be specified in a query.")
	}

	if len(q.GetKind()) == 1 {
		p.kind = q.GetKind()[0].GetName()
		if p.kind == "" {
			return nil, errInvalid("The query kind must not be empty.")
		}
	}

	if q.GetOffset() < 0 {
		return nil, errInvalid("The query offset must not be negative.")
	}

	if q.GetLimit() != nil {
		if q.GetLimit().GetValue() < 0 {
			return nil, errInvalid("The query limit must not be negative.")
		}

		p.hasLimit, p.limit = true, int(q.GetLimit().GetValue())
	}

	rest, anc, err := splitAncestor(q.GetFilter())
	if err != nil {
		return nil, err
	}

	p.filter, p.ancestor = stampFilter(rest, part), anc
	if anc != nil {
		p.ancestor = stampKey(anc, part, true)
	}

	if err = p.analyzeFilter(rest); err != nil {
		return nil, err
	}

	if err = p.planProjection(q); err != nil {
		return nil, err
	}

	if err = p.planOrders(q); err != nil {
		return nil, err
	}

	if p.kind == "" && !p.kindlessOK() {
		return nil, errInvalid("Kindless queries can only filter on ancestor and __key__.")
	}

	return p, nil
}

func (p *queryPlan) kindlessOK() bool {
	return !p.nonKey && !slices.ContainsFunc(p.explicit, func(o order) bool { return o.name != keyProperty })
}

// splitAncestor removes the HAS_ANCESTOR filter, which must sit at the top level of an AND.
func splitAncestor(f *datastorepb.Filter) (*datastorepb.Filter, *datastorepb.Key, error) {
	switch t := f.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		if t.PropertyFilter.GetOp() != datastorepb.PropertyFilter_HAS_ANCESTOR {
			return f, nil, nil
		}

		k, err := ancestorKey(t.PropertyFilter)

		return nil, k, err
	case *datastorepb.Filter_CompositeFilter:
		cf := t.CompositeFilter
		if cf.GetOp() != datastorepb.CompositeFilter_AND {
			if containsAncestor(f) {
				return nil, nil, errInvalid("HAS_ANCESTOR cannot be used inside an OR filter.")
			}

			return f, nil, nil
		}

		return splitAncestorAnd(cf)
	default:
		return nil, nil, nil
	}
}

func splitAncestorAnd(cf *datastorepb.CompositeFilter) (*datastorepb.Filter, *datastorepb.Key, error) {
	var anc *datastorepb.Key

	kept := make([]*datastorepb.Filter, 0, len(cf.GetFilters()))

	for _, child := range cf.GetFilters() {
		rest, k, err := splitAncestor(child)
		if err != nil {
			return nil, nil, err
		}

		if k != nil && anc != nil {
			return nil, nil, errInvalid("A query may have at most one HAS_ANCESTOR filter.")
		}

		if k != nil {
			anc = k
		}

		if rest != nil {
			kept = append(kept, rest)
		}
	}

	if len(kept) == 0 {
		return nil, anc, nil
	}

	return &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{
		CompositeFilter: &datastorepb.CompositeFilter{Op: datastorepb.CompositeFilter_AND, Filters: kept},
	}}, anc, nil
}

func containsAncestor(f *datastorepb.Filter) bool {
	switch t := f.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		return t.PropertyFilter.GetOp() == datastorepb.PropertyFilter_HAS_ANCESTOR
	case *datastorepb.Filter_CompositeFilter:
		return slices.ContainsFunc(t.CompositeFilter.GetFilters(), containsAncestor)
	}

	return false
}

func ancestorKey(pf *datastorepb.PropertyFilter) (*datastorepb.Key, error) {
	if pf.GetProperty().GetName() != keyProperty {
		return nil, errInvalid("HAS_ANCESTOR requires the __key__ property.")
	}

	k := pf.GetValue().GetKeyValue()
	if k == nil || len(k.GetPath()) == 0 {
		return nil, errInvalid("HAS_ANCESTOR requires a key value.")
	}

	return k, nil
}

func (p *queryPlan) analyzeFilter(f *datastorepb.Filter) error {
	if f == nil {
		return nil
	}

	notEq := 0

	if err := walkFilter(f, func(pf *datastorepb.PropertyFilter) error {
		return p.analyzeProp(pf, &notEq)
	}); err != nil {
		return err
	}

	if notEq > 1 {
		return errInvalid("A query may have at most one != or NOT_IN filter.")
	}

	return nil
}

func walkFilter(f *datastorepb.Filter, fn func(*datastorepb.PropertyFilter) error) error {
	switch t := f.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		return fn(t.PropertyFilter)
	case *datastorepb.Filter_CompositeFilter:
		if t.CompositeFilter.GetOp() == datastorepb.CompositeFilter_OPERATOR_UNSPECIFIED {
			return errInvalid("A composite filter requires an operator.")
		}

		for _, c := range t.CompositeFilter.GetFilters() {
			if err := walkFilter(c, fn); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *queryPlan) analyzeProp(pf *datastorepb.PropertyFilter, notEq *int) error {
	name := pf.GetProperty().GetName()
	if name == "" {
		return errInvalid("A property filter requires a property name.")
	}

	if pf.GetValue().GetValueType() == nil {
		return errInvalid("The filter on %q requires a value.", name)
	}

	if name == keyProperty && !isKeyValue(pf.GetValue(), pf.GetOp()) {
		return errInvalid("Filters on __key__ require key values.")
	}

	if name != keyProperty {
		p.nonKey = true
	}

	switch pf.GetOp() {
	case datastorepb.PropertyFilter_OPERATOR_UNSPECIFIED:
		return errInvalid("The filter on %q requires an operator.", name)
	case datastorepb.PropertyFilter_HAS_ANCESTOR:
		return errInvalid("HAS_ANCESTOR is invalid here.")
	case datastorepb.PropertyFilter_EQUAL:
		p.noteEq(name)
	case datastorepb.PropertyFilter_IN, datastorepb.PropertyFilter_NOT_IN:
		return p.analyzeList(pf, notEq)
	case datastorepb.PropertyFilter_NOT_EQUAL:
		*notEq++

		p.noteIneq(name)
	default:
		p.noteIneq(name)
	}

	return nil
}

func (p *queryPlan) analyzeList(pf *datastorepb.PropertyFilter, notEq *int) error {
	name := pf.GetProperty().GetName()
	isIn := pf.GetOp() == datastorepb.PropertyFilter_IN

	limit := maxNotInValues
	if isIn {
		limit = maxInValues
	}

	n, err := arrayFilterSize(pf, limit)
	if err != nil {
		return err
	}

	if n == 0 {
		return errInvalid("The %s filter on %q requires at least one value.", pf.GetOp(), name)
	}

	if isIn {
		p.noteEq(name)

		return nil
	}

	*notEq++

	p.noteIneq(name)

	return nil
}

func isKeyValue(v *datastorepb.Value, op datastorepb.PropertyFilter_Operator) bool {
	if op == datastorepb.PropertyFilter_IN || op == datastorepb.PropertyFilter_NOT_IN {
		return !slices.ContainsFunc(arrayElems(v), func(e *datastorepb.Value) bool { return e.GetKeyValue() == nil })
	}

	return v.GetKeyValue() != nil
}

func arrayFilterSize(pf *datastorepb.PropertyFilter, limit int) (int, error) {
	if !isArray(pf.GetValue()) {
		return 0, errInvalid("The %s filter on %q requires an array value.", pf.GetOp(), pf.GetProperty().GetName())
	}

	n := len(arrayElems(pf.GetValue()))
	if n > limit {
		return 0, errInvalid(
			"The %s filter on %q allows at most %d values.",
			pf.GetOp(),
			pf.GetProperty().GetName(),
			limit,
		)
	}

	return n, nil
}

func (p *queryPlan) noteEq(name string) {
	p.eqProps[name] = struct{}{}
}

func (p *queryPlan) noteIneq(name string) {
	if name != keyProperty && !slices.Contains(p.ineq, name) {
		p.ineq = append(p.ineq, name)
	}
}

func (p *queryPlan) planProjection(q *datastorepb.Query) error {
	seen := map[string]struct{}{}

	for _, pr := range q.GetProjection() {
		name := pr.GetProperty().GetName()
		if name == "" {
			return errInvalid("A projection requires a property name.")
		}

		if _, dup := seen[name]; dup {
			return errInvalid("Property %q is projected more than once.", name)
		}

		seen[name] = struct{}{}
		p.proj = append(p.proj, name)
	}

	p.keysOnly = len(p.proj) == 1 && p.proj[0] == keyProperty

	for _, d := range q.GetDistinctOn() {
		if d.GetName() == "" {
			return errInvalid("distinct_on requires a property name.")
		}

		p.distinct = append(p.distinct, d.GetName())
	}

	return nil
}

func (p *queryPlan) planOrders(q *datastorepb.Query) error {
	for _, o := range q.GetOrder() {
		name := o.GetProperty().GetName()
		if name == "" {
			return errInvalid("A sort order requires a property name.")
		}

		p.explicit = append(
			p.explicit,
			order{name: name, desc: o.GetDirection() == datastorepb.PropertyOrder_DESCENDING},
		)
	}

	if len(p.ineq) > 0 && len(p.explicit) > 0 && len(p.ineq) == 1 && p.explicit[0].name != p.ineq[0] {
		return errInvalid(
			"The first sort property must be the same as the property to which the inequality filter is applied. "+
				"In your query the first sort property is %s but the inequality filter is on %s",
			p.explicit[0].name, p.ineq[0])
	}

	eff := make([]order, 0, len(p.explicit)+1)

	for _, o := range p.explicit {
		if _, eq := p.eqProps[o.name]; eq && o.name != keyProperty {
			continue
		}

		eff = append(eff, o)
	}

	if len(p.explicit) == 0 {
		for _, n := range p.ineq {
			eff = append(eff, order{name: n})
		}
	}

	if !slices.ContainsFunc(eff, func(o order) bool { return o.name == keyProperty }) {
		eff = append(eff, order{name: keyProperty})
	}

	p.orders = eff

	return nil
}

// stampFilter clones f with every key value stamped into the query's partition.
func stampFilter(f *datastorepb.Filter, part partition) *datastorepb.Filter {
	if f == nil {
		return nil
	}

	out, _ := proto.Clone(f).(*datastorepb.Filter)

	_ = walkFilter(out, func(pf *datastorepb.PropertyFilter) error {
		stampValueKeys(pf.GetValue(), func(k *datastorepb.Key) *datastorepb.Key { return stampKey(k, part, true) })

		return nil
	})

	return out
}
