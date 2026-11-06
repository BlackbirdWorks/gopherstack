package gremlin

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const (
	mergeOnCreate = "onCreate"
	mergeOnMatch  = "onMatch"
	mergeOutV     = "outV"
	mergeInV      = "inV"
)

func regMerge(m map[string]stepFn) {
	m["mergeV"] = perTrav(stepMergeV)
	m["mergeE"] = perTrav(stepMergeE)
}

type mergeKV struct {
	val any
	key string
}

type mergeMap struct {
	out    any
	in     any
	id     string
	label  string
	props  []mergeKV
	hasID  bool
	hasLbl bool
	hasOut bool
	hasIn  bool
}

// mergeArg resolves the step argument (map, traversal yielding a map, or the incoming traverser) to a Map.
func (x *exec) mergeArg(a any, t *trav, fallback bool) (*Map, error) {
	if tr, ok := a.(*Traversal); ok {
		v, found, err := x.firstValue(tr, t)
		if err != nil {
			return nil, err
		}

		if !found {
			return nil, nil //nolint:nilnil // an empty traversal result is a null merge map
		}

		a = v
	}

	if a == nil && fallback {
		a = t.v
	}

	switch m := a.(type) {
	case nil:
		return nil, nil //nolint:nilnil // null merge map
	case *Map:
		return m, nil
	}

	return nil, malformed("mergeV()/mergeE() expects a Map but got %T", a)
}

func parseMergeMap(m *Map, edge bool) (*mergeMap, error) {
	mm := &mergeMap{}

	for i := range m.Len() {
		k, v := m.Entry(i)
		if err := mm.add(k, v, edge); err != nil {
			return nil, err
		}
	}

	return mm, nil
}

func (mm *mergeMap) add(k, v any, edge bool) error {
	switch key := k.(type) {
	case Token:
		return mm.addToken(key, v, edge)
	case string:
		nv := normalizeValue(v)
		if nv == nil {
			return malformed("Property value can not be null")
		}

		if !storableValue(nv) {
			return malformed("Property value type is not supported: %T", nv)
		}

		mm.props = append(mm.props, mergeKV{key: key, val: nv})

		return nil
	}

	return malformed("Unsupported merge map key: %v", k)
}

func (mm *mergeMap) addToken(key Token, v any, edge bool) error {
	switch {
	case key.Kind == kindT && key.Name == tokID:
		mm.id, mm.hasID = idString(v), true
	case key.Kind == kindT && key.Name == tokLabel:
		s, ok := v.(string)
		if !ok {
			return malformed("The label of a merge map must be a string")
		}

		mm.label, mm.hasLbl = s, true
	case edge && key.Kind == kindDirection && key.Name == dirOut,
		edge && key.Kind == kindMerge && key.Name == mergeOutV:
		mm.out, mm.hasOut = v, true
	case edge && key.Kind == kindDirection && key.Name == dirIn,
		edge && key.Kind == kindMerge && key.Name == mergeInV:
		mm.in, mm.hasIn = v, true
	default:
		return malformed("Unsupported merge map key: %s.%s", key.Kind, key.Name)
	}

	return nil
}

// mergeOptions evaluates option(Merge.x, map) modulators into parsed maps.
func (x *exec) mergeOptions(s *Step, t *trav, edge bool) (map[string]*mergeMap, error) {
	out := map[string]*mergeMap{}

	for _, o := range modsNamed(s, "option") {
		if len(o.Args) != 2 { //nolint:mnd // Merge token and map
			return nil, invalidArg("option() on %s() takes a Merge token and a map", s.Name)
		}

		tok, ok := o.Args[0].(Token)
		if !ok || tok.Kind != kindMerge {
			return nil, malformed("%s() only accepts option(Merge.<token>, map)", s.Name)
		}

		raw, err := x.mergeArg(o.Args[1], t, false)
		if err != nil {
			return nil, err
		}

		if raw == nil {
			continue
		}

		mm, err := parseMergeMap(raw, edge)
		if err != nil {
			return nil, err
		}

		out[tok.Name] = mm
	}

	return out, nil
}

func propMatches(cur, want any) bool {
	if list, ok := cur.([]any); ok {
		return slices.ContainsFunc(list, func(e any) bool { return equalValues(e, want) })
	}

	return cur != nil && equalValues(cur, want)
}

func propsMatch(props map[string]any, kvs []mergeKV) bool {
	for _, kv := range kvs {
		if !propMatches(props[kv.key], kv.val) {
			return false
		}
	}

	return true
}

func nodeLabelMatches(n *opencypher.Node, label string) bool {
	if strings.Contains(label, "::") {
		return strings.Join(n.Labels, "::") == label
	}

	return slices.Contains(n.Labels, label)
}

func (x *exec) matchNodes(mm *mergeMap) []*opencypher.Node {
	var cands []*opencypher.Node

	if mm.hasID {
		if n := x.st.Node(mm.id); n != nil {
			cands = []*opencypher.Node{n}
		}
	} else {
		cands = x.st.Nodes()
	}

	var out []*opencypher.Node

	for _, n := range cands {
		if (!mm.hasLbl || nodeLabelMatches(n, mm.label)) && propsMatch(n.Props, mm.props) {
			out = append(out, n)
		}
	}

	return out
}

// combinedCreate is the search map overlaid with onCreate; onCreate may not contradict T.id or T.label.
func combinedCreate(search, create *mergeMap) (*mergeMap, error) {
	if create == nil {
		return search, nil
	}

	out := *search
	out.props = slices.Clone(search.props)

	if create.hasID {
		if search.hasID && search.id != create.id {
			return nil, constraint("onCreate T.id conflicts with the merge map T.id")
		}

		out.id, out.hasID = create.id, true
	}

	if create.hasLbl {
		if search.hasLbl && search.label != create.label {
			return nil, constraint("onCreate T.label conflicts with the merge map T.label")
		}

		out.label, out.hasLbl = create.label, true
	}

	if create.hasOut {
		out.out, out.hasOut = create.out, true
	}

	if create.hasIn {
		out.in, out.hasIn = create.in, true
	}

	for _, kv := range create.props {
		if i := slices.IndexFunc(out.props, func(e mergeKV) bool { return e.key == kv.key }); i >= 0 {
			out.props[i] = kv
		} else {
			out.props = append(out.props, kv)
		}
	}

	return &out, nil
}

func applyMergeProps(el any, kvs []mergeKV) error {
	for _, kv := range kvs {
		if err := applyProp(el, propSpec{key: kv.key, val: kv.val}); err != nil {
			return err
		}
	}

	return nil
}

func stepMergeV(x *exec, s *Step, t *trav) ([]*trav, error) {
	var arg any
	if len(s.Args) > 0 {
		arg = s.Args[0]
	}

	raw, err := x.mergeArg(arg, t, true)
	if err != nil || raw == nil {
		return nil, err
	}

	search, err := parseMergeMap(raw, false)
	if err != nil {
		return nil, err
	}

	opts, err := x.mergeOptions(s, t, false)
	if err != nil {
		return nil, err
	}

	if matched := x.matchNodes(search); len(matched) > 0 {
		out := make([]*trav, 0, len(matched))

		for _, n := range matched {
			if om := opts[mergeOnMatch]; om != nil {
				if err = onMatchUpdate(n, om); err != nil {
					return nil, err
				}
			}

			out = append(out, t.next(n))
		}

		return out, nil
	}

	cm, err := combinedCreate(search, opts[mergeOnCreate])
	if err != nil {
		return nil, err
	}

	label := "vertex"
	if cm.hasLbl {
		label = cm.label
	}

	n, err := x.st.AddNode(cm.id, strings.Split(label, "::"), map[string]any{})
	if err != nil {
		return nil, err
	}

	if err = applyMergeProps(n, cm.props); err != nil {
		return nil, err
	}

	return []*trav{t.next(n)}, nil
}

func onMatchUpdate(el any, om *mergeMap) error {
	if om.hasID || om.hasLbl || om.hasOut || om.hasIn {
		return malformed("onMatch cannot modify T.id, T.label or the edge vertices")
	}

	return applyMergeProps(el, om.props)
}

// mergeEnd resolves an edge end given as a vertex, an id or a search map; nil when it does not exist.
func (x *exec) mergeEnd(v any) (*opencypher.Node, error) {
	switch e := v.(type) {
	case *opencypher.Node:
		return x.st.Node(e.ID), nil
	case VertexRef:
		return x.st.Node(e.ID), nil
	case *Map:
		mm, err := parseMergeMap(e, false)
		if err != nil {
			return nil, err
		}

		if ns := x.matchNodes(mm); len(ns) > 0 {
			return ns[0], nil
		}

		return nil, nil //nolint:nilnil // no vertex matches the search map
	}

	return x.st.Node(idString(v)), nil
}

func (x *exec) matchRels(mm *mergeMap, from, to *opencypher.Node) []*opencypher.Rel {
	var out []*opencypher.Rel

	for _, r := range x.st.Rels() {
		switch {
		case mm.hasID && r.ID != mm.id,
			mm.hasLbl && r.Type != mm.label,
			from != nil && r.Start != from.ID,
			to != nil && r.End != to.ID,
			!propsMatch(r.Props, mm.props):
			continue
		}

		out = append(out, r)
	}

	return out
}

func stepMergeE(x *exec, s *Step, t *trav) ([]*trav, error) {
	var arg any
	if len(s.Args) > 0 {
		arg = s.Args[0]
	}

	raw, err := x.mergeArg(arg, t, true)
	if err != nil || raw == nil {
		return nil, err
	}

	search, err := parseMergeMap(raw, true)
	if err != nil {
		return nil, err
	}

	opts, err := x.mergeOptions(s, t, true)
	if err != nil {
		return nil, err
	}

	if search, err = combinedEnds(search, opts); err != nil {
		return nil, err
	}

	from, to, err := x.searchEnds(search)
	if err != nil {
		return nil, err
	}

	if (search.hasOut && from == nil) || (search.hasIn && to == nil) {
		return x.createEdge(t, search, opts, from, to)
	}

	if matched := x.matchRels(search, from, to); len(matched) > 0 {
		out := make([]*trav, 0, len(matched))

		for _, r := range matched {
			if om := opts[mergeOnMatch]; om != nil {
				if err = onMatchUpdate(r, om); err != nil {
					return nil, err
				}
			}

			out = append(out, t.next(r))
		}

		return out, nil
	}

	return x.createEdge(t, search, opts, from, to)
}

// combinedEnds folds option(Merge.outV/inV, map) into the search map.
func combinedEnds(search *mergeMap, opts map[string]*mergeMap) (*mergeMap, error) {
	out := *search

	for name, dir := range map[string]*bool{mergeOutV: &out.hasOut, mergeInV: &out.hasIn} {
		o, ok := opts[name]
		if !ok {
			continue
		}

		m := NewMap()
		if o.hasID {
			m.Put(Token{Kind: kindT, Name: tokID}, o.id)
		}

		if o.hasLbl {
			m.Put(Token{Kind: kindT, Name: tokLabel}, o.label)
		}

		for _, kv := range o.props {
			m.Put(kv.key, kv.val)
		}

		*dir = true

		if name == mergeOutV {
			out.out = m
		} else {
			out.in = m
		}
	}

	return &out, nil
}

func (x *exec) searchEnds(mm *mergeMap) (*opencypher.Node, *opencypher.Node, error) {
	var from, to *opencypher.Node

	var err error

	if mm.hasOut {
		if from, err = x.mergeEnd(mm.out); err != nil {
			return nil, nil, err
		}
	}

	if mm.hasIn {
		if to, err = x.mergeEnd(mm.in); err != nil {
			return nil, nil, err
		}
	}

	return from, to, nil
}

func (x *exec) createEdge(
	t *trav, search *mergeMap, opts map[string]*mergeMap, from, to *opencypher.Node,
) ([]*trav, error) {
	cm, err := combinedCreate(search, opts[mergeOnCreate])
	if err != nil {
		return nil, err
	}

	if cm.hasOut != search.hasOut || cm.hasIn != search.hasIn || cm.out != search.out || cm.in != search.in {
		if from, to, err = x.searchEnds(cm); err != nil {
			return nil, err
		}
	}

	switch {
	case !cm.hasOut || !cm.hasIn:
		return nil, malformed("mergeE() requires both Direction.OUT and Direction.IN to create an edge")
	case !cm.hasLbl:
		return nil, malformed("mergeE() requires T.label to create an edge")
	case from == nil || to == nil:
		return nil, malformed("Could not find the from/to vertex of the edge to create in mergeE()")
	}

	r, err := x.st.AddRel(cm.id, cm.label, from, to, map[string]any{})
	if err != nil {
		return nil, err
	}

	if err = applyMergeProps(r, cm.props); err != nil {
		return nil, err
	}

	return []*trav{t.next(r)}, nil
}
