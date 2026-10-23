package gremlin

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func regMutate(m map[string]stepFn) {
	m["addV"] = perTrav(stepAddV)
	m["addE"] = perTrav(stepAddE)
	m["property"] = perTrav(stepProperty)
	m["drop"] = perTrav(stepDrop)
}

// propSpec is one property() call: cardinality, key (string or T token) and value.
type propSpec struct {
	key  any
	val  any
	card string
}

func (x *exec) parsePropSpec(args []any, t *trav) (propSpec, error) {
	card := ""

	if len(args) > 0 {
		if c, ok := args[0].(Token); ok && c.Kind == kindCardinality {
			card, args = c.Name, args[1:]
		}
	}

	if len(args) < 2 { //nolint:mnd // key and value
		return propSpec{}, invalidArg("property() requires a key and a value")
	}

	val := args[1]

	if tr, ok := val.(*Traversal); ok {
		v, found, err := x.firstValue(tr, t)
		if err != nil {
			return propSpec{}, err
		}

		if !found {
			return propSpec{}, malformed("The provided traverser does not map to a value: %s", tr)
		}

		val = v
	}

	return propSpec{key: args[0], val: normalizeValue(val), card: card}, nil
}

func (tr *Traversal) String() string {
	names := make([]string, len(tr.Steps))
	for i, s := range tr.Steps {
		names[i] = s.Name
	}

	return "[" + strings.Join(names, "-") + "]"
}

func storableValue(v any) bool {
	switch t := v.(type) {
	case string, bool, int64, float64:
		return true
	case []any:
		for _, e := range t {
			if !storableValue(e) {
				return false
			}
		}

		return true
	}

	return false
}

func applyProp(el any, p propSpec) error {
	props, ok := elemProps(el)
	if !ok {
		return malformed("The property() step can only be applied to graph elements")
	}

	key, ok := p.key.(string)
	if !ok {
		return malformed("Property key must be a string; id and label cannot be modified")
	}

	if p.val == nil {
		return malformed("Property value can not be null")
	}

	if !storableValue(p.val) {
		return malformed("Property value type is not supported: %T", p.val)
	}

	if _, isVertex := el.(*opencypher.Node); isVertex && (p.card == cardList || p.card == cardSet) {
		props[key] = appendCard(props[key], p.val, p.card)

		return nil
	}

	props[key] = p.val

	return nil
}

func appendCard(old, val any, card string) any {
	var list []any

	switch t := old.(type) {
	case nil:
	case []any:
		list = append(list, t...)
	default:
		list = append(list, t)
	}

	if card == cardSet {
		for _, e := range list {
			if equalValues(e, val) {
				return list
			}
		}
	}

	return append(list, val)
}

func stepProperty(x *exec, s *Step, t *trav) ([]*trav, error) {
	spec, err := x.parsePropSpec(s.Args, t)
	if err != nil {
		return nil, err
	}

	if k, ok := spec.key.(Token); ok {
		return nil, malformed("Property %s cannot be modified on an existing element", k.Name)
	}

	if err = applyProp(t.v, spec); err != nil {
		return nil, err
	}

	return []*trav{t}, nil
}

// creationProps evaluates the property() modulators of addV/addE into an id and a property map.
func (x *exec) creationProps(s *Step, t *trav) (string, map[string]any, error) {
	id := ""
	props := map[string]any{}

	for _, m := range modsNamed(s, "property") {
		spec, err := x.parsePropSpec(m.Args, t)
		if err != nil {
			return "", nil, err
		}

		if k, ok := spec.key.(Token); ok {
			if k.Name != tokID {
				return "", nil, malformed("Property %s cannot be set on creation", k.Name)
			}

			id = idString(spec.val)

			continue
		}

		holder := &opencypher.Node{Props: props}
		if err = applyProp(holder, spec); err != nil {
			return "", nil, err
		}
	}

	return id, props, nil
}

func (x *exec) labelArg(s *Step, t *trav) (string, error) {
	if len(s.Args) == 0 {
		return "", nil
	}

	switch a := s.Args[0].(type) {
	case string:
		return a, nil
	case *Traversal:
		v, ok, err := x.firstValue(a, t)
		if str, isStr := v.(string); err == nil && ok && isStr {
			return str, nil
		}

		return "", malformed("The label of %s() must be a string", s.Name)
	}

	return "", malformed("The label of %s() must be a string", s.Name)
}

func stepAddV(x *exec, s *Step, t *trav) ([]*trav, error) {
	label, err := x.labelArg(s, t)
	if err != nil {
		return nil, err
	}

	if label == "" {
		label = "vertex"
	}

	id, props, err := x.creationProps(s, t)
	if err != nil {
		return nil, err
	}

	n, err := x.st.AddNode(id, strings.Split(label, "::"), props)
	if err != nil {
		return nil, err
	}

	return []*trav{t.next(n)}, nil
}

func stepAddE(x *exec, s *Step, t *trav) ([]*trav, error) {
	label, err := x.labelArg(s, t)
	if err != nil {
		return nil, err
	}

	if label == "" {
		return nil, malformed("addE() requires an edge label")
	}

	from, to, err := x.edgeEnds(s, t)
	if err != nil {
		return nil, err
	}

	id, props, err := x.creationProps(s, t)
	if err != nil {
		return nil, err
	}

	r, err := x.st.AddRel(id, label, from, to, props)
	if err != nil {
		return nil, err
	}

	return []*trav{t.next(r)}, nil
}

func (x *exec) edgeEnds(s *Step, t *trav) (*opencypher.Node, *opencypher.Node, error) {
	var from, to *opencypher.Node

	var err error

	if m := modsNamed(s, "from"); len(m) > 0 {
		if from, err = x.endpoint(m[0], t); err != nil {
			return nil, nil, err
		}
	} else if n, ok := asNode(t.v); ok {
		from = n
	}

	if m := modsNamed(s, "to"); len(m) > 0 {
		if to, err = x.endpoint(m[0], t); err != nil {
			return nil, nil, err
		}
	} else if n, ok := asNode(t.v); ok && from != n {
		to = n
	}

	if from == nil || to == nil {
		return nil, nil, malformed(
			"The provided traverser does not map to a value: addE() needs from() and to() vertices",
		)
	}

	return from, to, nil
}

func (x *exec) endpoint(m *Step, t *trav) (*opencypher.Node, error) {
	if len(m.Args) != 1 {
		return nil, invalidArg("%s() takes one argument", m.Name)
	}

	var v any

	switch a := m.Args[0].(type) {
	case string:
		obj, ok := t.lookup(a)
		if !ok {
			return nil, malformed("The provided traverser does not map to a value: step label %q", a)
		}

		v = obj
	case *Traversal:
		got, ok, err := x.firstValue(a, t)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, malformed("The provided traverser does not map to a value: %s", a)
		}

		v = got
	default:
		v = a
	}

	return x.resolveVertex(v)
}

func (x *exec) resolveVertex(v any) (*opencypher.Node, error) {
	switch t := v.(type) {
	case *opencypher.Node:
		if x.st.Node(t.ID) == t {
			return t, nil
		}
	case VertexRef:
		if n := x.st.Node(t.ID); n != nil {
			return n, nil
		}
	}

	return nil, malformed("The provided from/to vertex does not exist: %v", v)
}

func stepDrop(x *exec, _ *Step, t *trav) ([]*trav, error) {
	switch v := t.v.(type) {
	case *opencypher.Node:
		x.st.DeleteNode(v)
	case *opencypher.Rel:
		x.st.DeleteRel(v)
	case *Prop:
		if props, ok := elemProps(v.Owner); ok {
			delete(props, v.Key)
		}
	default:
		return nil, malformed("The drop() step can only be applied to graph elements and properties")
	}

	return nil, nil
}
