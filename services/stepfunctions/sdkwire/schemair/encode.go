package schemair

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/aws/smithy-go"
)

// OpRef is an operation's schemas as the SDK package declares them.
type OpRef struct {
	Op, In, Out *smithy.Schema
	Token       string
}

// Encode captures the graph reachable from svc and ops. pool is every shape
// the SDK package declares; member targets are found in it by shape ID.
func Encode(svc *smithy.ServiceSchema, ops map[string]OpRef, pool []*smithy.Schema) (*Model, error) {
	e, err := newEncoder(pool)
	if err != nil {
		return nil, err
	}

	m := &Model{Version: svc.Version, Ops: make(map[string]Op, len(ops))}

	if m.Service, err = e.add(m, svc.Schema); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(ops))
	for name := range ops {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		if m.Ops[name], err = e.addOp(m, ops[name]); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func newEncoder(pool []*smithy.Schema) (*encoder, error) {
	e := &encoder{
		byID: map[smithy.ShapeID]*smithy.Schema{}, index: map[*smithy.Schema]int{},
		cs: codecs(), types: make(map[smithy.ShapeType]string), pre: map[*smithy.Schema]string{},
	}

	for n, t := range shapeTypes() {
		e.types[t] = n
	}

	for n, s := range preludeShapes() {
		e.byID[s.ID()] = s
		e.pre[s] = n
	}

	for _, s := range pool {
		if prev, dup := e.byID[s.ID()]; dup && prev != s {
			return nil, errf("duplicate shape %v", s.ID())
		}

		e.byID[s.ID()] = s
	}

	return e, nil
}

func (e *encoder) addOp(m *Model, ref OpRef) (Op, error) {
	op := Op{In: -1, Out: -1, Token: ref.Token}

	var err error

	if op.Schema, err = e.add(m, ref.Op); err != nil {
		return op, err
	}

	if ref.In != nil {
		if op.In, err = e.add(m, ref.In); err != nil {
			return op, err
		}
	}

	if ref.Out != nil {
		if op.Out, err = e.add(m, ref.Out); err != nil {
			return op, err
		}
	}

	return op, nil
}

// Write gzips the model as JSON.
func (m *Model) Write() ([]byte, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	_, _ = zw.Write(raw)

	if cerr := zw.Close(); cerr != nil {
		return nil, cerr
	}

	return buf.Bytes(), nil
}

type encoder struct {
	byID  map[smithy.ShapeID]*smithy.Schema
	index map[*smithy.Schema]int
	cs    map[string]codec
	types map[smithy.ShapeType]string
	pre   map[*smithy.Schema]string
}

func (e *encoder) add(m *Model, s *smithy.Schema) (int, error) {
	if i, ok := e.index[s]; ok {
		return i, nil
	}

	i := len(m.Shapes)
	e.index[s] = i

	if name, ok := e.pre[s]; ok {
		m.Shapes = append(m.Shapes, Shape{Prelude: name})

		return i, nil
	}

	ts, err := e.traits(s, true)
	if err != nil {
		return 0, err
	}

	m.Shapes = append(m.Shapes, Shape{
		Namespace: s.ID().Namespace, Name: s.ID().Name, Type: e.types[s.Type()], Traits: ts,
	})

	names := make([]string, 0, len(s.Members()))
	for n := range s.Members() {
		names = append(names, n)
	}

	sort.Strings(names)

	members := make([]Member, 0, len(names))

	for _, n := range names {
		mem := s.Member(n)

		target, ok := e.byID[mem.TargetID()]
		if !ok {
			return 0, errf("%v member %s: unknown target %v", s.ID(), n, mem.TargetID())
		}

		ti, terr := e.add(m, target)
		if terr != nil {
			return 0, terr
		}

		mts, merr := e.traits(mem, true)
		if merr != nil {
			return 0, merr
		}

		members = append(members, Member{Name: n, Target: ti, Traits: mts})
	}

	m.Shapes[i].Members = members

	return i, nil
}

func (e *encoder) traits(s *smithy.Schema, direct bool) ([]Trait, error) {
	names := make([]string, 0, len(e.cs))
	for n := range e.cs {
		names = append(names, n)
	}

	sort.Strings(names)

	var out []Trait

	for _, n := range names {
		t, ok := e.cs[n].probe(s, direct)
		if !ok {
			continue
		}

		raw, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}

		tr := Trait{Name: n}
		if string(raw) != "{}" {
			tr.Value = raw
		}

		out = append(out, tr)
	}

	return out, nil
}

// Equivalent reports the first difference between the graph rooted at a and
// the one rooted at b, as every trait view and member the protocols read.
func Equivalent(a, b *smithy.Schema) error {
	return (&comparer{cs: codecs(), seen: map[[2]*smithy.Schema]bool{}}).compare(a, b, "")
}

type comparer struct {
	cs   map[string]codec
	seen map[[2]*smithy.Schema]bool
}

func (c *comparer) compare(a, b *smithy.Schema, path string) error {
	if c.seen[[2]*smithy.Schema{a, b}] {
		return nil
	}

	c.seen[[2]*smithy.Schema{a, b}] = true

	if a.ID() != b.ID() || a.Type() != b.Type() || a.TargetID() != b.TargetID() {
		return errf("%s: identity %v/%d vs %v/%d", path, a.ID(), a.Type(), b.ID(), b.Type())
	}

	for n, cd := range c.cs {
		for _, direct := range []bool{false, true} {
			ta, oa := cd.probe(a, direct)
			tb, ob := cd.probe(b, direct)

			if oa != ob || (oa && !reflect.DeepEqual(ta, tb)) {
				return errf("%s: trait %s (direct=%v) differs", path, n, direct)
			}
		}
	}

	if len(a.Members()) != len(b.Members()) {
		return errf("%s: %d members vs %d", path, len(a.Members()), len(b.Members()))
	}

	for n, ma := range a.Members() {
		mb := b.Member(n)
		if mb == nil {
			return errf("%s: member %s missing", path, n)
		}

		if err := c.compare(ma, mb, path+"."+n); err != nil {
			return err
		}
	}

	return nil
}

// Check rebuilds the model and reports the first difference from the SDK's
// own graph.
func (m *Model) Check(svc *smithy.ServiceSchema, ops map[string]OpRef) error {
	raw, err := m.Write()
	if err != nil {
		return err
	}

	back, err := Read(bytes.NewReader(raw))
	if err != nil {
		return err
	}

	built, err := back.Build()
	if err != nil {
		return err
	}

	if built.Schema.Version != svc.Version {
		return errf("version %q vs %q", built.Schema.Version, svc.Version)
	}

	if err = Equivalent(svc.Schema, built.Schema.Schema); err != nil {
		return fmt.Errorf("service: %w", err)
	}

	if len(built.Ops) != len(ops) {
		return errf("%d ops vs %d", len(built.Ops), len(ops))
	}

	for name, ref := range ops {
		if err = checkOp(name, ref, built.Ops); err != nil {
			return err
		}
	}

	return nil
}

func checkOp(name string, ref OpRef, built map[string]Operation) error {
	got, ok := built[name]
	if !ok || got.Token != ref.Token {
		return errf("op %s: missing or token differs", name)
	}

	pairs := map[string][2]*smithy.Schema{
		"op": {ref.Op, got.Schema.Schema}, "in": {ref.In, got.Schema.Input}, "out": {ref.Out, got.Schema.Output},
	}

	for what, pair := range pairs {
		if pair[0] == nil || pair[1] == nil {
			if pair[0] != pair[1] {
				return errf("op %s %s: presence differs", name, what)
			}

			continue
		}

		if err := Equivalent(pair[0], pair[1]); err != nil {
			return errf("op %s %s: %v", name, what, err)
		}
	}

	return nil
}
