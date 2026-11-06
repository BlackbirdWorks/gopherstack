package sparql

import (
	"bytes"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type quadSet map[Quad]struct{}

// Store is a concurrency-safe in-memory RDF quad store.
type Store struct {
	mu    *lockmetrics.RWMutex
	quads map[Quad]uint64
	byS   map[Term]quadSet
	byP   map[Term]quadSet
	byO   map[Term]quadSet
	byG   map[Term]quadSet
	seq   uint64
}

// NewStore returns an empty store.
func NewStore() *Store {
	s := &Store{mu: lockmetrics.New("neptune-sparql")}
	s.reset()

	return s
}

func (s *Store) reset() {
	s.quads = make(map[Quad]uint64)
	s.seq = 0
	s.byS = make(map[Term]quadSet)
	s.byP = make(map[Term]quadSet)
	s.byO = make(map[Term]quadSet)
	s.byG = make(map[Term]quadSet)
}

func indexAdd(m map[Term]quadSet, k Term, q Quad) {
	set := m[k]
	if set == nil {
		set = make(quadSet)
		m[k] = set
	}

	set[q] = struct{}{}
}

func indexDel(m map[Term]quadSet, k Term, q Quad) {
	if set := m[k]; set != nil {
		delete(set, q)

		if len(set) == 0 {
			delete(m, k)
		}
	}
}

// normalize canonicalises literals the way Neptune stores them: dateTime values are converted to UTC.
func normalize(t Term) Term {
	if t.Kind == KindLiteral && t.Datatype == XSDDateTime {
		if tm, ok := parseDateTime(t.Value); ok {
			t.Value = formatDateTime(tm)
		}
	}

	return t
}

func canon(q Quad) Quad {
	q.S, q.P, q.O = normalize(q.S), normalize(q.P), normalize(q.O)

	if !q.G.Bound() {
		q.G = IRI(DefaultGraph)
	}

	return q
}

func (s *Store) add(q Quad) bool {
	q = canon(q)

	if _, ok := s.quads[q]; ok {
		return false
	}

	s.seq++
	s.quads[q] = s.seq
	indexAdd(s.byS, q.S, q)
	indexAdd(s.byP, q.P, q)
	indexAdd(s.byO, q.O, q)
	indexAdd(s.byG, q.G, q)

	return true
}

func (s *Store) remove(q Quad) bool {
	q = canon(q)

	if _, ok := s.quads[q]; !ok {
		return false
	}

	delete(s.quads, q)
	indexDel(s.byS, q.S, q)
	indexDel(s.byP, q.P, q)
	indexDel(s.byO, q.O, q)
	indexDel(s.byG, q.G, q)

	return true
}

// bestIndex picks the smallest index set among the bound positions; ok is false when nothing is bound.
func (s *Store) bestIndex(sub, p, o, g Term) (quadSet, bool) {
	var best quadSet

	have := false

	for _, c := range []struct {
		m map[Term]quadSet
		t Term
	}{{s.byS, sub}, {s.byP, p}, {s.byO, o}, {s.byG, g}} {
		if !c.t.Bound() {
			continue
		}

		if set := c.m[c.t]; !have || len(set) < len(best) {
			best, have = set, true
		}
	}

	return best, have
}

// match calls fn, in insertion order, for every quad matching the bound positions (unbound Terms are wildcards)
// until fn returns false. fn may not mutate the store.
func (s *Store) match(sub, p, o, g Term, fn func(Quad) bool) {
	var hits []Quad

	collect := func(q Quad) {
		if (sub.Bound() && q.S != sub) || (p.Bound() && q.P != p) || (o.Bound() && q.O != o) ||
			(g.Bound() && q.G != g) {
			return
		}

		hits = append(hits, q)
	}

	if best, have := s.bestIndex(sub, p, o, g); have {
		for q := range best {
			collect(q)
		}
	} else {
		for q := range s.quads {
			collect(q)
		}
	}

	sort.Slice(hits, func(i, j int) bool { return s.quads[hits[i]] < s.quads[hits[j]] })

	for _, q := range hits {
		if !fn(q) {
			return
		}
	}
}

// Len returns the number of quads.
func (s *Store) Len() int {
	s.mu.RLock("Len")
	defer s.mu.RUnlock()

	return len(s.quads)
}

// Add inserts quads, returning how many were new.
func (s *Store) Add(qs []Quad) int {
	s.mu.Lock("Add")
	defer s.mu.Unlock()

	n := 0

	for _, q := range qs {
		if s.add(q) {
			n++
		}
	}

	return n
}

// Clear removes every quad.
func (s *Store) Clear() {
	s.mu.Lock("Clear")
	defer s.mu.Unlock()

	s.reset()
}

// Graphs lists the graph names that currently hold triples, sorted.
func (s *Store) Graphs() []Term {
	s.mu.RLock("Graphs")
	defer s.mu.RUnlock()

	return s.graphsLocked()
}

func (s *Store) graphsLocked() []Term {
	out := make([]Term, 0, len(s.byG))
	for g := range s.byG {
		out = append(out, g)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })

	return out
}

// GraphQuads returns a sorted copy of the triples in graph g.
func (s *Store) GraphQuads(g Term) []Quad {
	s.mu.RLock("GraphQuads")
	defer s.mu.RUnlock()

	return s.sortedQuads(s.byG[g])
}

// ClearGraph removes every triple in g and reports how many were removed.
func (s *Store) ClearGraph(g Term) int {
	s.mu.Lock("ClearGraph")
	defer s.mu.Unlock()

	return s.clearGraph(g)
}

func (s *Store) clearGraph(g Term) int {
	qs := s.sortedQuads(s.byG[g])
	for _, q := range qs {
		s.remove(q)
	}

	return len(qs)
}

// ReplaceGraph atomically swaps the contents of g for qs (all forced into g).
func (s *Store) ReplaceGraph(g Term, qs []Quad) {
	s.mu.Lock("ReplaceGraph")
	defer s.mu.Unlock()

	s.clearGraph(g)

	for _, q := range qs {
		q.G = g
		s.add(q)
	}
}

func (s *Store) sortedQuads(set quadSet) []Quad {
	out := make([]Quad, 0, len(set))
	for q := range set {
		out = append(out, q)
	}

	sort.Slice(out, func(i, j int) bool { return s.quads[out[i]] < s.quads[out[j]] })

	return out
}

// Export serialises the store as N-Quads for persistence.
func (s *Store) Export() []byte {
	s.mu.RLock("Export")
	defer s.mu.RUnlock()

	var buf bytes.Buffer

	for _, q := range s.allQuads() {
		writeQuad(&buf, q, true)
	}

	return buf.Bytes()
}

// Import replaces the store's contents with N-Quads data produced by Export.
func (s *Store) Import(data []byte) error {
	res, err := ParseRDF(FormatNQuads, data, ParseOptions{KeepBlankLabels: true})
	if err != nil {
		return err
	}

	s.mu.Lock("Import")
	defer s.mu.Unlock()

	s.reset()

	for _, q := range res {
		s.add(q)
	}

	return nil
}

func formatDateTime(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond() == 0 {
		return t.Format("2006-01-02T15:04:05Z")
	}

	return strings.TrimRight(t.Format("2006-01-02T15:04:05.000000000"), "0") + "Z"
}

func parseDateTime(s string) (time.Time, bool) {
	for _, layout := range []string{
		time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

func (s *Store) allQuads() []Quad {
	out := make([]Quad, 0, len(s.quads))
	for q := range s.quads {
		out = append(out, q)
	}

	sort.Slice(out, func(i, j int) bool { return s.quads[out[i]] < s.quads[out[j]] })

	return out
}

// ApplyGraphWrite implements Graph Store PUT (replace) and POST (merge) into g. Quads that name other graphs
// are written there too, and every triple also lands in g, as Neptune documents.
func (s *Store) ApplyGraphWrite(g Term, qs []Quad, replace bool) {
	s.mu.Lock("GraphWrite")
	defer s.mu.Unlock()

	if replace {
		s.clearGraph(g)

		for _, q := range qs {
			if q.G != g {
				s.clearGraph(q.G)
			}
		}
	}

	for _, q := range qs {
		s.add(q)

		if q.G != g {
			q.G = g
			s.add(q)
		}
	}
}
