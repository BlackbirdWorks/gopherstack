// Package opencypher is an in-process openCypher engine over an in-memory property graph.
//
// Supported: MATCH, OPTIONAL MATCH, WHERE, CREATE, MERGE (ON CREATE/ON MATCH SET), SET, REMOVE, DELETE,
// DETACH DELETE, WITH, UNWIND, RETURN with DISTINCT, ORDER BY, SKIP, LIMIT, parameters, variable-length
// relationships, CASE, list/map expressions, the common scalar functions and the count, collect, sum, avg,
// min and max aggregates. Not supported: CALL, UNION, FOREACH, LOAD CSV, pattern predicates in WHERE,
// list comprehensions, shortestPath, constraints and indexes.
package opencypher

import (
	"context"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"
)

// IDProperty is the property key that sets or reads an entity id.
const IDProperty = "~id"

// Node is a labelled vertex.
type Node struct {
	Props  map[string]any
	ID     string
	Labels []string
	seq    int
}

// Rel is a directed, typed edge.
type Rel struct {
	Props map[string]any
	ID    string
	Type  string
	Start string
	End   string
	seq   int
}

// Path is an alternating node/relationship walk.
type Path struct {
	Nodes []*Node
	Rels  []*Rel
}

// Graph is a thread-safe in-memory property graph with an openCypher executor.
type Graph struct {
	funcs map[string]scalarFunc
	sem   chan struct{}
	nodes map[string]*Node
	rels  map[string]*Rel
	out   map[string][]*Rel
	in    map[string][]*Rel
	seq   int
}

// NewGraph returns an empty graph.
func NewGraph() *Graph {
	g := &Graph{sem: make(chan struct{}, 1), funcs: builtinFuncs()}
	g.reset()

	return g
}

func (g *Graph) reset() {
	g.nodes = make(map[string]*Node)
	g.rels = make(map[string]*Rel)
	g.out = make(map[string][]*Rel)
	g.in = make(map[string][]*Rel)
}

// Counts returns the number of nodes and relationships.
func (g *Graph) Counts() (int, int) {
	g.sem <- struct{}{}
	defer func() { <-g.sem }()

	return len(g.nodes), len(g.rels)
}

func (g *Graph) nextSeq() int {
	g.seq++

	return g.seq
}

func (g *Graph) allNodes() []*Node {
	ns := make([]*Node, 0, len(g.nodes))
	for _, n := range g.nodes {
		ns = append(ns, n)
	}

	sort.Slice(ns, func(i, j int) bool { return ns[i].seq < ns[j].seq })

	return ns
}

func (g *Graph) addNode(id string, labels []string, props map[string]any) (*Node, error) {
	if id == "" {
		id = uuid.NewString()
	}

	if _, dup := g.nodes[id]; dup {
		return nil, constraint("Vertex with id %q already exists", id)
	}

	n := &Node{ID: id, Labels: dedupe(labels), Props: props, seq: g.nextSeq()}
	if n.Props == nil {
		n.Props = map[string]any{}
	}

	g.nodes[id] = n

	return n, nil
}

func (g *Graph) addRel(typ string, from, to *Node, props map[string]any) *Rel {
	r := &Rel{ID: uuid.NewString(), Type: typ, Start: from.ID, End: to.ID, Props: props, seq: g.nextSeq()}
	if r.Props == nil {
		r.Props = map[string]any{}
	}

	g.rels[r.ID] = r
	g.out[from.ID] = append(g.out[from.ID], r)
	g.in[to.ID] = append(g.in[to.ID], r)

	return r
}

func (g *Graph) deleteRel(r *Rel) {
	delete(g.rels, r.ID)
	g.out[r.Start] = without(g.out[r.Start], r)
	g.in[r.End] = without(g.in[r.End], r)
}

func (g *Graph) deleteNode(n *Node) {
	delete(g.nodes, n.ID)
	delete(g.out, n.ID)
	delete(g.in, n.ID)
}

func without(rs []*Rel, r *Rel) []*Rel {
	out := make([]*Rel, 0, len(rs))

	for _, x := range rs {
		if x != r {
			out = append(out, x)
		}
	}

	return out
}

func dedupe(ss []string) []string {
	seen := make(map[string]bool, len(ss))
	out := make([]string, 0, len(ss))

	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}

	return out
}

type snapshot struct {
	nodes map[string]*Node
	rels  map[string]*Rel
	out   map[string][]*Rel
	in    map[string][]*Rel
	seq   int
}

func cloneProps(p map[string]any) map[string]any {
	c := make(map[string]any, len(p))
	maps.Copy(c, p)

	return c
}

func (g *Graph) snapshot() snapshot {
	s := snapshot{
		nodes: make(map[string]*Node, len(g.nodes)), rels: make(map[string]*Rel, len(g.rels)),
		out: make(map[string][]*Rel, len(g.out)), in: make(map[string][]*Rel, len(g.in)), seq: g.seq,
	}

	for id, n := range g.nodes {
		c := *n
		c.Props, c.Labels = cloneProps(n.Props), append([]string(nil), n.Labels...)
		s.nodes[id] = &c
	}

	for id, r := range g.rels {
		c := *r
		c.Props = cloneProps(r.Props)
		s.rels[id] = &c
	}

	for id, rs := range g.out {
		for _, r := range rs {
			s.out[id] = append(s.out[id], s.rels[r.ID])
		}
	}

	for id, rs := range g.in {
		for _, r := range rs {
			s.in[id] = append(s.in[id], s.rels[r.ID])
		}
	}

	return s
}

func (g *Graph) restore(s snapshot) {
	g.nodes, g.rels, g.out, g.in, g.seq = s.nodes, s.rels, s.out, s.in, s.seq
}

// Execute runs one openCypher query. Write queries are atomic: a failure leaves the graph unchanged.
func (g *Graph) Execute(ctx context.Context, src string, params map[string]any) (*Result, error) {
	q, err := parse(src)
	if err != nil {
		return nil, err
	}

	if aerr := g.acquire(ctx); aerr != nil {
		return nil, aerr
	}
	defer g.release()

	return g.runLocked(ctx, q, params)
}

func (g *Graph) acquire(ctx context.Context) error {
	select {
	case g.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for graph: %w", ctx.Err())
	}
}

func (g *Graph) release() { <-g.sem }

func (g *Graph) runLocked(ctx context.Context, q *query, params map[string]any) (*Result, error) {
	var snap snapshot
	if q.write {
		snap = g.snapshot()
	}

	x := &exec{g: g, params: params, ctx: ctx}

	res, err := x.run(q)
	if err != nil {
		if q.write {
			g.restore(snap)
		}

		return nil, err
	}

	res.Write = q.write

	return res, nil
}

// Tx is an explicit transaction holding the graph exclusively until Commit or Rollback.
type Tx struct {
	g    *Graph
	snap snapshot
	done bool
}

// Begin starts an explicit transaction, waiting for other users of the graph.
func (g *Graph) Begin(ctx context.Context) (*Tx, error) {
	if err := g.acquire(ctx); err != nil {
		return nil, err
	}

	return &Tx{g: g, snap: g.snapshot()}, nil
}

// Execute runs a query inside the transaction.
func (t *Tx) Execute(ctx context.Context, src string, params map[string]any) (*Result, error) {
	q, err := parse(src)
	if err != nil {
		return nil, err
	}

	if t.done {
		return nil, constraint("transaction is closed")
	}

	return t.g.runLocked(ctx, q, params)
}

// Commit keeps the transaction's writes and releases the graph.
func (t *Tx) Commit() {
	if !t.done {
		t.done = true
		t.g.release()
	}
}

// Rollback discards the transaction's writes and releases the graph.
func (t *Tx) Rollback() {
	if !t.done {
		t.done = true
		t.g.restore(t.snap)
		t.g.release()
	}
}
