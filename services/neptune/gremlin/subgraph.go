package gremlin

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func regSubgraph(m map[string]stepFn) {
	m["subgraph"] = stepSubgraph
}

// subgraphAcc collects the edges, and their endpoints, that pass through subgraph(key).
type subgraphAcc struct {
	nodeIdx map[string]bool
	relIdx  map[string]bool
	nodes   []*opencypher.Node
	rels    []*opencypher.Rel
}

// GraphProp is one property of a SubGraph element; vertices may carry several values per key.
type GraphProp struct {
	Key  string
	Vals []any
	IDs  []int64
}

// GraphVertex is a vertex of a SubGraph with its properties.
type GraphVertex struct {
	ID    string
	Label string
	Props []GraphProp
}

// GraphEdge is an edge of a SubGraph with its properties.
type GraphEdge struct {
	Edge  Edge
	Props []GraphProp
}

// SubGraph is the TinkerGraph-like result of cap() over subgraph(key).
type SubGraph struct {
	Vertices []*GraphVertex
	Edges    []*GraphEdge
}

func stepSubgraph(x *exec, s *Step, in []*trav) ([]*trav, error) {
	keys := stringArgs(s.Args)
	if len(keys) != 1 {
		return nil, invalidArg("subgraph() takes a side-effect key")
	}

	acc, ok := x.side[keys[0]].(*subgraphAcc)
	if !ok {
		acc = &subgraphAcc{nodeIdx: map[string]bool{}, relIdx: map[string]bool{}}
		x.side[keys[0]] = acc
	}

	for _, t := range in {
		r, isEdge := t.v.(*opencypher.Rel)
		if !isEdge {
			return nil, malformed("The incoming object of subgraph() must be an edge")
		}

		acc.add(x.st, r)
	}

	return in, nil
}

func (a *subgraphAcc) add(st *opencypher.Store, r *opencypher.Rel) {
	for _, id := range []string{r.Start, r.End} {
		if n := st.Node(id); n != nil && !a.nodeIdx[id] {
			a.nodeIdx[id] = true
			a.nodes = append(a.nodes, n)
		}
	}

	if !a.relIdx[r.ID] {
		a.relIdx[r.ID] = true
		a.rels = append(a.rels, r)
	}
}

func (a *subgraphAcc) snapshot(st *opencypher.Store) *SubGraph {
	g := &SubGraph{}

	for _, n := range a.nodes {
		g.Vertices = append(g.Vertices, &GraphVertex{ID: n.ID, Label: vertexLabel(n), Props: graphProps(n)})
	}

	for _, r := range a.rels {
		g.Edges = append(g.Edges, &GraphEdge{Edge: *detachEdge(st, r), Props: graphProps(r)})
	}

	return g
}

func graphProps(owner any) []GraphProp {
	props, _ := elemProps(owner)
	_, isVertex := owner.(*opencypher.Node)

	var out []GraphProp

	for _, k := range sortedKeys(props) {
		vals := propValues(owner, k)
		if len(vals) == 0 {
			continue
		}

		gp := GraphProp{Key: k, Vals: vals}
		if isVertex {
			for range vals {
				gp.IDs = append(gp.IDs, (&Prop{Owner: owner, Key: k}).ID())
			}
		}

		out = append(out, gp)
	}

	return out
}

func containsGraph(v any) bool {
	switch t := v.(type) {
	case *SubGraph:
		return true
	case []any:
		return slices.ContainsFunc(t, containsGraph)
	case Set:
		return containsGraph([]any(t))
	case *Map:
		for i := range t.Len() {
			k, val := t.Entry(i)
			if containsGraph(k) || containsGraph(val) {
				return true
			}
		}
	}

	return false
}
