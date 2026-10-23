package gremlin

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func regNav(m map[string]stepFn) {
	for name, dir := range map[string]string{"out": dirOut, "in": dirIn, "both": dirBoth} {
		m[name] = flatStep(adjacentVertices(dir))
	}

	for name, dir := range map[string]string{"outE": dirOut, "inE": dirIn, "bothE": dirBoth} {
		m[name] = flatStep(incidentEdges(dir))
	}

	m["outV"] = flatStep(edgeEnd(dirOut))
	m["inV"] = flatStep(edgeEnd(dirIn))
	m["bothV"] = flatStep(edgeEnd(dirBoth))
	m["otherV"] = flatStep(otherVertex)
}

func edgesOf(x *exec, n *opencypher.Node, dir string, labels []string) []*opencypher.Rel {
	var rels []*opencypher.Rel

	if dir == dirOut || dir == dirBoth {
		rels = append(rels, x.st.OutRels(n.ID)...)
	}

	if dir == dirIn || dir == dirBoth {
		rels = append(rels, x.st.InRels(n.ID)...)
	}

	if len(labels) == 0 {
		return rels
	}

	out := rels[:0:0]

	for _, r := range rels {
		if slices.Contains(labels, r.Type) {
			out = append(out, r)
		}
	}

	return out
}

func vertexInput(s *Step, t *trav) (*opencypher.Node, error) {
	n, ok := asNode(t.v)
	if !ok {
		return nil, malformed("The %s() step can only be applied to vertices", s.Name)
	}

	return n, nil
}

func adjacentVertices(dir string) func(*exec, *Step, *trav) ([]any, error) {
	return func(x *exec, s *Step, t *trav) ([]any, error) {
		n, err := vertexInput(s, t)
		if err != nil {
			return nil, err
		}

		var out []any

		for _, r := range edgesOf(x, n, dir, stringArgs(s.Args)) {
			out = append(out, otherEnd(x, r, n, dir))
		}

		return out, nil
	}
}

func otherEnd(x *exec, r *opencypher.Rel, from *opencypher.Node, dir string) *opencypher.Node {
	if dir == dirOut || (dir == dirBoth && r.Start == from.ID) {
		return x.st.Node(r.End)
	}

	return x.st.Node(r.Start)
}

func incidentEdges(dir string) func(*exec, *Step, *trav) ([]any, error) {
	return func(x *exec, s *Step, t *trav) ([]any, error) {
		n, err := vertexInput(s, t)
		if err != nil {
			return nil, err
		}

		rels := edgesOf(x, n, dir, stringArgs(s.Args))
		out := make([]any, len(rels))

		for i, r := range rels {
			out[i] = r
		}

		return out, nil
	}
}

func edgeInput(s *Step, t *trav) (*opencypher.Rel, error) {
	r, ok := t.v.(*opencypher.Rel)
	if !ok {
		return nil, malformed("The %s() step can only be applied to edges", s.Name)
	}

	return r, nil
}

func edgeEnd(which string) func(*exec, *Step, *trav) ([]any, error) {
	return func(x *exec, s *Step, t *trav) ([]any, error) {
		r, err := edgeInput(s, t)
		if err != nil {
			return nil, err
		}

		switch which {
		case dirOut:
			return []any{x.st.Node(r.Start)}, nil
		case dirIn:
			return []any{x.st.Node(r.End)}, nil
		}

		return []any{x.st.Node(r.Start), x.st.Node(r.End)}, nil
	}
}

func otherVertex(x *exec, s *Step, t *trav) ([]any, error) {
	r, err := edgeInput(s, t)
	if err != nil {
		return nil, err
	}

	if t.path == nil || t.path.parent == nil {
		return nil, malformed("The path does not contain a vertex before the edge for otherV()")
	}

	prev, ok := asNode(t.path.parent.obj)
	if !ok {
		return nil, malformed("The path does not contain a vertex before the edge for otherV()")
	}

	if r.Start == prev.ID {
		return []any{x.st.Node(r.End)}, nil
	}

	return []any{x.st.Node(r.Start)}, nil
}
