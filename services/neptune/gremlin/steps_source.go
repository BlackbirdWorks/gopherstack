package gremlin

import (
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func regSource(m map[string]stepFn) {
	m["V"] = flatStep(stepV)
	m["E"] = flatStep(stepE)
	m["inject"] = flatStep(func(_ *exec, s *Step, _ *trav) ([]any, error) {
		out := make([]any, len(s.Args))
		for i, a := range s.Args {
			out[i] = normalizeValue(a)
		}

		return out, nil
	})
}

func idArgs(args []any) []string {
	var ids []string

	for _, a := range flattenArgs(args) {
		switch t := a.(type) {
		case VertexRef:
			ids = append(ids, t.ID)
		case nil:
		default:
			ids = append(ids, idString(a))
		}
	}

	return ids
}

func stepV(x *exec, s *Step, _ *trav) ([]any, error) {
	ids := idArgs(s.Args)
	if len(ids) == 0 {
		nodes := x.st.Nodes()
		out := make([]any, len(nodes))

		for i, n := range nodes {
			out[i] = n
		}

		return out, nil
	}

	var out []any

	for _, id := range ids {
		if n := x.st.Node(id); n != nil {
			out = append(out, n)
		}
	}

	return out, nil
}

func stepE(x *exec, s *Step, _ *trav) ([]any, error) {
	ids := idArgs(s.Args)
	if len(ids) == 0 {
		rels := x.st.Rels()
		out := make([]any, len(rels))

		for i, r := range rels {
			out[i] = r
		}

		return out, nil
	}

	var out []any

	for _, id := range ids {
		if r := x.st.Rel(id); r != nil {
			out = append(out, r)
		}
	}

	return out, nil
}

// normalizeValue folds numeric types to the stored int64/float64 and lists to []any.
func normalizeValue(v any) any {
	switch t := v.(type) {
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = normalizeValue(e)
		}

		return out
	case Set:
		return normalizeValue([]any(t))
	}

	return normalizeNumber(v)
}

func asNode(v any) (*opencypher.Node, bool) {
	n, ok := v.(*opencypher.Node)

	return n, ok
}
