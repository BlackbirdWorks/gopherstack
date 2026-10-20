package bolt

// Structure tags.
const (
	tagNode       byte = 0x4E
	tagRel        byte = 0x52
	tagUnboundRel byte = 0x72
	tagPath       byte = 0x50
)

// Node is a Bolt graph node. ElementID is sent from protocol 5.0.
type Node struct {
	Props     map[string]any
	ElementID string
	Labels    []string
	ID        int64
}

// Relationship is a Bolt relationship with both endpoints.
type Relationship struct {
	Props          map[string]any
	ElementID      string
	Type           string
	StartElementID string
	EndElementID   string
	ID             int64
	StartID        int64
	EndID          int64
}

// UnboundRelationship is a relationship inside a Path.
type UnboundRelationship struct {
	Props     map[string]any
	ElementID string
	Type      string
	ID        int64
}

// Path is an alternating walk; Indices holds (rel index, node index) pairs, relationship indices 1-based and
// negative when traversed backwards.
type Path struct {
	Nodes   []Node
	Rels    []UnboundRelationship
	Indices []int64
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}

	return m
}

func (e *Encoder) nodeFields(n Node) []any {
	f := []any{n.ID, n.Labels, orEmpty(n.Props)}
	if e.version.Major >= 5 { //nolint:mnd // element ids arrive in 5.0
		f = append(f, n.ElementID)
	}

	if n.Labels == nil {
		f[1] = []string{}
	}

	return f
}

func (e *Encoder) relFields(r Relationship) []any {
	f := []any{r.ID, r.StartID, r.EndID, r.Type, orEmpty(r.Props)}
	if e.version.Major >= 5 { //nolint:mnd // element ids arrive in 5.0
		f = append(f, r.ElementID, r.StartElementID, r.EndElementID)
	}

	return f
}

func (e *Encoder) unboundFields(r UnboundRelationship) []any {
	f := []any{r.ID, r.Type, orEmpty(r.Props)}
	if e.version.Major >= 5 { //nolint:mnd // element ids arrive in 5.0
		f = append(f, r.ElementID)
	}

	return f
}

func pathNodes(p Path) []any {
	out := make([]any, len(p.Nodes))
	for i, n := range p.Nodes {
		out[i] = n
	}

	return out
}

func pathRels(p Path, _ *Encoder) []any {
	out := make([]any, len(p.Rels))
	for i, r := range p.Rels {
		out[i] = r
	}

	return out
}
