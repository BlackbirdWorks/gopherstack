package opencypher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

const (
	floatKey = "~f"
	mapKey   = "~m"
)

type nodeDump struct {
	Props  map[string]any `json:"props"`
	ID     string         `json:"id"`
	Labels []string       `json:"labels"`
	Seq    int            `json:"seq"`
}

type relDump struct {
	Props map[string]any `json:"props"`
	ID    string         `json:"id"`
	Type  string         `json:"type"`
	Start string         `json:"start"`
	End   string         `json:"end"`
	Seq   int            `json:"seq"`
}

type graphDump struct {
	Nodes []nodeDump `json:"nodes"`
	Rels  []relDump  `json:"rels"`
	Seq   int        `json:"seq"`
}

// Export serialises the graph to JSON, preserving integer/float distinctions.
func (g *Graph) Export(ctx context.Context) ([]byte, error) {
	if err := g.acquire(ctx); err != nil {
		return nil, err
	}
	defer g.release()

	d := graphDump{Seq: g.seq, Nodes: []nodeDump{}, Rels: []relDump{}}

	for _, n := range g.allNodes() {
		d.Nodes = append(d.Nodes, nodeDump{ID: n.ID, Labels: n.Labels, Props: encodeProps(n.Props), Seq: n.seq})
	}

	rels := make([]*Rel, 0, len(g.rels))
	for _, r := range g.rels {
		rels = append(rels, r)
	}

	sort.Slice(rels, func(i, j int) bool { return rels[i].seq < rels[j].seq })

	for _, r := range rels {
		d.Rels = append(d.Rels, relDump{
			ID: r.ID, Type: r.Type, Start: r.Start, End: r.End, Props: encodeProps(r.Props), Seq: r.seq,
		})
	}

	b, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("encode graph: %w", err)
	}

	return b, nil
}

// Import replaces the graph contents with a dump produced by Export.
func (g *Graph) Import(ctx context.Context, data []byte) error {
	var d graphDump
	if err := decodeUseNumber(data, &d); err != nil {
		return fmt.Errorf("decode graph: %w", err)
	}

	if err := g.acquire(ctx); err != nil {
		return err
	}
	defer g.release()

	g.reset()
	g.seq = d.Seq

	for _, n := range d.Nodes {
		g.nodes[n.ID] = &Node{ID: n.ID, Labels: n.Labels, Props: decodeProps(n.Props), seq: n.Seq}
	}

	for _, r := range d.Rels {
		if g.nodes[r.Start] == nil || g.nodes[r.End] == nil {
			continue
		}

		rel := &Rel{ID: r.ID, Type: r.Type, Start: r.Start, End: r.End, Props: decodeProps(r.Props), seq: r.Seq}
		g.rels[r.ID] = rel
		g.out[r.Start] = append(g.out[r.Start], rel)
		g.in[r.End] = append(g.in[r.End], rel)
	}

	return nil
}

func decodeUseNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	return dec.Decode(v)
}

func encodeProps(p map[string]any) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = encodeValue(v)
	}

	return out
}

func encodeValue(v any) any {
	switch t := v.(type) {
	case float64:
		return map[string]any{floatKey: strconv.FormatFloat(t, 'g', -1, 64)}
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = encodeValue(e)
		}

		return out
	case map[string]any:
		return map[string]any{mapKey: encodeProps(t)}
	default:
		return v
	}
}

func decodeProps(p map[string]any) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = decodeValue(v)
	}

	return out
}

func decodeValue(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}

		f, _ := t.Float64()

		return f
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = decodeValue(e)
		}

		return out
	case map[string]any:
		return decodeTagged(t)
	default:
		return v
	}
}

func decodeTagged(m map[string]any) any {
	if s, ok := m[floatKey].(string); ok && len(m) == 1 {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return math.NaN()
		}

		return f
	}

	if inner, ok := m[mapKey].(map[string]any); ok && len(m) == 1 {
		return decodeProps(inner)
	}

	return decodeProps(m)
}
