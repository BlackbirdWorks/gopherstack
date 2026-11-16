package opencypher

import (
	"fmt"
	"reflect"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/services/neptune/streamlog"
)

const (
	typeVertexLabel = "vl"
	typeVertexProp  = "vp"
	typeEdge        = "e"
	typeEdgeProp    = "ep"
	keyLabel        = "label"
	dataString      = "String"
)

// Stream returns the graph's Neptune Streams commit log.
func (g *Graph) Stream() *streamlog.Log { return g.stream }

func (g *Graph) recordCommit(before snapshot) {
	if !g.stream.Enabled() {
		return
	}

	g.stream.Commit(diffGraph(before, g.nodes, g.rels))
}

func diffGraph(before snapshot, nodes map[string]*Node, rels map[string]*Rel) []streamlog.Change {
	out := make([]streamlog.Change, 0, len(before.nodes)+len(nodes))

	for _, r := range sortedRels(before.rels) {
		if rels[r.ID] == nil {
			out = append(out, relChanges(streamlog.OpRemove, r)...)
		}
	}

	for _, n := range sortedNodes(before.nodes) {
		if nodes[n.ID] == nil {
			out = append(out, nodeChanges(streamlog.OpRemove, n)...)
		}
	}

	for _, n := range sortedNodes(nodes) {
		if old := before.nodes[n.ID]; old != nil {
			out = append(out, nodeUpdate(old, n)...)
		}
	}

	for _, r := range sortedRels(rels) {
		if old := before.rels[r.ID]; old != nil {
			out = append(out, propUpdate(typeEdgeProp, r.ID, old.Props, r.Props)...)
		}
	}

	for _, n := range sortedNodes(nodes) {
		if before.nodes[n.ID] == nil {
			out = append(out, nodeChanges(streamlog.OpAdd, n)...)
		}
	}

	for _, r := range sortedRels(rels) {
		if before.rels[r.ID] == nil {
			out = append(out, relChanges(streamlog.OpAdd, r)...)
		}
	}

	return out
}

func sortedNodes(m map[string]*Node) []*Node {
	out := make([]*Node, 0, len(m))
	for _, n := range m {
		out = append(out, n)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })

	return out
}

func sortedRels(m map[string]*Rel) []*Rel {
	out := make([]*Rel, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })

	return out
}

func labelChange(op, id string, label string) streamlog.Change {
	return streamlog.Change{Op: op, Data: streamlog.Data{
		ID: id, Type: typeVertexLabel, Key: keyLabel, Value: &streamlog.Value{Value: label, DataType: dataString},
	}}
}

func nodeChanges(op string, n *Node) []streamlog.Change {
	out := make([]streamlog.Change, 0, len(n.Labels)+len(n.Props))

	if op == streamlog.OpAdd {
		for _, l := range n.Labels {
			out = append(out, labelChange(op, n.ID, l))
		}
	}

	out = append(out, propChanges(op, typeVertexProp, n.ID, n.Props)...)

	if op == streamlog.OpRemove {
		for _, l := range n.Labels {
			out = append(out, labelChange(op, n.ID, l))
		}
	}

	return out
}

func relChanges(op string, r *Rel) []streamlog.Change {
	edge := streamlog.Change{Op: op, Data: streamlog.Data{
		ID: r.ID, Type: typeEdge, Key: keyLabel, From: r.Start, To: r.End,
		Value: &streamlog.Value{Value: r.Type, DataType: dataString},
	}}
	props := propChanges(op, typeEdgeProp, r.ID, r.Props)

	if op == streamlog.OpAdd {
		return append([]streamlog.Change{edge}, props...)
	}

	return append(props, edge)
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func propChanges(op, typ, id string, props map[string]any) []streamlog.Change {
	out := make([]streamlog.Change, 0, len(props))

	for _, k := range sortedKeys(props) {
		out = append(out, valueChanges(op, typ, id, k, props[k])...)
	}

	return out
}

func valueChanges(op, typ, id, key string, v any) []streamlog.Change {
	vals := []any{v}
	if l, ok := v.([]any); ok {
		vals = l
	}

	out := make([]streamlog.Change, 0, len(vals))

	for _, e := range vals {
		val, dt := streamValue(e)
		out = append(out, streamlog.Change{Op: op, Data: streamlog.Data{
			ID: id, Type: typ, Key: key, Value: &streamlog.Value{Value: val, DataType: dt},
		}})
	}

	return out
}

func streamValue(v any) (any, string) {
	switch t := v.(type) {
	case string:
		return t, dataString
	case bool:
		return t, "Boolean"
	case int:
		return t, "Int"
	case int64:
		return t, "Long"
	case float64:
		return t, "Double"
	default:
		return fmt.Sprint(t), dataString
	}
}

func nodeUpdate(old, cur *Node) []streamlog.Change {
	var out []streamlog.Change

	for _, l := range old.Labels {
		if !slices.Contains(cur.Labels, l) {
			out = append(out, labelChange(streamlog.OpRemove, cur.ID, l))
		}
	}

	out = append(out, propUpdate(typeVertexProp, cur.ID, old.Props, cur.Props)...)

	for _, l := range cur.Labels {
		if !slices.Contains(old.Labels, l) {
			out = append(out, labelChange(streamlog.OpAdd, cur.ID, l))
		}
	}

	return out
}

func propUpdate(typ, id string, old, cur map[string]any) []streamlog.Change {
	var removes, adds []streamlog.Change

	for _, k := range sortedKeys(old) {
		nv, ok := cur[k]
		if ok && reflect.DeepEqual(old[k], nv) {
			continue
		}

		removes = append(removes, valueChanges(streamlog.OpRemove, typ, id, k, old[k])...)
	}

	for _, k := range sortedKeys(cur) {
		ov, ok := old[k]
		if ok && reflect.DeepEqual(ov, cur[k]) {
			continue
		}

		adds = append(adds, valueChanges(streamlog.OpAdd, typ, id, k, cur[k])...)
	}

	return append(removes, adds...)
}
