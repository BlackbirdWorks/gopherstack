package opencypher

import (
	"sort"
	"strings"
)

// NodeStructure is a distinct property-key and outgoing-edge-label signature and how many nodes have it.
type NodeStructure struct {
	NodeProperties             []string `json:"nodeProperties"`
	DistinctOutgoingEdgeLabels []string `json:"distinctOutgoingEdgeLabels"`
	Count                      int      `json:"count"`
}

// EdgeStructure is a distinct edge property-key signature and how many edges have it.
type EdgeStructure struct {
	EdgeProperties []string `json:"edgeProperties"`
	Count          int      `json:"count"`
}

// Summary is the Neptune graph summary payload for a property graph.
type Summary struct {
	NodeLabels              []string         `json:"nodeLabels"`
	EdgeLabels              []string         `json:"edgeLabels"`
	NodeProperties          []map[string]int `json:"nodeProperties"`
	EdgeProperties          []map[string]int `json:"edgeProperties"`
	NodeStructures          []NodeStructure  `json:"nodeStructures"`
	EdgeStructures          []EdgeStructure  `json:"edgeStructures"`
	NumNodes                int              `json:"numNodes"`
	NumEdges                int              `json:"numEdges"`
	NumNodeLabels           int              `json:"numNodeLabels"`
	NumEdgeLabels           int              `json:"numEdgeLabels"`
	NumNodeProperties       int              `json:"numNodeProperties"`
	NumEdgeProperties       int              `json:"numEdgeProperties"`
	TotalNodePropertyValues int              `json:"totalNodePropertyValues"`
	TotalEdgePropertyValues int              `json:"totalEdgePropertyValues"`
}

// Summary describes the graph's labels, property keys and structures for the graph summary endpoint.
func (g *Graph) Summary() Summary {
	g.sem <- struct{}{}
	defer func() { <-g.sem }()

	nodeLabels, edgeLabels := map[string]struct{}{}, map[string]struct{}{}
	nodeProps, edgeProps := map[string]int{}, map[string]int{}
	nodeSigs := map[string]*NodeStructure{}
	edgeSigs := map[string]*EdgeStructure{}

	for _, n := range g.nodes {
		for _, l := range n.Labels {
			nodeLabels[l] = struct{}{}
		}

		keys := sortedKeys(n.Props)
		for _, k := range keys {
			nodeProps[k]++
		}

		out := map[string]struct{}{}
		for _, r := range g.out[n.ID] {
			out[r.Type] = struct{}{}
		}

		outs := sortedSet(out)
		sig := strings.Join(keys, "\x00") + "\x01" + strings.Join(outs, "\x00")

		if nodeSigs[sig] == nil {
			nodeSigs[sig] = &NodeStructure{NodeProperties: keys, DistinctOutgoingEdgeLabels: outs}
		}

		nodeSigs[sig].Count++
	}

	for _, r := range g.rels {
		edgeLabels[r.Type] = struct{}{}

		keys := sortedKeys(r.Props)
		for _, k := range keys {
			edgeProps[k]++
		}

		sig := strings.Join(keys, "\x00")
		if edgeSigs[sig] == nil {
			edgeSigs[sig] = &EdgeStructure{EdgeProperties: keys}
		}

		edgeSigs[sig].Count++
	}

	s := Summary{
		NumNodes: len(g.nodes), NumEdges: len(g.rels),
		NodeLabels: sortedSet(nodeLabels), EdgeLabels: sortedSet(edgeLabels),
		NodeProperties: countList(nodeProps), EdgeProperties: countList(edgeProps),
		NodeStructures: []NodeStructure{}, EdgeStructures: []EdgeStructure{},
	}
	s.NumNodeLabels, s.NumEdgeLabels = len(s.NodeLabels), len(s.EdgeLabels)
	s.NumNodeProperties, s.NumEdgeProperties = len(nodeProps), len(edgeProps)

	for _, c := range nodeProps {
		s.TotalNodePropertyValues += c
	}

	for _, c := range edgeProps {
		s.TotalEdgePropertyValues += c
	}

	for _, v := range nodeSigs {
		s.NodeStructures = append(s.NodeStructures, *v)
	}

	for _, v := range edgeSigs {
		s.EdgeStructures = append(s.EdgeStructures, *v)
	}

	sort.Slice(s.NodeStructures, func(i, j int) bool {
		return structLess(
			s.NodeStructures[i].Count,
			s.NodeStructures[j].Count,
			strings.Join(s.NodeStructures[i].NodeProperties, ","),
			strings.Join(s.NodeStructures[j].NodeProperties, ","),
		)
	})
	sort.Slice(s.EdgeStructures, func(i, j int) bool {
		return structLess(
			s.EdgeStructures[i].Count,
			s.EdgeStructures[j].Count,
			strings.Join(s.EdgeStructures[i].EdgeProperties, ","),
			strings.Join(s.EdgeStructures[j].EdgeProperties, ","),
		)
	})

	return s
}

func structLess(ci, cj int, ki, kj string) bool {
	if ci != cj {
		return ci > cj
	}

	return ki < kj
}

func sortedSet(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

func countList(m map[string]int) []map[string]int {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}

		return keys[i] < keys[j]
	})

	out := make([]map[string]int, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]int{k: m[k]})
	}

	return out
}
