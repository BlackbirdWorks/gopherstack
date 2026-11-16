package opencypher

import "fmt"

// Storage counts the graph's index entries and dictionary terms.
type Storage struct {
	Entries   int
	Terms     int
	TermBytes int
	Blobs     int
	BlobBytes int
}

// BlobThreshold is the term size from which a dictionary entry counts as a blob.
const BlobThreshold = 1024

// Storage describes the graph as Neptune's quad indexes and term dictionary would hold it: one entry per label,
// property value and edge.
func (g *Graph) Storage() Storage {
	g.sem <- struct{}{}
	defer func() { <-g.sem }()

	terms := map[string]struct{}{}

	var st Storage

	note := func(v any) {
		s := fmt.Sprint(v)
		if _, seen := terms[s]; seen {
			return
		}

		terms[s] = struct{}{}

		if len(s) >= BlobThreshold {
			st.Blobs++
			st.BlobBytes += len(s)

			return
		}

		st.Terms++
		st.TermBytes += len(s)
	}

	for _, n := range g.nodes {
		note(n.ID)

		for _, l := range n.Labels {
			st.Entries++

			note(l)
		}

		for k, v := range n.Props {
			st.Entries++

			note(k)
			note(v)
		}
	}

	for _, r := range g.rels {
		st.Entries++

		note(r.ID)
		note(r.Type)

		for k, v := range r.Props {
			st.Entries++

			note(k)
			note(v)
		}
	}

	return st
}

// RebuildIndexes recreates the adjacency indexes from the relationships in creation order.
func (g *Graph) RebuildIndexes() {
	g.sem <- struct{}{}
	defer func() { <-g.sem }()

	g.out, g.in = make(map[string][]*Rel), make(map[string][]*Rel)

	for _, r := range sortedRels(g.rels) {
		g.out[r.Start] = append(g.out[r.Start], r)
		g.in[r.End] = append(g.in[r.End], r)
	}
}
