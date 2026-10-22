package opencypher

import (
	"context"
	"sort"
)

// Store is exclusive access to a graph's entities, handed to other query languages (Gremlin) so they share
// the same data, locking and rollback as openCypher.
type Store struct{ g *Graph }

// Update runs fn with exclusive access; any error rolls every change back.
func (g *Graph) Update(ctx context.Context, fn func(*Store) error) error {
	if err := g.acquire(ctx); err != nil {
		return err
	}
	defer g.release()

	snap := g.snapshot()
	if err := fn(&Store{g: g}); err != nil {
		g.restore(snap)

		return err
	}

	return nil
}

// View runs fn with exclusive access for reads only.
func (g *Graph) View(ctx context.Context, fn func(*Store) error) error {
	if err := g.acquire(ctx); err != nil {
		return err
	}
	defer g.release()

	return fn(&Store{g: g})
}

// Nodes returns every node in creation order.
func (s *Store) Nodes() []*Node { return s.g.allNodes() }

// Rels returns every relationship in creation order.
func (s *Store) Rels() []*Rel {
	rs := make([]*Rel, 0, len(s.g.rels))
	for _, r := range s.g.rels {
		rs = append(rs, r)
	}

	sort.Slice(rs, func(i, j int) bool { return rs[i].seq < rs[j].seq })

	return rs
}

// Node returns the node with id, or nil.
func (s *Store) Node(id string) *Node { return s.g.nodes[id] }

// Rel returns the relationship with id, or nil.
func (s *Store) Rel(id string) *Rel { return s.g.rels[id] }

// OutRels returns the relationships leaving id.
func (s *Store) OutRels(id string) []*Rel { return s.g.out[id] }

// InRels returns the relationships entering id.
func (s *Store) InRels(id string) []*Rel { return s.g.in[id] }

// AddNode creates a node; an empty id generates one.
func (s *Store) AddNode(id string, labels []string, props map[string]any) (*Node, error) {
	return s.g.addNode(id, labels, props)
}

// AddRel creates a relationship; an empty id generates one.
func (s *Store) AddRel(id, typ string, from, to *Node, props map[string]any) (*Rel, error) {
	if id == "" {
		return s.g.addRel(typ, from, to, props), nil
	}

	if _, dup := s.g.rels[id]; dup {
		return nil, constraint("Edge with id %q already exists", id)
	}

	return s.g.addRelID(id, typ, from, to, props), nil
}

// DeleteRel removes a relationship.
func (s *Store) DeleteRel(r *Rel) {
	if s.g.rels[r.ID] == r {
		s.g.deleteRel(r)
	}
}

// DeleteNode removes a node and its relationships.
func (s *Store) DeleteNode(n *Node) {
	if s.g.nodes[n.ID] != n {
		return
	}

	edges := append(append([]*Rel(nil), s.g.out[n.ID]...), s.g.in[n.ID]...)
	for _, e := range edges {
		s.DeleteRel(e)
	}

	s.g.deleteNode(n)
}
