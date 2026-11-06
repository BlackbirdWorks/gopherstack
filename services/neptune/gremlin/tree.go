package gremlin

import "github.com/blackbirdworks/gopherstack/services/neptune/opencypher"

func regTree(m map[string]stepFn) {
	m["tree"] = stepTree
}

// Tree is a Gremlin Tree: an insertion-ordered map from a path element to the subtree below it.
type Tree struct {
	idx  map[string]int
	keys []any
	kids []*Tree
}

// NewTree returns an empty Tree.
func NewTree() *Tree { return &Tree{idx: map[string]int{}} }

// Len is the number of children.
func (t *Tree) Len() int { return len(t.keys) }

// Entry returns the i-th key and its subtree.
func (t *Tree) Entry(i int) (any, *Tree) { return t.keys[i], t.kids[i] }

func (t *Tree) child(k any) *Tree {
	ks := keyOf(k)
	if i, ok := t.idx[ks]; ok {
		return t.kids[i]
	}

	c := NewTree()
	t.idx[ks] = len(t.keys)
	t.keys = append(t.keys, k)
	t.kids = append(t.kids, c)

	return c
}

// asMap renders the tree as nested Maps, the shape GraphBinary and keyOf use.
func (t *Tree) asMap() *Map {
	m := NewMap()
	for i, k := range t.keys {
		m.Put(k, t.kids[i].asMap())
	}

	return m
}

func (x *exec) addPath(root *Tree, by []*Step, t *trav) error {
	var rev []*pathNode

	for n := t.path; n != nil; n = n.parent {
		rev = append(rev, n)
	}

	cur := root

	for depth := range rev {
		n := rev[len(rev)-1-depth]

		k, ok, err := x.applyBy(by, depth, &trav{v: n.obj, sack: t.sack, path: n, loops: t.loops})
		if err != nil {
			return err
		}

		if !ok {
			return malformed("The provided traverser does not map to a value")
		}

		cur = cur.child(k)
	}

	return nil
}

func stepTree(x *exec, s *Step, in []*trav) ([]*trav, error) {
	keys := stringArgs(s.Args)
	if len(keys) > 1 {
		return nil, invalidArg("tree() takes at most one side-effect key")
	}

	tree := NewTree()

	if len(keys) == 1 {
		if existing, ok := x.side[keys[0]].(*Tree); ok {
			tree = existing
		} else {
			x.side[keys[0]] = tree
		}
	}

	by := byMods(s)

	for _, t := range in {
		if err := x.addPath(tree, by, t); err != nil {
			return nil, err
		}
	}

	if len(keys) == 1 {
		return in, nil
	}

	return []*trav{rootTrav()[0].next(tree)}, nil
}

func detachTree(st *opencypher.Store, t *Tree) *Tree {
	out := NewTree()
	for i, k := range t.keys {
		c := out.child(detach(st, k))
		*c = *detachTree(st, t.kids[i])
	}

	return out
}
