package azuregraph

import "slices"

// store is an insertion-ordered map of directory objects keyed by lowercase ID.
type store[T any] struct {
	items map[string]*T
	order []string
}

func newStore[T any]() store[T] { return store[T]{items: map[string]*T{}} }

func (s *store[T]) get(id string) (*T, bool) {
	v, ok := s.items[id]

	return v, ok
}

func (s *store[T]) put(id string, v *T) {
	if _, ok := s.items[id]; !ok {
		s.order = append(s.order, id)
	}

	s.items[id] = v
}

func (s *store[T]) del(id string) {
	if _, ok := s.items[id]; !ok {
		return
	}

	delete(s.items, id)

	if i := slices.Index(s.order, id); i >= 0 {
		s.order = slices.Delete(s.order, i, i+1)
	}
}

func (s *store[T]) all() []*T {
	out := make([]*T, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.items[id])
	}

	return out
}

func (s *store[T]) slice() []T {
	out := make([]T, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, *s.items[id])
	}

	return out
}

func (s *store[T]) reset() { *s = newStore[T]() }
