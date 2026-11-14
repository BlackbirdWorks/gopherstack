package sparql

// Storage counts the store's quads and dictionary terms.
type Storage struct {
	Entries   int
	Terms     int
	TermBytes int
	Blobs     int
	BlobBytes int
}

// BlobThreshold is the term size from which a dictionary entry counts as a blob.
const BlobThreshold = 1024

// Storage reports the quad count and the distinct terms a Neptune dictionary would hold.
func (s *Store) Storage() Storage {
	s.mu.RLock("Storage")
	defer s.mu.RUnlock()

	seen := map[Term]struct{}{}
	st := Storage{Entries: len(s.quads)}

	for q := range s.quads {
		for _, t := range [...]Term{q.S, q.P, q.O, q.G} {
			if _, ok := seen[t]; ok {
				continue
			}

			seen[t] = struct{}{}

			if len(t.Value) >= BlobThreshold {
				st.Blobs++
				st.BlobBytes += len(t.Value)

				continue
			}

			st.Terms++
			st.TermBytes += len(t.Value)
		}
	}

	return st
}

// RebuildIndexes recreates the subject, predicate, object and graph indexes from the quads.
func (s *Store) RebuildIndexes() {
	s.mu.Lock("RebuildIndexes")
	defer s.mu.Unlock()

	s.byS = make(map[Term]quadSet)
	s.byP = make(map[Term]quadSet)
	s.byO = make(map[Term]quadSet)
	s.byG = make(map[Term]quadSet)

	for q := range s.quads {
		indexAdd(s.byS, q.S, q)
		indexAdd(s.byP, q.P, q)
		indexAdd(s.byO, q.O, q)
		indexAdd(s.byG, q.G, q)
	}
}
