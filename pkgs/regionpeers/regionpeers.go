// Package regionpeers lets a single-region service handler serve extra regions
// by lazily building one sibling handler per non-home region.
package regionpeers

import (
	"encoding/json"
	"maps"
	"slices"
	"sync"
)

const snapshotKey = "regions"

// Set holds the lazily built per-region siblings of one home handler. A nil
// *Set is valid and means the service serves only its home region.
type Set[T any] struct {
	peers map[string]*T
	build func(region string) *T
	home  string
	mu    sync.Mutex
}

// New returns a Set whose siblings are made by build. Requests for home or an
// empty region are never routed to a sibling.
func New[T any](home string, build func(region string) *T) *Set[T] {
	return &Set[T]{home: home, build: build, peers: make(map[string]*T)}
}

// Get returns the sibling for region, creating it on first use. It returns nil
// when the home handler should serve the request.
func (s *Set[T]) Get(region string) *T {
	if s == nil || region == "" || region == s.home {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.peers[region]
	if !ok {
		p = s.build(region)
		s.peers[region] = p
	}

	return p
}

// All returns the siblings built so far, ordered by region, without removing them.
func (s *Set[T]) All() []*T {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*T, 0, len(s.peers))
	for _, r := range slices.Sorted(maps.Keys(s.peers)) {
		out = append(out, s.peers[r])
	}

	return out
}

// Drain removes and returns every sibling so the caller can reset or close them.
func (s *Set[T]) Drain() []*T {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*T, 0, len(s.peers))
	for _, r := range slices.Sorted(maps.Keys(s.peers)) {
		out = append(out, s.peers[r])
	}

	s.peers = make(map[string]*T)

	return out
}

// Snapshot returns base unchanged when no sibling exists, so single-region
// snapshots stay byte-identical; otherwise it adds a "regions" object.
func (s *Set[T]) Snapshot(base []byte, snap func(*T) []byte) []byte {
	if s == nil || base == nil {
		return base
	}

	s.mu.Lock()
	regions := make(map[string]json.RawMessage, len(s.peers))

	for r, p := range s.peers {
		if data := snap(p); data != nil {
			regions[r] = data
		}
	}
	s.mu.Unlock()

	if len(regions) == 0 {
		return base
	}

	doc := make(map[string]json.RawMessage)
	if err := json.Unmarshal(base, &doc); err != nil {
		return base
	}

	enc, err := json.Marshal(regions)
	if err != nil {
		return base
	}

	doc[snapshotKey] = enc

	out, err := json.Marshal(doc)
	if err != nil {
		return base
	}

	return out
}

// Restore rebuilds the siblings from the "regions" object in data, dropping
// any that exist now. Data without that key (older snapshots) just clears them.
func (s *Set[T]) Restore(data []byte, restore func(*T, []byte) error, closeFn func(*T)) error {
	if s == nil {
		return nil
	}

	for _, p := range s.Drain() {
		closeFn(p)
	}

	var doc struct {
		Regions map[string]json.RawMessage `json:"regions"`
	}

	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}

	for _, r := range slices.Sorted(maps.Keys(doc.Regions)) {
		if p := s.Get(r); p != nil {
			if err := restore(p, doc.Regions[r]); err != nil {
				return err
			}
		}
	}

	return nil
}
