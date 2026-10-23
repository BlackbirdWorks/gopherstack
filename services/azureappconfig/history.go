package azureappconfig

import (
	"time"
)

// asOf reconstructs the live key-values at time at.
func (s *store) asOf(at time.Time) []KeyValue {
	state := map[string]histEntry{}

	for _, e := range s.History {
		if e.At.After(at) {
			break
		}

		state[kvID(e.KV.Key, e.KV.Label)] = e
	}

	out := make([]KeyValue, 0, len(state))

	for _, e := range state {
		if !e.Deleted {
			out = append(out, cloneKV(e.KV))
		}
	}

	sortKVs(out)

	return out
}

func (s *store) current() []KeyValue {
	out := make([]KeyValue, 0, len(s.KVs))

	for _, kv := range s.KVs {
		out = append(out, cloneKV(*kv))
	}

	sortKVs(out)

	return out
}

func (s *store) liveAt(at, now time.Time) []KeyValue {
	if at.IsZero() || !at.Before(now) {
		return s.current()
	}

	return s.asOf(at)
}

// ListKeyValues returns the key-values matching sel, as of at when non-zero.
func (b *InMemoryBackend) ListKeyValues(storeName string, sel Selector, at time.Time) ([]KeyValue, error) {
	b.mu.RLock("ListKeyValues")
	defer b.mu.RUnlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return nil, err
	}

	return pick(s.liveAt(at, b.now()), sel), nil
}

// AllKeyValues returns every current key-value of the store.
func (b *InMemoryBackend) AllKeyValues(storeName string) ([]KeyValue, error) {
	return b.ListKeyValues(storeName, Selector{}, time.Time{})
}

func pick(kvs []KeyValue, sel Selector) []KeyValue {
	out := make([]KeyValue, 0, len(kvs))

	for _, kv := range kvs {
		if sel.matches(kv) {
			out = append(out, kv)
		}
	}

	return out
}

// ListRevisions returns every recorded state of the key-values matching sel up to at (zero: all).
func (b *InMemoryBackend) ListRevisions(storeName string, sel Selector, at time.Time) ([]KeyValue, error) {
	b.mu.RLock("ListRevisions")
	defer b.mu.RUnlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return nil, err
	}

	var out []KeyValue

	for _, e := range s.History {
		if e.Deleted || (!at.IsZero() && e.At.After(at)) || !sel.matches(e.KV) {
			continue
		}

		out = append(out, cloneKV(e.KV))
	}

	return out, nil
}

// ListKeyNames returns the distinct keys matching the name filter.
func (b *InMemoryBackend) ListKeyNames(storeName, name string, at time.Time) ([]string, error) {
	sel, err := NewSelector(name, "", nil)
	if err != nil {
		return nil, err
	}

	kvs, err := b.ListKeyValues(storeName, sel, at)
	if err != nil {
		return nil, err
	}

	var out []string

	for _, kv := range kvs {
		if len(out) == 0 || out[len(out)-1] != kv.Key {
			out = append(out, kv.Key)
		}
	}

	return out, nil
}

// ListLabelNames returns the distinct non-null labels matching the name filter.
func (b *InMemoryBackend) ListLabelNames(storeName, name string, at time.Time) ([]string, error) {
	f, err := parseFilter(name)
	if err != nil {
		return nil, err
	}

	kvs, err := b.ListKeyValues(storeName, Selector{}, at)
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}

	var out []string

	for _, kv := range kvs {
		if kv.Label != "" && !seen[kv.Label] && f.matches(kv.Label) {
			seen[kv.Label] = true
			out = append(out, kv.Label)
		}
	}

	sortStrings(out)

	return out, nil
}
