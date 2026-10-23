package azureappconfig

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"
)

// SnapshotSpec describes a snapshot to create.
type SnapshotSpec struct {
	Tags        map[string]string
	Composition string
	Filters     []Filter
	Retention   int64
}

func validSnapshotName(name string) bool {
	if name == "" || len(name) > maxKeyLen/40 {
		return false
	}

	return !strings.ContainsAny(name, "/%\x00")
}

func normalizeSpec(spec SnapshotSpec) (SnapshotSpec, error) {
	if len(spec.Filters) == 0 || len(spec.Filters) > maxSnapshotFilt {
		return spec, fmt.Errorf("%w: a snapshot needs 1 to %d filters", ErrInvalidArgument, maxSnapshotFilt)
	}

	switch spec.Composition {
	case "":
		spec.Composition = compositionKey
	case compositionKey, compositionKeyLabel:
	default:
		return spec, fmt.Errorf("%w: unknown composition_type %q", ErrInvalidArgument, spec.Composition)
	}

	if spec.Retention == 0 {
		spec.Retention = defaultRetention
	}

	if spec.Retention < minRetention || spec.Retention > maxRetention {
		return spec, fmt.Errorf("%w: retention_period must be %d to %d seconds",
			ErrInvalidArgument, minRetention, maxRetention)
	}

	return spec, nil
}

func snapshotItems(live []KeyValue, spec SnapshotSpec) ([]KeyValue, error) {
	chosen := map[string]KeyValue{}

	for _, f := range spec.Filters {
		sel, err := NewSelector(f.Key, f.Label, nil)
		if err != nil {
			return nil, err
		}

		for _, kv := range pick(live, sel) {
			id := kv.Key
			if spec.Composition == compositionKeyLabel {
				id = kvID(kv.Key, kv.Label)
			}

			chosen[id] = kv
		}
	}

	out := make([]KeyValue, 0, len(chosen))
	for _, kv := range chosen {
		out = append(out, kv)
	}

	sortKVs(out)

	return out, nil
}

// CreateSnapshot captures the key-values selected by the spec; the snapshot is ready immediately.
func (b *InMemoryBackend) CreateSnapshot(storeName, name string, spec SnapshotSpec) (Snapshot, error) {
	if !validSnapshotName(name) {
		return Snapshot{}, fmt.Errorf("%w: invalid snapshot name", ErrInvalidArgument)
	}

	spec, err := normalizeSpec(spec)
	if err != nil {
		return Snapshot{}, err
	}

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return Snapshot{}, err
	}

	if _, ok := s.snapshot(name, b.now()); ok {
		return Snapshot{}, ErrSnapshotExists
	}

	items, err := snapshotItems(s.current(), spec)
	if err != nil {
		return Snapshot{}, err
	}

	snap := &Snapshot{
		Name: name, Status: snapshotReady, Composition: spec.Composition, Filters: spec.Filters,
		Retention: spec.Retention, Tags: maps.Clone(spec.Tags), Items: items, ETag: randomText(etagBytes),
		Created: b.now().UTC(),
	}

	for _, kv := range items {
		snap.Size += kvBytes(kv)
	}

	s.Snapshots[name] = snap
	s.bump()

	return *snap, nil
}

func (s *store) snapshot(name string, now time.Time) (*Snapshot, bool) {
	snap, ok := s.Snapshots[name]
	if !ok {
		return nil, false
	}

	if snap.Expires != nil && now.After(*snap.Expires) {
		delete(s.Snapshots, name)

		return nil, false
	}

	return snap, true
}

// GetSnapshot returns a snapshot's metadata and items.
func (b *InMemoryBackend) GetSnapshot(storeName, name string) (Snapshot, error) {
	b.mu.Lock("GetSnapshot")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return Snapshot{}, err
	}

	snap, ok := s.snapshot(name, b.now())
	if !ok {
		return Snapshot{}, ErrSnapshotNotFound
	}

	return *snap, nil
}

// SetSnapshotStatus archives (ready -> archived) or recovers (archived -> ready) a snapshot.
func (b *InMemoryBackend) SetSnapshotStatus(storeName, name, status string, p Precondition) (Snapshot, error) {
	if status != snapshotReady && status != snapshotArchived {
		return Snapshot{}, fmt.Errorf(
			"%w: status must be %q or %q",
			ErrInvalidArgument,
			snapshotReady,
			snapshotArchived,
		)
	}

	b.mu.Lock("SetSnapshotStatus")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return Snapshot{}, err
	}

	snap, ok := s.snapshot(name, b.now())
	if !ok {
		return Snapshot{}, ErrSnapshotNotFound
	}

	if (p.IfMatch != "" && p.IfMatch != "*" && p.IfMatch != snap.ETag) ||
		(p.IfNoneMatch != "" && (p.IfNoneMatch == "*" || p.IfNoneMatch == snap.ETag)) {
		return Snapshot{}, ErrPrecondition
	}

	if snap.Status == status {
		return Snapshot{}, ErrSnapshotStatus
	}

	snap.Status, snap.ETag = status, randomText(etagBytes)

	if status == snapshotArchived {
		exp := b.now().UTC().Add(time.Duration(snap.Retention) * time.Second)
		snap.Expires = &exp
	} else {
		snap.Expires = nil
	}

	s.bump()

	return *snap, nil
}

// ListSnapshots returns snapshots whose name matches the filter and whose status is in statuses.
func (b *InMemoryBackend) ListSnapshots(storeName, nameFilter string, statuses []string) ([]Snapshot, error) {
	names, err := parseFilter(nameFilter)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ListSnapshots")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return nil, err
	}

	var out []Snapshot

	for name := range s.Snapshots {
		snap, ok := s.snapshot(name, b.now())
		if ok && names.matches(snap.Name) && statusAllowed(snap.Status, statuses) {
			out = append(out, *snap)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func statusAllowed(status string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}

	for _, a := range allowed {
		if strings.EqualFold(a, status) {
			return true
		}
	}

	return false
}

// SnapshotKeyValues returns the items of a snapshot matching sel.
func (b *InMemoryBackend) SnapshotKeyValues(storeName, name string, sel Selector) ([]KeyValue, error) {
	snap, err := b.GetSnapshot(storeName, name)
	if err != nil {
		return nil, err
	}

	out := make([]KeyValue, 0, len(snap.Items))
	for _, kv := range pick(snap.Items, sel) {
		out = append(out, cloneKV(kv))
	}

	return out, nil
}
