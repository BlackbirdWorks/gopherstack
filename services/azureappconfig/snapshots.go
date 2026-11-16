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

func normalizeSpec(spec SnapshotSpec, sku string) (SnapshotSpec, error) {
	maxRet, defRet := maxRetention, defaultRetention
	if sku == skuFree || sku == skuDeveloper {
		maxRet, defRet = freeMaxRetention, freeMaxRetention
	}

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
		spec.Retention = defRet
	}

	if spec.Retention < minRetention || spec.Retention > maxRet {
		return spec, fmt.Errorf("%w: retention_period must be %d to %d seconds",
			ErrInvalidArgument, minRetention, maxRet)
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

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	spec, err := normalizeSpec(spec, b.settingsLocked(storeName).SKU)
	if err != nil {
		return Snapshot{}, err
	}

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

	now := b.now().UTC()
	snap := &Snapshot{
		Name: name, Status: snapshotProvisioning, Composition: spec.Composition, Filters: spec.Filters,
		Retention: spec.Retention, Tags: maps.Clone(spec.Tags), Items: items, ETag: randomText(etagBytes),
		Created: now, ReadyAt: now.Add(snapshotProvisionDelay),
	}

	for _, kv := range items {
		snap.Size += kvBytes(kv)
	}

	if s.snapshotBytes()+snap.Size > storageLimit(b.settingsLocked(storeName).SKU) {
		snap.Error, snap.Items, snap.Size = "The allotted quota for snapshot creation has been surpassed.", nil, 0
	}

	s.Snapshots[name] = snap
	s.bump()

	return snap.view(now), nil
}

func (s *store) snapshotBytes() int64 {
	var n int64

	for _, sn := range s.Snapshots {
		n += sn.Size
	}

	return n
}

// settle moves a provisioning snapshot to ready (or failed) once its provisioning time has passed.
func (sn *Snapshot) settle(now time.Time) {
	if sn.Status != snapshotProvisioning || now.Before(sn.ReadyAt) {
		return
	}

	sn.Status = snapshotReady
	if sn.Error != "" {
		sn.Status = snapshotFailed
	}
}

// view is the client-visible copy: a snapshot that is still provisioning (or failed) exposes no items.
func (sn *Snapshot) view(now time.Time) Snapshot {
	sn.settle(now)

	out := *sn
	if sn.Status == snapshotProvisioning || sn.Status == snapshotFailed {
		out.Items, out.Size = nil, 0
	}

	return out
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

	return snap.view(b.now()), nil
}

// SnapshotOperation reports the provisioning operation of a snapshot: status Running, Succeeded or Failed.
func (b *InMemoryBackend) SnapshotOperation(storeName, name string) (string, string, error) {
	b.mu.Lock("SnapshotOperation")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return "", "", err
	}

	snap, ok := s.snapshot(name, b.now())
	if !ok {
		return "", "", ErrSnapshotNotFound
	}

	snap.settle(b.now())

	switch snap.Status {
	case snapshotProvisioning:
		return "Running", "", nil
	case snapshotFailed:
		return "Failed", snap.Error, nil
	default:
		return "Succeeded", "", nil
	}
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

	snap.settle(b.now())

	if snap.Status == status || snap.Status == snapshotProvisioning || snap.Status == snapshotFailed {
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

	return snap.view(b.now()), nil
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
		if !ok {
			continue
		}

		view := snap.view(b.now())
		if names.matches(view.Name) && statusAllowed(view.Status, statuses) {
			out = append(out, view)
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
