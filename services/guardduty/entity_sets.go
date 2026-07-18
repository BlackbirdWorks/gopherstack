package guardduty

import (
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CreateThreatEntitySet creates a new threat entity set.
//
//nolint:dupl // ThreatEntitySet and TrustedEntitySet have identical creation patterns
func (b *InMemoryBackend) CreateThreatEntitySet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
) (*ThreatEntitySet, error) {
	b.mu.Lock("CreateThreatEntitySet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.threatEntitySetsByDetector.Get(detectorID) {
		if existing.Name == name {
			return nil, ErrThreatEntitySetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &ThreatEntitySet{
		ThreatEntitySetID: id,
		DetectorID:        detectorID,
		Name:              name,
		Format:            format,
		Location:          location,
		Status:            status,
		Tags:              tags,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	b.threatEntitySets.Put(s)

	if tags != nil {
		b.tags[b.threatEntitySetARN(detectorID, id)] = maps.Clone(tags)
	}

	return s, nil
}

// GetThreatEntitySet retrieves a threat entity set.
func (b *InMemoryBackend) GetThreatEntitySet(detectorID, setID string) (*ThreatEntitySet, error) {
	b.mu.RLock("GetThreatEntitySet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.threatEntitySets.Get(detectorKey(detectorID, setID))
	if !ok {
		return nil, ErrThreatEntitySetNotFound
	}

	cp := *s

	return &cp, nil
}

// ListThreatEntitySets returns threat entity set IDs for a detector.
func (b *InMemoryBackend) ListThreatEntitySets(detectorID string) ([]string, error) {
	b.mu.RLock("ListThreatEntitySets")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.threatEntitySetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.ThreatEntitySetID
	}

	sort.Strings(ids)

	return ids, nil
}

// UpdateThreatEntitySet updates a threat entity set.
func (b *InMemoryBackend) UpdateThreatEntitySet(
	detectorID, setID, name, location string,
	activate *bool,
) error {
	b.mu.Lock("UpdateThreatEntitySet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.threatEntitySets.Get(detectorKey(detectorID, setID))
	if !ok {
		return ErrThreatEntitySetNotFound
	}

	if name != "" {
		s.Name = name
	}

	if location != "" {
		s.Location = location
	}

	if activate != nil {
		if *activate {
			s.Status = statusActive
		} else {
			s.Status = statusInactive
		}
	}

	s.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteThreatEntitySet removes a threat entity set.
func (b *InMemoryBackend) DeleteThreatEntitySet(detectorID, setID string) error {
	b.mu.Lock("DeleteThreatEntitySet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.threatEntitySets.Delete(detectorKey(detectorID, setID)) {
		return ErrThreatEntitySetNotFound
	}

	delete(b.tags, b.threatEntitySetARN(detectorID, setID))

	return nil
}

// CreateTrustedEntitySet creates a new trusted entity set.
//
//nolint:dupl // ThreatEntitySet and TrustedEntitySet have identical creation patterns
func (b *InMemoryBackend) CreateTrustedEntitySet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
) (*TrustedEntitySet, error) {
	b.mu.Lock("CreateTrustedEntitySet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.trustedEntitySetsByDetector.Get(detectorID) {
		if existing.Name == name {
			return nil, ErrTrustedEntitySetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &TrustedEntitySet{
		TrustedEntitySetID: id,
		DetectorID:         detectorID,
		Name:               name,
		Format:             format,
		Location:           location,
		Status:             status,
		Tags:               tags,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	b.trustedEntitySets.Put(s)

	if tags != nil {
		b.tags[b.trustedEntitySetARN(detectorID, id)] = maps.Clone(tags)
	}

	return s, nil
}

// GetTrustedEntitySet retrieves a trusted entity set.
func (b *InMemoryBackend) GetTrustedEntitySet(detectorID, setID string) (*TrustedEntitySet, error) {
	b.mu.RLock("GetTrustedEntitySet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.trustedEntitySets.Get(detectorKey(detectorID, setID))
	if !ok {
		return nil, ErrTrustedEntitySetNotFound
	}

	cp := *s

	return &cp, nil
}

// ListTrustedEntitySets returns trusted entity set IDs for a detector.
func (b *InMemoryBackend) ListTrustedEntitySets(detectorID string) ([]string, error) {
	b.mu.RLock("ListTrustedEntitySets")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.trustedEntitySetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.TrustedEntitySetID
	}

	sort.Strings(ids)

	return ids, nil
}

// UpdateTrustedEntitySet updates a trusted entity set.
func (b *InMemoryBackend) UpdateTrustedEntitySet(
	detectorID, setID, name, location string,
	activate *bool,
) error {
	b.mu.Lock("UpdateTrustedEntitySet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.trustedEntitySets.Get(detectorKey(detectorID, setID))
	if !ok {
		return ErrTrustedEntitySetNotFound
	}

	if name != "" {
		s.Name = name
	}

	if location != "" {
		s.Location = location
	}

	if activate != nil {
		if *activate {
			s.Status = statusActive
		} else {
			s.Status = statusInactive
		}
	}

	s.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteTrustedEntitySet removes a trusted entity set.
func (b *InMemoryBackend) DeleteTrustedEntitySet(detectorID, setID string) error {
	b.mu.Lock("DeleteTrustedEntitySet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.trustedEntitySets.Delete(detectorKey(detectorID, setID)) {
		return ErrTrustedEntitySetNotFound
	}

	delete(b.tags, b.trustedEntitySetARN(detectorID, setID))

	return nil
}
