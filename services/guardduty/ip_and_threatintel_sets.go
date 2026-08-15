package guardduty

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CreateIPSet creates a new IP set.
//
//nolint:dupl // IPSet and ThreatIntelSet have identical creation patterns
func (b *InMemoryBackend) CreateIPSet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
	expectedBucketOwner string,
) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.ipSetsByDetector.Get(detectorID) {
		if existing.Name == name {
			return nil, ErrIPSetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &IPSet{
		IPSetID:             id,
		Name:                name,
		Format:              format,
		Location:            location,
		Status:              status,
		Tags:                tags,
		DetectorID:          detectorID,
		CreatedAt:           now,
		UpdatedAt:           now,
		ExpectedBucketOwner: expectedBucketOwner,
	}
	b.ipSets.Put(s)

	arn := b.ipSetARN(detectorID, id)
	if tags != nil {
		b.tags[arn] = maps.Clone(tags)
	}

	return s, nil
}

// GetIPSet retrieves an IP set.
func (b *InMemoryBackend) GetIPSet(detectorID, ipSetID string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.ipSets.Get(detectorKey(detectorID, ipSetID))
	if !ok {
		return nil, ErrIPSetNotFound
	}

	return s, nil
}

// UpdateIPSet updates an IP set.
func (b *InMemoryBackend) UpdateIPSet(
	detectorID, ipSetID, name, location string,
	activate *bool,
	expectedBucketOwner string,
) error {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.ipSets.Get(detectorKey(detectorID, ipSetID))
	if !ok {
		return ErrIPSetNotFound
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

	if expectedBucketOwner != "" {
		s.ExpectedBucketOwner = expectedBucketOwner
	}

	s.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteIPSet removes an IP set.
func (b *InMemoryBackend) DeleteIPSet(detectorID, ipSetID string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.ipSets.Delete(detectorKey(detectorID, ipSetID)) {
		return ErrIPSetNotFound
	}

	delete(b.tags, b.ipSetARN(detectorID, ipSetID))

	return nil
}

// ListIPSets returns IP set IDs for a detector.
func (b *InMemoryBackend) ListIPSets(detectorID string) ([]string, error) {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.ipSetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.IPSetID
	}

	slices.Sort(ids)

	return ids, nil
}

// CreateThreatIntelSet creates a new threat intelligence set.
//
//nolint:dupl // IPSet and ThreatIntelSet have identical creation patterns
func (b *InMemoryBackend) CreateThreatIntelSet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
	expectedBucketOwner string,
) (*ThreatIntelSet, error) {
	b.mu.Lock("CreateThreatIntelSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.threatIntelSetsByDetector.Get(detectorID) {
		if existing.Name == name {
			return nil, ErrThreatIntelSetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &ThreatIntelSet{
		ThreatIntelSetID:    id,
		Name:                name,
		Format:              format,
		Location:            location,
		Status:              status,
		Tags:                tags,
		DetectorID:          detectorID,
		CreatedAt:           now,
		UpdatedAt:           now,
		ExpectedBucketOwner: expectedBucketOwner,
	}
	b.threatIntelSets.Put(s)

	arn := b.threatIntelSetARN(detectorID, id)
	if tags != nil {
		b.tags[arn] = maps.Clone(tags)
	}

	return s, nil
}

// GetThreatIntelSet retrieves a threat intelligence set.
func (b *InMemoryBackend) GetThreatIntelSet(detectorID, setID string) (*ThreatIntelSet, error) {
	b.mu.RLock("GetThreatIntelSet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.threatIntelSets.Get(detectorKey(detectorID, setID))
	if !ok {
		return nil, ErrThreatIntelSetNotFound
	}

	return s, nil
}

// UpdateThreatIntelSet updates a threat intelligence set.
func (b *InMemoryBackend) UpdateThreatIntelSet(
	detectorID, setID, name, location string,
	activate *bool,
	expectedBucketOwner string,
) error {
	b.mu.Lock("UpdateThreatIntelSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.threatIntelSets.Get(detectorKey(detectorID, setID))
	if !ok {
		return ErrThreatIntelSetNotFound
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

	if expectedBucketOwner != "" {
		s.ExpectedBucketOwner = expectedBucketOwner
	}

	s.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteThreatIntelSet removes a threat intelligence set.
func (b *InMemoryBackend) DeleteThreatIntelSet(detectorID, setID string) error {
	b.mu.Lock("DeleteThreatIntelSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.threatIntelSets.Delete(detectorKey(detectorID, setID)) {
		return ErrThreatIntelSetNotFound
	}

	delete(b.tags, b.threatIntelSetARN(detectorID, setID))

	return nil
}

// ListThreatIntelSets returns threat intel set IDs for a detector.
func (b *InMemoryBackend) ListThreatIntelSets(detectorID string) ([]string, error) {
	b.mu.RLock("ListThreatIntelSets")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.threatIntelSetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.ThreatIntelSetID
	}

	slices.Sort(ids)

	return ids, nil
}
