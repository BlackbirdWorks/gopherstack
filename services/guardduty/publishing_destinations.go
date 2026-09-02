package guardduty

import (
	"maps"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// CreatePublishingDestination creates a new publishing destination for a detector.
func (b *InMemoryBackend) CreatePublishingDestination(
	detectorID, destType string,
	props DestinationProperties,
	tags map[string]string,
) (*PublishingDestination, error) {
	b.mu.Lock("CreatePublishingDestination")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	dest := &PublishingDestination{
		DestinationID:         id,
		DestinationType:       destType,
		Status:                "PUBLISHING",
		DestinationProperties: props,
		DetectorID:            detectorID,
		Tags:                  tags,
	}
	b.publishingDestinations.Put(dest)

	if tags != nil {
		b.tags[b.publishingDestinationARN(detectorID, id)] = maps.Clone(tags)
	}

	return dest, nil
}

// DeletePublishingDestination removes a publishing destination.
func (b *InMemoryBackend) DeletePublishingDestination(detectorID, destID string) error {
	b.mu.Lock("DeletePublishingDestination")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.publishingDestinations.Delete(detectorKey(detectorID, destID)) {
		return ErrPublishingDestNotFound
	}

	delete(b.tags, b.publishingDestinationARN(detectorID, destID))

	return nil
}

// DescribePublishingDestination retrieves a publishing destination.
func (b *InMemoryBackend) DescribePublishingDestination(detectorID, destID string) (*PublishingDestination, error) {
	b.mu.RLock("DescribePublishingDestination")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	dest, ok := b.publishingDestinations.Get(detectorKey(detectorID, destID))
	if !ok {
		return nil, ErrPublishingDestNotFound
	}

	cp := *dest

	return &cp, nil
}

// ListPublishingDestinations returns publishing destinations for a detector.
func (b *InMemoryBackend) ListPublishingDestinations(
	detectorID string, maxResults int32, nextToken string,
) ([]*PublishingDestination, string, error) {
	b.mu.RLock("ListPublishingDestinations")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, "", ErrDetectorNotFound
	}

	items := b.publishingDestinationsByDetector.Get(detectorID)
	all := make([]*PublishingDestination, 0, len(items))

	for _, dest := range items {
		cp := *dest
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DestinationID < all[j].DestinationID })

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrValidation
	}

	size := resolvePageSize(int(maxResults))
	page, next := paginate(all, offset, size)

	return page, next, nil
}

// UpdatePublishingDestination updates a publishing destination.
func (b *InMemoryBackend) UpdatePublishingDestination(
	detectorID, destID string,
	props DestinationProperties,
) error {
	b.mu.Lock("UpdatePublishingDestination")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	dest, ok := b.publishingDestinations.Get(detectorKey(detectorID, destID))
	if !ok {
		return ErrPublishingDestNotFound
	}

	dest.DestinationProperties = props

	return nil
}
