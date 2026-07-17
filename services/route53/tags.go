package route53

import (
	"fmt"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// tagResourceTypeHealthCheck and tagResourceTypeHostedZone are the wire
// values of the AWS TagResourceType enum used by the tag-family operations
// (ListTagsForResource[s], ChangeTagsForResource).
const (
	tagResourceTypeHealthCheck = "healthcheck"
	tagResourceTypeHostedZone  = "hostedzone"
)

// checkTagResourceExists validates that resourceID exists as the given
// resourceType. AWS returns NoSuchHostedZone/NoSuchHealthCheck (404) for the
// tag-family operations when the target resource does not exist; an unknown
// resourceType is InvalidInput (400).
func (b *InMemoryBackend) checkTagResourceExists(resourceType, resourceID string) error {
	switch resourceType {
	case tagResourceTypeHostedZone:
		if _, ok := b.zones.Get(resourceID); !ok {
			return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, resourceID)
		}
	case tagResourceTypeHealthCheck:
		if !b.healthChecks.Has(resourceID) {
			return fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, resourceID)
		}
	default:
		return fmt.Errorf("%w: unsupported ResourceType %q", ErrInvalidInput, resourceType)
	}

	return nil
}

// ListTagsForResource returns the tags for a single hosted zone or health check.
func (b *InMemoryBackend) ListTagsForResource(resourceType, resourceID string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if err := b.checkTagResourceExists(resourceType, resourceID); err != nil {
		return nil, err
	}

	if t, exists := b.tags[resourceID]; exists {
		return t.Clone(), nil
	}

	return make(map[string]string), nil
}

// ListTagsForResources returns the tags for a batch of same-type resources.
// If any resourceID does not exist, the whole call fails (matching AWS,
// which validates the entire ResourceIds list before returning tags).
func (b *InMemoryBackend) ListTagsForResources(
	resourceType string,
	resourceIDs []string,
) (map[string]map[string]string, error) {
	b.mu.RLock("ListTagsForResources")
	defer b.mu.RUnlock()

	for _, id := range resourceIDs {
		if err := b.checkTagResourceExists(resourceType, id); err != nil {
			return nil, err
		}
	}

	result := make(map[string]map[string]string)
	for _, id := range resourceIDs {
		if t, ok := b.tags[id]; ok {
			result[id] = t.Clone()
		} else {
			result[id] = make(map[string]string)
		}
	}

	return result, nil
}

func (b *InMemoryBackend) ChangeTagsForResource(
	resourceType, resourceID string,
	addTags map[string]string,
	removeKeys []string,
) error {
	b.mu.Lock("ChangeTagsForResource")
	defer b.mu.Unlock()

	if err := b.checkTagResourceExists(resourceType, resourceID); err != nil {
		return err
	}

	if b.tags[resourceID] == nil {
		b.tags[resourceID] = svcTags.New("route53." + resourceID + ".tags")
	}

	if len(addTags) > 0 {
		b.tags[resourceID].Merge(addTags)
	}
	if len(removeKeys) > 0 {
		b.tags[resourceID].DeleteKeys(removeKeys)
	}

	return nil
}
