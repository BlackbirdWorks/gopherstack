package pinpoint

import (
	"strings"
)

// attributeTypeEndpointCustom, attributeTypeEndpointMetric, and
// attributeTypeEndpointUser are the AttributeType path-parameter values
// accepted by RemoveAttributes; each selects a different per-endpoint map.
const (
	attributeTypeEndpointCustom = "endpoint-custom-attributes"
	attributeTypeEndpointMetric = "endpoint-metric-attributes"
	attributeTypeEndpointUser   = "endpoint-user-attributes"
)

// matchesAttributePattern reports whether name matches an entry from the
// RemoveAttributes Blacklist. Entries may be an exact attribute name or a
// glob ending in "*" (AWS documents trailing-wildcard prefix matching).
func matchesAttributePattern(name, pattern string) bool {
	if trunk, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(name, trunk)
	}

	return name == pattern
}

// RemoveAttributes removes the attributes named in blacklist, of the given
// attributeType category, from every endpoint in the application. It returns
// the updated attributesResource echoing back what was removed.
func (b *InMemoryBackend) RemoveAttributes(
	appID, attributeType string, blacklist []string,
) (*attributesResource, error) {
	b.mu.Lock("RemoveAttributes")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	for _, e := range b.endpoints.All() {
		if e.ApplicationID != appID {
			continue
		}

		removeMatchingAttributes(e, attributeType, blacklist)
	}

	return &attributesResource{
		ApplicationID: appID,
		AttributeType: attributeType,
		Attributes:    append([]string{}, blacklist...),
	}, nil
}

// removeMatchingAttributes deletes every key matching a blacklist pattern
// from the endpoint map selected by attributeType.
func removeMatchingAttributes(e *Endpoint, attributeType string, blacklist []string) {
	switch attributeType {
	case attributeTypeEndpointMetric:
		for k := range e.Metrics {
			if matchesAnyPattern(k, blacklist) {
				delete(e.Metrics, k)
			}
		}
	case attributeTypeEndpointUser:
		for k := range e.UserAttributes {
			if matchesAnyPattern(k, blacklist) {
				delete(e.UserAttributes, k)
			}
		}
	case attributeTypeEndpointCustom:
		fallthrough
	default:
		for k := range e.Attributes {
			if matchesAnyPattern(k, blacklist) {
				delete(e.Attributes, k)
			}
		}
	}
}

// matchesAnyPattern reports whether name matches any Blacklist pattern.
func matchesAnyPattern(name string, patterns []string) bool {
	for _, p := range patterns {
		if matchesAttributePattern(name, p) {
			return true
		}
	}

	return false
}
