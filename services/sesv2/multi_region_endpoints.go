package sesv2

import (
	"fmt"
	"maps"
)

// ---- multi-region endpoints ----

func (b *InMemoryBackend) CreateMultiRegionEndpoint(endpointName string) (string, error) {
	b.mu.Lock("CreateMultiRegionEndpoint")
	defer b.mu.Unlock()

	b.multiRegionEndpoints[endpointName] = map[string]any{
		"EndpointName": endpointName,
		keyStatus:      "READY",
	}

	return "READY", nil
}

func (b *InMemoryBackend) GetMultiRegionEndpoint(endpointName string) (map[string]any, error) {
	b.mu.RLock("GetMultiRegionEndpoint")
	defer b.mu.RUnlock()

	ep, ok := b.multiRegionEndpoints[endpointName]
	if !ok {
		return nil, fmt.Errorf("%w: MultiRegionEndpoint %s not found", ErrNotFound, endpointName)
	}

	out := make(map[string]any, len(ep))
	maps.Copy(out, ep)

	return out, nil
}

func (b *InMemoryBackend) DeleteMultiRegionEndpoint(endpointName string) error {
	b.mu.Lock("DeleteMultiRegionEndpoint")
	defer b.mu.Unlock()

	delete(b.multiRegionEndpoints, endpointName)

	return nil
}

func (b *InMemoryBackend) ListMultiRegionEndpoints(
	nextToken string,
	pageSize int,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListMultiRegionEndpoints")

	all := make([]map[string]any, 0, len(b.multiRegionEndpoints))
	for _, ep := range b.multiRegionEndpoints {
		cp := make(map[string]any, len(ep))
		maps.Copy(cp, ep)
		all = append(all, cp)
	}

	b.mu.RUnlock()

	return paginateMaps(all, nextToken, pageSize, "EndpointName")
}
