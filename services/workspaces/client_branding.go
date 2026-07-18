package workspaces

import "maps"

// ImportClientBranding stores branding data for a resource.
func (b *InMemoryBackend) ImportClientBranding(
	resourceID string, platforms map[string]map[string]any,
) error {
	b.mu.Lock("ImportClientBranding")
	defer b.mu.Unlock()

	cb, ok := b.clientBranding.Get(resourceID)
	if !ok {
		cb = &storedClientBranding{
			ResourceID: resourceID,
			Platforms:  make(map[string]map[string]any),
		}
		b.clientBranding.Put(cb)
	}

	maps.Copy(cb.Platforms, platforms)

	return nil
}

// DescribeClientBranding returns branding data for a resource.
func (b *InMemoryBackend) DescribeClientBranding(
	resourceID string,
) (map[string]map[string]any, error) {
	b.mu.RLock("DescribeClientBranding")
	defer b.mu.RUnlock()

	cb, ok := b.clientBranding.Get(resourceID)
	if !ok {
		return map[string]map[string]any{}, nil
	}

	out := make(map[string]map[string]any, len(cb.Platforms))
	maps.Copy(out, cb.Platforms)

	return out, nil
}

// DeleteClientBranding removes branding data for specific platforms.
func (b *InMemoryBackend) DeleteClientBranding(resourceID string, platforms []string) error {
	b.mu.Lock("DeleteClientBranding")
	defer b.mu.Unlock()

	cb, ok := b.clientBranding.Get(resourceID)
	if !ok {
		return nil
	}

	for _, p := range platforms {
		delete(cb.Platforms, p)
	}

	return nil
}

// DescribeClientProperties returns client properties for resource IDs.
func (b *InMemoryBackend) DescribeClientProperties(
	resourceIDs []string,
) (map[string]storedClientProps, error) {
	b.mu.RLock("DescribeClientProperties")
	defer b.mu.RUnlock()

	out := make(map[string]storedClientProps, len(resourceIDs))
	for _, id := range resourceIDs {
		out[id] = b.clientProperties[id]
	}

	return out, nil
}

// ModifyClientProperties sets client properties for a resource.
func (b *InMemoryBackend) ModifyClientProperties(resourceID, reconnectEnabled string) error {
	b.mu.Lock("ModifyClientProperties")
	defer b.mu.Unlock()

	b.clientProperties[resourceID] = storedClientProps{ReconnectEnabled: reconnectEnabled}

	return nil
}
