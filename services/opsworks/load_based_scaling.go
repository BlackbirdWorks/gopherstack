package opsworks

// SetLoadBasedAutoScaling sets load-based auto-scaling config for a layer.
func (b *InMemoryBackend) SetLoadBasedAutoScaling(
	layerID string,
	enable bool,
	upScaling, downScaling *ScalingParameters,
) error {
	b.mu.Lock("SetLoadBasedAutoScaling")
	defer b.mu.Unlock()

	if !b.layers.Has(layerID) {
		return ErrLayerNotFound
	}

	b.loadBasedAutoScale.Put(&storedLoadBasedAutoScaling{
		UpScaling:   upScaling,
		DownScaling: downScaling,
		LayerID:     layerID,
		Enable:      enable,
	})

	return nil
}

// DescribeLoadBasedAutoScaling returns load-based auto-scaling config for layers.
func (b *InMemoryBackend) DescribeLoadBasedAutoScaling(layerIDs []string) ([]*LoadBasedAutoScaling, error) {
	b.mu.RLock("DescribeLoadBasedAutoScaling")
	defer b.mu.RUnlock()

	result := make([]*LoadBasedAutoScaling, 0, len(layerIDs))
	for _, id := range layerIDs {
		l, ok := b.loadBasedAutoScale.Get(id)
		if !ok {
			result = append(result, &LoadBasedAutoScaling{LayerID: id})

			continue
		}
		result = append(result, l.toLoadBasedAutoScaling())
	}

	return result, nil
}
