package opsworks

import (
	"time"

	"github.com/google/uuid"
)

// CreateLayer creates a new layer in a stack.
func (b *InMemoryBackend) CreateLayer(stackID, layerType, name, shortname string) (*Layer, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateLayer")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	l := &storedLayer{
		CreatedAt: now,
		StackID:   stackID,
		LayerID:   id,
		Arn:       b.layerARN(id),
		Type:      layerType,
		Name:      name,
		Shortname: shortname,
	}
	b.layers.Put(l)

	return l.toLayer(), nil
}

// DescribeLayers returns layers filtered by stack and/or layer IDs.
func (b *InMemoryBackend) DescribeLayers(stackID string, layerIDs []string) ([]*Layer, error) {
	b.mu.RLock("DescribeLayers")
	defer b.mu.RUnlock()

	if len(layerIDs) > 0 {
		result := make([]*Layer, 0, len(layerIDs))
		for _, id := range layerIDs {
			l, ok := b.layers.Get(id)
			if !ok {
				return nil, ErrLayerNotFound
			}
			result = append(result, l.toLayer())
		}

		return result, nil
	}

	source := stackScoped(stackID, b.layers.All, b.layersByStack.Get)

	result := make([]*Layer, 0, len(source))
	for _, l := range source {
		result = append(result, l.toLayer())
	}

	return result, nil
}

// UpdateLayer updates a layer's name.
func (b *InMemoryBackend) UpdateLayer(layerID, name string) error {
	b.mu.Lock("UpdateLayer")
	defer b.mu.Unlock()

	l, ok := b.layers.Get(layerID)
	if !ok {
		return ErrLayerNotFound
	}

	if name != "" {
		l.Name = name
	}

	return nil
}

// DeleteLayer deletes a layer.
func (b *InMemoryBackend) DeleteLayer(layerID string) error {
	b.mu.Lock("DeleteLayer")
	defer b.mu.Unlock()

	if !b.layers.Delete(layerID) {
		return ErrLayerNotFound
	}

	return nil
}
