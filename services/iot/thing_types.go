package iot

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateThingType creates a new IoT Thing Type.
func (b *InMemoryBackend) CreateThingType(input *CreateThingTypeInput) (*ThingType, error) {
	if input.ThingTypeName == "" {
		return nil, fmt.Errorf("%w: ThingTypeName is required", ErrValidation)
	}

	if err := validateEntityName("ThingTypeName", input.ThingTypeName); err != nil {
		return nil, err
	}

	defer b.flushEvents()

	b.mu.Lock("CreateThingType")
	defer b.mu.Unlock()

	if b.thingTypes.Has(input.ThingTypeName) {
		return nil, fmt.Errorf("%w: thing type %q already exists", ErrAlreadyExists, input.ThingTypeName)
	}

	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("thingtype/%s", input.ThingTypeName))
	tt := &ThingType{
		ThingTypeName:        input.ThingTypeName,
		ThingTypeID:          uuid.NewString(),
		ThingTypeARN:         arn,
		Description:          input.Description,
		SearchableAttributes: append([]string(nil), input.SearchableAttributes...),
		CreatedAt:            time.Now(),
	}

	b.thingTypes.Put(tt)
	b.putResourceTagsLocked(tt.ThingTypeARN, input.Tags)
	b.queueThingTypeEventLocked(opCreated, tt)

	return tt, nil
}

// DescribeThingType returns a ThingType by name.
func (b *InMemoryBackend) DescribeThingType(thingTypeName string) (*ThingType, error) {
	b.mu.RLock("DescribeThingType")
	defer b.mu.RUnlock()

	tt, ok := b.thingTypes.Get(thingTypeName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrThingTypeNotFound, thingTypeName)
	}

	cp := *tt

	return &cp, nil
}

// ListThingTypes returns all thing types sorted by name.
func (b *InMemoryBackend) ListThingTypes() []*ThingType {
	b.mu.RLock("ListThingTypes")
	defer b.mu.RUnlock()

	items := b.thingTypes.Snapshot()
	out := make([]*ThingType, 0, len(items))

	for _, v := range items {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// checkAssignableThingTypeLocked rejects a missing thing type and a deprecated one ("You can not associate new
// things with deprecated thing type"). An empty name means no type.
func (b *InMemoryBackend) checkAssignableThingTypeLocked(name string) error {
	if name == "" {
		return nil
	}

	tt, ok := b.thingTypes.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingTypeNotFound, name)
	}

	if tt.Deprecated {
		return fmt.Errorf(
			"%w: thing type %q is deprecated and cannot be associated with new things",
			ErrValidation,
			name,
		)
	}

	return nil
}

// DeprecateThingType marks a thing type as deprecated (or un-deprecates it).
func (b *InMemoryBackend) DeprecateThingType(input *DeprecateThingTypeInput) error {
	defer b.flushEvents()

	b.mu.Lock("DeprecateThingType")
	defer b.mu.Unlock()

	tt, ok := b.thingTypes.Get(input.ThingTypeName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingTypeNotFound, input.ThingTypeName)
	}

	if input.UndoDeprecate {
		tt.Deprecated = false
		tt.DeprecationDate = time.Time{}
	} else {
		tt.Deprecated = true
		tt.DeprecationDate = time.Now()
	}

	b.queueThingTypeEventLocked(opUpdated, tt)

	return nil
}

// DeleteThingType deletes a thing type by name. The type must be deprecated first.
func (b *InMemoryBackend) DeleteThingType(thingTypeName string) error {
	defer b.flushEvents()

	b.mu.Lock("DeleteThingType")
	defer b.mu.Unlock()

	tt, ok := b.thingTypes.Get(thingTypeName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingTypeNotFound, thingTypeName)
	}

	if !tt.Deprecated {
		return fmt.Errorf("%w: thing type %q must be deprecated before deletion", ErrValidation, thingTypeName)
	}

	for _, t := range b.things.All() {
		if t.ThingTypeName == thingTypeName {
			return fmt.Errorf("%w: thing type %q still has things associated with it", ErrValidation, thingTypeName)
		}
	}

	b.queueThingTypeEventLocked(opDeleted, tt)
	b.thingTypes.Delete(thingTypeName)
	delete(b.resourceTags, tt.ThingTypeARN)

	return nil
}

// UpdateThingType updates the description and/or searchable attributes of an
// existing thing type. Searchable attributes are additive, matching the real
// AWS IoT behavior (existing searchable attributes cannot be removed this way).
func (b *InMemoryBackend) UpdateThingType(input *UpdateThingTypeInput) error {
	if input.ThingTypeName == "" {
		return fmt.Errorf("%w: ThingTypeName is required", ErrValidation)
	}

	defer b.flushEvents()

	b.mu.Lock("UpdateThingType")
	defer b.mu.Unlock()

	tt, ok := b.thingTypes.Get(input.ThingTypeName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrThingTypeNotFound, input.ThingTypeName)
	}

	if input.Description != nil {
		tt.Description = *input.Description
	}

	for _, attr := range input.SearchableAttributes {
		if !slices.Contains(tt.SearchableAttributes, attr) {
			tt.SearchableAttributes = append(tt.SearchableAttributes, attr)
		}
	}

	b.queueThingTypeEventLocked(opUpdated, tt)

	return nil
}
