package cloudwatchlogs

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// CreateDelivery creates a delivery between a delivery source and destination.
func (b *InMemoryBackend) CreateDelivery(
	deliverySourceName, deliveryDestinationArn string,
	tags map[string]string,
) (*Delivery, error) {
	if deliverySourceName == "" {
		return nil, fmt.Errorf("%w: deliverySourceName is required", ErrValidation)
	}

	if deliveryDestinationArn == "" {
		return nil, fmt.Errorf("%w: deliveryDestinationArn is required", ErrValidation)
	}

	id := uuid.New().String()
	deliveryArn := arn.Build("logs", b.region, b.accountID, "delivery:"+id)
	now := time.Now().UnixMilli()

	d := &Delivery{
		ID:                     id,
		Arn:                    deliveryArn,
		DeliverySourceName:     deliverySourceName,
		DeliveryDestinationArn: deliveryDestinationArn,
		Tags:                   maps.Clone(tags),
		CreationTime:           now,
	}

	b.mu.Lock("CreateDelivery")
	defer b.mu.Unlock()

	b.deliveries.Put(d)

	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp, nil
}

// DescribeDeliveries lists deliveries with pagination.
func (b *InMemoryBackend) DescribeDeliveries(
	limit int,
	nextToken string,
) ([]Delivery, string, error) {
	b.mu.RLock("DescribeDeliveries")
	defer b.mu.RUnlock()

	all := make([]Delivery, 0, b.deliveries.Len())
	for _, d := range b.deliveries.All() {
		cp := *d
		cp.Tags = maps.Clone(d.Tags)
		all = append(all, cp)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreationTime < all[j].CreationTime })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []Delivery{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// GetDelivery returns a single delivery by ID.
func (b *InMemoryBackend) GetDelivery(id string) (*Delivery, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: id is required", ErrValidation)
	}

	b.mu.RLock("GetDelivery")
	defer b.mu.RUnlock()

	d, ok := b.deliveries.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: delivery %s not found", ErrDeliveryNotFound, id)
	}
	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp, nil
}

// DeleteDelivery deletes a delivery by ID.
func (b *InMemoryBackend) DeleteDelivery(id string) error {
	if id == "" {
		return fmt.Errorf("%w: id is required", ErrValidation)
	}

	b.mu.Lock("DeleteDelivery")
	defer b.mu.Unlock()

	if !b.deliveries.Delete(id) {
		return fmt.Errorf("%w: delivery %s not found", ErrDeliveryNotFound, id)
	}

	return nil
}

// AddDeliveryInternal seeds a Delivery directly into the store for testing.
// It overwrites any existing delivery with the same ID.
func (b *InMemoryBackend) AddDeliveryInternal(delivery Delivery) {
	b.mu.Lock("AddDeliveryInternal")
	defer b.mu.Unlock()

	d := delivery
	d.Tags = maps.Clone(delivery.Tags)
	b.deliveries.Put(&d)
}

// PutDeliveryDestination creates or updates a delivery destination.
func (b *InMemoryBackend) PutDeliveryDestination(
	name, targetArn, outputFormat string,
	tags map[string]string,
) (*DeliveryDestination, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("PutDeliveryDestination")
	defer b.mu.Unlock()

	existing, exists := b.deliveryDestinations.Get(name)
	if exists {
		existing.TargetArn = targetArn
		existing.OutputFormat = outputFormat
		if tags != nil {
			existing.Tags = tags
		}
		cp := *existing

		return &cp, nil
	}

	dest := DeliveryDestination{
		Name:         name,
		Arn:          "arn:aws:logs:" + b.region + ":" + b.accountID + ":delivery-destination:" + name,
		TargetArn:    targetArn,
		OutputFormat: outputFormat,
		Tags:         tags,
		CreatedAt:    time.Now().UTC(),
	}
	stored := dest
	b.deliveryDestinations.Put(&stored)

	return &dest, nil
}

// GetDeliveryDestination returns a delivery destination by name.
func (b *InMemoryBackend) GetDeliveryDestination(name string) (*DeliveryDestination, error) {
	b.mu.RLock("GetDeliveryDestination")
	defer b.mu.RUnlock()

	dest, ok := b.deliveryDestinations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}
	cp := *dest

	return &cp, nil
}

// DescribeDeliveryDestinations returns all delivery destinations sorted by name.
func (b *InMemoryBackend) DescribeDeliveryDestinations() []DeliveryDestination {
	b.mu.RLock("DescribeDeliveryDestinations")
	defer b.mu.RUnlock()

	out := make([]DeliveryDestination, 0, b.deliveryDestinations.Len())
	for _, d := range b.deliveryDestinations.All() {
		out = append(out, *d)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteDeliveryDestination removes a delivery destination by name.
func (b *InMemoryBackend) DeleteDeliveryDestination(name string) error {
	b.mu.Lock("DeleteDeliveryDestination")
	defer b.mu.Unlock()

	if !b.deliveryDestinations.Delete(name) {
		return fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	return nil
}

// PutDeliveryDestinationPolicy stores a policy on a delivery destination.
func (b *InMemoryBackend) PutDeliveryDestinationPolicy(name, policy string) error {
	b.mu.Lock("PutDeliveryDestinationPolicy")
	defer b.mu.Unlock()

	dest, ok := b.deliveryDestinations.Get(name)
	if !ok {
		return fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	dest.Policy = policy

	return nil
}

// GetDeliveryDestinationPolicy returns the policy for a delivery destination.
func (b *InMemoryBackend) GetDeliveryDestinationPolicy(name string) (string, error) {
	b.mu.RLock("GetDeliveryDestinationPolicy")
	defer b.mu.RUnlock()

	dest, ok := b.deliveryDestinations.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	return dest.Policy, nil
}

// DeleteDeliveryDestinationPolicy removes the policy from a delivery destination.
func (b *InMemoryBackend) DeleteDeliveryDestinationPolicy(name string) error {
	b.mu.Lock("DeleteDeliveryDestinationPolicy")
	defer b.mu.Unlock()

	dest, ok := b.deliveryDestinations.Get(name)
	if !ok {
		return fmt.Errorf("%w: delivery destination %q not found", ErrDeliveryDestinationNotFound, name)
	}

	dest.Policy = ""

	return nil
}

// PutDeliverySource creates or updates a delivery source.
func (b *InMemoryBackend) PutDeliverySource(
	name, logType string,
	resourceArns []string,
	tags map[string]string,
) (*DeliverySource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("PutDeliverySource")
	defer b.mu.Unlock()

	existing, exists := b.deliverySources.Get(name)
	if exists {
		existing.LogType = logType
		existing.ResourceArns = resourceArns
		if tags != nil {
			existing.Tags = tags
		}
		cp := *existing

		return &cp, nil
	}

	src := DeliverySource{
		Name:         name,
		Arn:          "arn:aws:logs:" + b.region + ":" + b.accountID + ":delivery-source:" + name,
		LogType:      logType,
		ResourceArns: resourceArns,
		Tags:         tags,
		CreatedAt:    time.Now().UTC(),
	}
	stored := src
	b.deliverySources.Put(&stored)

	return &src, nil
}

// GetDeliverySource returns a delivery source by name.
func (b *InMemoryBackend) GetDeliverySource(name string) (*DeliverySource, error) {
	b.mu.RLock("GetDeliverySource")
	defer b.mu.RUnlock()

	src, ok := b.deliverySources.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: delivery source %q not found", ErrDeliverySourceNotFound, name)
	}
	cp := *src

	return &cp, nil
}

// DescribeDeliverySources returns all delivery sources sorted by name.
func (b *InMemoryBackend) DescribeDeliverySources() []DeliverySource {
	b.mu.RLock("DescribeDeliverySources")
	defer b.mu.RUnlock()

	out := make([]DeliverySource, 0, b.deliverySources.Len())
	for _, s := range b.deliverySources.All() {
		out = append(out, *s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteDeliverySource removes a delivery source by name.
func (b *InMemoryBackend) DeleteDeliverySource(name string) error {
	b.mu.Lock("DeleteDeliverySource")
	defer b.mu.Unlock()

	if !b.deliverySources.Delete(name) {
		return fmt.Errorf("%w: delivery source %q not found", ErrDeliverySourceNotFound, name)
	}

	return nil
}

// UpdateDeliveryConfiguration updates the field delimiter for a delivery.
func (b *InMemoryBackend) UpdateDeliveryConfiguration(id, fieldDelimiter string, recordFields []string) error {
	b.mu.Lock("UpdateDeliveryConfiguration")
	defer b.mu.Unlock()

	delivery, ok := b.deliveries.Get(id)
	if !ok {
		return fmt.Errorf("%w: delivery %q not found", ErrDeliveryNotFound, id)
	}

	if fieldDelimiter != "" {
		delivery.FieldDelimiter = fieldDelimiter
	}

	if len(recordFields) > 0 {
		delivery.RecordFields = recordFields
	}

	return nil
}
