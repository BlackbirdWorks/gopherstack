package cloudwatchlogs

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// PutDestination creates or updates a log routing destination.
func (b *InMemoryBackend) PutDestination(name, targetArn, roleArn string) (*CWLDestination, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: destinationName is required", ErrValidation)
	}

	b.mu.Lock("PutDestination")
	defer b.mu.Unlock()

	existing, exists := b.destinations.Get(name)
	if exists {
		existing.TargetArn = targetArn
		existing.RoleArn = roleArn
		cp := *existing

		return &cp, nil
	}

	dest := CWLDestination{
		DestinationName: name,
		TargetArn:       targetArn,
		RoleArn:         roleArn,
		Arn:             "arn:aws:logs:" + b.region + ":" + b.accountID + ":destination:" + name,
		CreatedAt:       time.Now().UTC(),
	}
	stored := dest
	b.destinations.Put(&stored)

	return &dest, nil
}

// PutDestinationPolicy attaches an access policy to a destination.
func (b *InMemoryBackend) PutDestinationPolicy(name, policy string) error {
	b.mu.Lock("PutDestinationPolicy")
	defer b.mu.Unlock()

	dest, ok := b.destinations.Get(name)
	if !ok {
		return fmt.Errorf("%w: destination %q not found", ErrDestinationNotFound, name)
	}

	dest.AccessPolicy = policy

	return nil
}

// DescribeDestinations returns destinations optionally filtered by name prefix.
func (b *InMemoryBackend) DescribeDestinations(namePrefix string) []CWLDestination {
	b.mu.RLock("DescribeDestinations")
	defer b.mu.RUnlock()

	out := make([]CWLDestination, 0, b.destinations.Len())

	for _, d := range b.destinations.All() {
		if namePrefix == "" || strings.HasPrefix(d.DestinationName, namePrefix) {
			out = append(out, *d)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].DestinationName < out[j].DestinationName })

	return out
}

// DeleteDestination removes a log routing destination.
func (b *InMemoryBackend) DeleteDestination(name string) error {
	b.mu.Lock("DeleteDestination")
	defer b.mu.Unlock()

	if !b.destinations.Delete(name) {
		return fmt.Errorf("%w: destination %q not found", ErrDestinationNotFound, name)
	}

	return nil
}
