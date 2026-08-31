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
		// PutDestinationPolicy's own deserializer declares only
		// InvalidParameterException/OperationAbortedException/
		// ServiceUnavailableException -- no ResourceNotFoundException, unlike
		// DeleteDestination below, which does declare it. ErrDestinationNotFound
		// stays correct for DeleteDestination; this call site overrides.
		return fmt.Errorf("%w: destination %q not found", ErrValidation, name)
	}

	dest.AccessPolicy = policy

	return nil
}

// DescribeDestinations returns destinations optionally filtered by name
// prefix, with pagination. Field-diffed against DescribeDestinationsInput/
// Output (api_op_DescribeDestinations.go): limit/nextToken were previously
// unmodeled entirely, so a real client paging through more destinations than
// fit in one response had no way to fetch subsequent pages -- every call
// silently returned the complete, unpaginated result set instead.
func (b *InMemoryBackend) DescribeDestinations(
	namePrefix string,
	limit int,
	nextToken string,
) ([]CWLDestination, string) {
	b.mu.RLock("DescribeDestinations")
	defer b.mu.RUnlock()

	all := make([]CWLDestination, 0, b.destinations.Len())

	for _, d := range b.destinations.All() {
		if namePrefix == "" || strings.HasPrefix(d.DestinationName, namePrefix) {
			all = append(all, *d)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DestinationName < all[j].DestinationName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []CWLDestination{}, ""
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

	return all[startIdx:end], outToken
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
