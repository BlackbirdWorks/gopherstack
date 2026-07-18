package lakeformation

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// CreateLakeFormationOptIn adds an opt-in enforcement entry for a principal and resource.
func (b *InMemoryBackend) CreateLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error {
	b.mu.Lock("CreateLakeFormationOptIn")
	defer b.mu.Unlock()

	for _, o := range b.lakeFormationOptIns {
		if principalEqual(o.Principal, principal) && resourceEqual(o.Resource, resource) {
			return awserr.New("opt-in already exists for this principal and resource", awserr.ErrAlreadyExists)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)
	b.lakeFormationOptIns = append(b.lakeFormationOptIns, &LFOptIn{
		Principal:     principal,
		Resource:      resource,
		LastModified:  now,
		LastUpdatedBy: "lakeformation.amazonaws.com",
	})

	return nil
}

// DeleteLakeFormationOptIn removes an opt-in enforcement entry for a principal and resource.
func (b *InMemoryBackend) DeleteLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error {
	if principal == nil {
		return fmt.Errorf("principal is required: %w", ErrValidation)
	}

	if resource == nil {
		return fmt.Errorf("resource is required: %w", ErrValidation)
	}

	b.mu.Lock("DeleteLakeFormationOptIn")
	defer b.mu.Unlock()

	updated := make([]*LFOptIn, 0, len(b.lakeFormationOptIns))
	found := false

	for _, o := range b.lakeFormationOptIns {
		if principalEqual(o.Principal, principal) && resourceEqual(o.Resource, resource) {
			found = true

			continue
		}

		updated = append(updated, o)
	}

	if !found {
		return awserr.New("opt-in not found for this principal and resource", awserr.ErrNotFound)
	}

	b.lakeFormationOptIns = updated

	return nil
}

// ListLakeFormationOptIns returns a paginated list of opt-in entries.
// Optional principalIdentifier acts as a filter.
func (b *InMemoryBackend) ListLakeFormationOptIns(
	principalIdentifier string,
	resource *Resource,
	maxResults int,
	nextToken string,
) ([]*LFOptIn, string) {
	b.mu.RLock("ListLakeFormationOptIns")
	defer b.mu.RUnlock()

	all := make([]*LFOptIn, 0, len(b.lakeFormationOptIns))

	for _, o := range b.lakeFormationOptIns {
		if principalIdentifier != "" && principalID(o.Principal) != principalIdentifier {
			continue
		}
		if resource != nil && !resourceEqual(o.Resource, resource) {
			continue
		}

		cp := &LFOptIn{
			LastModified:  o.LastModified,
			LastUpdatedBy: o.LastUpdatedBy,
		}

		if o.Principal != nil {
			p := *o.Principal
			cp.Principal = &p
		}

		if o.Resource != nil {
			cp.Resource = copyResource(o.Resource)
		}

		all = append(all, cp)
	}

	sort.Slice(all, func(i, j int) bool {
		pi := principalID(all[i].Principal)
		pj := principalID(all[j].Principal)
		if pi != pj {
			return pi < pj
		}

		return resourceToKey(all[i].Resource) < resourceToKey(all[j].Resource)
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}
