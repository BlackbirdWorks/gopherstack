package memorydb

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateACL creates a new ACL.
func (b *InMemoryBackend) CreateACL(ctx context.Context, req *createACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if err := validateResourceName(req.ACLName, "ACL"); err != nil {
		return nil, err
	}

	if _, exists := b.aclsStore(region).Get(req.ACLName); exists {
		return nil, ErrACLAlreadyExists
	}

	aclARN := arn.Build("memorydb", region, b.accountID, "acl/"+req.ACLName)

	userNames := req.UserNames
	if userNames == nil {
		userNames = []string{}
	}

	a := &ACL{
		Name:      req.ACLName,
		ARN:       aclARN,
		Status:    aclStatusActive,
		UserNames: userNames,
		Tags:      tagsFromSlice(req.Tags),
		CreatedAt: time.Now(),
	}

	b.aclsStore(region).Put(a)
	b.arnToResourceStore(region)[aclARN] = resourceRef{Kind: resourceKindACL, Name: req.ACLName}

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.ACLName,
		SourceType: resourceKindACL,
		Message:    "ACL " + req.ACLName + " created",
	})

	return cloneACL(a), nil
}

// DescribeACLs returns ACLs, optionally filtered by name.
func (b *InMemoryBackend) DescribeACLs(ctx context.Context, name string) ([]*ACL, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.acls[region]

	if name != "" {
		a, ok := tableGet(t, name)
		if !ok {
			return nil, ErrACLNotFound
		}

		return []*ACL{cloneACL(a)}, nil
	}

	all := tableAll(t)
	result := make([]*ACL, 0, len(all))
	for _, a := range all {
		result = append(result, cloneACL(a))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteACL removes an ACL.
func (b *InMemoryBackend) DeleteACL(ctx context.Context, name string) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	a, ok := b.aclsStore(region).Get(name)
	if !ok {
		return nil, ErrACLNotFound
	}

	if name == openAccessACL {
		return nil, fmt.Errorf("cannot delete system ACL %q: %w", name, ErrValidation)
	}

	for _, c := range tableAll(b.clusters[region]) {
		if c.ACLName == name {
			return nil, fmt.Errorf("ACL %q is associated with cluster %q: %w", name, c.Name, ErrACLInUse)
		}
	}

	b.aclsStore(region).Delete(name)
	delete(b.arnToResourceStore(region), a.ARN)

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindACL,
		Message:    "ACL " + name + " deleted",
	})

	return a, nil
}

// UpdateACL modifies an existing ACL.
func (b *InMemoryBackend) UpdateACL(ctx context.Context, req *updateACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	a, ok := b.aclsStore(region).Get(req.ACLName)
	if !ok {
		return nil, ErrACLNotFound
	}

	existing := make(map[string]bool, len(a.UserNames))
	for _, u := range a.UserNames {
		existing[u] = true
	}

	for _, u := range req.UserNamesToAdd {
		if _, exists := tableGet(b.users[region], u); !exists {
			return nil, fmt.Errorf("user %q not found: %w", u, ErrUserNotFound)
		}
	}

	for _, u := range req.UserNamesToAdd {
		if !existing[u] {
			a.UserNames = append(a.UserNames, u)
			existing[u] = true
		}
	}

	if len(req.UserNamesToRemove) > 0 {
		toRemove := make(map[string]bool, len(req.UserNamesToRemove))
		for _, u := range req.UserNamesToRemove {
			toRemove[u] = true
		}
		filtered := make([]string, 0, len(a.UserNames))
		for _, u := range a.UserNames {
			if !toRemove[u] {
				filtered = append(filtered, u)
			}
		}
		a.UserNames = filtered
	}

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.ACLName,
		SourceType: resourceKindACL,
		Message:    "ACL " + req.ACLName + " modified",
	})

	return cloneACL(a), nil
}

// -- SubnetGroup operations -------------------------------------------------------

// cloneACL returns a shallow copy of the ACL with separate tag and user slices.
func cloneACL(a *ACL) *ACL {
	if a == nil {
		return nil
	}

	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.UserNames = append([]string(nil), a.UserNames...)

	return &cp
}

// AddACLInternal inserts an ACL directly into the backend for testing.
func (b *InMemoryBackend) AddACLInternal(name string) *ACL {
	b.mu.Lock()
	defer b.mu.Unlock()

	aclARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "acl/"+name)
	a := &ACL{
		Name:      name,
		ARN:       aclARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
	}
	b.aclsStore(b.defaultRegion).Put(a)
	b.arnToResourceStore(b.defaultRegion)[aclARN] = resourceRef{Kind: resourceKindACL, Name: name}

	return a
}
