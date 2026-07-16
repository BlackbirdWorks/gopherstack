package neptune

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) parameterGroupGet(region, name string) (*DBParameterGroup, bool) {
	return b.parameterGroups.Get(regionKey(region, name))
}

func (b *InMemoryBackend) parameterGroupHas(region, name string) bool {
	return b.parameterGroups.Has(regionKey(region, name))
}

func (b *InMemoryBackend) parameterGroupPut(v *DBParameterGroup) { b.parameterGroups.Put(v) }

func (b *InMemoryBackend) parameterGroupDelete(region, name string) {
	b.parameterGroups.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) parameterGroupsInRegion(region string) []*DBParameterGroup {
	return b.parameterGroupsByRegion.Get(region)
}

// parameterGroupARN returns the region-scoped ARN for a Neptune DB parameter group.
func (b *InMemoryBackend) parameterGroupARN(region, name string) string {
	return arn.Build("rds", region, b.accountID, "pg:"+name)
}

// CopyDBParameterGroup copies a Neptune DB parameter group.
func (b *InMemoryBackend) CopyDBParameterGroup(
	ctx context.Context,
	sourceName, targetName, targetDescription string,
) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBParameterGroup")
	defer b.mu.Unlock()
	src, err := copyPreconditions(
		func(n string) (*DBParameterGroup, bool) { return b.parameterGroupGet(region, n) },
		sourceName, targetName,
		"SourceDBParameterGroupIdentifier is required",
		"TargetDBParameterGroupIdentifier is required",
		ErrParameterGroupNotFound, ErrParameterGroupAlreadyExists,
	)
	if err != nil {
		return nil, err
	}
	pg := &DBParameterGroup{
		region:                 region,
		DBParameterGroupName:   targetName,
		DBParameterGroupArn:    b.parameterGroupARN(region, targetName),
		DBParameterGroupFamily: src.DBParameterGroupFamily,
		Description:            resolveCopyDescription(targetDescription, src.Description),
	}
	b.parameterGroupPut(pg)
	cp := *pg

	return &cp, nil
}

// CreateDBParameterGroup creates a Neptune DB parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(
	ctx context.Context, name, family, description string,
) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBParameterGroupName is required", ErrInvalidParameter)
	}
	if family == "" || !validNeptuneParameterGroupFamily(family) {
		return nil, fmt.Errorf(
			"%w: DBParameterGroupFamily %q is not valid; must be one of neptune1.2, neptune1.3, neptune1.4",
			ErrInvalidParameter,
			family,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBParameterGroup")
	defer b.mu.Unlock()
	if b.parameterGroupHas(region, name) {
		return nil, fmt.Errorf(
			"%w: parameter group %s already exists",
			ErrParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBParameterGroup{
		region:                 region,
		DBParameterGroupName:   name,
		DBParameterGroupArn:    b.parameterGroupARN(region, name),
		DBParameterGroupFamily: family,
		Description:            description,
	}
	b.parameterGroupPut(pg)
	cp := *pg

	return &cp, nil
}

// DeleteDBParameterGroup deletes a Neptune DB parameter group.
func (b *InMemoryBackend) DeleteDBParameterGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBParameterGroup")
	defer b.mu.Unlock()
	if !b.parameterGroupHas(region, name) {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	b.parameterGroupDelete(region, name)

	return nil
}

// DescribeDBParameterGroups returns all Neptune DB parameter groups or a specific one.
func (b *InMemoryBackend) DescribeDBParameterGroups(
	ctx context.Context,
	name string,
) ([]DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.parameterGroupGet(region, name)
		if !exists {
			return nil, fmt.Errorf(
				"%w: parameter group %s not found",
				ErrParameterGroupNotFound,
				name,
			)
		}
		cp := *pg

		return []DBParameterGroup{cp}, nil
	}
	groups := b.parameterGroupsInRegion(region)
	result := make([]DBParameterGroup, 0, len(groups))
	for _, pg := range groups {
		result = append(result, *pg)
	}
	slices.SortFunc(result, func(a, b DBParameterGroup) int {
		return strings.Compare(a.DBParameterGroupName, b.DBParameterGroupName)
	})

	return result, nil
}

// ModifyDBParameterGroup modifies a Neptune DB parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(
	ctx context.Context,
	name string,
) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// ResetDBParameterGroup resets a Neptune DB parameter group to its default values.
func (b *InMemoryBackend) ResetDBParameterGroup(
	ctx context.Context,
	name string,
) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// AddParameterGroupInternal creates a DB parameter group directly. Used for seeding tests.
func (b *InMemoryBackend) AddParameterGroupInternal(name, family string) *DBParameterGroup {
	b.mu.Lock("AddParameterGroupInternal")
	defer b.mu.Unlock()
	pg := &DBParameterGroup{
		region:                 b.region,
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            "seeded for tests",
	}
	b.parameterGroupPut(pg)
	cp := *pg

	return &cp
}
