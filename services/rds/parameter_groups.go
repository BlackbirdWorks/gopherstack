package rds

import (
	"fmt"
	"maps"
	"slices"
)

// CreateDBParameterGroup creates a new DB parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBParameterGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups.Get(normalizeID(name)); exists {
		return nil, fmt.Errorf("%w: parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
		Parameters:             make(map[string]DBParameter),
	}
	b.parameterGroups.Put(pg)
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// copyDBParameterGroup returns a deep copy of the given parameter group.
func copyDBParameterGroup(pg *DBParameterGroup) DBParameterGroup {
	cp := *pg
	cp.Parameters = make(map[string]DBParameter, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return cp
}

// DescribeDBParameterGroups returns parameter groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeDBParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.parameterGroups.Get(normalizeID(name))
		if !exists {
			return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, b.parameterGroups.Len())
	for _, pg := range b.parameterGroups.All() {
		result = append(result, copyDBParameterGroup(pg))
	}
	slices.SortFunc(result, func(a, b DBParameterGroup) int {
		if a.DBParameterGroupName < b.DBParameterGroupName {
			return -1
		}
		if a.DBParameterGroupName > b.DBParameterGroupName {
			return 1
		}

		return 0
	})

	return result, nil
}

// DeleteDBParameterGroup removes the given parameter group.
func (b *InMemoryBackend) DeleteDBParameterGroup(name string) error {
	b.mu.Lock("DeleteDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups.Get(normalizeID(name))
	if !exists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	b.parameterGroups.Delete(normalizeID(name))
	// Use pg.DBParameterGroupName (the stored, creation-time casing) rather
	// than the raw name argument -- see normalizeID.
	delete(b.tags, b.rdsARN("pg", pg.DBParameterGroupName))

	return nil
}

// ModifyDBParameterGroup modifies parameters in a parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(name string, params []DBParameter) (*DBParameterGroup, error) {
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups.Get(normalizeID(name))
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	for _, p := range params {
		if p.Source == "" {
			p.Source = "user"
		}
		if p.ApplyMethod == "" {
			p.ApplyMethod = "pending-reboot"
		}
		pg.Parameters[p.ParameterName] = p
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// DescribeDBParameters returns parameters for a parameter group.
func (b *InMemoryBackend) DescribeDBParameters(groupName string) ([]DBParameter, error) {
	b.mu.RLock("DescribeDBParameters")
	defer b.mu.RUnlock()
	pg, exists := b.parameterGroups.Get(normalizeID(groupName))
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, groupName)
	}
	result := make([]DBParameter, 0, len(pg.Parameters))
	for _, p := range pg.Parameters {
		result = append(result, p)
	}
	slices.SortFunc(result, func(a, b DBParameter) int {
		if a.ParameterName < b.ParameterName {
			return -1
		}
		if a.ParameterName > b.ParameterName {
			return 1
		}

		return 0
	})

	return result, nil
}

// ResetDBParameterGroup resets parameters in a parameter group.
func (b *InMemoryBackend) ResetDBParameterGroup(
	name string,
	resetAll bool,
	params []string,
) (*DBParameterGroup, error) {
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups.Get(normalizeID(name))
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	if resetAll {
		for k, p := range pg.Parameters {
			p.ParameterValue = ""
			pg.Parameters[k] = p
		}
	} else {
		for _, pName := range params {
			if p, ok := pg.Parameters[pName]; ok {
				p.ParameterValue = ""
				pg.Parameters[pName] = p
			}
		}
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// CopyDBParameterGroup creates a copy of the source parameter group.
func (b *InMemoryBackend) CopyDBParameterGroup(
	sourceGroupName, targetGroupName, targetDescription string,
) (*DBParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf(
			"%w: SourceDBParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}
	if targetGroupName == "" {
		return nil, fmt.Errorf(
			"%w: TargetDBParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CopyDBParameterGroup")
	defer b.mu.Unlock()

	src, exists := b.parameterGroups.Get(normalizeID(sourceGroupName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: parameter group %s not found",
			ErrParameterGroupNotFound,
			sourceGroupName,
		)
	}

	if _, alreadyExists := b.parameterGroups.Get(normalizeID(targetGroupName)); alreadyExists {
		return nil, fmt.Errorf(
			"%w: parameter group %s already exists",
			ErrParameterGroupAlreadyExists,
			targetGroupName,
		)
	}

	pg := copyParameterGroupTo(src, targetGroupName, targetDescription)
	b.parameterGroups.Put(pg)

	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// DescribeEngineDefaultParameters returns default parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultParameters(_ string) []DBParameter {
	return []DBParameter{}
}
