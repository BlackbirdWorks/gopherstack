package rds

import (
	"fmt"
	"slices"
)

// CreateDBClusterParameterGroup creates a new cluster parameter group.
func (b *InMemoryBackend) CreateDBClusterParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups.Get(normalizeID(name)); exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupArn:    b.rdsARN("cluster-pg", name),
		DBParameterGroupFamily: family,
		Description:            description,
		Parameters:             make(map[string]DBParameter),
	}
	b.clusterParameterGroups.Put(pg)
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// DescribeDBClusterParameterGroups returns cluster parameter groups.
func (b *InMemoryBackend) DescribeDBClusterParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroups.Get(normalizeID(name))
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, b.clusterParameterGroups.Len())
	for _, pg := range b.clusterParameterGroups.All() {
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

// CopyDBClusterParameterGroup creates a copy of the source cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	sourceGroupName, targetGroupName, targetDescription string,
) (*DBParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf(
			"%w: SourceDBClusterParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}
	if targetGroupName == "" {
		return nil, fmt.Errorf(
			"%w: TargetDBClusterParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()

	src, exists := b.clusterParameterGroups.Get(normalizeID(sourceGroupName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrParameterGroupNotFound,
			sourceGroupName,
		)
	}

	if _, alreadyExists := b.clusterParameterGroups.Get(normalizeID(targetGroupName)); alreadyExists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrParameterGroupAlreadyExists,
			targetGroupName,
		)
	}

	pg := copyParameterGroupTo(src, targetGroupName, targetDescription)
	pg.DBParameterGroupArn = b.rdsARN("cluster-pg", targetGroupName)
	b.clusterParameterGroups.Put(pg)

	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// DeleteDBClusterParameterGroup deletes a cluster parameter group.
func (b *InMemoryBackend) DeleteDBClusterParameterGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups.Get(normalizeID(name))
	if !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	b.clusterParameterGroups.Delete(normalizeID(name))
	// Use pg.DBParameterGroupName (the stored, creation-time casing) rather
	// than the raw name argument -- see normalizeID.
	arn := "arn:aws:rds:" + b.region + ":" + b.accountID + ":cluster-pg:" + pg.DBParameterGroupName
	delete(b.tags, arn)

	return nil
}

// ModifyDBClusterParameterGroup modifies parameters in a cluster parameter group.
func (b *InMemoryBackend) ModifyDBClusterParameterGroup(name string, params []DBParameter) (string, error) {
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups.Get(normalizeID(name))
	if !exists {
		return "", fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	if pg.Parameters == nil {
		pg.Parameters = make(map[string]DBParameter)
	}
	for _, p := range params {
		pg.Parameters[p.ParameterName] = p
	}

	return name, nil
}

// DescribeDBClusterParameters returns parameters for a cluster parameter group.
func (b *InMemoryBackend) DescribeDBClusterParameters(groupName string) ([]DBParameter, error) {
	b.mu.RLock("DescribeDBClusterParameters")
	defer b.mu.RUnlock()
	pg, exists := b.clusterParameterGroups.Get(normalizeID(groupName))
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, groupName)
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

// ResetDBClusterParameterGroup resets parameters in a cluster parameter group.
func (b *InMemoryBackend) ResetDBClusterParameterGroup(
	groupName string,
	resetAllParameters bool,
	params []string,
) (string, error) {
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups.Get(normalizeID(groupName))
	if !exists {
		return "", fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, groupName)
	}
	if resetAllParameters {
		pg.Parameters = make(map[string]DBParameter)
	} else {
		for _, name := range params {
			delete(pg.Parameters, name)
		}
	}

	return groupName, nil
}

// DescribeEngineDefaultClusterParameters returns default cluster parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultClusterParameters(_ string) []DBParameter {
	return []DBParameter{}
}
