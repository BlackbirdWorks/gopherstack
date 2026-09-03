package docdb

import (
	"context"
	"fmt"
	"maps"
	"sort"
)

func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	ctx context.Context,
	name, family, description string,
	tags map[string]string,
) (*DBClusterParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if b.clusterParameterGroupHas(region, name) {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBClusterParameterGroup{
		region:                      region,
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 description,
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, name),
		Tags:                        copyTags(tags),
		Parameters:                  make(map[string]string),
	}
	b.clusterParameterGroupPut(pg)
	pgArn := b.clusterParameterGroupARN(region, name)
	if len(tags) > 0 {
		b.tagsStore(region)[pgArn] = tagsFromMap(tags)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterParameterGroups(
	ctx context.Context,
	name string,
) ([]DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroupGet(region, name)
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
		}
		cp := *pg
		cp.Tags = copyTags(pg.Tags)

		return []DBClusterParameterGroup{cp}, nil
	}
	pgStore := b.clusterParameterGroupsInRegion(region)
	result := make([]DBClusterParameterGroup, 0, len(pgStore))
	for _, pg := range pgStore {
		cp := *pg
		cp.Tags = copyTags(pg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterParameterGroupName < result[j].DBClusterParameterGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterParameterGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if !b.clusterParameterGroupHas(region, name) {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	for _, c := range b.clustersInRegion(region) {
		if c.DBClusterParameterGroupName == name {
			return fmt.Errorf(
				"%w: parameter group %s is used by cluster %s",
				ErrParameterGroupInUse,
				name,
				c.DBClusterIdentifier,
			)
		}
	}
	b.clusterParameterGroupDelete(region, name)
	delete(b.tagsStore(region), b.clusterParameterGroupARN(region, name))

	return nil
}

func (b *InMemoryBackend) ModifyDBClusterParameterGroup(
	ctx context.Context,
	name string,
	parameters map[string]string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}

	if pg.Parameters == nil {
		pg.Parameters = make(map[string]string)
	}

	maps.Copy(pg.Parameters, parameters)

	cp := *pg
	cp.Tags = copyTags(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp, nil
}

// CopyDBClusterParameterGroup copies a DB cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	ctx context.Context,
	sourceGroupName, targetName, targetDescription string,
) (*DBClusterParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	if targetName == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()
	src, exists := b.clusterParameterGroupGet(region, sourceGroupName)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			sourceGroupName,
		)
	}
	if b.clusterParameterGroupHas(region, targetName) {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			targetName,
		)
	}
	desc := targetDescription
	if desc == "" {
		desc = src.Description
	}
	pg := &DBClusterParameterGroup{
		region:                      region,
		DBClusterParameterGroupName: targetName,
		DBParameterGroupFamily:      src.DBParameterGroupFamily,
		Description:                 desc,
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, targetName),
		Parameters:                  maps.Clone(src.Parameters),
	}
	b.clusterParameterGroupPut(pg)
	cp := *pg
	cp.Tags = copyTags(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp, nil
}

// DescribeDBClusterParameters returns the parameters for a DB cluster parameter group.
func (b *InMemoryBackend) DescribeDBClusterParameters(
	ctx context.Context,
	groupName, source string,
) ([]DBClusterParameter, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterParameters")
	defer b.mu.RUnlock()
	if groupName == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	pg, exists := b.clusterParameterGroupGet(region, groupName)
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, groupName)
	}

	defaults := clusterParameterDefaults()

	params := make([]DBClusterParameter, 0, len(defaults))
	for _, p := range defaults {
		if pg.Parameters != nil {
			if v, ok := pg.Parameters[p.ParameterName]; ok {
				p.ParameterValue = v
				p.Source = "user"
			}
		}

		if source != "" && p.Source != source {
			continue
		}

		params = append(params, p)
	}

	return params, nil
}

// applyMethodForType returns the real AWS ApplyMethod (types.ApplyMethod:
// "immediate" or "pending-reboot") a parameter's ApplyType implies: a
// "static" parameter always requires a DB instance reboot to take effect,
// while a "dynamic" one applies immediately -- see ResetDBClusterParameterGroup's
// own doc comment ("dynamic parameters are updated immediately and static
// parameters are set to pending-reboot").
func applyMethodForType(applyType string) string {
	if applyType == "static" {
		return "pending-reboot"
	}

	return "immediate"
}

// clusterParameterDefaults returns the built-in DocDB cluster-parameter
// catalog (shared by DescribeDBClusterParameters and
// DescribeEngineDefaultClusterParameters) with ApplyMethod populated per
// applyMethodForType -- previously omitted entirely from the wire response
// (cosmetic but real: AWS's own Parameter shape always carries it).
func clusterParameterDefaults() []DBClusterParameter {
	return []DBClusterParameter{
		{
			ParameterName:  "tls",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TLS setting",
			Source:         "system",
			ApplyType:      "static",
			ApplyMethod:    applyMethodForType("static"),
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
		{
			ParameterName:  "ttl_monitor",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TTL monitor setting",
			Source:         "system",
			ApplyType:      "dynamic",
			ApplyMethod:    applyMethodForType("dynamic"),
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
	}
}

// DescribeEngineDefaultClusterParameters returns the default parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultClusterParameters(
	_ context.Context,
	_ string,
) []DBClusterParameter {
	defaults := clusterParameterDefaults()
	for i := range defaults {
		defaults[i].Source = "engine-default"
	}

	return defaults
}

// ResetDBClusterParameterGroup resets a parameter group to its default
// values: when resetAll is true, every user override is discarded (the
// whole group reverts to engine defaults); otherwise only the overrides
// named in paramNames are discarded, leaving the rest untouched. This
// previously validated the group and returned an unchanged clone without
// ever touching pg.Parameters -- a disguised no-op that made
// ResetDBClusterParameterGroup silently do nothing regardless of what a
// real caller asked to reset (real AWS's own doc comment: "To reset the
// entire cluster parameter group, specify ... ResetAllParameters. To reset
// specific parameters, submit a list of ... ParameterName").
func (b *InMemoryBackend) ResetDBClusterParameterGroup(
	ctx context.Context,
	name string,
	resetAll bool,
	paramNames []string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	switch {
	case resetAll:
		pg.Parameters = make(map[string]string)
	case len(paramNames) > 0:
		for _, p := range paramNames {
			delete(pg.Parameters, p)
		}
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp, nil
}
