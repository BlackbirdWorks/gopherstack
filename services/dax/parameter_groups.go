package dax

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
)

// CreateParameterGroup creates a DAX parameter group.
func (b *InMemoryBackend) CreateParameterGroup(name, description string) (*ParameterGroup, error) {
	if err := validateResourceName(name, "ParameterGroupName"); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateParameterGroup")
	defer b.mu.Unlock()

	if b.paramGroups.Has(name) {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupAlreadyExists, name)
	}

	params := make(map[string]string, len(defaultParameterValues))
	maps.Copy(params, defaultParameterValues)

	pg := &ParameterGroup{
		ParameterGroupName: name,
		Description:        description,
		Parameters:         params,
	}

	b.paramGroups.Put(pg)

	b.emitEventLocked(name, EventSourceTypeParameterGroup,
		fmt.Sprintf("Parameter group %s created.", name))

	cp := *pg
	cp.Parameters = make(map[string]string, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return &cp, nil
}

// DescribeParameterGroups returns DAX parameter groups with pagination.
func (b *InMemoryBackend) DescribeParameterGroups(
	names []string,
	maxResults int,
	nextToken string,
) ([]*ParameterGroup, string, error) {
	b.mu.RLock("DescribeParameterGroups")
	defer b.mu.RUnlock()

	return describeNamedGroups(
		b.paramGroups, names, maxResults, nextToken, ErrParameterGroupNotFound,
		func(pg *ParameterGroup) string { return pg.ParameterGroupName }, paramGroupCopy,
	)
}

// UpdateParameterGroup updates parameter values in a parameter group.
func (b *InMemoryBackend) UpdateParameterGroup(input UpdateParameterGroupInput) (*ParameterGroup, error) {
	if input.ParameterGroupName == "" {
		return nil, fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	b.mu.Lock("UpdateParameterGroup")
	defer b.mu.Unlock()

	pg, ok := b.paramGroups.Get(input.ParameterGroupName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupNotFound, input.ParameterGroupName)
	}

	for _, pv := range input.ParameterNameValues {
		if _, known := defaultParameterValues[pv.ParameterName]; !known {
			return nil, fmt.Errorf(
				"%w: unknown parameter %q",
				ErrInvalidParameterValue,
				pv.ParameterName,
			)
		}

		if pv.ParameterValue == "" {
			return nil, fmt.Errorf(
				"%w: value for %q must be a non-negative integer",
				ErrInvalidParameterValue, pv.ParameterName,
			)
		}

		val, err := strconv.ParseInt(pv.ParameterValue, 10, 64)
		if err != nil || val < 0 {
			return nil, fmt.Errorf(
				"%w: value for %q must be a non-negative integer, got %q",
				ErrInvalidParameterValue,
				pv.ParameterName,
				pv.ParameterValue,
			)
		}

		pg.Parameters[pv.ParameterName] = pv.ParameterValue
	}

	for _, cluster := range b.clusters.All() {
		if cluster.ParameterGroup.ParameterGroupName != input.ParameterGroupName {
			continue
		}
		nodeIDs := make([]string, 0, len(cluster.Nodes))
		for _, n := range cluster.Nodes {
			nodeIDs = append(nodeIDs, n.NodeID)
		}
		cluster.ParameterGroup.ParameterApplyStatus = "pending-reboot"
		cluster.ParameterGroup.NodeIDsToReboot = nodeIDs
	}

	b.emitEventLocked(input.ParameterGroupName, EventSourceTypeParameterGroup,
		fmt.Sprintf("Parameter group %s updated.", input.ParameterGroupName))

	return paramGroupCopy(pg), nil
}

// DeleteParameterGroup deletes a DAX parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	b.mu.Lock("DeleteParameterGroup")
	defer b.mu.Unlock()

	if !b.paramGroups.Has(name) {
		return fmt.Errorf("%w: %s", ErrParameterGroupNotFound, name)
	}

	for _, cluster := range b.clusters.All() {
		if cluster.ParameterGroup.ParameterGroupName == name {
			return fmt.Errorf("%w: parameter group %s is in use by cluster %s",
				ErrParameterGroupInUse, name, cluster.ClusterName)
		}
	}

	b.paramGroups.Delete(name)

	return nil
}

// buildParameter constructs a Parameter from a name, value, and source.
func buildParameter(name, value, source string) *Parameter {
	return &Parameter{
		ParameterName:  name,
		ParameterValue: value,
		Description:    defaultParameterDescriptions[name],
		Source:         source,
		DataType:       "integer",
		IsModifiable:   "TRUE",
		// ChangeType uses the real DAX enum casing (types.ChangeType:
		// "IMMEDIATE" | "REQUIRES_REBOOT"); the deserializer stores whatever
		// string arrives verbatim, so a mismatched casing like the former
		// "requires-reboot" would never equal types.ChangeTypeRequiresReboot
		// for any client comparing against the SDK's typed constant.
		ChangeType:    "REQUIRES_REBOOT",
		AllowedValues: defaultParameterAllowedValues[name],
		ParameterType: ParameterTypeDefault,
	}
}

// paginateParameters applies pagination to a sorted parameter slice.
func paginateParameters(all []*Parameter, maxResults int, nextToken string) ([]*Parameter, string) {
	start := 0
	if nextToken != "" {
		idx, err := strconv.Atoi(nextToken)
		if err == nil && idx >= 0 && idx < len(all) {
			start = idx
		}
	}

	if start >= len(all) {
		return []*Parameter{}, ""
	}

	end := start + maxResults
	newNextToken := ""
	if end < len(all) {
		newNextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[start:end], newNextToken
}

// DescribeParameters returns the parameters for a specific parameter group with pagination.
// sourceFilter, when non-empty, narrows the result to parameters whose Source
// ("user" or "system") matches -- mirrors the real DAX API's Source request field.
func (b *InMemoryBackend) DescribeParameters(
	paramGroupName string,
	maxResults int,
	nextToken string,
	sourceFilter string,
) ([]*Parameter, string, error) {
	if paramGroupName == "" {
		return nil, "", fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	if maxResults <= 0 {
		maxResults = maxPageSizeDefault
	}

	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	pg, ok := b.paramGroups.Get(paramGroupName)
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrParameterGroupNotFound, paramGroupName)
	}

	params := make([]*Parameter, 0, len(pg.Parameters))

	for name, value := range pg.Parameters {
		source := "user"
		if def, isDefault := defaultParameterValues[name]; isDefault && value == def {
			source = "system"
		}

		if sourceFilter != "" && sourceFilter != source {
			continue
		}

		params = append(params, buildParameter(name, value, source))
	}

	sort.Slice(params, func(i, j int) bool {
		return params[i].ParameterName < params[j].ParameterName
	})

	page, token := paginateParameters(params, maxResults, nextToken)

	return page, token, nil
}

// DescribeDefaultParameters returns the default DAX 1.0 parameter definitions with pagination.
func (b *InMemoryBackend) DescribeDefaultParameters(maxResults int, nextToken string) ([]*Parameter, string, error) {
	if maxResults <= 0 {
		maxResults = maxPageSizeDefault
	}

	params := make([]*Parameter, 0, len(defaultParameterValues))

	for name, value := range defaultParameterValues {
		params = append(params, buildParameter(name, value, "system"))
	}

	sort.Slice(params, func(i, j int) bool {
		return params[i].ParameterName < params[j].ParameterName
	})

	page, token := paginateParameters(params, maxResults, nextToken)

	return page, token, nil
}

// ResetParameterGroup resets parameter group parameters to defaults.
func (b *InMemoryBackend) ResetParameterGroup(name string, parameterNames []string) (*ParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ParameterGroupName is required", ErrParameterGroupNotFound)
	}

	b.mu.Lock("ResetParameterGroup")
	defer b.mu.Unlock()

	pg, ok := b.paramGroups.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrParameterGroupNotFound, name)
	}

	if len(parameterNames) == 0 {
		// Reset all to defaults.
		maps.Copy(pg.Parameters, defaultParameterValues)
	} else {
		for _, pname := range parameterNames {
			if def, found := defaultParameterValues[pname]; found {
				pg.Parameters[pname] = def
			}
		}
	}

	b.emitEventLocked(name, EventSourceTypeParameterGroup,
		fmt.Sprintf("Parameter group %s reset to defaults.", name))

	return paramGroupCopy(pg), nil
}

// paramGroupCopy returns a deep copy of a ParameterGroup.
func paramGroupCopy(pg *ParameterGroup) *ParameterGroup {
	cp := *pg
	cp.Parameters = make(map[string]string, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return &cp
}
