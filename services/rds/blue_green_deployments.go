package rds

import (
	"fmt"
	"net/url"
	"slices"
)

// CreateBlueGreenDeployment creates a new Blue/Green Deployment.
func (b *InMemoryBackend) CreateBlueGreenDeployment(
	name, source string,
) (*BlueGreenDeployment, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: BlueGreenDeploymentName must not be empty", ErrInvalidParameter)
	}
	if source == "" {
		return nil, fmt.Errorf("%w: Source must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CreateBlueGreenDeployment")
	defer b.mu.Unlock()

	id := "bgd-" + name

	if _, exists := b.blueGreenDeployments.Get(id); exists {
		return nil, fmt.Errorf(
			"%w: Blue/Green Deployment %s already exists",
			ErrBlueGreenDeploymentAlreadyExists,
			name,
		)
	}

	target := source + "-green"
	deployment := &BlueGreenDeployment{
		BlueGreenDeploymentIdentifier: id,
		BlueGreenDeploymentName:       name,
		Source:                        source,
		Target:                        target,
		Status:                        blueGreenDeploymentStatusAvailable,
	}
	b.blueGreenDeployments.Put(deployment)

	cp := *deployment

	return &cp, nil
}

// DescribeBlueGreenDeployments returns blue/green deployments, optionally by ID.
func (b *InMemoryBackend) DescribeBlueGreenDeployments(id string) ([]BlueGreenDeployment, error) {
	b.mu.RLock("DescribeBlueGreenDeployments")
	defer b.mu.RUnlock()
	if id != "" {
		d, exists := b.blueGreenDeployments.Get(id)
		if !exists {
			return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
		}
		cp := *d

		return []BlueGreenDeployment{cp}, nil
	}
	result := make([]BlueGreenDeployment, 0, b.blueGreenDeployments.Len())
	for _, d := range b.blueGreenDeployments.All() {
		result = append(result, *d)
	}
	slices.SortFunc(result, func(a, b BlueGreenDeployment) int {
		if a.BlueGreenDeploymentIdentifier < b.BlueGreenDeploymentIdentifier {
			return -1
		}
		if a.BlueGreenDeploymentIdentifier > b.BlueGreenDeploymentIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// isKnownBlueGreenDeploymentFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for DescribeBlueGreenDeployments
// (rds@v1.124.1 api_op_DescribeBlueGreenDeployments.go:46-64).
func isKnownBlueGreenDeploymentFilterName(name string) bool {
	switch name {
	case filterNameBlueGreenDeploymentIdentifier, filterNameBlueGreenDeploymentName,
		filterNameSource, filterNameTarget:
		return true
	default:
		return false
	}
}

// applyBlueGreenDeploymentFilters narrows deployments per the AWS
// DescribeBlueGreenDeployments Filters contract: each filter ANDs together,
// and a filter's Values list is OR-matched against the corresponding
// deployment field. An unrecognized filter name returns
// InvalidParameterValue, matching real AWS.
func applyBlueGreenDeploymentFilters(
	vals url.Values, deployments []BlueGreenDeployment,
) ([]BlueGreenDeployment, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return deployments, nil
	}

	for name := range filters {
		if !isKnownBlueGreenDeploymentFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]BlueGreenDeployment, 0, len(deployments))
	for _, d := range deployments {
		if matchesAllBlueGreenDeploymentFilters(d, filters) {
			filtered = append(filtered, d)
		}
	}

	return filtered, nil
}

func matchesAllBlueGreenDeploymentFilters(d BlueGreenDeployment, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameBlueGreenDeploymentIdentifier:
			if !slices.Contains(values, d.BlueGreenDeploymentIdentifier) {
				return false
			}
		case filterNameBlueGreenDeploymentName:
			if !slices.Contains(values, d.BlueGreenDeploymentName) {
				return false
			}
		case filterNameSource:
			if !slices.Contains(values, d.Source) {
				return false
			}
		case filterNameTarget:
			if !slices.Contains(values, d.Target) {
				return false
			}
		}
	}

	return true
}

// DeleteBlueGreenDeployment deletes the named blue/green deployment.
func (b *InMemoryBackend) DeleteBlueGreenDeployment(id string) (*BlueGreenDeployment, error) {
	b.mu.Lock("DeleteBlueGreenDeployment")
	defer b.mu.Unlock()
	d, exists := b.blueGreenDeployments.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
	}
	cp := *d
	b.blueGreenDeployments.Delete(id)

	return &cp, nil
}

// SwitchoverBlueGreenDeployment switches over a blue/green deployment.
func (b *InMemoryBackend) SwitchoverBlueGreenDeployment(id string) (*BlueGreenDeployment, error) {
	b.mu.Lock("SwitchoverBlueGreenDeployment")
	defer b.mu.Unlock()
	d, exists := b.blueGreenDeployments.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: blue/green deployment %s not found", ErrBlueGreenDeploymentNotFound, id)
	}
	d.Status = "switchover-completed"
	cp := *d

	return &cp, nil
}
