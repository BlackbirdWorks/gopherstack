package ec2

import (
	"fmt"
	"slices"
	"sort"
)

// DeleteLaunchTemplate removes a launch template by ID.
func (b *InMemoryBackend) DeleteLaunchTemplate(id string) error {
	if id == "" {
		return fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLaunchTemplate")
	defer b.mu.Unlock()

	if _, ok := b.launchTemplates.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}
	b.launchTemplates.Delete(id)

	return nil
}

// DescribeLaunchTemplateVersions returns versions of a specific launch template.
// In this mock, every template has exactly one version.
func (b *InMemoryBackend) DescribeLaunchTemplateVersions(id string) ([]*LaunchTemplate, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeLaunchTemplateVersions")
	defer b.mu.RUnlock()

	lt, ok := b.launchTemplates.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}

	cp := *lt

	return []*LaunchTemplate{&cp}, nil
}

// ---- VPC endpoint delete ----

// DescribeLaunchTemplatesSorted returns launch templates sorted by ID.
func (b *InMemoryBackend) DescribeLaunchTemplatesSorted(names []string) []*LaunchTemplate {
	ts := b.DescribeLaunchTemplates(names)
	sort.Slice(ts, func(i, j int) bool {
		return ts[i].ID < ts[j].ID
	})

	return ts
}

// DescribeLaunchTemplatesSortedByName returns launch templates sorted by name (no filter).
func (b *InMemoryBackend) DescribeLaunchTemplatesSortedByName() []*LaunchTemplate {
	ts := b.DescribeLaunchTemplates(nil)
	slices.SortFunc(ts, func(a, b *LaunchTemplate) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return ts
}
