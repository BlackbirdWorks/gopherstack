package ec2

import (
	"fmt"
	"slices"
	"sort"
)

// GetLaunchTemplate returns a copy of the LaunchTemplate matching idOrName (by ID
// first, then by Name), with ImageID/InstanceType resolved to the requested version
// -- a version number, "$Latest", "$Default", or "" (meaning "$Default") -- per
// resolveLaunchTemplateVersion. Returns (nil, ErrLaunchTemplateNotFound) if idOrName
// doesn't match a template, or (nil, ErrLaunchTemplateVersionNotFound) if version
// doesn't match one of its recorded versions.
func (b *InMemoryBackend) GetLaunchTemplate(idOrName, version string) (*LaunchTemplate, error) {
	if idOrName == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId or LaunchTemplateName is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetLaunchTemplate")
	defer b.mu.RUnlock()

	lt := b.findLaunchTemplateLocked(idOrName)
	if lt == nil {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, idOrName)
	}

	ver, err := resolveLaunchTemplateVersion(lt, version)
	if err != nil {
		return nil, err
	}

	cp := *lt
	cp.ImageID = ver.ImageID
	cp.InstanceType = ver.InstanceType

	return &cp, nil
}

// findLaunchTemplateLocked looks up a launch template by ID first, then by
// Name, returning the live (not copied) record. Must be called with b.mu held.
func (b *InMemoryBackend) findLaunchTemplateLocked(idOrName string) *LaunchTemplate {
	if lt, ok := b.launchTemplates.Get(idOrName); ok {
		return lt
	}

	for _, lt := range b.launchTemplates.All() {
		if lt.Name == idOrName {
			return lt
		}
	}

	return nil
}

// DeleteLaunchTemplate removes a launch template by ID and returns the
// deleted template.
func (b *InMemoryBackend) DeleteLaunchTemplate(id string) (*LaunchTemplate, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLaunchTemplate")
	defer b.mu.Unlock()

	lt, ok := b.launchTemplates.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, id)
	}
	cp := *lt
	b.launchTemplates.Delete(id)
	delete(b.tags, id)

	return &cp, nil
}

// DescribeLaunchTemplateVersions returns the current state of a specific launch
// template as a single item. lt.Versions now holds real per-version history (see
// GetLaunchTemplate/resolveLaunchTemplateVersion), but this op's own handler
// (handleDescribeLaunchTemplateVersions) still only surfaces this one merged view --
// expanding it to the full Versions list is a separate, larger wire-shape change.
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
