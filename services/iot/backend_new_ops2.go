package iot

import (
	"fmt"
	"slices"
)

// ---------------------------------------------------------------------------
// UpdateThingType
// ---------------------------------------------------------------------------

// UpdateThingType updates the description of an existing ThingType.
func (b *InMemoryBackend) UpdateThingType(thingTypeName, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tt, ok := b.thingTypes[thingTypeName]
	if !ok {
		return fmt.Errorf("thing type %q not found: %w", thingTypeName, ErrResourceNotFound)
	}

	tt.Description = description

	return nil
}

// ---------------------------------------------------------------------------
// DeleteCommandExecution
// ---------------------------------------------------------------------------

// DeleteCommandExecution deletes a command execution record.
func (b *InMemoryBackend) DeleteCommandExecution(commandID, executionID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := b.commandExecutionKey(commandID, executionID)
	if _, ok := b.commandExecutions[key]; !ok {
		return fmt.Errorf("command execution %q/%q not found: %w", commandID, executionID, ErrResourceNotFound)
	}

	delete(b.commandExecutions, key)

	return nil
}

// ---------------------------------------------------------------------------
// UpdateThingGroupsForThing
// ---------------------------------------------------------------------------

// UpdateThingGroupsForThing adds/removes a thing from the given thing groups.
func (b *InMemoryBackend) UpdateThingGroupsForThing(
	thingName string,
	thingGroupsToAdd []string,
	thingGroupsToRemove []string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, g := range thingGroupsToAdd {
		b.addThingToGroupByName(thingName, g)

		existing := b.thingThingGroups[thingName]
		if !slices.Contains(existing, g) {
			b.thingThingGroups[thingName] = append(existing, g)
		}
	}

	for _, g := range thingGroupsToRemove {
		members := b.thingGroupMembers[g]
		filtered := make([]string, 0, len(members))
		for _, m := range members {
			if m != thingName {
				filtered = append(filtered, m)
			}
		}
		b.thingGroupMembers[g] = filtered

		groups := b.thingThingGroups[thingName]
		filteredG := make([]string, 0, len(groups))
		for _, gr := range groups {
			if gr != g {
				filteredG = append(filteredG, gr)
			}
		}
		b.thingThingGroups[thingName] = filteredG
	}

	return nil
}

// ---------------------------------------------------------------------------
// DisassociateSbomFromPackageVersion
// ---------------------------------------------------------------------------

// DisassociateSbomFromPackageVersion removes the SBOM association from a package version.
func (b *InMemoryBackend) DisassociateSbomFromPackageVersion(packageName, versionName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := packageVersionKey(packageName, versionName)
	delete(b.packageVersionSboms, key)

	return nil
}
