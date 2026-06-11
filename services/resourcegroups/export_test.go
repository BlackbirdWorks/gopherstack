package resourcegroups

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// GroupCount returns the total number of groups across all regions (for white-box testing).
func GroupCount(b *InMemoryBackend) int {
	b.mu.RLock("GroupCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionGroups := range b.groups {
		total += len(regionGroups)
	}

	return total
}

// TagSyncTaskCount returns the total number of tag-sync tasks across all regions.
func TagSyncTaskCount(b *InMemoryBackend) int {
	b.mu.RLock("TagSyncTaskCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionTasks := range b.tagSyncTasks {
		total += len(regionTasks)
	}

	return total
}

// GroupResourceCount returns the total number of resource ARNs stored across all groups and regions.
func GroupResourceCount(b *InMemoryBackend) int {
	b.mu.RLock("GroupResourceCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionResources := range b.groupResources {
		for _, arns := range regionResources {
			total += len(arns)
		}
	}

	return total
}

// GroupConfigurationCount returns the number of groups that have a stored configuration (across all regions).
func GroupConfigurationCount(b *InMemoryBackend) int {
	b.mu.RLock("GroupConfigurationCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionConfigs := range b.groupConfigurations {
		total += len(regionConfigs)
	}

	return total
}

// HandlerOpsLen returns the number of pre-built dispatch operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// AddGroupInternal inserts a group directly into the backend's default region for test seeding,
// bypassing all validation. It is intended for use only in tests.
func AddGroupInternal(b *InMemoryBackend, name, description string) *Group {
	b.mu.Lock("AddGroupInternal")
	defer b.mu.Unlock()

	region := b.region
	groupARN := "arn:aws:resource-groups:" + region + ":" + b.accountID + ":group/" + name
	g := &Group{
		Name:        name,
		ARN:         groupARN,
		Description: description,
		Tags:        tags.New("rg." + name + ".tags"),
	}

	if b.groups[region] == nil {
		b.groups[region] = make(map[string]*Group)
	}

	b.groups[region][name] = g

	if b.arnIndex[region] == nil {
		b.arnIndex[region] = make(map[string]string)
	}

	b.arnIndex[region][groupARN] = name

	cp := *g

	return &cp
}
