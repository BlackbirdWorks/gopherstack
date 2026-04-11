package resourcegroups

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// GroupCount returns the number of groups in the backend (for white-box testing).
func GroupCount(b *InMemoryBackend) int {
	b.mu.RLock("GroupCount")
	defer b.mu.RUnlock()

	return len(b.groups)
}

// TagSyncTaskCount returns the number of tag-sync tasks in the backend.
func TagSyncTaskCount(b *InMemoryBackend) int {
	b.mu.RLock("TagSyncTaskCount")
	defer b.mu.RUnlock()

	return len(b.tagSyncTasks)
}

// GroupResourceCount returns the total number of resource ARNs stored across all groups.
func GroupResourceCount(b *InMemoryBackend) int {
	b.mu.RLock("GroupResourceCount")
	defer b.mu.RUnlock()

	total := 0
	for _, arns := range b.groupResources {
		total += len(arns)
	}

	return total
}

// GroupConfigurationCount returns the number of groups that have a stored configuration.
func GroupConfigurationCount(b *InMemoryBackend) int {
	b.mu.RLock("GroupConfigurationCount")
	defer b.mu.RUnlock()

	return len(b.groupConfigurations)
}

// HandlerOpsLen returns the number of pre-built dispatch operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// AddGroupInternal inserts a group directly into the backend for test seeding,
// bypassing all validation. It is intended for use only in tests.
func AddGroupInternal(b *InMemoryBackend, name, description string) *Group {
	b.mu.Lock("AddGroupInternal")
	defer b.mu.Unlock()

	groupARN := "arn:aws:resource-groups:us-east-1:" + b.accountID + ":group/" + name
	g := &Group{
		Name:        name,
		ARN:         groupARN,
		Description: description,
		Tags:        tags.New("rg." + name + ".tags"),
	}
	b.groups[name] = g
	b.arnIndex[groupARN] = name

	cp := *g

	return &cp
}
