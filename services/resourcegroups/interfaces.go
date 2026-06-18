package resourcegroups

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// StorageBackend defines the interface for Resource Groups backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Group CRUD operations.
	CreateGroup(
		ctx context.Context,
		name, description string,
		resourceQuery *ResourceQuery,
		inputTags *tags.Tags,
		configuration []GroupConfigurationItem,
	) (*Group, error)
	GetGroup(ctx context.Context, nameOrARN string) (*Group, error)
	UpdateGroup(ctx context.Context, nameOrARN, description, displayName string, criticality int) (*Group, error)
	UpdateGroupQuery(ctx context.Context, nameOrARN string, query *ResourceQuery) (*Group, error)
	DeleteGroup(ctx context.Context, nameOrARN string) error
	ListGroups(ctx context.Context, filters []ListGroupsFilter) []Group

	// Tag operations on group resources.
	GetTagsByARN(ctx context.Context, resourceARN string) (map[string]string, error)
	AddTagsByARN(ctx context.Context, resourceARN string, newTags map[string]string) (map[string]string, error)
	RemoveTagsByARN(ctx context.Context, resourceARN string, keys []string) error

	// Account-level settings (not region-scoped).
	GetAccountSettings() AccountSettings
	UpdateAccountSettings(desiredStatus string) error

	// Group configuration.
	PutGroupConfiguration(ctx context.Context, nameOrARN string, items []GroupConfigurationItem) error
	GetGroupConfigurationItems(ctx context.Context, nameOrARN string) ([]GroupConfigurationItem, error)

	// Resource grouping.
	GroupResources(ctx context.Context, nameOrARN string, resourceARNs []string) ([]string, error)
	UngroupResources(ctx context.Context, nameOrARN string, resourceARNs []string) (*UngroupResourcesResult, error)
	ListGroupResources(ctx context.Context, nameOrARN string) ([]ResourceIdentifier, error)
	ListGroupingStatuses(ctx context.Context, nameOrARN string) ([]GroupingStatusItem, error)
	SearchResources(ctx context.Context, q *ResourceQuery) ([]ResourceIdentifier, error)

	// Tag-sync tasks.
	StartTagSyncTask(
		ctx context.Context,
		nameOrARN, roleARN, tagKey, tagValue string,
		resourceQuery *ResourceQuery,
	) (*TagSyncTask, error)
	CancelTagSyncTask(ctx context.Context, taskARN string) error
	GetTagSyncTask(ctx context.Context, taskARN string) (*TagSyncTask, error)
	ListTagSyncTasks(ctx context.Context, filters []ListTagSyncTasksFilter) ([]TagSyncTask, error)

	// Lifecycle.
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
