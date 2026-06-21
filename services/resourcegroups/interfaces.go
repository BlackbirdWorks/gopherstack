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
	// ListGroups returns groups sorted by name with optional filtering and pagination.
	// Returns the page of groups and a continuation token (empty when exhausted).
	ListGroups(ctx context.Context, filters []ListGroupsFilter, nextToken string, maxResults int) ([]Group, string)

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
	// ListGroupResources returns resource identifiers for a group with optional filtering and pagination.
	// Returns identifiers, a continuation token (empty when exhausted), and any error.
	ListGroupResources(
		ctx context.Context,
		nameOrARN string,
		filters []ListGroupResourcesFilter,
		nextToken string,
		maxResults int,
	) ([]ResourceIdentifier, string, error)
	// ListGroupingStatuses returns grouping/ungrouping status history with optional pagination.
	// Returns statuses, a continuation token (empty when exhausted), and any error.
	ListGroupingStatuses(
		ctx context.Context,
		nameOrARN string,
		nextToken string,
		maxResults int,
	) ([]GroupingStatusItem, string, error)
	// SearchResources searches grouped resources filtered by the ResourceQuery.
	// Returns identifiers, a continuation token (empty when exhausted), and any error.
	SearchResources(
		ctx context.Context,
		q *ResourceQuery,
		nextToken string,
		maxResults int,
	) ([]ResourceIdentifier, string, error)

	// Tag-sync tasks.
	StartTagSyncTask(
		ctx context.Context,
		nameOrARN, roleARN, tagKey, tagValue string,
		resourceQuery *ResourceQuery,
	) (*TagSyncTask, error)
	CancelTagSyncTask(ctx context.Context, taskARN string) error
	GetTagSyncTask(ctx context.Context, taskARN string) (*TagSyncTask, error)
	// ListTagSyncTasks returns tasks with optional filtering and pagination.
	// Returns tasks, a continuation token (empty when exhausted), and any error.
	ListTagSyncTasks(
		ctx context.Context,
		filters []ListTagSyncTasksFilter,
		nextToken string,
		maxResults int,
	) ([]TagSyncTask, string, error)

	// Lifecycle.
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
