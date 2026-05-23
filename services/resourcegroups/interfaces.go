package resourcegroups

import "github.com/blackbirdworks/gopherstack/pkgs/tags"

// StorageBackend defines the interface for Resource Groups backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Group CRUD operations.
	CreateGroup(
		name, description string,
		resourceQuery *ResourceQuery,
		inputTags *tags.Tags,
		configuration []GroupConfigurationItem,
	) (*Group, error)
	GetGroup(nameOrARN string) (*Group, error)
	UpdateGroup(nameOrARN, description, displayName string, criticality int) (*Group, error)
	UpdateGroupQuery(nameOrARN string, query *ResourceQuery) (*Group, error)
	DeleteGroup(nameOrARN string) error
	ListGroups(filters []ListGroupsFilter) []Group

	// Tag operations on group resources.
	GetTagsByARN(resourceARN string) (map[string]string, error)
	AddTagsByARN(resourceARN string, newTags map[string]string) (map[string]string, error)
	RemoveTagsByARN(resourceARN string, keys []string) error

	// Account-level settings.
	GetAccountSettings() AccountSettings
	UpdateAccountSettings(desiredStatus string) error

	// Group configuration.
	PutGroupConfiguration(nameOrARN string, items []GroupConfigurationItem) error
	GetGroupConfigurationItems(nameOrARN string) ([]GroupConfigurationItem, error)

	// Resource grouping.
	GroupResources(nameOrARN string, resourceARNs []string) ([]string, error)
	UngroupResources(nameOrARN string, resourceARNs []string) (*UngroupResourcesResult, error)
	ListGroupResources(nameOrARN string) ([]ResourceIdentifier, error)
	ListGroupingStatuses(nameOrARN string) ([]GroupingStatusItem, error)
	SearchResources(q *ResourceQuery) ([]ResourceIdentifier, error)

	// Tag-sync tasks.
	StartTagSyncTask(
		nameOrARN, roleARN, tagKey, tagValue string,
		resourceQuery *ResourceQuery,
	) (*TagSyncTask, error)
	CancelTagSyncTask(taskARN string) error
	GetTagSyncTask(taskARN string) (*TagSyncTask, error)
	ListTagSyncTasks(filters []ListTagSyncTasksFilter) ([]TagSyncTask, error)

	// Lifecycle.
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
