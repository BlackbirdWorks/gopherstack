package scheduler

// StorageBackend defines the interface for EventBridge Scheduler backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Schedule operations
	CreateSchedule(
		name, groupName, expr, description, timezone string,
		target Target,
		state string,
		ftw FlexibleTimeWindow,
		opts ...ScheduleOption,
	) (*Schedule, error)
	GetSchedule(name, groupName string) (*Schedule, error)
	ListSchedules(groupName, namePrefix, state string) []*Schedule
	DeleteSchedule(name, groupName string) error
	UpdateSchedule(
		name, groupName, expr, description, timezone string,
		target Target,
		state string,
		ftw FlexibleTimeWindow,
		opts ...ScheduleOption,
	) (*Schedule, error)

	// Schedule group operations
	CreateScheduleGroup(name, description string, initialTags map[string]string) (*ScheduleGroup, error)
	GetScheduleGroup(name string) (*ScheduleGroup, error)
	DeleteScheduleGroup(name string) error
	ListScheduleGroups(namePrefix string) []*ScheduleGroup

	// Tag operations
	TagResource(resourceARN string, kv map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
