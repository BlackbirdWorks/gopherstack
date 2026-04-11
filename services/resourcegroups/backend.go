package resourcegroups

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a resource group is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource group already exists.
	ErrAlreadyExists = awserr.New("BadRequestException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when request validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrTagSyncTaskNotFound is returned when a tag-sync task is not found.
	ErrTagSyncTaskNotFound = awserr.New("NotFoundException: tag-sync task not found", awserr.ErrNotFound)
)

// ResourceQuery represents a tag-based resource query for a group.
type ResourceQuery struct {
	Type  string `json:"Type"`
	Query string `json:"Query"`
}

// Group represents a Resource Group.
// Field names use PascalCase JSON tags to match what the AWS SDK expects in responses.
type Group struct {
	Tags          *tags.Tags     `json:"Tags,omitempty"`
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	Name          string         `json:"Name"`
	ARN           string         `json:"GroupArn"`
	Description   string         `json:"Description"`
}

// GroupConfigurationParameter is a key-value parameter for a group configuration item.
type GroupConfigurationParameter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// GroupConfigurationItem is a single configuration item for a group.
type GroupConfigurationItem struct {
	Type       string                        `json:"Type"`
	Parameters []GroupConfigurationParameter `json:"Parameters,omitempty"`
}

// AccountSettings holds account-level settings for Resource Groups.
type AccountSettings struct {
	GroupLifecycleEventsDesiredStatus string `json:"GroupLifecycleEventsDesiredStatus,omitempty"`
	GroupLifecycleEventsStatus        string `json:"GroupLifecycleEventsStatus,omitempty"`
	GroupLifecycleEventsStatusMessage string `json:"GroupLifecycleEventsStatusMessage,omitempty"`
}

// TagSyncTask represents a tag-sync task for an application group.
type TagSyncTask struct {
	CreatedAt     time.Time      `json:"CreatedAt"`
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	ErrorMessage  string         `json:"ErrorMessage,omitempty"`
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
	Status        string         `json:"Status"`
}

// ResourceIdentifier holds an ARN and resource type.
type ResourceIdentifier struct {
	ResourceArn  string `json:"ResourceArn,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}

// GroupingStatusItem holds the grouping/ungrouping status for a resource.
type GroupingStatusItem struct {
	UpdatedAt    time.Time `json:"UpdatedAt"`
	ErrorCode    string    `json:"ErrorCode,omitempty"`
	ErrorMessage string    `json:"ErrorMessage,omitempty"`
	ResourceArn  string    `json:"ResourceArn,omitempty"`
	Action       string    `json:"Action,omitempty"`
	Status       string    `json:"Status,omitempty"`
}

// TagSyncTaskStatus constants.
const (
	tagSyncTaskStatusActive = "ACTIVE"
)

// InMemoryBackend is the in-memory store for Resource Groups.
type InMemoryBackend struct {
	groups              map[string]*Group
	arnIndex            map[string]string // ARN → group name
	groupConfigurations map[string][]GroupConfigurationItem
	groupResources      map[string][]string // group name → []resourceARN
	groupingStatuses    map[string][]GroupingStatusItem
	tagSyncTasks        map[string]*TagSyncTask // taskARN → task
	mu                  *lockmetrics.RWMutex
	accountSettings     AccountSettings
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		groups:              make(map[string]*Group),
		arnIndex:            make(map[string]string),
		groupConfigurations: make(map[string][]GroupConfigurationItem),
		groupResources:      make(map[string][]string),
		groupingStatuses:    make(map[string][]GroupingStatusItem),
		tagSyncTasks:        make(map[string]*TagSyncTask),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("resourcegroups"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateGroup creates a new resource group.
// The Tags field in the returned Group points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) CreateGroup(
	name, description string,
	resourceQuery *ResourceQuery,
	inputTags *tags.Tags,
) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; ok {
		return nil, fmt.Errorf("%w: group %s already exists", ErrAlreadyExists, name)
	}

	groupARN := arn.Build("resource-groups", b.region, b.accountID, "group/"+name)

	// Clone caller-provided tags into a backend-owned collection so that the
	// caller cannot mutate backend state by keeping a reference to inputTags.
	var backendTags *tags.Tags
	if inputTags == nil {
		backendTags = tags.New("rg." + name + ".tags")
	} else {
		backendTags = tags.FromMap("rg."+name+".tags", inputTags.Clone())
	}

	g := &Group{Name: name, ARN: groupARN, Description: description, Tags: backendTags, ResourceQuery: resourceQuery}
	b.groups[name] = g
	b.arnIndex[groupARN] = name

	cp := *g

	return &cp, nil
}

// DeleteGroup deletes a resource group by name or ARN.
func (b *InMemoryBackend) DeleteGroup(nameOrARN string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	name := nameOrARN
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		name = nameOrARN[idx+len("group/"):]
	}

	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	g := b.groups[name]
	delete(b.arnIndex, g.ARN)
	g.Tags.Close()
	delete(b.groups, name)

	return nil
}

// ListGroups returns all resource groups.
func (b *InMemoryBackend) ListGroups() []Group {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	out := make([]Group, 0, len(b.groups))
	for _, g := range b.groups {
		cp := *g
		cp.Tags = nil
		out = append(out, cp)
	}

	return out
}

// GetTagsByARN returns the tags for the resource group identified by ARN.
func (b *InMemoryBackend) GetTagsByARN(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTagsByARN")
	defer b.mu.RUnlock()

	g := b.findByARN(resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	return g.Tags.Clone(), nil
}

// AddTagsByARN merges newTags into the resource group identified by ARN and
// returns the resulting tag set.
func (b *InMemoryBackend) AddTagsByARN(resourceARN string, newTags map[string]string) (map[string]string, error) {
	b.mu.Lock("AddTagsByARN")
	defer b.mu.Unlock()

	g := b.findByARN(resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.Merge(newTags)

	return g.Tags.Clone(), nil
}

// RemoveTagsByARN removes the specified tag keys from the resource group
// identified by ARN.
func (b *InMemoryBackend) RemoveTagsByARN(resourceARN string, keys []string) error {
	b.mu.Lock("RemoveTagsByARN")
	defer b.mu.Unlock()

	g := b.findByARN(resourceARN)
	if g == nil {
		return fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.DeleteKeys(keys)

	return nil
}

// findByARN looks up a group by its ARN (must be called under a lock).
func (b *InMemoryBackend) findByARN(resourceARN string) *Group {
	name, ok := b.arnIndex[resourceARN]
	if !ok {
		return nil
	}

	return b.groups[name]
}

// GetGroup returns a resource group by name or ARN.
// The Tags field in the returned Group points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) GetGroup(nameOrARN string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	// Support ARN-based lookup: extract the group name from the ARN suffix.
	// e.g. "arn:aws:resource-groups:us-east-1:123:group/my-group" → "my-group"
	name := nameOrARN
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		name = nameOrARN[idx+len("group/"):]
	}

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	cp := *g

	return &cp, nil
}

// UpdateGroup updates the description of a resource group identified by name or ARN.
func (b *InMemoryBackend) UpdateGroup(nameOrARN, description string) (*Group, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	name := nameOrARN
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		name = nameOrARN[idx+len("group/"):]
	}

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	g.Description = description
	cp := *g

	return &cp, nil
}

// UpdateGroupQuery updates the resource query of a resource group identified by name or ARN.
func (b *InMemoryBackend) UpdateGroupQuery(nameOrARN string, query *ResourceQuery) (*Group, error) {
	b.mu.Lock("UpdateGroupQuery")
	defer b.mu.Unlock()

	name := nameOrARN
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		name = nameOrARN[idx+len("group/"):]
	}

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	g.ResourceQuery = query
	cp := *g

	return &cp, nil
}

// Reset clears all in-memory state. It closes all group Tags to release
// Prometheus metrics before discarding the groups map.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, g := range b.groups {
		if g.Tags != nil {
			g.Tags.Close()
		}
	}

	b.groups = make(map[string]*Group)
	b.arnIndex = make(map[string]string)
	b.groupConfigurations = make(map[string][]GroupConfigurationItem)
	b.groupResources = make(map[string][]string)
	b.groupingStatuses = make(map[string][]GroupingStatusItem)
	b.tagSyncTasks = make(map[string]*TagSyncTask)
	b.accountSettings = AccountSettings{}
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// resolveGroupName extracts the group name from a name-or-ARN value.
func resolveGroupName(nameOrARN string) string {
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		return nameOrARN[idx+len("group/"):]
	}

	return nameOrARN
}

// GetAccountSettings returns the account-level settings.
func (b *InMemoryBackend) GetAccountSettings() AccountSettings {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	return b.accountSettings
}

// PutGroupConfiguration stores a configuration for the named group.
func (b *InMemoryBackend) PutGroupConfiguration(nameOrARN string, items []GroupConfigurationItem) error {
	b.mu.Lock("PutGroupConfiguration")
	defer b.mu.Unlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	cp := make([]GroupConfigurationItem, len(items))
	copy(cp, items)
	b.groupConfigurations[name] = cp

	return nil
}

// GetGroupConfigurationItems returns the stored configuration for a group.
func (b *InMemoryBackend) GetGroupConfigurationItems(nameOrARN string) ([]GroupConfigurationItem, error) {
	b.mu.RLock("GetGroupConfigurationItems")
	defer b.mu.RUnlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	items := b.groupConfigurations[name]
	if items == nil {
		return []GroupConfigurationItem{}, nil
	}

	cp := make([]GroupConfigurationItem, len(items))
	copy(cp, items)

	return cp, nil
}

// GroupResources associates a list of resource ARNs with a group.
func (b *InMemoryBackend) GroupResources(nameOrARN string, resourceARNs []string) ([]string, error) {
	b.mu.Lock("GroupResources")
	defer b.mu.Unlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	existing := make(map[string]struct{}, len(b.groupResources[name]))
	for _, a := range b.groupResources[name] {
		existing[a] = struct{}{}
	}

	now := time.Now().UTC()
	var succeeded []string

	for _, a := range resourceARNs {
		if _, dup := existing[a]; !dup {
			b.groupResources[name] = append(b.groupResources[name], a)
			existing[a] = struct{}{}
		}

		succeeded = append(succeeded, a)
		b.groupingStatuses[name] = append(b.groupingStatuses[name], GroupingStatusItem{
			ResourceArn: a,
			Action:      "GROUP",
			Status:      "SUCCESS",
			UpdatedAt:   now,
		})
	}

	if succeeded == nil {
		succeeded = []string{}
	}

	return succeeded, nil
}

// ListGroupResources returns all resource ARNs associated with a group.
func (b *InMemoryBackend) ListGroupResources(nameOrARN string) ([]ResourceIdentifier, error) {
	b.mu.RLock("ListGroupResources")
	defer b.mu.RUnlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	arns := b.groupResources[name]
	out := make([]ResourceIdentifier, 0, len(arns))

	for _, a := range arns {
		out = append(out, ResourceIdentifier{ResourceArn: a})
	}

	return out, nil
}

// ListGroupingStatuses returns the grouping/ungrouping status history for a group.
func (b *InMemoryBackend) ListGroupingStatuses(nameOrARN string) ([]GroupingStatusItem, error) {
	b.mu.RLock("ListGroupingStatuses")
	defer b.mu.RUnlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	statuses := b.groupingStatuses[name]
	out := make([]GroupingStatusItem, len(statuses))
	copy(out, statuses)

	return out, nil
}

// SearchResources returns resource identifiers that were grouped into any group.
// In this in-memory mock, it returns all resources from all groups as a basic implementation.
func (b *InMemoryBackend) SearchResources(_ *ResourceQuery) ([]ResourceIdentifier, error) {
	b.mu.RLock("SearchResources")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	var out []ResourceIdentifier

	for _, arns := range b.groupResources {
		for _, a := range arns {
			if _, ok := seen[a]; !ok {
				seen[a] = struct{}{}
				out = append(out, ResourceIdentifier{ResourceArn: a})
			}
		}
	}

	if out == nil {
		out = []ResourceIdentifier{}
	}

	return out, nil
}

// StartTagSyncTask creates a new tag-sync task for an application group.
func (b *InMemoryBackend) StartTagSyncTask(
	nameOrARN, roleARN, tagKey, tagValue string,
	resourceQuery *ResourceQuery,
) (*TagSyncTask, error) {
	b.mu.Lock("StartTagSyncTask")
	defer b.mu.Unlock()

	name := resolveGroupName(nameOrARN)

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	taskARN := arn.Build(
		"resource-groups",
		b.region,
		b.accountID,
		"tag-sync-task/"+name+"-"+time.Now().Format("20060102150405"),
	)

	task := &TagSyncTask{
		TaskArn:       taskARN,
		GroupArn:      g.ARN,
		GroupName:     name,
		RoleArn:       roleARN,
		TagKey:        tagKey,
		TagValue:      tagValue,
		ResourceQuery: resourceQuery,
		Status:        tagSyncTaskStatusActive,
		CreatedAt:     time.Now().UTC(),
	}

	b.tagSyncTasks[taskARN] = task

	cp := *task

	return &cp, nil
}

// CancelTagSyncTask cancels a tag-sync task by ARN.
func (b *InMemoryBackend) CancelTagSyncTask(taskARN string) error {
	b.mu.Lock("CancelTagSyncTask")
	defer b.mu.Unlock()

	if _, ok := b.tagSyncTasks[taskARN]; !ok {
		return fmt.Errorf("%w: task %s not found", ErrTagSyncTaskNotFound, taskARN)
	}

	delete(b.tagSyncTasks, taskARN)

	return nil
}

// GetTagSyncTask returns a tag-sync task by ARN.
func (b *InMemoryBackend) GetTagSyncTask(taskARN string) (*TagSyncTask, error) {
	b.mu.RLock("GetTagSyncTask")
	defer b.mu.RUnlock()

	task, ok := b.tagSyncTasks[taskARN]
	if !ok {
		return nil, fmt.Errorf("%w: task %s not found", ErrTagSyncTaskNotFound, taskARN)
	}

	cp := *task

	return &cp, nil
}

// ListTagSyncTasksFilter holds filter criteria for listing tag-sync tasks.
type ListTagSyncTasksFilter struct {
	GroupArn  string `json:"GroupArn,omitempty"`
	GroupName string `json:"GroupName,omitempty"`
}

// ListTagSyncTasks returns all tag-sync tasks, optionally filtered by group.
func (b *InMemoryBackend) ListTagSyncTasks(filters []ListTagSyncTasksFilter) ([]TagSyncTask, error) {
	b.mu.RLock("ListTagSyncTasks")
	defer b.mu.RUnlock()

	var out []TagSyncTask

	for _, task := range b.tagSyncTasks {
		if len(filters) == 0 {
			cp := *task
			out = append(out, cp)

			continue
		}

		for _, f := range filters {
			if (f.GroupArn == "" || f.GroupArn == task.GroupArn) &&
				(f.GroupName == "" || f.GroupName == task.GroupName) {
				cp := *task
				out = append(out, cp)

				break
			}
		}
	}

	if out == nil {
		out = []TagSyncTask{}
	}

	return out, nil
}
