package resourcegroups

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
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

// Grouping action constants.
const (
	groupingActionGroup   = "GROUP"
	groupingActionUngroup = "UNGROUP"
	groupingStatusSuccess = "SUCCESS"
	groupingStatusFailed  = "FAILED"
)

// GroupingStatus error codes for failed grouping operations.
const (
	groupingErrInvalidARN           = "INVALID_ARN"
	groupingErrResourceNotFound     = "RESOURCE_NOT_FOUND"
)

// TagSyncTask status constants.
const (
	tagSyncTaskStatusActive    = "ACTIVE"
	tagSyncTaskStatusCancelled = "CANCELLED"
)

// tagSyncTaskTTL is the maximum age of a completed or cancelled tag-sync task
// before it is evicted from memory during list operations.
const tagSyncTaskTTL = 24 * time.Hour

// AccountLifecycleEventStatus constants.
const (
	accountLifecycleEventsActive   = "ACTIVE"
	accountLifecycleEventsInactive = "INACTIVE"
)

// groupNameMaxLen and groupDescMaxLen match AWS limits.
const (
	groupNameMaxLen = 300
	groupDescMaxLen = 512
)

// groupNameRe matches valid Resource Groups group names (AWS rule).
var groupNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.−\-]+$`) //nolint:gocritic

// groupNameReservedPrefixes lists prefixes that AWS does not allow for group names.
var groupNameReservedPrefixes = []string{"aws", "AWS"} //nolint:gochecknoglobals

// validResourceQueryTypes lists the only two supported query types.
var validResourceQueryTypes = map[string]bool{ //nolint:gochecknoglobals
	"TAG_FILTERS_1_0":          true,
	"CLOUDFORMATION_STACK_1_0": true,
}

// ListGroupsFilterName constants for ListGroups Filters field.
const (
	listGroupsFilterConfigurationType = "configuration-type"
	listGroupsFilterResourceType      = "resource-type"
)

// validConfigTypes maps each recognized configuration Type to its allowed
// parameter names.  An empty slice means the type takes no parameters.
var validConfigTypes = map[string][]string{ //nolint:gochecknoglobals
	"AWS::EC2::HostManagement":                   {"allowed-resource-types", "any-of-allowed-resource-types", "deletion-protection"},
	"AWS::EC2::CapacityReservationPool":          {},
	"AWS::ResourceGroups::Generic":               {"allowed-resource-types", "any-of-allowed-resource-types"},
	"AWS::AppRegistry::Application":              {"allowed-resource-types"},
	"AWS::NetworkFirewall::RuleGroup":             {"allowed-resource-types"},
	"AWS::Route53Resolver::FirewallRuleGroup":     {"allowed-resource-types"},
	"AWS::ServiceCatalogAppRegistry::Application": {"allowed-resource-types"},
}

// ResourceQuery represents a tag-based resource query for a group.
type ResourceQuery struct {
	Type  string `json:"Type"`
	Query string `json:"Query"`
}

// Group represents a Resource Group.
// Field names use PascalCase JSON tags to match what the AWS SDK expects in responses.
type Group struct {
	Tags           *tags.Tags        `json:"Tags,omitempty"`
	ResourceQuery  *ResourceQuery    `json:"ResourceQuery,omitempty"`
	ApplicationTag map[string]string `json:"ApplicationTag,omitempty"`
	Name           string            `json:"Name"`
	ARN            string            `json:"GroupArn"`
	Description    string            `json:"Description,omitempty"`
	OwnerId        string            `json:"OwnerId,omitempty"`
	DisplayName    string            `json:"DisplayName,omitempty"`
	Criticality    int               `json:"Criticality,omitempty"`
}

// ListGroupsFilter holds a single filter for the ListGroups operation.
type ListGroupsFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// validateGroupName validates that a group name conforms to AWS naming rules.
func validateGroupName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if len(name) > groupNameMaxLen {
		return fmt.Errorf("%w: Name must be at most %d characters", ErrValidation, groupNameMaxLen)
	}

	if !groupNameRe.MatchString(name) {
		return fmt.Errorf("%w: Name must match pattern [a-zA-Z0-9_.−-]+", ErrValidation)
	}

	nameLower := strings.ToLower(name)
	for _, prefix := range groupNameReservedPrefixes {
		if strings.HasPrefix(nameLower, strings.ToLower(prefix)) {
			return fmt.Errorf("%w: Name must not start with reserved prefix %q", ErrValidation, prefix)
		}
	}

	return nil
}

// validateDescription validates that a description conforms to AWS length rules.
func validateDescription(desc string) error {
	if len(desc) > groupDescMaxLen {
		return fmt.Errorf("%w: Description must be at most %d characters", ErrValidation, groupDescMaxLen)
	}

	return nil
}

// validateResourceQuery validates that a ResourceQuery is well-formed.
func validateResourceQuery(q *ResourceQuery) error {
	if q == nil {
		return nil
	}

	if !validResourceQueryTypes[q.Type] {
		return fmt.Errorf(
			"%w: ResourceQuery.Type must be TAG_FILTERS_1_0 or CLOUDFORMATION_STACK_1_0, got %q",
			ErrValidation,
			q.Type,
		)
	}

	if q.Query == "" {
		return fmt.Errorf("%w: ResourceQuery.Query must be a non-empty JSON string", ErrValidation)
	}

	var raw json.RawMessage
	if err := json.Unmarshal([]byte(q.Query), &raw); err != nil {
		return fmt.Errorf("%w: ResourceQuery.Query is not valid JSON: %s", ErrValidation, err.Error())
	}

	return nil
}

// validateConfiguration validates each GroupConfigurationItem against the allow-list.
func validateConfiguration(items []GroupConfigurationItem) error {
	for _, item := range items {
		allowedParams, ok := validConfigTypes[item.Type]
		if !ok {
			return fmt.Errorf(
				"%w: unsupported configuration type %q; must be one of AWS::EC2::HostManagement, "+
					"AWS::EC2::CapacityReservationPool, AWS::ResourceGroups::Generic, "+
					"AWS::AppRegistry::Application, etc.",
				ErrValidation,
				item.Type,
			)
		}

		if len(allowedParams) == 0 && len(item.Parameters) > 0 {
			return fmt.Errorf(
				"%w: configuration type %q does not accept any parameters",
				ErrValidation,
				item.Type,
			)
		}

		allowed := make(map[string]bool, len(allowedParams))
		for _, p := range allowedParams {
			allowed[p] = true
		}

		for _, param := range item.Parameters {
			if !allowed[param.Name] {
				return fmt.Errorf(
					"%w: parameter %q is not valid for configuration type %q",
					ErrValidation,
					param.Name,
					item.Type,
				)
			}
		}
	}

	return nil
}

// validateTagKeys validates that no reserved aws: prefix tag keys are present.
func validateTagKeys(tagMap map[string]string) error {
	for k := range tagMap {
		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return fmt.Errorf(
				"%w: tag key %q uses the reserved prefix \"aws:\"; these keys are managed by AWS",
				ErrValidation,
				k,
			)
		}
	}

	return nil
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
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
	Status        string         `json:"Status"`
	ErrorMessage  string         `json:"ErrorMessage,omitempty"`
}

// ResourceIdentifier holds an ARN and resource type.
type ResourceIdentifier struct {
	ResourceArn  string `json:"ResourceArn,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}

// GroupingStatusItem holds the grouping/ungrouping status for a resource.
type GroupingStatusItem struct {
	UpdatedAt    time.Time `json:"UpdatedAt"`
	ResourceArn  string    `json:"ResourceArn,omitempty"`
	Action       string    `json:"Action,omitempty"`
	Status       string    `json:"Status,omitempty"`
	ErrorCode    string    `json:"ErrorCode,omitempty"`
	ErrorMessage string    `json:"ErrorMessage,omitempty"`
}

// ListTagSyncTasksFilter holds filter criteria for listing tag-sync tasks.
type ListTagSyncTasksFilter struct {
	GroupArn  string `json:"GroupArn,omitempty"`
	GroupName string `json:"GroupName,omitempty"`
}

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

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

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

// resolveGroupName extracts the group name from a name-or-ARN value.
func resolveGroupName(nameOrARN string) string {
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		return nameOrARN[idx+len("group/"):]
	}

	return nameOrARN
}

// CreateGroup creates a new resource group.
// The Tags field in the returned Group points to a fresh Tags copy; it is
// safe to read but callers should not pass it back to mutation methods.
func (b *InMemoryBackend) CreateGroup(
	name, description string,
	resourceQuery *ResourceQuery,
	inputTags *tags.Tags,
) (*Group, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

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

// GetGroup returns a resource group by name or ARN.
func (b *InMemoryBackend) GetGroup(nameOrARN string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	name := resolveGroupName(nameOrARN)

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

	name := resolveGroupName(nameOrARN)

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

	name := resolveGroupName(nameOrARN)

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	g.ResourceQuery = query
	cp := *g

	return &cp, nil
}

// DeleteGroup deletes a resource group by name or ARN.
// It cascades to remove all associated resources, configurations,
// grouping-status records, and tag-sync tasks for the group.
func (b *InMemoryBackend) DeleteGroup(nameOrARN string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	name := resolveGroupName(nameOrARN)

	g, ok := b.groups[name]
	if !ok {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	delete(b.arnIndex, g.ARN)
	g.Tags.Close()
	delete(b.groups, name)

	// Cascade: remove all derived state for this group.
	delete(b.groupResources, name)
	delete(b.groupingStatuses, name)
	delete(b.groupConfigurations, name)

	// Cancel any tag-sync tasks bound to this group.
	for taskARN, task := range b.tagSyncTasks {
		if task.GroupName == name {
			delete(b.tagSyncTasks, taskARN)
		}
	}

	return nil
}

// ListGroups returns all resource groups sorted by name.
func (b *InMemoryBackend) ListGroups() []Group {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	out := make([]Group, 0, len(b.groups))

	for _, g := range b.groups {
		cp := *g
		cp.Tags = nil
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

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

// GetAccountSettings returns the account-level settings.
func (b *InMemoryBackend) GetAccountSettings() AccountSettings {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	return b.accountSettings
}

// UpdateAccountSettings updates the account-level lifecycle event desired status.
func (b *InMemoryBackend) UpdateAccountSettings(desiredStatus string) error {
	if desiredStatus != accountLifecycleEventsActive && desiredStatus != accountLifecycleEventsInactive {
		return fmt.Errorf(
			"%w: GroupLifecycleEventsDesiredStatus must be %s or %s",
			ErrValidation,
			accountLifecycleEventsActive,
			accountLifecycleEventsInactive,
		)
	}

	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	b.accountSettings.GroupLifecycleEventsDesiredStatus = desiredStatus
	b.accountSettings.GroupLifecycleEventsStatus = desiredStatus

	return nil
}

// PutGroupConfiguration stores a deep copy of items for the named group.
func (b *InMemoryBackend) PutGroupConfiguration(nameOrARN string, items []GroupConfigurationItem) error {
	b.mu.Lock("PutGroupConfiguration")
	defer b.mu.Unlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	b.groupConfigurations[name] = cloneConfigItems(items)

	return nil
}

// GetGroupConfigurationItems returns a deep copy of the stored configuration for a group.
func (b *InMemoryBackend) GetGroupConfigurationItems(nameOrARN string) ([]GroupConfigurationItem, error) {
	b.mu.RLock("GetGroupConfigurationItems")
	defer b.mu.RUnlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	return cloneConfigItems(b.groupConfigurations[name]), nil
}

// cloneConfigItems returns a deep copy of a GroupConfigurationItem slice.
func cloneConfigItems(items []GroupConfigurationItem) []GroupConfigurationItem {
	if items == nil {
		return []GroupConfigurationItem{}
	}

	cp := make([]GroupConfigurationItem, len(items))

	for i, item := range items {
		cp[i] = GroupConfigurationItem{Type: item.Type}
		if len(item.Parameters) > 0 {
			cp[i].Parameters = make([]GroupConfigurationParameter, len(item.Parameters))
			for j, p := range item.Parameters {
				cp[i].Parameters[j] = GroupConfigurationParameter{Name: p.Name}
				if len(p.Values) > 0 {
					cp[i].Parameters[j].Values = make([]string, len(p.Values))
					copy(cp[i].Parameters[j].Values, p.Values)
				}
			}
		}
	}

	return cp
}

// GroupResources associates a list of resource ARNs with a group.
// Duplicate ARNs are silently ignored; each ARN is only added once.
func (b *InMemoryBackend) GroupResources(nameOrARN string, resourceARNs []string) ([]string, error) {
	b.mu.Lock("GroupResources")
	defer b.mu.Unlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	if b.groupResources[name] == nil {
		b.groupResources[name] = []string{}
	}

	existing := make(map[string]struct{}, len(b.groupResources[name]))

	for _, a := range b.groupResources[name] {
		existing[a] = struct{}{}
	}

	now := time.Now().UTC()
	succeeded := make([]string, 0, len(resourceARNs))

	for _, a := range resourceARNs {
		if _, dup := existing[a]; !dup {
			b.groupResources[name] = append(b.groupResources[name], a)
			existing[a] = struct{}{}
		}

		succeeded = append(succeeded, a)
		b.groupingStatuses[name] = append(b.groupingStatuses[name], GroupingStatusItem{
			ResourceArn: a,
			Action:      groupingActionGroup,
			Status:      groupingStatusSuccess,
			UpdatedAt:   now,
		})
	}

	return succeeded, nil
}

// UngroupResources removes a list of resource ARNs from a group.
// ARNs that are not currently in the group are silently ignored.
func (b *InMemoryBackend) UngroupResources(nameOrARN string, resourceARNs []string) ([]string, error) {
	b.mu.Lock("UngroupResources")
	defer b.mu.Unlock()

	name := resolveGroupName(nameOrARN)
	if _, ok := b.groups[name]; !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	remove := make(map[string]struct{}, len(resourceARNs))
	for _, a := range resourceARNs {
		remove[a] = struct{}{}
	}

	kept := b.groupResources[name][:0:0]
	for _, a := range b.groupResources[name] {
		if _, ok := remove[a]; !ok {
			kept = append(kept, a)
		}
	}

	b.groupResources[name] = kept

	now := time.Now().UTC()
	succeeded := make([]string, 0, len(resourceARNs))

	for _, a := range resourceARNs {
		succeeded = append(succeeded, a)
		b.groupingStatuses[name] = append(b.groupingStatuses[name], GroupingStatusItem{
			ResourceArn: a,
			Action:      groupingActionUngroup,
			Status:      groupingStatusSuccess,
			UpdatedAt:   now,
		})
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

// SearchResources returns resource identifiers that have been grouped into any group.
// The in-memory implementation returns all known grouped resource ARNs, de-duplicated.
func (b *InMemoryBackend) SearchResources(_ *ResourceQuery) ([]ResourceIdentifier, error) {
	b.mu.RLock("SearchResources")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	out := make([]ResourceIdentifier, 0, len(b.groupResources))

	for _, arns := range b.groupResources {
		for _, a := range arns {
			if _, ok := seen[a]; !ok {
				seen[a] = struct{}{}
				out = append(out, ResourceIdentifier{ResourceArn: a})
			}
		}
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

// CancelTagSyncTask cancels and removes a tag-sync task by ARN.
func (b *InMemoryBackend) CancelTagSyncTask(taskARN string) error {
	b.mu.Lock("CancelTagSyncTask")
	defer b.mu.Unlock()

	if _, ok := b.tagSyncTasks[taskARN]; !ok {
		return fmt.Errorf("%w: task %s not found", ErrTagSyncTaskNotFound, taskARN)
	}

	delete(b.tagSyncTasks, taskARN)

	return nil
}

// GetTagSyncTask returns a copy of a tag-sync task by ARN.
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

// ListTagSyncTasks returns all tag-sync tasks, optionally filtered by group ARN or name.
// Inactive tasks older than tagSyncTaskTTL are evicted before the result is assembled.
// Results are sorted by TaskArn for deterministic ordering.
func (b *InMemoryBackend) ListTagSyncTasks(filters []ListTagSyncTasksFilter) ([]TagSyncTask, error) {
	b.mu.Lock("ListTagSyncTasks")
	defer b.mu.Unlock()

	cutoff := time.Now().UTC().Add(-tagSyncTaskTTL)

	// Evict stale non-active tasks.
	for taskARN, task := range b.tagSyncTasks {
		if task.Status != tagSyncTaskStatusActive && task.CreatedAt.Before(cutoff) {
			delete(b.tagSyncTasks, taskARN)
		}
	}

	out := make([]TagSyncTask, 0, len(b.tagSyncTasks))

	for _, task := range b.tagSyncTasks {
		if !taskMatchesFilters(task, filters) {
			continue
		}

		out = append(out, *task)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].TaskArn < out[j].TaskArn })

	return out, nil
}

// taskMatchesFilters returns true when task satisfies all provided filter criteria.
// An empty filter list matches all tasks.
func taskMatchesFilters(task *TagSyncTask, filters []ListTagSyncTasksFilter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if (f.GroupArn == "" || f.GroupArn == task.GroupArn) &&
			(f.GroupName == "" || f.GroupName == task.GroupName) {
			return true
		}
	}

	return false
}
