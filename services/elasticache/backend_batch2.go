package elasticache

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ----------------------------------------
// Batch-2 errors
// ----------------------------------------

var (
	ErrUserNotInGroup    = errors.New("user is a member of a user group and cannot be deleted")
	ErrUserNotFound2     = ErrUserNotFound
	ErrGroupUserNotFound = errors.New("one or more specified user IDs do not exist")
)

// ----------------------------------------
// ServerlessCreateOpts holds all fields for full serverless cache creation.
// ----------------------------------------

// ServerlessCreateOpts carries all fields for serverless cache creation.
type ServerlessCreateOpts struct {
	Tags                   map[string]string
	Name                   string
	Description            string
	Engine                 string
	KmsKeyID               string
	UserGroupID            string
	SubnetGroupName        string
	DailySnapshotTime      string
	MajorEngineVersion     string
	SecurityGroupIDs       []string
	SubnetIDs              []string
	SnapshotRetentionLimit int32
}

// ServerlessModifyOpts carries all fields for serverless cache modification.
type ServerlessModifyOpts struct {
	SnapshotRetentionLimit *int32
	Description            string
	UserGroupID            string
	DailySnapshotTime      string
	SecurityGroupIDs       []string
	RemoveUserGroup        bool
}

// ----------------------------------------
// CreateServerlessCacheFull
// ----------------------------------------

// CreateServerlessCacheFull creates a serverless cache with the full set of options.
func (b *InMemoryBackend) CreateServerlessCacheFull(
	ctx context.Context,
	opts ServerlessCreateOpts,
) (*ServerlessCache, error) {
	b.mu.Lock("CreateServerlessCacheFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.serverlessCachesStore(region)
	if _, exists := store[opts.Name]; exists {
		return nil, ErrServerlessCacheAlreadyExists
	}

	engine := opts.Engine
	if engine == "" {
		engine = engineRedis
	}

	suffix := randomSuffix()
	host := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", opts.Name, suffix, region)
	readerHost := fmt.Sprintf("%s.serverless.%s.%s.cache.amazonaws.com", opts.Name+"-ro", suffix, region)
	port := 6379
	if engine == engineMemcached {
		port = 11211
	}

	ep := &ServerlessCacheEndpoint{Address: host, Port: port}
	readerEp := &ServerlessCacheEndpoint{Address: readerHost, Port: port}

	majorVer := opts.MajorEngineVersion
	if majorVer == "" {
		majorVer = majorVersionStr(engine)
	}

	sc := &ServerlessCache{
		Name:                   opts.Name,
		Description:            opts.Description,
		Status:                 statusServerlessAvailable,
		ARN:                    b.serverlessCacheARN(region, opts.Name),
		Engine:                 engine,
		KmsKeyID:               opts.KmsKeyID,
		UserGroupID:            opts.UserGroupID,
		SubnetGroupName:        opts.SubnetGroupName,
		DailySnapshotTime:      opts.DailySnapshotTime,
		MajorEngineVersion:     majorVer,
		SubnetIDs:              opts.SubnetIDs,
		SecurityGroupIDs:       opts.SecurityGroupIDs,
		SnapshotRetentionLimit: opts.SnapshotRetentionLimit,
		CreatedAt:              time.Now(),
		Tags:                   tags.New("elasticache.serverless." + opts.Name + ".tags"),
		Endpoint:               ep,
		ReaderEndpoint:         readerEp,
	}

	if len(opts.Tags) > 0 {
		for k, v := range opts.Tags {
			sc.Tags.Set(k, v)
		}
	}

	store[opts.Name] = sc
	b.appendEventLocked(opts.Name, "serverless-cache", "serverless cache created")

	cp := *sc

	return &cp, nil
}

// majorVersionStr returns a human-readable major version string for an engine.
func majorVersionStr(engine string) string {
	switch engine {
	case engineMemcached:
		return "1"
	case engineValkey:
		return "8"
	default:
		return "7"
	}
}

// ----------------------------------------
// ModifyServerlessCacheFull
// ----------------------------------------

// ModifyServerlessCacheFull modifies a serverless cache with the full set of options.
func (b *InMemoryBackend) ModifyServerlessCacheFull(
	ctx context.Context,
	name string,
	opts ServerlessModifyOpts,
) (*ServerlessCache, error) {
	b.mu.Lock("ModifyServerlessCacheFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sc, ok := b.serverlessCachesStore(region)[name]
	if !ok {
		return nil, ErrServerlessCacheNotFound
	}

	if opts.Description != "" {
		sc.Description = opts.Description
	}

	if opts.UserGroupID != "" {
		sc.UserGroupID = opts.UserGroupID
	} else if opts.RemoveUserGroup {
		sc.UserGroupID = ""
	}

	if opts.DailySnapshotTime != "" {
		sc.DailySnapshotTime = opts.DailySnapshotTime
	}

	if opts.SnapshotRetentionLimit != nil {
		sc.SnapshotRetentionLimit = *opts.SnapshotRetentionLimit
	}

	if len(opts.SecurityGroupIDs) > 0 {
		sc.SecurityGroupIDs = opts.SecurityGroupIDs
	}

	result := *sc

	return &result, nil
}

// ----------------------------------------
// CreateSubnetGroupFull — with VpcId
// ----------------------------------------

// CreateSubnetGroupFull creates a cache subnet group with an explicit VPC ID.
func (b *InMemoryBackend) CreateSubnetGroupFull(
	ctx context.Context,
	name, description, vpcID string,
	subnetIDs []string,
) (*CacheSubnetGroup, error) {
	b.mu.Lock("CreateSubnetGroupFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.subnetGroupsStore(region)
	if _, exists := store[name]; exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sg := &CacheSubnetGroup{
		Name:        name,
		Description: description,
		VpcID:       vpcID,
		SubnetIDs:   subnetIDs,
		ARN:         b.subnetGroupARN(region, name),
		Tags:        tags.New("elasticache.sg." + name + ".tags"),
	}
	store[name] = sg

	cp := *sg

	return &cp, nil
}

// ----------------------------------------
// CopySnapshotFull — with KmsKeyId
// ----------------------------------------

// CopySnapshotFull copies a snapshot and optionally re-encrypts with a different KMS key.
func (b *InMemoryBackend) CopySnapshotFull(
	ctx context.Context,
	sourceSnapshotName, targetSnapshotName, kmsKeyID string,
) (*CacheSnapshot, error) {
	b.mu.Lock("CopySnapshotFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.snapshotsStore(region)

	src, ok := store[sourceSnapshotName]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if _, exists := store[targetSnapshotName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	cp := *src
	cp.SnapshotName = targetSnapshotName
	cp.ARN = b.snapshotARN(region, targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.SnapshotSource = snapshotSourceManual
	cp.Tags = tags.New("elasticache.snapshot." + targetSnapshotName + ".tags")

	if kmsKeyID != "" {
		cp.KmsKeyID = kmsKeyID
	}

	store[targetSnapshotName] = &cp
	b.appendEventLocked(targetSnapshotName, "snapshot", "snapshot copied from "+sourceSnapshotName)

	result := cp

	return &result, nil
}

// ----------------------------------------
// CreateUserGroupValidated — validates users exist
// ----------------------------------------

// CreateUserGroupValidated creates a user group, validating that all specified user IDs exist.
func (b *InMemoryBackend) CreateUserGroupValidated(
	ctx context.Context,
	groupID, description, engine string,
	userIDs []string,
) (*UserGroup, error) {
	b.mu.Lock("CreateUserGroupValidated")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ugStore := b.userGroupsStore(region)
	if _, exists := ugStore[groupID]; exists {
		return nil, ErrUserGroupAlreadyExists
	}

	userStore := b.usersStore(region)
	for _, uid := range userIDs {
		if _, ok := userStore[uid]; !ok {
			return nil, fmt.Errorf("user %q: %w", uid, ErrGroupUserNotFound)
		}
	}

	if engine == "" {
		engine = engineRedis
	}

	ug := &UserGroup{
		UserGroupID: groupID,
		Description: description,
		Status:      statusActive,
		ARN:         b.userGroupARN(region, groupID),
		Engine:      engine,
		UserIDs:     userIDs,
		CreatedAt:   time.Now(),
		Tags:        tags.New("elasticache.usergroup." + groupID + ".tags"),
	}
	ugStore[groupID] = ug
	b.appendEventLocked(groupID, "user-group", "user group created")

	cp := *ug

	return &cp, nil
}

// ----------------------------------------
// DeleteUserSafe — checks group membership
// ----------------------------------------

// DeleteUserSafe deletes a user, but returns an error if the user is still a member of any user group.
func (b *InMemoryBackend) DeleteUserSafe(ctx context.Context, userID string) (*User, error) {
	b.mu.Lock("DeleteUserSafe")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.usersStore(region)
	u, ok := store[userID]
	if !ok {
		return nil, ErrUserNotFound
	}

	for _, ug := range b.userGroupsStore(region) {
		if slices.Contains(ug.UserIDs, userID) {
			return nil, fmt.Errorf("user %q belongs to group %q: %w", userID, ug.UserGroupID, ErrUserNotInGroup)
		}
	}

	result := *u
	delete(store, userID)
	b.appendEventLocked(userID, "user", "user deleted")

	return &result, nil
}

// ----------------------------------------
// Update action tracking
// ----------------------------------------

// AppendUpdateActions appends new update action records.
func (b *InMemoryBackend) AppendUpdateActions(actions []*UpdateAction) {
	b.mu.Lock("AppendUpdateActions")
	defer b.mu.Unlock()
	b.appendUpdateActionsLocked(actions...)
}

// ListUpdateActionsByServiceUpdate returns update actions filtered by service update name.
func (b *InMemoryBackend) ListUpdateActionsByServiceUpdate(serviceUpdateName string) []*UpdateAction {
	b.mu.RLock("ListUpdateActionsByServiceUpdate")
	defer b.mu.RUnlock()

	if serviceUpdateName == "" {
		result := make([]*UpdateAction, len(b.updateActions))
		copy(result, b.updateActions)

		return result
	}

	var result []*UpdateAction

	for _, a := range b.updateActions {
		if a.ServiceUpdateName == serviceUpdateName {
			result = append(result, a)
		}
	}

	return result
}

// ----------------------------------------
// Seeded service updates
// ----------------------------------------

// BuiltinServiceUpdates returns a slice of pre-seeded service updates for testing realism.
func BuiltinServiceUpdates() []ServiceUpdate {
	return []ServiceUpdate{
		{ServiceUpdateName: "20240101-001-security-patch", Status: "available"},
		{ServiceUpdateName: "20240201-002-engine-upgrade", Status: "available"},
		{ServiceUpdateName: "20231101-003-security-hotfix", Status: "expired"},
	}
}

// DescribeServiceUpdatesFull returns service updates, filtered by name and/or status list.
func (b *InMemoryBackend) DescribeServiceUpdatesFull(
	serviceUpdateName string,
	status []string,
	marker string,
	maxRecords int,
) ([]ServiceUpdate, string, error) {
	b.mu.RLock("DescribeServiceUpdatesFull")
	defer b.mu.RUnlock()

	all := BuiltinServiceUpdates()
	statusSet := make(map[string]bool, len(status))
	for _, s := range status {
		statusSet[s] = true
	}

	filtered := make([]ServiceUpdate, 0, len(all))
	for _, su := range all {
		if serviceUpdateName != "" && su.ServiceUpdateName != serviceUpdateName {
			continue
		}

		if len(statusSet) > 0 && !statusSet[su.Status] {
			continue
		}

		filtered = append(filtered, su)
	}

	p := newServiceUpdatePage(filtered, marker, maxRecords)

	return p.data, p.next, nil
}

// serviceUpdatePageResult is a simple pagination result for service updates.
type serviceUpdatePageResult struct {
	next string
	data []ServiceUpdate
}

func newServiceUpdatePage(items []ServiceUpdate, marker string, maxRecords int) serviceUpdatePageResult {
	if maxRecords <= 0 {
		maxRecords = elasticacheDefaultMaxRecords
	}

	start := 0
	if marker != "" {
		for i, su := range items {
			if su.ServiceUpdateName == marker {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxRecords, len(items))
	page := items[start:end]
	next := ""

	if end < len(items) {
		next = items[end-1].ServiceUpdateName
	}

	return serviceUpdatePageResult{data: page, next: next}
}

// ----------------------------------------
// DescribeUpdateActionsFull — returns tracked update actions
// ----------------------------------------

// DescribeUpdateActionsFull returns update actions filtered by service update name, with pagination.
func (b *InMemoryBackend) DescribeUpdateActionsFull(
	serviceUpdateName, marker string,
	maxRecords int,
) ([]UpdateAction, string, error) {
	b.mu.RLock("DescribeUpdateActionsFull")
	defer b.mu.RUnlock()

	if maxRecords <= 0 {
		maxRecords = elasticacheDefaultMaxRecords
	}

	all := make([]UpdateAction, 0, len(b.updateActions))
	for _, a := range b.updateActions {
		if serviceUpdateName != "" && a.ServiceUpdateName != serviceUpdateName {
			continue
		}

		all = append(all, *a)
	}

	start := 0
	if marker != "" {
		for i, ua := range all {
			key := ua.ReplicationGroupID + ua.CacheClusterID + ua.ServiceUpdateName
			if key == marker {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxRecords, len(all))
	page := all[start:end]
	next := ""

	if end < len(all) {
		ua := all[end-1]
		next = ua.ReplicationGroupID + ua.CacheClusterID + ua.ServiceUpdateName
	}

	return page, next, nil
}
