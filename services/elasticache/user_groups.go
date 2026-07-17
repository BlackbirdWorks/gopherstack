package elasticache

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) userGroupARN(region, id string) string {
	return arn.Build("elasticache", region, b.accountID, "usergroup:"+id)
}

// CreateUserGroup creates a new user group.
func (b *InMemoryBackend) CreateUserGroup(
	ctx context.Context,
	groupID, description, engine string,
	userIDs []string,
) (*UserGroup, error) {
	b.mu.Lock("CreateUserGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.userGroupsStore(region)
	if _, exists := tbl.Get(groupID); exists {
		return nil, ErrUserGroupAlreadyExists
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
	tbl.Put(ug)
	b.appendEventLocked(groupID, "user-group", "user group created")

	return ug, nil
}

// DeleteUserGroup deletes a user group by ID.
func (b *InMemoryBackend) DeleteUserGroup(ctx context.Context, groupID string) (*UserGroup, error) {
	b.mu.Lock("DeleteUserGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.userGroupsStore(region)
	ug, ok := tbl.Get(groupID)
	if !ok {
		return nil, ErrUserGroupNotFound
	}

	result := *ug
	tbl.Delete(groupID)
	b.appendEventLocked(groupID, "user-group", "user group deleted")

	return &result, nil
}

// DescribeUserGroups returns a paginated list of user groups, optionally filtered by groupID.
func (b *InMemoryBackend) DescribeUserGroups(
	ctx context.Context,
	groupID, marker string,
	maxRecords int,
) (page.Page[UserGroup], error) {
	b.mu.RLock("DescribeUserGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.userGroupsStore(region), groupID, ErrUserGroupNotFound, nil,
		func(ug UserGroup) string { return ug.UserGroupID }, marker, maxRecords)
}

// ModifyUserGroup adds or removes users from a user group.
func (b *InMemoryBackend) ModifyUserGroup(
	ctx context.Context,
	groupID string,
	userIDsToAdd, userIDsToRemove []string,
) (*UserGroup, error) {
	b.mu.Lock("ModifyUserGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ug, ok := b.userGroupsStore(region).Get(groupID)
	if !ok {
		return nil, ErrUserGroupNotFound
	}

	removeSet := make(map[string]bool, len(userIDsToRemove))
	for _, id := range userIDsToRemove {
		removeSet[id] = true
	}

	filtered := make([]string, 0, len(ug.UserIDs))
	for _, id := range ug.UserIDs {
		if !removeSet[id] {
			filtered = append(filtered, id)
		}
	}

	filtered = append(filtered, userIDsToAdd...)
	ug.UserIDs = filtered
	result := *ug

	return &result, nil
}

// AddUserGroupInternal seeds a user group for testing.
func (b *InMemoryBackend) AddUserGroupInternal(ug *UserGroup) {
	b.mu.Lock("AddUserGroupInternal")
	defer b.mu.Unlock()
	b.userGroupsStore(b.region).Put(ug)
}

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
	if _, exists := ugStore.Get(groupID); exists {
		return nil, ErrUserGroupAlreadyExists
	}

	userStore := b.usersStore(region)
	for _, uid := range userIDs {
		if _, ok := userStore.Get(uid); !ok {
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
	ugStore.Put(ug)
	b.appendEventLocked(groupID, "user-group", "user group created")

	cp := *ug

	return &cp, nil
}

// ----------------------------------------
// DeleteUserSafe — checks group membership
// ----------------------------------------
