package elasticache

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) userARN(region, userID string) string {
	return arn.Build("elasticache", region, b.accountID, "user:"+userID)
}

// ----------------------------------------
// CreateCacheSecurityGroup
// ----------------------------------------

// CreateUser creates a new ElastiCache user.
func (b *InMemoryBackend) CreateUser(
	ctx context.Context,
	userID, userName, accessString, engine string,
	noPasswordRequired bool,
) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.usersStore(region)
	if _, exists := tbl.Get(userID); exists {
		return nil, ErrUserAlreadyExists
	}

	if engine == "" {
		engine = engineRedis
	}

	u := &User{
		UserID:             userID,
		UserName:           userName,
		Status:             statusActive,
		ARN:                b.userARN(region, userID),
		Engine:             engine,
		AccessString:       accessString,
		NoPasswordRequired: noPasswordRequired,
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.user." + userID + ".tags"),
	}
	tbl.Put(u)
	b.appendEventLocked(userID, "user", "user created")

	return u, nil
}

// AddUserInternal seeds a user for testing.
func (b *InMemoryBackend) AddUserInternal(u *User) {
	b.mu.Lock("AddUserInternal")
	defer b.mu.Unlock()
	b.usersStore(b.region).Put(u)
}

// DeleteUserSafe deletes a user, but returns an error if the user is still a member of any user group.
func (b *InMemoryBackend) DeleteUserSafe(ctx context.Context, userID string) (*User, error) {
	b.mu.Lock("DeleteUserSafe")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.usersStore(region)
	u, ok := tbl.Get(userID)
	if !ok {
		return nil, ErrUserNotFound
	}

	for _, ug := range b.userGroupsStore(region).All() {
		if slices.Contains(ug.UserIDs, userID) {
			return nil, fmt.Errorf("user %q belongs to group %q: %w", userID, ug.UserGroupID, ErrUserNotInGroup)
		}
	}

	result := *u
	tbl.Delete(userID)
	b.appendEventLocked(userID, "user", "user deleted")

	return &result, nil
}

// ----------------------------------------
// Update action tracking
// ----------------------------------------

// DeleteUser deletes a user by ID.
func (b *InMemoryBackend) DeleteUser(ctx context.Context, userID string) (*User, error) {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.usersStore(region)
	u, ok := tbl.Get(userID)
	if !ok {
		return nil, ErrUserNotFound
	}

	for _, ug := range b.userGroupsStore(region).All() {
		if slices.Contains(ug.UserIDs, userID) {
			return nil, fmt.Errorf("user %q belongs to group %q: %w", userID, ug.UserGroupID, ErrUserNotInGroup)
		}
	}

	result := *u
	tbl.Delete(userID)
	b.appendEventLocked(userID, "user", "user deleted")

	return &result, nil
}

// DescribeUsers returns a paginated list of users, optionally filtered by userID.
func (b *InMemoryBackend) DescribeUsers(
	ctx context.Context,
	userID, marker string,
	maxRecords int,
) (page.Page[User], error) {
	b.mu.RLock("DescribeUsers")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.usersStore(region), userID, ErrUserNotFound, nil,
		func(u User) string { return u.UserID }, marker, maxRecords)
}

// ModifyUser modifies a user's access string and/or password settings.
func (b *InMemoryBackend) ModifyUser(
	ctx context.Context,
	userID, accessString string,
	noPasswordRequired bool,
) (*User, error) {
	b.mu.Lock("ModifyUser")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	u, ok := b.usersStore(region).Get(userID)
	if !ok {
		return nil, ErrUserNotFound
	}

	if accessString != "" {
		u.AccessString = accessString
	}

	u.NoPasswordRequired = noPasswordRequired
	result := *u

	return &result, nil
}
