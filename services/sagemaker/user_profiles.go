package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrUserProfileNotFound is returned when a user profile does not exist.
	ErrUserProfileNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrUserProfileAlreadyExists is returned when a user profile already exists.
	ErrUserProfileAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// UserProfileKey is the composite key for user profiles.
type userProfileKey struct {
	DomainID        string
	UserProfileName string
}

// userProfileKeyString flattens a userProfileKey to the single delimited
// string used as the store.Table primary key for b.userProfiles.
func userProfileKeyString(k userProfileKey) string {
	return k.DomainID + "|" + k.UserProfileName
}

// UserProfile represents a SageMaker Studio user profile. UserSettings is
// stored as opaque JSON (the json.RawMessage passthrough convention used
// elsewhere in this service for deeply-nested config shapes).
type UserProfile struct {
	CreationTime               time.Time         `json:"CreationTime"`
	LastModifiedTime           time.Time         `json:"LastModifiedTime"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	DomainID                   string            `json:"DomainId"`
	UserProfileName            string            `json:"UserProfileName"`
	UserProfileArn             string            `json:"UserProfileArn"`
	Status                     string            `json:"Status"`
	SingleSignOnUserIdentifier string            `json:"SingleSignOnUserIdentifier,omitempty"`
	SingleSignOnUserValue      string            `json:"SingleSignOnUserValue,omitempty"`
	UserSettings               json.RawMessage   `json:"UserSettings,omitempty"`
}

func cloneUserProfile(up *UserProfile) *UserProfile {
	cp := *up
	cp.Tags = maps.Clone(up.Tags)
	cp.UserSettings = append(json.RawMessage(nil), up.UserSettings...)

	return &cp
}

// CreateUserProfileOptions bundles CreateUserProfile's optional fields.
type CreateUserProfileOptions struct {
	SingleSignOnUserIdentifier string
	SingleSignOnUserValue      string
	UserSettings               json.RawMessage
}

// CreateUserProfile creates a new user profile in a domain.
func (b *InMemoryBackend) CreateUserProfile(
	ctx context.Context,
	domainID, name string,
	tags map[string]string,
	opts CreateUserProfileOptions,
) (*UserProfile, error) {
	b.mu.Lock("CreateUserProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := userProfileKeyString(userProfileKey{DomainID: domainID, UserProfileName: name})
	if _, ok := b.userProfilesStore(region).Get(key); ok {
		return nil, fmt.Errorf(
			"%w: user profile %s in domain %s already exists",
			ErrUserProfileAlreadyExists,
			name,
			domainID,
		)
	}

	upArn := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		fmt.Sprintf("user-profile/%s/%s", domainID, name),
	)
	now := time.Now()

	up := &UserProfile{
		DomainID:                   domainID,
		UserProfileName:            name,
		UserProfileArn:             upArn,
		Status:                     statusInService,
		CreationTime:               now,
		LastModifiedTime:           now,
		Tags:                       mergeTags(nil, tags),
		SingleSignOnUserIdentifier: opts.SingleSignOnUserIdentifier,
		SingleSignOnUserValue:      opts.SingleSignOnUserValue,
		UserSettings:               opts.UserSettings,
	}
	b.userProfilesStore(region).Put(up)

	return cloneUserProfile(up), nil
}

// DescribeUserProfile returns a user profile.
func (b *InMemoryBackend) DescribeUserProfile(ctx context.Context, domainID, name string) (*UserProfile, error) {
	b.mu.RLock("DescribeUserProfile")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	key := userProfileKeyString(userProfileKey{DomainID: domainID, UserProfileName: name})

	up, ok := b.userProfilesStoreRO(region).Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: user profile %q in domain %q not found",
			ErrUserProfileNotFound,
			name,
			domainID,
		)
	}

	return cloneUserProfile(up), nil
}

// Enum values for ListUserProfiles' SortBy (aws-sdk-go-v2/service/sagemaker
// types.UserProfileSortKey).
const (
	userProfileSortKeyCreationTime     = "CreationTime"
	userProfileSortKeyLastModifiedTime = "LastModifiedTime"
)

// ListUserProfilesParams bundles ListUserProfiles' filter/sort/pagination
// criteria.
type ListUserProfilesParams struct {
	DomainIDEquals          string
	UserProfileNameContains string
	SortBy                  string
	SortOrder               string
	NextToken               string
	MaxResults              int32
}

// ListUserProfiles returns user profiles matching params, sorted per
// params.SortBy (default CreationTime)/params.SortOrder (default
// Ascending), capped at params.MaxResults.
func (b *InMemoryBackend) ListUserProfiles(
	ctx context.Context, params ListUserProfilesParams,
) ([]*UserProfile, string) {
	b.mu.RLock("ListUserProfiles")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.userProfilesStoreRO(region)
	list := make([]*UserProfile, 0, store.Len())

	for _, up := range store.All() {
		if params.DomainIDEquals != "" && up.DomainID != params.DomainIDEquals {
			continue
		}

		if params.UserProfileNameContains != "" &&
			!strings.Contains(up.UserProfileName, params.UserProfileNameContains) {
			continue
		}

		list = append(list, cloneUserProfile(up))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch params.SortBy {
		case userProfileSortKeyLastModifiedTime:
			less = list[i].LastModifiedTime.Before(list[j].LastModifiedTime)
		case userProfileSortKeyCreationTime:
			fallthrough
		default:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// DeleteUserProfile deletes a user profile.
func (b *InMemoryBackend) DeleteUserProfile(ctx context.Context, domainID, name string) error {
	b.mu.Lock("DeleteUserProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.userProfilesStore(region)

	key := userProfileKeyString(userProfileKey{DomainID: domainID, UserProfileName: name})
	if _, ok := store.Get(key); !ok {
		return fmt.Errorf(
			"%w: user profile %q in domain %q not found",
			ErrUserProfileNotFound,
			name,
			domainID,
		)
	}

	store.Delete(key)

	return nil
}

// UpdateUserProfile updates a user profile in a domain. Returns the updated profile.
func (b *InMemoryBackend) UpdateUserProfile(
	ctx context.Context,
	domainID, userProfileName string,
) (*UserProfile, error) {
	b.mu.Lock("UpdateUserProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := userProfileKeyString(userProfileKey{DomainID: domainID, UserProfileName: userProfileName})

	up, ok := b.userProfilesStore(region).Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: user profile %q not found in domain %q",
			ErrUserProfileNotFound,
			userProfileName,
			domainID,
		)
	}

	up.LastModifiedTime = time.Now()

	return cloneUserProfile(up), nil
}
