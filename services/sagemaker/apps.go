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
	// ErrAppNotFound is returned when a SageMaker app does not exist.
	ErrAppNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAppAlreadyExists is returned when an app already exists.
	ErrAppAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// appKey is the composite key for SageMaker apps. Exactly one of
// UserProfileName/SpaceName is populated per CreateAppInput's real "if
// SpaceName is not set, UserProfileName must be set" contract.
type appKey struct {
	DomainID        string
	UserProfileName string
	SpaceName       string
	AppType         string
	AppName         string
}

// appKeyString flattens an appKey to the single delimited string used as the
// store.Table primary key for b.apps.
func appKeyString(k appKey) string {
	return k.DomainID + "|" + k.UserProfileName + "|" + k.SpaceName + "|" + k.AppType + "|" + k.AppName
}

// App represents a SageMaker Studio app. ResourceSpec is stored as opaque
// JSON (the json.RawMessage passthrough convention used elsewhere in this
// service for deeply-nested config shapes).
type App struct {
	CreationTime    time.Time         `json:"CreationTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	DomainID        string            `json:"DomainId"`
	UserProfileName string            `json:"UserProfileName,omitempty"`
	SpaceName       string            `json:"SpaceName,omitempty"`
	AppType         string            `json:"AppType"`
	AppName         string            `json:"AppName"`
	AppArn          string            `json:"AppArn"`
	Status          string            `json:"Status"`
	ResourceSpec    json.RawMessage   `json:"ResourceSpec,omitempty"`
	RecoveryMode    bool              `json:"RecoveryMode,omitempty"`
}

func cloneApp(a *App) *App {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.ResourceSpec = append(json.RawMessage(nil), a.ResourceSpec...)

	return &cp
}

// CreateAppOptions bundles CreateApp's optional fields.
type CreateAppOptions struct {
	SpaceName    string
	ResourceSpec json.RawMessage
	RecoveryMode bool
}

// CreateApp creates a new SageMaker Studio app, owned by either a
// UserProfile or a Space (exactly one of userProfile/opts.SpaceName must be
// set, matching CreateAppInput's real contract).
func (b *InMemoryBackend) CreateApp(
	ctx context.Context,
	domainID, userProfile, appType, appName string,
	tags map[string]string,
	opts CreateAppOptions,
) (*App, error) {
	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := appKeyString(appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		SpaceName:       opts.SpaceName,
		AppType:         appType,
		AppName:         appName,
	})
	if _, ok := b.appsStore(region).Get(key); ok {
		return nil, fmt.Errorf("%w: app %s already exists", ErrAppAlreadyExists, appName)
	}

	owner := userProfile
	if owner == "" {
		owner = opts.SpaceName
	}

	appArn := arn.Build("sagemaker", region, b.accountID,
		fmt.Sprintf("app/%s/%s/%s/%s", domainID, owner, appType, appName))
	now := time.Now()

	a := &App{
		DomainID:        domainID,
		UserProfileName: userProfile,
		SpaceName:       opts.SpaceName,
		AppType:         appType,
		AppName:         appName,
		AppArn:          appArn,
		Status:          statusInService,
		CreationTime:    now,
		Tags:            mergeTags(nil, tags),
		ResourceSpec:    opts.ResourceSpec,
		RecoveryMode:    opts.RecoveryMode,
	}
	b.appsStore(region).Put(a)

	return cloneApp(a), nil
}

// DescribeApp returns an app owned by either the given userProfile or
// spaceName (exactly one is expected to be non-empty, mirroring Create).
func (b *InMemoryBackend) DescribeApp(
	ctx context.Context,
	domainID, userProfile, spaceName, appType, appName string,
) (*App, error) {
	b.mu.RLock("DescribeApp")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	key := appKeyString(appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		SpaceName:       spaceName,
		AppType:         appType,
		AppName:         appName,
	})

	a, ok := b.appsStoreRO(region).Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: app %q not found", ErrAppNotFound, appName)
	}

	return cloneApp(a), nil
}

// ListAppsParams bundles ListApps' filter/sort/pagination criteria.
type ListAppsParams struct {
	DomainIDEquals        string
	UserProfileNameEquals string
	SpaceNameEquals       string
	SortOrder             string
	NextToken             string
	MaxResults            int32
}

// ListApps returns apps matching params, sorted by CreationTime (the real
// AppSortKey's only value and documented default) per params.SortOrder
// (default Ascending), capped at params.MaxResults.
func (b *InMemoryBackend) ListApps(ctx context.Context, params ListAppsParams) ([]*App, string) {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.appsStoreRO(region)
	list := make([]*App, 0, store.Len())

	for _, a := range store.All() {
		if params.DomainIDEquals != "" && a.DomainID != params.DomainIDEquals {
			continue
		}

		if params.UserProfileNameEquals != "" && a.UserProfileName != params.UserProfileNameEquals {
			continue
		}

		if params.SpaceNameEquals != "" && a.SpaceName != params.SpaceNameEquals {
			continue
		}

		list = append(list, cloneApp(a))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		if desc {
			return list[i].CreationTime.After(list[j].CreationTime)
		}

		return list[i].CreationTime.Before(list[j].CreationTime)
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// DeleteApp deletes an app owned by either the given userProfile or
// spaceName (exactly one is expected to be non-empty, mirroring Create).
func (b *InMemoryBackend) DeleteApp(
	ctx context.Context, domainID, userProfile, spaceName, appType, appName string,
) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.appsStore(region)

	key := appKeyString(appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		SpaceName:       spaceName,
		AppType:         appType,
		AppName:         appName,
	})
	if _, ok := store.Get(key); !ok {
		return fmt.Errorf("%w: app %q not found", ErrAppNotFound, appName)
	}

	store.Delete(key)

	return nil
}
