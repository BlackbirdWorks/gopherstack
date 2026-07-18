package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
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

// appKey is the composite key for SageMaker apps.
type appKey struct {
	DomainID        string
	UserProfileName string
	AppType         string
	AppName         string
}

// appKeyString flattens an appKey to the single delimited string used as the
// store.Table primary key for b.apps.
func appKeyString(k appKey) string {
	return k.DomainID + "|" + k.UserProfileName + "|" + k.AppType + "|" + k.AppName
}

// App represents a SageMaker Studio app.
type App struct {
	CreationTime    time.Time         `json:"CreationTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	DomainID        string            `json:"DomainId"`
	UserProfileName string            `json:"UserProfileName"`
	AppType         string            `json:"AppType"`
	AppName         string            `json:"AppName"`
	AppArn          string            `json:"AppArn"`
	Status          string            `json:"Status"`
}

func cloneApp(a *App) *App {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

// CreateApp creates a new SageMaker Studio app.
func (b *InMemoryBackend) CreateApp(
	ctx context.Context,
	domainID, userProfile, appType, appName string,
	tags map[string]string,
) (*App, error) {
	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := appKeyString(appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
	})
	if _, ok := b.appsStore(region).Get(key); ok {
		return nil, fmt.Errorf("%w: app %s already exists", ErrAppAlreadyExists, appName)
	}

	appArn := arn.Build("sagemaker", region, b.accountID,
		fmt.Sprintf("app/%s/%s/%s/%s", domainID, userProfile, appType, appName))
	now := time.Now()

	a := &App{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
		AppArn:          appArn,
		Status:          statusInService,
		CreationTime:    now,
		Tags:            mergeTags(nil, tags),
	}
	b.appsStore(region).Put(a)

	return cloneApp(a), nil
}

// DescribeApp returns an app.
func (b *InMemoryBackend) DescribeApp(
	ctx context.Context,
	domainID, userProfile, appType, appName string,
) (*App, error) {
	b.mu.RLock("DescribeApp")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	key := appKeyString(appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
	})

	a, ok := b.appsStoreRO(region).Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: app %q not found", ErrAppNotFound, appName)
	}

	return cloneApp(a), nil
}

// ListApps returns all apps, optionally filtered by domain.
//
//nolint:dupl // App and UserProfile share pagination structure but are distinct resource types
func (b *InMemoryBackend) ListApps(ctx context.Context, domainID, nextToken string) ([]*App, string) {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.appsStoreRO(region)
	list := make([]*App, 0, store.Len())

	for _, a := range store.All() {
		if domainID == "" || a.DomainID == domainID {
			list = append(list, cloneApp(a))
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].AppName < list[j].AppName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*App{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteApp deletes an app (marks as Deleted).
func (b *InMemoryBackend) DeleteApp(ctx context.Context, domainID, userProfile, appType, appName string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.appsStore(region)

	key := appKeyString(appKey{
		DomainID:        domainID,
		UserProfileName: userProfile,
		AppType:         appType,
		AppName:         appName,
	})
	if _, ok := store.Get(key); !ok {
		return fmt.Errorf("%w: app %q not found", ErrAppNotFound, appName)
	}

	store.Delete(key)

	return nil
}
