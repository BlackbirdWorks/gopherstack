package quicksight

import (
	"sort"
	"time"
)

const (
	filterAppID   = "APP_ID"
	filterAppName = "APP_NAME"
)

// storedApp is the persisted representation of a Q App.
type storedApp struct {
	CreatedTime     time.Time            `json:"createdTime"`
	LastUpdatedTime time.Time            `json:"lastUpdatedTime"`
	AppID           string               `json:"appId"`
	Arn             string               `json:"arn"`
	Name            string               `json:"name"`
	Visibility      string               `json:"visibility"`
	Permissions     []ResourcePermission `json:"permissions"`
}

func appKey(accountID, appID string) string {
	return accountID + "/" + appID
}

func (a *storedApp) toApp() *App {
	return &App{
		CreatedTime:     a.CreatedTime,
		LastUpdatedTime: a.LastUpdatedTime,
		AppID:           a.AppID,
		Arn:             a.Arn,
		Name:            a.Name,
		Visibility:      a.Visibility,
		Permissions:     clonePermissions(a.Permissions),
	}
}

// AddAppInternal seeds a Q App directly into the store. Real AWS has no
// CreateApp operation -- apps are provisioned only through the Amazon Q
// Apps console -- so this is the only way this backend's App family is ever
// populated, mirroring services/cloudwatchlogs's AddAnomalyInternal test
// seam for the same console-only-creation resource shape. accountID/region
// come from the backend itself (one InMemoryBackend per account/region, see
// provider.go), matching CreateBackupVault-style ARN construction elsewhere
// in this package.
func (b *InMemoryBackend) AddAppInternal(app App) *App {
	b.mu.Lock("AddAppInternal")
	defer b.mu.Unlock()

	if app.Arn == "" {
		app.Arn = b.buildARN("app", app.AppID)
	}

	if app.CreatedTime.IsZero() {
		app.CreatedTime = time.Now().UTC()
	}

	if app.LastUpdatedTime.IsZero() {
		app.LastUpdatedTime = app.CreatedTime
	}

	if app.Visibility == "" {
		app.Visibility = "PRIVATE"
	}

	sa := &storedApp{
		CreatedTime:     app.CreatedTime,
		LastUpdatedTime: app.LastUpdatedTime,
		AppID:           app.AppID,
		Arn:             app.Arn,
		Name:            app.Name,
		Visibility:      app.Visibility,
		Permissions:     clonePermissions(app.Permissions),
	}
	b.apps.Put(sa)

	return sa.toApp()
}

// DescribeApp returns a Q App by ID.
func (b *InMemoryBackend) DescribeApp(accountID, appID string) (*App, error) {
	b.mu.RLock("DescribeApp")
	defer b.mu.RUnlock()

	a, ok := b.apps.Get(appKey(accountID, appID))
	if !ok {
		return nil, ErrAppNotFound
	}

	return a.toApp(), nil
}

// DeleteApp deletes a Q App by ID.
func (b *InMemoryBackend) DeleteApp(accountID, appID string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	if !b.apps.Has(appKey(accountID, appID)) {
		return ErrAppNotFound
	}

	b.apps.Delete(appKey(accountID, appID))

	return nil
}

// ListApps returns every Q App in the account, sorted by AppId, offset-paginated.
// accountID is unused: one InMemoryBackend serves exactly one account
// (provider.go), so b.apps.All() is already account-scoped -- the same
// pattern ListDashboards/ListAnalyses use.
func (b *InMemoryBackend) ListApps(
	_ string, maxResults int32, nextToken string,
) ([]*App, string, error) {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	all := b.apps.All()
	sort.Slice(all, func(i, j int) bool { return all[i].AppID < all[j].AppID })

	page, next := paginateOffset(all, maxResults, nextToken)

	result := make([]*App, 0, len(page))
	for _, a := range page {
		result = append(result, a.toApp())
	}

	return result, next, nil
}

// SearchApps filters Q Apps by AppId/AppName (StringEquals/StringLike).
// DIRECT_QUICKSIGHT_OWNER/DIRECT_QUICKSIGHT_SOLE_OWNER/
// DIRECT_QUICKSIGHT_VIEWER_OR_OWNER filters pass through permissively --
// this backend doesn't track per-action ownership semantics for principals,
// the same disclosed simplification folders.go/spaces.go/topics.go/
// actionconnector.go already document for their own Search ops. accountID
// is unused for the same one-backend-per-account reason as ListApps.
func (b *InMemoryBackend) SearchApps(
	_ string, filters []SearchFilter, maxResults int32, nextToken string,
) ([]*App, string, error) {
	b.mu.RLock("SearchApps")
	defer b.mu.RUnlock()

	var matched []*storedApp

	for _, a := range b.apps.All() {
		if appMatchesFilters(a, filters) {
			matched = append(matched, a)
		}
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].AppID < matched[j].AppID })

	page, next := paginateOffset(matched, maxResults, nextToken)

	result := make([]*App, 0, len(page))
	for _, a := range page {
		result = append(result, a.toApp())
	}

	return result, next, nil
}

func appMatchesFilters(a *storedApp, filters []SearchFilter) bool {
	for _, f := range filters {
		switch f.Name {
		case filterAppID:
			if !matchesStringOp(a.AppID, f.Operator, f.Value, filterOperatorStringLike) {
				return false
			}
		case filterAppName:
			if !matchesStringOp(a.Name, f.Operator, f.Value, filterOperatorStringLike) {
				return false
			}
		}
	}

	return true
}

// DescribeAppPermissions returns an app's resource permissions.
func (b *InMemoryBackend) DescribeAppPermissions(
	accountID, appID string,
) (*App, []ResourcePermission, error) {
	b.mu.RLock("DescribeAppPermissions")
	defer b.mu.RUnlock()

	a, ok := b.apps.Get(appKey(accountID, appID))
	if !ok {
		return nil, nil, ErrAppNotFound
	}

	return a.toApp(), clonePermissions(a.Permissions), nil
}

// UpdateAppPermissions grants/revokes permissions and optionally updates
// visibility. Real AWS only accepts PRIVATE for Visibility on this op
// ("Setting an app to PUBLIC through this operation is not supported",
// quicksight@v1.129.0 api_op_UpdateAppPermissions.go) -- an empty visibility
// leaves the current value unchanged, a non-PRIVATE value is rejected.
func (b *InMemoryBackend) UpdateAppPermissions(
	accountID, appID string, grant, revoke []ResourcePermission, visibility string,
) (*App, []ResourcePermission, error) {
	b.mu.Lock("UpdateAppPermissions")
	defer b.mu.Unlock()

	a, ok := b.apps.Get(appKey(accountID, appID))
	if !ok {
		return nil, nil, ErrAppNotFound
	}

	if visibility != "" {
		if visibility != "PRIVATE" {
			return nil, nil, ErrValidation
		}

		a.Visibility = visibility
	}

	a.Permissions = applyGrantRevoke(a.Permissions, grant, revoke)
	a.LastUpdatedTime = time.Now().UTC()

	return a.toApp(), clonePermissions(a.Permissions), nil
}
