package pinpoint

import (
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AddAppInternal seeds an application directly without going through the HTTP layer.
func (b *InMemoryBackend) AddAppInternal(app *App) {
	b.mu.Lock("AddAppInternal")
	defer b.mu.Unlock()

	b.apps.Put(app)
	b.arnIndex[app.ARN] = app
}

// CreateApp creates a new Pinpoint application.
func (b *InMemoryBackend) CreateApp(region, accountID, name string, tags map[string]string) (*App, error) {
	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	appID := uuid.NewString()
	appARN := arn.Build("mobiletargeting", region, accountID, "apps/"+appID)

	app := &App{
		ID:           appID,
		Name:         name,
		ARN:          appARN,
		Tags:         nonNilTagsCopy(tags),
		CreationDate: nowRFC3339(),
	}

	b.apps.Put(app)
	b.arnIndex[appARN] = app

	return cloneApp(app), nil
}

// GetApp retrieves a Pinpoint application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock("GetApp")
	defer b.mu.RUnlock()

	app, ok := b.apps.Get(appID)
	if !ok {
		return nil, ErrAppNotFound
	}

	return cloneApp(app), nil
}

// DeleteApp deletes a Pinpoint application by ID.
func (b *InMemoryBackend) DeleteApp(appID string) (*App, error) {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	app, ok := b.apps.Get(appID)
	if !ok {
		return nil, ErrAppNotFound
	}

	b.apps.Delete(appID)
	delete(b.arnIndex, app.ARN)

	b.purgeAppStateLocked(appID)

	return cloneApp(app), nil
}

// GetApps returns all Pinpoint applications sorted by name.
func (b *InMemoryBackend) GetApps() ([]*App, error) {
	b.mu.RLock("GetApps")
	defer b.mu.RUnlock()

	all := b.apps.All()
	apps := make([]*App, 0, len(all))

	for _, app := range all {
		apps = append(apps, cloneApp(app))
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Name < apps[j].Name
	})

	return apps, nil
}

func cloneApp(a *App) *App {
	cp := *a
	cp.Tags = nonNilTagsCopy(a.Tags)

	return &cp
}
