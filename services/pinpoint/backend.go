package pinpoint

import (
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"
)

// ErrAppNotFound is returned when a Pinpoint application is not found.
var ErrAppNotFound = awserr.New("NotFoundException: app not found", awserr.ErrNotFound)

// StorageBackend is the storage interface for the Pinpoint service.
type StorageBackend interface {
	CreateApp(region, accountID, name string, tags map[string]string) (*App, error)
	GetApp(appID string) (*App, error)
	DeleteApp(appID string) (*App, error)
	GetApps() ([]*App, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	apps      map[string]*App
	arnIndex  map[string]string
	mu        *lockmetrics.RWMutex
	region    string
	accountID string
}

// NewInMemoryBackend creates a new Pinpoint in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		region:    region,
		accountID: accountID,
		apps:      make(map[string]*App),
		arnIndex:  make(map[string]string),
		mu:        lockmetrics.New("pinpoint"),
	}
}

// copyApp returns a copy of an App with a cloned Tags map.
func copyApp(app *App) *App {
	cp := *app
	if app.Tags != nil {
		cp.Tags = maps.Clone(app.Tags)
	}

	return &cp
}

// CreateApp creates a new Pinpoint application.
func (b *InMemoryBackend) CreateApp(region, accountID, name string, tags map[string]string) (*App, error) {
	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	appID := uuid.NewString()
	appARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s", appID))

	appTags := make(map[string]string)
	maps.Copy(appTags, tags)

	app := &App{
		ID:           appID,
		Name:         name,
		ARN:          appARN,
		Tags:         appTags,
		CreationDate: nowRFC3339(),
	}

	b.apps[appID] = app
	b.arnIndex[appARN] = appID

	return copyApp(app), nil
}

// GetApp retrieves a Pinpoint application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock("GetApp")
	defer b.mu.RUnlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	return copyApp(app), nil
}

// DeleteApp deletes a Pinpoint application by ID.
func (b *InMemoryBackend) DeleteApp(appID string) (*App, error) {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.apps, appID)
	delete(b.arnIndex, app.ARN)

	return copyApp(app), nil
}

// GetApps returns all Pinpoint applications sorted by name.
func (b *InMemoryBackend) GetApps() ([]*App, error) {
	b.mu.RLock("GetApps")
	defer b.mu.RUnlock()

	apps := make([]*App, 0, len(b.apps))

	for _, app := range b.apps {
		apps = append(apps, copyApp(app))
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Name < apps[j].Name
	})

	return apps, nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return ErrAppNotFound
	}

	if app.Tags == nil {
		app.Tags = make(map[string]string)
	}

	maps.Copy(app.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return ErrAppNotFound
	}

	for _, k := range tagKeys {
		delete(app.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return nil, ErrAppNotFound
	}

	result := make(map[string]string, len(app.Tags))
	maps.Copy(result, app.Tags)

	return result, nil
}

// findByARN looks up an app by its ARN using the O(1) index. Must be called with lock held.
func (b *InMemoryBackend) findByARN(resourceARN string) *App {
	appID, ok := b.arnIndex[resourceARN]
	if !ok {
		return nil
	}

	return b.apps[appID]
}
