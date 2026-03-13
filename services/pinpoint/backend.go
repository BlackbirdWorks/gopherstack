package pinpoint

import (
	"fmt"
	"maps"
	"sort"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/google/uuid"
)

// Sentinel errors returned by the backend.
var (
	ErrAppNotFound = awserr.New("NotFoundException: app not found", awserr.ErrNotFound)
	ErrAppExists   = awserr.New("ConflictException: app already exists", awserr.ErrAlreadyExists)
)

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
	region    string
	accountID string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new Pinpoint in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		region:    region,
		accountID: accountID,
		apps:      make(map[string]*App),
	}
}

// CreateApp creates a new Pinpoint application.
func (b *InMemoryBackend) CreateApp(region, accountID, name string, tags map[string]string) (*App, error) {
	b.mu.Lock()
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

	return app, nil
}

// GetApp retrieves a Pinpoint application by ID.
func (b *InMemoryBackend) GetApp(appID string) (*App, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	return app, nil
}

// DeleteApp deletes a Pinpoint application by ID.
func (b *InMemoryBackend) DeleteApp(appID string) (*App, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.apps[appID]
	if !ok {
		return nil, ErrAppNotFound
	}

	delete(b.apps, appID)

	return app, nil
}

// GetApps returns all Pinpoint applications sorted by name.
func (b *InMemoryBackend) GetApps() ([]*App, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	apps := make([]*App, 0, len(b.apps))

	for _, app := range b.apps {
		apps = append(apps, app)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Name < apps[j].Name
	})

	return apps, nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
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
	b.mu.Lock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return nil, ErrAppNotFound
	}

	result := make(map[string]string, len(app.Tags))
	maps.Copy(result, app.Tags)

	return result, nil
}

// findByARN looks up an app by its ARN. Must be called with lock held.
func (b *InMemoryBackend) findByARN(resourceARN string) *App {
	for _, app := range b.apps {
		if app.ARN == resourceARN {
			return app
		}
	}

	return nil
}
