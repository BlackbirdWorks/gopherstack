package kinesisanalytics

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when an application does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when an application already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrConcurrentUpdate is returned when the application version does not match.
	ErrConcurrentUpdate = errors.New("ConcurrentModificationException: application version mismatch")
)

const (
	// statusReady is the application status when stopped.
	statusReady = "READY"
	// statusRunning is the application status when running.
	statusRunning = "RUNNING"
)

// StorageBackend is the interface for the Kinesis Analytics in-memory backend.
type StorageBackend interface {
	CreateApplication(region, accountID, name, description, code string, tags map[string]string) (*Application, error)
	DeleteApplication(name string, createTimestamp *time.Time) error
	DescribeApplication(name string) (*Application, error)
	ListApplications(exclusiveStart string, limit int) ([]*Application, bool)
	StartApplication(name string) error
	StopApplication(name string) error
	UpdateApplication(name string, currentVersionID int64, codeUpdate string) (*Application, error)
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	apps      map[string]*Application
	region    string
	accountID string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory Kinesis Analytics backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		apps:      make(map[string]*Application),
		region:    region,
		accountID: accountID,
	}
}

// applicationARN builds the ARN for a Kinesis Analytics application.
func applicationARN(region, accountID, name string) string {
	return arn.Build("kinesisanalytics", region, accountID, fmt.Sprintf("application/%s", name))
}

// CreateApplication creates a new Kinesis Analytics application.
func (b *InMemoryBackend) CreateApplication(
	region, accountID, name, description, code string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.apps[name]; exists {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	t := make(map[string]string)
	maps.Copy(t, tags)

	app := &Application{
		ApplicationName:        name,
		ApplicationARN:         applicationARN(region, accountID, name),
		ApplicationDescription: description,
		ApplicationCode:        code,
		ApplicationStatus:      statusReady,
		ApplicationVersionID:   1,
		CreateTimestamp:        &now,
		LastUpdateTimestamp:    &now,
		Tags:                   t,
	}

	b.apps[name] = app

	return app, nil
}

// DeleteApplication deletes a Kinesis Analytics application.
func (b *InMemoryBackend) DeleteApplication(name string, _ *time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.apps[name]; !exists {
		return ErrNotFound
	}

	delete(b.apps, name)

	return nil
}

// DescribeApplication returns the details for a Kinesis Analytics application.
func (b *InMemoryBackend) DescribeApplication(name string) (*Application, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	app, exists := b.apps[name]
	if !exists {
		return nil, ErrNotFound
	}

	return app, nil
}

// ListApplications returns all applications, with optional pagination.
func (b *InMemoryBackend) ListApplications(exclusiveStart string, limit int) ([]*Application, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]*Application, 0, len(b.apps))

	for _, app := range b.apps {
		all = append(all, app)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ApplicationName < all[j].ApplicationName
	})

	if exclusiveStart != "" {
		idx := -1

		for i, a := range all {
			if a.ApplicationName == exclusiveStart {
				idx = i

				break
			}
		}

		if idx >= 0 {
			all = all[idx+1:]
		}
	}

	if limit > 0 && len(all) > limit {
		return all[:limit], true
	}

	return all, false
}

// StartApplication transitions an application to RUNNING.
func (b *InMemoryBackend) StartApplication(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	app.ApplicationStatus = statusRunning

	return nil
}

// StopApplication transitions an application to READY.
func (b *InMemoryBackend) StopApplication(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	app.ApplicationStatus = statusReady

	return nil
}

// UpdateApplication updates the application code and version.
func (b *InMemoryBackend) UpdateApplication(
	name string,
	currentVersionID int64,
	codeUpdate string,
) (*Application, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return nil, ErrNotFound
	}

	if app.ApplicationVersionID != currentVersionID {
		return nil, ErrConcurrentUpdate
	}

	if codeUpdate != "" {
		app.ApplicationCode = codeUpdate
	}

	now := time.Now().UTC()
	app.ApplicationVersionID++
	app.LastUpdateTimestamp = &now

	return app, nil
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, app := range b.apps {
		if app.ApplicationARN == resourceARN {
			result := make(map[string]string, len(app.Tags))
			maps.Copy(result, app.Tags)

			return result, nil
		}
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, app := range b.apps {
		if app.ApplicationARN == resourceARN {
			if app.Tags == nil {
				app.Tags = make(map[string]string)
			}

			maps.Copy(app.Tags, tags)

			return nil
		}
	}

	return ErrNotFound
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, app := range b.apps {
		if app.ApplicationARN == resourceARN {
			for _, k := range tagKeys {
				delete(app.Tags, k)
			}

			return nil
		}
	}

	return ErrNotFound
}
