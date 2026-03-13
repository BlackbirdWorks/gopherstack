package kinesisanalyticsv2

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
)

const (
	// ApplicationStatusReady indicates a running application that is ready.
	ApplicationStatusReady = "READY"
	// ApplicationStatusRunning indicates a running application.
	ApplicationStatusRunning = "RUNNING"
	// ApplicationStatusDeleting indicates an application being deleted.
	ApplicationStatusDeleting = "DELETING"
)

// Tag represents a key-value tag pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Application represents a Kinesis Data Analytics v2 application.
type Application struct {
	CreatedAt              time.Time `json:"-"`
	ApplicationARN         string    `json:"ApplicationARN"`
	ApplicationName        string    `json:"ApplicationName"`
	ApplicationStatus      string    `json:"ApplicationStatus"`
	RuntimeEnvironment     string    `json:"RuntimeEnvironment"`
	ServiceExecutionRole   string    `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription string    `json:"ApplicationDescription,omitempty"`
	ApplicationMode        string    `json:"ApplicationMode,omitempty"`
	Tags                   []Tag     `json:"-"`
	ApplicationVersionID   int64     `json:"ApplicationVersionId"`
}

// Snapshot represents an application snapshot.
type Snapshot struct {
	SnapshotCreation   time.Time `json:"-"`
	ApplicationARN     string    `json:"ApplicationARN"`
	SnapshotName       string    `json:"SnapshotName"`
	SnapshotStatus     string    `json:"SnapshotStatus"`
	ApplicationVersion int64     `json:"ApplicationVersionId"`
}

// InMemoryBackend stores Kinesis Data Analytics v2 state in memory.
type InMemoryBackend struct {
	applications map[string]*Application // key: applicationName
	snapshots    map[string][]*Snapshot  // key: applicationName → snapshots
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory Kinesis Data Analytics v2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications: make(map[string]*Application),
		snapshots:    make(map[string][]*Snapshot),
		mu:           lockmetrics.New("kinesisanalyticsv2"),
		accountID:    accountID,
		region:       region,
	}
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// applicationARN builds an ARN for a Kinesis Data Analytics v2 application.
func (b *InMemoryBackend) applicationARN(name string) string {
	return arn.Build("kinesisanalytics", b.region, b.accountID, fmt.Sprintf("application/%s", name))
}

// CreateApplication creates a new Kinesis Data Analytics v2 application.
func (b *InMemoryBackend) CreateApplication(
	name, runtimeEnv, serviceRole, description, mode string,
	tags []Tag,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; ok {
		return nil, ErrAlreadyExists
	}

	appARN := b.applicationARN(name)
	app := &Application{
		ApplicationARN:         appARN,
		ApplicationName:        name,
		ApplicationStatus:      ApplicationStatusReady,
		RuntimeEnvironment:     runtimeEnv,
		ServiceExecutionRole:   serviceRole,
		ApplicationDescription: description,
		ApplicationMode:        mode,
		ApplicationVersionID:   1,
		Tags:                   cloneTags(tags),
		CreatedAt:              time.Now().UTC(),
	}
	b.applications[name] = app

	return app, nil
}

// DescribeApplication retrieves an application by name.
func (b *InMemoryBackend) DescribeApplication(name string) (*Application, error) {
	b.mu.RLock("DescribeApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications[name]
	if !ok {
		return nil, ErrNotFound
	}

	return app, nil
}

// ListApplications returns all applications.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]*Application, 0, len(b.applications))
	for _, app := range b.applications {
		out = append(out, app)
	}

	return out
}

// UpdateApplication updates an application's description and service role.
func (b *InMemoryBackend) UpdateApplication(name string, serviceRole, description string) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return nil, ErrNotFound
	}

	if serviceRole != "" {
		app.ServiceExecutionRole = serviceRole
	}

	if description != "" {
		app.ApplicationDescription = description
	}

	app.ApplicationVersionID++

	return app, nil
}

// DeleteApplication deletes an application by name.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; !ok {
		return ErrNotFound
	}

	delete(b.applications, name)
	delete(b.snapshots, name)

	return nil
}

// StartApplication sets the application status to RUNNING.
func (b *InMemoryBackend) StartApplication(name string) error {
	b.mu.Lock("StartApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return ErrNotFound
	}

	app.ApplicationStatus = ApplicationStatusRunning

	return nil
}

// StopApplication sets the application status to READY.
func (b *InMemoryBackend) StopApplication(name string) error {
	b.mu.Lock("StopApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return ErrNotFound
	}

	app.ApplicationStatus = ApplicationStatusReady

	return nil
}

// CreateApplicationSnapshot creates a snapshot for an application.
func (b *InMemoryBackend) CreateApplicationSnapshot(appName, snapshotName string) (*Snapshot, error) {
	b.mu.Lock("CreateApplicationSnapshot")
	defer b.mu.Unlock()

	app, ok := b.applications[appName]
	if !ok {
		return nil, ErrNotFound
	}

	snaps := b.snapshots[appName]
	for _, s := range snaps {
		if s.SnapshotName == snapshotName {
			return nil, ErrAlreadyExists
		}
	}

	snap := &Snapshot{
		ApplicationARN:     app.ApplicationARN,
		SnapshotName:       snapshotName,
		SnapshotStatus:     "READY",
		ApplicationVersion: app.ApplicationVersionID,
		SnapshotCreation:   time.Now().UTC(),
	}
	b.snapshots[appName] = append(b.snapshots[appName], snap)

	return snap, nil
}

// ListApplicationSnapshots returns all snapshots for an application.
func (b *InMemoryBackend) ListApplicationSnapshots(appName string) ([]*Snapshot, error) {
	b.mu.RLock("ListApplicationSnapshots")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, ErrNotFound
	}

	snaps := b.snapshots[appName]
	out := make([]*Snapshot, len(snaps))
	copy(out, snaps)

	return out, nil
}

// DeleteApplicationSnapshot deletes a snapshot.
func (b *InMemoryBackend) DeleteApplicationSnapshot(appName, snapshotName string) error {
	b.mu.Lock("DeleteApplicationSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.applications[appName]; !ok {
		return ErrNotFound
	}

	snaps := b.snapshots[appName]
	for i, s := range snaps {
		if s.SnapshotName == snapshotName {
			b.snapshots[appName] = append(snaps[:i], snaps[i+1:]...)

			return nil
		}
	}

	return ErrNotFound
}

// TagResource adds tags to an application.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return ErrNotFound
	}

	for _, t := range tags {
		found := false

		for i, existing := range app.Tags {
			if existing.Key == t.Key {
				app.Tags[i].Value = t.Value
				found = true

				break
			}
		}

		if !found {
			app.Tags = append(app.Tags, t)
		}
	}

	return nil
}

// UntagResource removes tags from an application.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return ErrNotFound
	}

	keySet := make(map[string]struct{}, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	filtered := app.Tags[:0]
	for _, t := range app.Tags {
		if _, remove := keySet[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}

	app.Tags = filtered

	return nil
}

// ListTagsForResource returns tags for an application.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	app := b.findByARN(resourceARN)
	if app == nil {
		return nil, ErrNotFound
	}

	return cloneTags(app.Tags), nil
}

// findByARN finds an application by its ARN (must be called with lock held).
func (b *InMemoryBackend) findByARN(resourceARN string) *Application {
	for _, app := range b.applications {
		if app.ApplicationARN == resourceARN {
			return app
		}
	}

	return nil
}

// GenerateApplicationARN exposes the ARN builder for testing.
func (b *InMemoryBackend) GenerateApplicationARN(name string) string {
	return b.applicationARN(name)
}

// cloneTags returns a copy of a tag slice.
func cloneTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}

	result := make([]Tag, len(tags))
	copy(result, tags)

	return result
}

// tagsToMap converts a tag slice to a map for display.
func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToTags converts a map to a tag slice.
func mapToTags(m map[string]string) []Tag {
	tags := make([]Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, Tag{Key: k, Value: v})
	}

	return tags
}

// applicationSummary is a compact view of an application used in listings.
type applicationSummary struct {
	ApplicationARN       string `json:"ApplicationARN"`
	ApplicationName      string `json:"ApplicationName"`
	ApplicationStatus    string `json:"ApplicationStatus"`
	RuntimeEnvironment   string `json:"RuntimeEnvironment"`
	ApplicationMode      string `json:"ApplicationMode,omitempty"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

// toSummary converts an Application to a summary.
func toSummary(app *Application) applicationSummary {
	return applicationSummary{
		ApplicationARN:       app.ApplicationARN,
		ApplicationName:      app.ApplicationName,
		ApplicationStatus:    app.ApplicationStatus,
		RuntimeEnvironment:   app.RuntimeEnvironment,
		ApplicationMode:      app.ApplicationMode,
		ApplicationVersionID: app.ApplicationVersionID,
	}
}

// snapshotDetail is the full snapshot view.
type snapshotDetail struct {
	ApplicationARN            string  `json:"ApplicationARN"`
	SnapshotName              string  `json:"SnapshotName"`
	SnapshotStatus            string  `json:"SnapshotStatus"`
	ApplicationVersion        int64   `json:"ApplicationVersionId"`
	SnapshotCreationTimestamp float64 `json:"SnapshotCreationTimestamp"`
}

// toSnapshotDetail converts a Snapshot to a snapshotDetail.
func toSnapshotDetail(s *Snapshot) snapshotDetail {
	return snapshotDetail{
		ApplicationARN:            s.ApplicationARN,
		SnapshotName:              s.SnapshotName,
		SnapshotStatus:            s.SnapshotStatus,
		ApplicationVersion:        s.ApplicationVersion,
		SnapshotCreationTimestamp: float64(s.SnapshotCreation.Unix()),
	}
}
