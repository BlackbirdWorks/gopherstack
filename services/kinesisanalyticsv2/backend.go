package kinesisanalyticsv2

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

const kav2DefaultPageSize = 50

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrConcurrentModification is returned when the application version does not match.
	ErrConcurrentModification = awserr.New(
		"ConcurrentModificationException",
		awserr.ErrInvalidParameter,
	)
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
)

// CloudWatchLoggingOptionDesc describes a CloudWatch logging option.
type CloudWatchLoggingOptionDesc struct {
	CloudWatchLoggingOptionID string `json:"CloudWatchLoggingOptionId"`
	LogStreamARN              string `json:"LogStreamARN"`
	RoleARN                   string `json:"RoleARN,omitempty"`
}

// LambdaProcessorDesc describes a Lambda input processor.
type LambdaProcessorDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// InputProcessingConfigurationDesc describes an input processing configuration.
type InputProcessingConfigurationDesc struct {
	InputLambdaProcessor *LambdaProcessorDesc `json:"InputLambdaProcessor,omitempty"`
}

// KinesisStreamsInputDesc describes a Kinesis Streams input.
type KinesisStreamsInputDesc struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN,omitempty"`
}

// KinesisFirehoseInputDesc describes a Kinesis Firehose input.
type KinesisFirehoseInputDesc struct {
	ResourceARN string `json:"ResourceARN"`
	RoleARN     string `json:"RoleARN,omitempty"`
}

// InputDescription describes an application input configuration.
type InputDescription struct {
	InputProcessingConfigurationDescription *InputProcessingConfigurationDesc `json:"InputProcessingConfigurationDescription,omitempty"` //nolint:lll // AWS API name
	KinesisStreamsInputDescription          *KinesisStreamsInputDesc          `json:"KinesisStreamsInputDescription,omitempty"`          //nolint:lll // AWS API name
	KinesisFirehoseInputDescription         *KinesisFirehoseInputDesc         `json:"KinesisFirehoseInputDescription,omitempty"`         //nolint:lll // AWS API name
	InputID                                 string                            `json:"InputId"`
	NamePrefix                              string                            `json:"NamePrefix,omitempty"`
}

// KinesisStreamsOutputDesc describes a Kinesis Streams output.
type KinesisStreamsOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// KinesisFirehoseOutputDesc describes a Kinesis Firehose output.
type KinesisFirehoseOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// LambdaOutputDesc describes a Lambda output.
type LambdaOutputDesc struct {
	ResourceARN string `json:"ResourceARN"`
}

// DestinationSchemaDesc describes the destination record format.
type DestinationSchemaDesc struct {
	RecordFormatType string `json:"RecordFormatType"`
}

// OutputDescription describes an application output configuration.
type OutputDescription struct {
	KinesisStreamsOutputDescription  *KinesisStreamsOutputDesc  `json:"KinesisStreamsOutputDescription,omitempty"`
	KinesisFirehoseOutputDescription *KinesisFirehoseOutputDesc `json:"KinesisFirehoseOutputDescription,omitempty"`
	LambdaOutputDescription          *LambdaOutputDesc          `json:"LambdaOutputDescription,omitempty"`
	DestinationSchema                *DestinationSchemaDesc     `json:"DestinationSchema,omitempty"`
	OutputID                         string                     `json:"OutputId"`
	Name                             string                     `json:"Name,omitempty"`
}

// S3ReferenceDataSourceDesc describes the S3 source for reference data.
type S3ReferenceDataSourceDesc struct {
	BucketARN string `json:"BucketARN"`
	FileKey   string `json:"FileKey"`
}

// ReferenceDataSourceDescription describes a reference data source.
type ReferenceDataSourceDescription struct {
	S3ReferenceDataSourceDescription *S3ReferenceDataSourceDesc `json:"S3ReferenceDataSourceDescription,omitempty"`
	ReferenceID                      string                     `json:"ReferenceId"`
	TableName                        string                     `json:"TableName,omitempty"`
}

// VpcConfigurationDescription describes a VPC configuration.
type VpcConfigurationDescription struct {
	VpcConfigurationID string   `json:"VpcConfigurationId"`
	VpcID              string   `json:"VpcId,omitempty"`
	SubnetIDs          []string `json:"SubnetIds"`
	SecurityGroupIDs   []string `json:"SecurityGroupIds"`
}

const (
	// ApplicationStatusReady indicates a running application that is ready.
	ApplicationStatusReady = "READY"
	// ApplicationStatusRunning indicates a running application.
	ApplicationStatusRunning = "RUNNING"
	// ApplicationStatusDeleting indicates an application being deleted.
	ApplicationStatusDeleting = "DELETING"
)

// ApplicationOperation represents a single KDA v2 application operation record.
type ApplicationOperation struct {
	StartTimestamp  time.Time `json:"-"`
	EndTimestamp    time.Time `json:"-"`
	OperationID     string    `json:"OperationId"`
	ApplicationName string    `json:"ApplicationName"`
	Operation       string    `json:"Operation"`
	OperationStatus string    `json:"OperationStatus"`
}

// ApplicationVersionSummary is a compact view of an application version.
type ApplicationVersionSummary struct {
	ApplicationStatus    string `json:"ApplicationStatus"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

// DiscoveredSchema holds the inferred schema from DiscoverInputSchema.
type DiscoveredSchema struct {
	RecordFormat       string     `json:"RecordFormat"`
	RecordEncoding     string     `json:"RecordEncoding,omitempty"`
	ParsedInputRecords [][]string `json:"ParsedInputRecords,omitempty"`
}

// Tag represents a key-value tag pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Application represents a Kinesis Data Analytics v2 application.
type Application struct {
	CreatedAt                       time.Time                        `json:"-"`
	ApplicationARN                  string                           `json:"ApplicationARN"`
	ApplicationName                 string                           `json:"ApplicationName"`
	ApplicationStatus               string                           `json:"ApplicationStatus"`
	RuntimeEnvironment              string                           `json:"RuntimeEnvironment"`
	ServiceExecutionRole            string                           `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription          string                           `json:"ApplicationDescription,omitempty"`
	ApplicationMode                 string                           `json:"ApplicationMode,omitempty"`
	MaintenanceWindowStartTime      string                           `json:"MaintenanceWindowStartTime,omitempty"`
	Tags                            []Tag                            `json:"-"`
	CloudWatchLoggingOptionDescs    []CloudWatchLoggingOptionDesc    `json:"-"`
	InputDescriptions               []InputDescription               `json:"-"`
	OutputDescriptions              []OutputDescription              `json:"-"`
	ReferenceDataSourceDescriptions []ReferenceDataSourceDescription `json:"-"`
	VpcConfigurationDescriptions    []VpcConfigurationDescription    `json:"-"`
	ApplicationVersionID            int64                            `json:"ApplicationVersionId"`
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
// All resource maps are nested by region (outer key = region) so same-named
// resources in different regions are fully isolated.
type InMemoryBackend struct {
	applications    map[string]map[string]*Application            // region → applicationName → Application
	applicationARNs map[string]map[string]string                  // region → applicationARN → applicationName
	snapshots       map[string]map[string][]*Snapshot             // region → applicationName → []Snapshot
	operations      map[string]map[string][]*ApplicationOperation // region → applicationName → []Operation
	versions        map[string]map[string][]*Application          // region → applicationName → []version
	mu              *lockmetrics.RWMutex
	accountID       string
	defaultRegion   string
	nextID          int64
}

// NewInMemoryBackend creates a new in-memory Kinesis Data Analytics v2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:    make(map[string]map[string]*Application),
		applicationARNs: make(map[string]map[string]string),
		snapshots:       make(map[string]map[string][]*Snapshot),
		operations:      make(map[string]map[string][]*ApplicationOperation),
		versions:        make(map[string]map[string][]*Application),
		mu:              lockmetrics.New("kinesisanalyticsv2"),
		accountID:       accountID,
		defaultRegion:   region,
	}
}

// Region returns the backend default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// --- Per-region store accessors (callers must hold b.mu) ---

// applicationsStore returns the application map for region, lazily creating it.
func (b *InMemoryBackend) applicationsStore(region string) map[string]*Application {
	if b.applications[region] == nil {
		b.applications[region] = make(map[string]*Application)
	}

	return b.applications[region]
}

// arnIndexStore returns the ARN-to-name index for region, lazily creating it.
func (b *InMemoryBackend) arnIndexStore(region string) map[string]string {
	if b.applicationARNs[region] == nil {
		b.applicationARNs[region] = make(map[string]string)
	}

	return b.applicationARNs[region]
}

// snapshotsStore returns the snapshot map for region, lazily creating it.
func (b *InMemoryBackend) snapshotsStore(region string) map[string][]*Snapshot {
	if b.snapshots[region] == nil {
		b.snapshots[region] = make(map[string][]*Snapshot)
	}

	return b.snapshots[region]
}

// versionsStore returns the version map for region, lazily creating it.
func (b *InMemoryBackend) versionsStore(region string) map[string][]*Application {
	if b.versions[region] == nil {
		b.versions[region] = make(map[string][]*Application)
	}

	return b.versions[region]
}

// applicationARN builds an ARN for a Kinesis Data Analytics v2 application.
func (b *InMemoryBackend) applicationARN(region, name string) string {
	return arn.Build("kinesisanalytics", region, b.accountID, "application/"+name)
}

// CreateApplication creates a new Kinesis Data Analytics v2 application.
func (b *InMemoryBackend) CreateApplication(
	ctx context.Context,
	name, runtimeEnv, serviceRole, description, mode string,
	tags []Tag,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	apps := b.applicationsStore(region)
	if _, ok := apps[name]; ok {
		return nil, ErrAlreadyExists
	}

	appARN := b.applicationARN(region, name)
	app := &Application{
		ApplicationARN:                  appARN,
		ApplicationName:                 name,
		ApplicationStatus:               ApplicationStatusReady,
		RuntimeEnvironment:              runtimeEnv,
		ServiceExecutionRole:            serviceRole,
		ApplicationDescription:          description,
		ApplicationMode:                 mode,
		ApplicationVersionID:            1,
		Tags:                            cloneTags(tags),
		CreatedAt:                       time.Now().UTC(),
		CloudWatchLoggingOptionDescs:    []CloudWatchLoggingOptionDesc{},
		InputDescriptions:               []InputDescription{},
		OutputDescriptions:              []OutputDescription{},
		ReferenceDataSourceDescriptions: []ReferenceDataSourceDescription{},
		VpcConfigurationDescriptions:    []VpcConfigurationDescription{},
	}
	apps[name] = app
	b.arnIndexStore(region)[appARN] = name
	b.versionsStore(region)[name] = []*Application{appCopy(app)}

	return app, nil
}

// DescribeApplication retrieves an application by name.
// Returns a deep copy so callers cannot mutate internal state.
func (b *InMemoryBackend) DescribeApplication(ctx context.Context, name string) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications[region][name]
	if !ok {
		return nil, ErrNotFound
	}

	return appCopy(app), nil
}

// ListApplications returns applications with optional pagination.
func (b *InMemoryBackend) ListApplications(ctx context.Context, nextToken string) ([]*Application, string) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	regionApps := b.applications[region]
	out := make([]*Application, 0, len(regionApps))
	for _, app := range regionApps {
		out = append(out, app)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ApplicationName < out[j].ApplicationName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(out) {
		return []*Application{}, ""
	}
	end := startIdx + kav2DefaultPageSize
	var outToken string
	if end < len(out) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(out)
	}

	return out[startIdx:end], outToken
}

// UpdateApplication updates an application's description and service role.
func (b *InMemoryBackend) UpdateApplication(
	ctx context.Context,
	name string,
	serviceRole, description string,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
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
func (b *InMemoryBackend) DeleteApplication(ctx context.Context, name string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	apps := b.applications[region]
	app, ok := apps[name]
	if !ok {
		return ErrNotFound
	}

	delete(b.arnIndexStore(region), app.ApplicationARN)
	delete(apps, name)
	delete(b.snapshotsStore(region), name)
	delete(b.versionsStore(region), name)
	if b.operations[region] != nil {
		delete(b.operations[region], name)
	}

	return nil
}

// StartApplication sets the application status to RUNNING.
// Returns ResourceInUseException if the application is not in READY state,
// matching real AWS Kinesis Analytics v2 behavior.
func (b *InMemoryBackend) StartApplication(ctx context.Context, name string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("StartApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	if app.ApplicationStatus != ApplicationStatusReady {
		return ErrAlreadyExists
	}

	app.ApplicationStatus = ApplicationStatusRunning

	return nil
}

// StopApplication sets the application status to READY.
// Returns ResourceInUseException if the application is not in RUNNING state,
// matching real AWS Kinesis Analytics v2 behavior.
func (b *InMemoryBackend) StopApplication(ctx context.Context, name string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("StopApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	if app.ApplicationStatus != ApplicationStatusRunning {
		return ErrAlreadyExists
	}

	app.ApplicationStatus = ApplicationStatusReady

	return nil
}

// CreateApplicationSnapshot creates a snapshot for an application.
func (b *InMemoryBackend) CreateApplicationSnapshot(
	ctx context.Context,
	appName, snapshotName string,
) (*Snapshot, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateApplicationSnapshot")
	defer b.mu.Unlock()

	app, ok := b.applications[region][appName]
	if !ok {
		return nil, ErrNotFound
	}

	// Real AWS requires application to be RUNNING before snapshot creation.
	if app.ApplicationStatus != ApplicationStatusRunning {
		return nil, ErrAlreadyExists
	}

	snaps := b.snapshotsStore(region)[appName]
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
	b.snapshotsStore(region)[appName] = append(b.snapshotsStore(region)[appName], snap)

	return snap, nil
}

// DescribeApplicationSnapshot retrieves a snapshot by application name and snapshot name.
func (b *InMemoryBackend) DescribeApplicationSnapshot(
	ctx context.Context,
	appName, snapshotName string,
) (*Snapshot, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplicationSnapshot")
	defer b.mu.RUnlock()

	if _, ok := b.applications[region][appName]; !ok {
		return nil, ErrNotFound
	}

	for _, s := range b.snapshots[region][appName] {
		if s.SnapshotName == snapshotName {
			return s, nil
		}
	}

	return nil, ErrNotFound
}

// ListApplicationSnapshots returns snapshots for an application with optional pagination, sorted by creation time.
func (b *InMemoryBackend) ListApplicationSnapshots(
	ctx context.Context,
	appName, nextToken string,
) ([]*Snapshot, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplicationSnapshots")
	defer b.mu.RUnlock()

	if _, ok := b.applications[region][appName]; !ok {
		return nil, "", ErrNotFound
	}

	snaps := b.snapshots[region][appName]
	out := make([]*Snapshot, len(snaps))
	copy(out, snaps)

	sort.Slice(out, func(i, j int) bool {
		return out[i].SnapshotCreation.Before(out[j].SnapshotCreation)
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(out) {
		return []*Snapshot{}, "", nil
	}
	end := startIdx + kav2DefaultPageSize
	var outToken string
	if end < len(out) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(out)
	}

	return out[startIdx:end], outToken, nil
}

// DeleteApplicationSnapshot deletes a snapshot.
func (b *InMemoryBackend) DeleteApplicationSnapshot(ctx context.Context, appName, snapshotName string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.applications[region][appName]; !ok {
		return ErrNotFound
	}

	snaps := b.snapshotsStore(region)[appName]
	for i, s := range snaps {
		if s.SnapshotName == snapshotName {
			b.snapshotsStore(region)[appName] = append(snaps[:i], snaps[i+1:]...)

			return nil
		}
	}

	return ErrNotFound
}

// TagResource adds tags to an application.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceARN string, tags []Tag) error {
	region := regionFromARN(resourceARN, b.defaultRegion)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	app := b.findByARN(region, resourceARN)
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
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceARN string, tagKeys []string) error {
	region := regionFromARN(resourceARN, b.defaultRegion)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	app := b.findByARN(region, resourceARN)
	if app == nil {
		return ErrNotFound
	}

	keySet := make(map[string]struct{}, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	filtered := make([]Tag, 0, len(app.Tags))
	for _, t := range app.Tags {
		if _, remove := keySet[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}

	app.Tags = filtered

	return nil
}

// ListTagsForResource returns tags for an application, sorted by key.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, resourceARN string) ([]Tag, error) {
	region := regionFromARN(resourceARN, b.defaultRegion)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	app := b.findByARN(region, resourceARN)
	if app == nil {
		return nil, ErrNotFound
	}

	cp := cloneTags(app.Tags)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Key < cp[j].Key })

	return cp, nil
}

// findByARN finds an application by its ARN using O(1) index lookup.
// Must be called with lock held.
func (b *InMemoryBackend) findByARN(region, resourceARN string) *Application {
	arnIndex := b.applicationARNs[region]
	if arnIndex == nil {
		return nil
	}

	if name, ok := arnIndex[resourceARN]; ok {
		return b.applications[region][name]
	}

	return nil
}

// GenerateApplicationARN exposes the ARN builder for testing.
func (b *InMemoryBackend) GenerateApplicationARN(name string) string {
	return b.applicationARN(b.defaultRegion, name)
}

// Reset clears all state and resets the ID counter.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.applications = make(map[string]map[string]*Application)
	b.applicationARNs = make(map[string]map[string]string)
	b.snapshots = make(map[string]map[string][]*Snapshot)
	b.operations = make(map[string]map[string][]*ApplicationOperation)
	b.versions = make(map[string]map[string][]*Application)
	b.nextID = 0
}

// AddApplicationInternal is a test-only seed helper that stores an application directly.
func (b *InMemoryBackend) AddApplicationInternal(ctx context.Context, app *Application) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	cp := appCopy(app)
	b.applicationsStore(region)[cp.ApplicationName] = cp
	b.arnIndexStore(region)[cp.ApplicationARN] = cp.ApplicationName
}

// newResourceID generates a unique resource ID. Must be called under b.mu.
func (b *InMemoryBackend) newResourceID(prefix string) string {
	b.nextID++

	return fmt.Sprintf("%s-%d", prefix, b.nextID)
}

// checkAndBumpVersion validates the current version and increments it.
// A zero/negative currentVersionID is treated as "skip version check".
// Must be called under b.mu.
func checkAndBumpVersion(app *Application, currentVersionID int64) error {
	if currentVersionID > 0 && app.ApplicationVersionID != currentVersionID {
		return ErrConcurrentModification
	}

	app.ApplicationVersionID++

	return nil
}

// AddApplicationCloudWatchLoggingOption adds a CloudWatch logging option to an application.
func (b *InMemoryBackend) AddApplicationCloudWatchLoggingOption(
	ctx context.Context,
	name string, currentVersionID int64, logStreamARN, roleARN string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationCloudWatchLoggingOption")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.CloudWatchLoggingOptionDescs = append(
		app.CloudWatchLoggingOptionDescs,
		CloudWatchLoggingOptionDesc{
			CloudWatchLoggingOptionID: b.newResourceID("cwl"),
			LogStreamARN:              logStreamARN,
			RoleARN:                   roleARN,
		},
	)

	return nil
}

// AddApplicationInput adds an input configuration to an application.
func (b *InMemoryBackend) AddApplicationInput(
	ctx context.Context,
	name string, currentVersionID int64, input InputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInput")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	input.InputID = b.newResourceID("input")
	app.InputDescriptions = append(app.InputDescriptions, input)

	return nil
}

// AddApplicationInputProcessingConfiguration sets a processing config on an existing input.
func (b *InMemoryBackend) AddApplicationInputProcessingConfiguration(
	ctx context.Context,
	name string,
	currentVersionID int64,
	inputID string,
	config *InputProcessingConfigurationDesc,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	// Find input before bumping version to avoid phantom increments on NotFound.
	idx := -1

	for i := range app.InputDescriptions {
		if app.InputDescriptions[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.InputDescriptions[idx].InputProcessingConfigurationDescription = config

	return nil
}

// AddApplicationOutput adds an output configuration to an application.
func (b *InMemoryBackend) AddApplicationOutput(
	ctx context.Context,
	name string, currentVersionID int64, output OutputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationOutput")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	output.OutputID = b.newResourceID("output")
	app.OutputDescriptions = append(app.OutputDescriptions, output)

	return nil
}

// AddApplicationReferenceDataSource adds a reference data source to an application.
func (b *InMemoryBackend) AddApplicationReferenceDataSource(
	ctx context.Context,
	name string, currentVersionID int64, ref ReferenceDataSourceDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationReferenceDataSource")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	ref.ReferenceID = b.newResourceID("ref")
	app.ReferenceDataSourceDescriptions = append(app.ReferenceDataSourceDescriptions, ref)

	return nil
}

// AddApplicationVpcConfiguration adds a VPC configuration to an application.
func (b *InMemoryBackend) AddApplicationVpcConfiguration(
	ctx context.Context,
	name string, currentVersionID int64, vpc VpcConfigurationDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationVpcConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	vpc.VpcConfigurationID = b.newResourceID("vpc")

	if vpc.SubnetIDs == nil {
		vpc.SubnetIDs = []string{}
	}

	if vpc.SecurityGroupIDs == nil {
		vpc.SecurityGroupIDs = []string{}
	}

	app.VpcConfigurationDescriptions = append(app.VpcConfigurationDescriptions, vpc)

	return nil
}

// DeleteApplicationCloudWatchLoggingOption removes a CloudWatch logging option from an application.
func (b *InMemoryBackend) DeleteApplicationCloudWatchLoggingOption(
	ctx context.Context,
	name string, currentVersionID int64, loggingOptionID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationCloudWatchLoggingOption")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	// Find before bumping to avoid a phantom version increment on NotFound.
	idx := -1

	for i, opt := range app.CloudWatchLoggingOptionDescs {
		if opt.CloudWatchLoggingOptionID == loggingOptionID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.CloudWatchLoggingOptionDescs = append(
		app.CloudWatchLoggingOptionDescs[:idx],
		app.CloudWatchLoggingOptionDescs[idx+1:]...,
	)

	return nil
}

// DeleteApplicationInputProcessingConfiguration removes the processing config from an input.
func (b *InMemoryBackend) DeleteApplicationInputProcessingConfiguration(
	ctx context.Context,
	name string, currentVersionID int64, inputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i := range app.InputDescriptions {
		if app.InputDescriptions[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.InputDescriptions[idx].InputProcessingConfigurationDescription = nil

	return nil
}

// DeleteApplicationOutput removes an output configuration from an application.
func (b *InMemoryBackend) DeleteApplicationOutput(
	ctx context.Context,
	name string, currentVersionID int64, outputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationOutput")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i, out := range app.OutputDescriptions {
		if out.OutputID == outputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.OutputDescriptions = append(
		app.OutputDescriptions[:idx],
		app.OutputDescriptions[idx+1:]...,
	)

	return nil
}

// cloneTags returns a copy of a tag slice. Always returns a non-nil slice.
func cloneTags(tags []Tag) []Tag {
	result := make([]Tag, len(tags))
	copy(result, tags)

	return result
}

// copyCWLOptions returns a deep copy of the CloudWatch logging option slice.
func copyCWLOptions(src []CloudWatchLoggingOptionDesc) []CloudWatchLoggingOptionDesc {
	out := make([]CloudWatchLoggingOptionDesc, len(src))
	copy(out, src)

	return out
}

// copyInputDescs returns a deep copy of the input description slice.
func copyInputDescs(src []InputDescription) []InputDescription {
	out := make([]InputDescription, len(src))
	copy(out, src)

	return out
}

// copyOutputDescs returns a deep copy of the output description slice.
func copyOutputDescs(src []OutputDescription) []OutputDescription {
	out := make([]OutputDescription, len(src))
	copy(out, src)

	return out
}

// copyRefDataSources returns a deep copy of the reference data source description slice.
func copyRefDataSources(src []ReferenceDataSourceDescription) []ReferenceDataSourceDescription {
	out := make([]ReferenceDataSourceDescription, len(src))
	copy(out, src)

	return out
}

// copyVpcConfigs returns a deep copy of the VPC configuration description slice.
// Nil SubnetIDs/SecurityGroupIDs are normalized to empty slices.
func copyVpcConfigs(src []VpcConfigurationDescription) []VpcConfigurationDescription {
	out := make([]VpcConfigurationDescription, len(src))

	for i, v := range src {
		entry := v

		if v.SubnetIDs == nil {
			entry.SubnetIDs = []string{}
		} else {
			entry.SubnetIDs = make([]string, len(v.SubnetIDs))
			copy(entry.SubnetIDs, v.SubnetIDs)
		}

		if v.SecurityGroupIDs == nil {
			entry.SecurityGroupIDs = []string{}
		} else {
			entry.SecurityGroupIDs = make([]string, len(v.SecurityGroupIDs))
			copy(entry.SecurityGroupIDs, v.SecurityGroupIDs)
		}

		out[i] = entry
	}

	return out
}

// appCopy returns a deep copy of an Application, safe to return to callers.
func appCopy(src *Application) *Application {
	cp := *src
	cp.Tags = cloneTags(src.Tags)
	cp.CloudWatchLoggingOptionDescs = copyCWLOptions(src.CloudWatchLoggingOptionDescs)
	cp.InputDescriptions = copyInputDescs(src.InputDescriptions)
	cp.OutputDescriptions = copyOutputDescs(src.OutputDescriptions)
	cp.ReferenceDataSourceDescriptions = copyRefDataSources(src.ReferenceDataSourceDescriptions)
	cp.VpcConfigurationDescriptions = copyVpcConfigs(src.VpcConfigurationDescriptions)

	return &cp
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

// DeleteApplicationReferenceDataSource removes a reference data source from an application.
func (b *InMemoryBackend) DeleteApplicationReferenceDataSource(
	ctx context.Context,
	name string, currentVersionID int64, referenceID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationReferenceDataSource")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	idx := -1

	for i, ref := range app.ReferenceDataSourceDescriptions {
		if ref.ReferenceID == referenceID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.ReferenceDataSourceDescriptions = append(
		app.ReferenceDataSourceDescriptions[:idx],
		app.ReferenceDataSourceDescriptions[idx+1:]...,
	)

	return nil
}

// DeleteApplicationVpcConfiguration removes a VPC configuration from an application.
func (b *InMemoryBackend) DeleteApplicationVpcConfiguration(
	ctx context.Context,
	name string, currentVersionID int64, vpcConfigurationID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplicationVpcConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return ErrNotFound
	}

	idx := -1

	for i, vpc := range app.VpcConfigurationDescriptions {
		if vpc.VpcConfigurationID == vpcConfigurationID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.VpcConfigurationDescriptions = append(
		app.VpcConfigurationDescriptions[:idx],
		app.VpcConfigurationDescriptions[idx+1:]...,
	)

	return nil
}

// DescribeApplicationOperation returns a single operation by ID.
func (b *InMemoryBackend) DescribeApplicationOperation(
	ctx context.Context,
	name, operationID string,
) (*ApplicationOperation, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplicationOperation")
	defer b.mu.RUnlock()

	if _, ok := b.applications[region][name]; !ok {
		return nil, ErrNotFound
	}

	for _, op := range b.operations[region][name] {
		if op.OperationID == operationID {
			cp := *op

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

// ListApplicationOperations returns operations for an application with optional pagination.
func (b *InMemoryBackend) ListApplicationOperations(
	ctx context.Context,
	name, nextToken string,
) ([]*ApplicationOperation, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplicationOperations")
	defer b.mu.RUnlock()

	if _, ok := b.applications[region][name]; !ok {
		return nil, "", ErrNotFound
	}

	ops := b.operations[region][name]
	out := make([]*ApplicationOperation, len(ops))
	copy(out, ops)

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(out) {
		return []*ApplicationOperation{}, "", nil
	}
	end := startIdx + kav2DefaultPageSize
	var outToken string

	if end < len(out) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(out)
	}

	return out[startIdx:end], outToken, nil
}

// DescribeApplicationVersion returns the application state at a specific version ID.
func (b *InMemoryBackend) DescribeApplicationVersion(
	ctx context.Context,
	name string,
	versionID int64,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplicationVersion")
	defer b.mu.RUnlock()

	if _, ok := b.applications[region][name]; !ok {
		return nil, ErrNotFound
	}

	for _, v := range b.versions[region][name] {
		if v.ApplicationVersionID == versionID {
			return appCopy(v), nil
		}
	}

	return nil, ErrNotFound
}

// ListApplicationVersions returns version summaries for an application.
func (b *InMemoryBackend) ListApplicationVersions(
	ctx context.Context,
	name, nextToken string,
) ([]*ApplicationVersionSummary, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplicationVersions")
	defer b.mu.RUnlock()

	if _, ok := b.applications[region][name]; !ok {
		return nil, "", ErrNotFound
	}

	vers := b.versions[region][name]
	summaries := make([]*ApplicationVersionSummary, 0, len(vers))

	for _, v := range vers {
		summaries = append(summaries, &ApplicationVersionSummary{
			ApplicationVersionID: v.ApplicationVersionID,
			ApplicationStatus:    v.ApplicationStatus,
		})
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(summaries) {
		return []*ApplicationVersionSummary{}, "", nil
	}
	end := startIdx + kav2DefaultPageSize
	var outToken string

	if end < len(summaries) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(summaries)
	}

	return summaries[startIdx:end], outToken, nil
}

// RollbackApplication rolls back an application to its previous version.
func (b *InMemoryBackend) RollbackApplication(
	ctx context.Context,
	name string,
	currentVersionID int64,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("RollbackApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return nil, ErrNotFound
	}

	if currentVersionID > 0 && app.ApplicationVersionID != currentVersionID {
		return nil, ErrConcurrentModification
	}

	const minVersionsForRollback = 2
	vers := b.versions[region][name]
	if len(vers) < minVersionsForRollback {
		return nil, ErrValidation
	}

	// Roll back to the second-to-last stored version.
	prev := appCopy(vers[len(vers)-2])
	prev.ApplicationVersionID = app.ApplicationVersionID + 1
	b.applications[region][name] = prev
	b.versions[region][name] = append(b.versions[region][name], appCopy(prev))

	return appCopy(prev), nil
}

// UpdateApplicationMaintenanceConfiguration sets the maintenance window start time.
func (b *InMemoryBackend) UpdateApplicationMaintenanceConfiguration(
	ctx context.Context,
	name string, maintenanceWindowStartTime string,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateApplicationMaintenanceConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[region][name]
	if !ok {
		return nil, ErrNotFound
	}

	app.MaintenanceWindowStartTime = maintenanceWindowStartTime

	return appCopy(app), nil
}

// DiscoverInputSchema returns a synthetic discovered schema for a resource ARN.
func (b *InMemoryBackend) DiscoverInputSchema(
	_ context.Context,
	resourceARN, _ /* roleARN */, _ /* inputStartingPosition */ string,
) (*DiscoveredSchema, error) {
	if resourceARN == "" {
		return nil, ErrValidation
	}

	return &DiscoveredSchema{
		RecordFormat:   "JSON",
		RecordEncoding: "UTF-8",
		ParsedInputRecords: [][]string{
			{"column1", "column2", "column3"},
		},
	}, nil
}

// parseNextToken parses a pagination token (integer offset) into a slice index.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}
