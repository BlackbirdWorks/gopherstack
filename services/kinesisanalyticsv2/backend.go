package kinesisanalyticsv2

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

// OperationStatusSuccessful is the real Kinesis Analytics v2 OperationStatus
// enum value ("SUCCESSFUL", not "SUCCESS") for a completed operation.
// gopherstack applies application-lifecycle operations
// (Start/Stop/UpdateApplication/RollbackApplication) synchronously, so every
// recorded operation goes straight to SUCCESSFUL -- there is no IN_PROGRESS
// window to observe via DescribeApplicationOperation/ListApplicationOperations.
const OperationStatusSuccessful = "SUCCESSFUL"

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
	CreatedAt                  time.Time `json:"-"`
	ApplicationARN             string    `json:"ApplicationARN"`
	ApplicationName            string    `json:"ApplicationName"`
	ApplicationStatus          string    `json:"ApplicationStatus"`
	RuntimeEnvironment         string    `json:"RuntimeEnvironment"`
	ServiceExecutionRole       string    `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription     string    `json:"ApplicationDescription,omitempty"`
	ApplicationMode            string    `json:"ApplicationMode,omitempty"`
	MaintenanceWindowStartTime string    `json:"MaintenanceWindowStartTime,omitempty"`
	// Region is the owning region, used only to derive the store.Table
	// composite key (region#name) and the byRegion index -- ApplicationName
	// alone is only unique within a region, matching the pre-Phase-3.3
	// nested map[region]map[name]*Application layout. Never serialized on the
	// wire: handler.go always builds a dedicated response DTO
	// (applicationDetailOutput/applicationSummary), never marshals Application
	// directly.
	Region                          string                           `json:"-"`
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
	SnapshotCreation time.Time `json:"-"`
	ApplicationARN   string    `json:"ApplicationARN"`
	SnapshotName     string    `json:"SnapshotName"`
	SnapshotStatus   string    `json:"SnapshotStatus"`
	// Region and AppName are the owning region and application name, used
	// only to derive the store.Table composite key (region#appName#name) and
	// the byApp index -- SnapshotName alone is only unique within an
	// application. Never serialized on the wire: handler.go always builds a
	// dedicated snapshotDetail response DTO.
	Region             string `json:"-"`
	AppName            string `json:"-"`
	ApplicationVersion int64  `json:"ApplicationVersionId"`
}

// InMemoryBackend stores Kinesis Data Analytics v2 state in memory.
//
// applications and snapshots are store.Table-backed (Phase 3.3); see
// store_setup.go for the composite keys and secondary indexes that replace
// the pre-Phase-3.3 nested map[region]map[name]* layout. operations and
// versions are left as plain nested maps of slices: both are order-sensitive
// append histories (versions is read by positional index in
// RollbackApplication; operations is returned in insertion order with no
// explicit sort), and store.Index does not preserve insertion order, so
// neither fits a store.Table+Index conversion -- see pkgs/store's package
// doc and .claude/memories/pkgs-catalog.md. Neither was persisted before
// this conversion and neither is persisted after it (see persistence.go).
type InMemoryBackend struct {
	applications         *store.Table[Application]
	applicationsByRegion *store.Index[Application]
	applicationsByARN    *store.Index[Application]
	snapshots            *store.Table[Snapshot]
	snapshotsByApp       *store.Index[Snapshot]
	registry             *store.Registry
	operations           map[string]map[string][]*ApplicationOperation // region → applicationName → []Operation
	versions             map[string]map[string][]*Application          // region → applicationName → []version
	mu                   *lockmetrics.RWMutex
	accountID            string
	defaultRegion        string
	nextID               int64
}

// NewInMemoryBackend creates a new in-memory Kinesis Data Analytics v2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:      store.NewRegistry(),
		operations:    make(map[string]map[string][]*ApplicationOperation),
		versions:      make(map[string]map[string][]*Application),
		mu:            lockmetrics.New("kinesisanalyticsv2"),
		accountID:     accountID,
		defaultRegion: region,
	}
	registerAllTables(b)

	return b
}

// Region returns the backend default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// --- Per-region store accessors (callers must hold b.mu) ---

// findApplication looks up an application by region and name.
func (b *InMemoryBackend) findApplication(region, name string) (*Application, bool) {
	return b.applications.Get(applicationKey(region, name))
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

	if b.applications.Has(applicationKey(region, name)) {
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
		Region:                          region,
		ApplicationVersionID:            1,
		Tags:                            cloneTags(tags),
		CreatedAt:                       time.Now().UTC(),
		CloudWatchLoggingOptionDescs:    []CloudWatchLoggingOptionDesc{},
		InputDescriptions:               []InputDescription{},
		OutputDescriptions:              []OutputDescription{},
		ReferenceDataSourceDescriptions: []ReferenceDataSourceDescription{},
		VpcConfigurationDescriptions:    []VpcConfigurationDescription{},
	}
	b.applications.Put(app)
	b.versionsStore(region)[name] = []*Application{appCopy(app)}

	return app, nil
}

// SeedApplicationConfiguration sets a newly created application's initial
// input/output/reference-data-source/VPC/CloudWatch-logging configuration in
// one step, without bumping ApplicationVersionId or appending a second
// version-history entry -- this mirrors real AWS, where CreateApplication's
// inline ApplicationConfiguration is part of the application's first
// version (ApplicationVersionId stays 1), unlike the separately-versioned
// Add* operations. Callers (handleCreateApplication) must invoke this
// immediately after CreateApplication succeeds, before the new application
// is exposed to any other caller. Returns ErrNotFound if name doesn't exist.
func (b *InMemoryBackend) SeedApplicationConfiguration(
	ctx context.Context,
	name string,
	inputs []InputDescription,
	outputs []OutputDescription,
	refDataSources []ReferenceDataSourceDescription,
	vpcConfigs []VpcConfigurationDescription,
	cwlOptions []CloudWatchLoggingOptionDesc,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("SeedApplicationConfiguration")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	for i := range inputs {
		inputs[i].InputID = b.newResourceID("input")
	}

	app.InputDescriptions = append(app.InputDescriptions, inputs...)

	for i := range outputs {
		outputs[i].OutputID = b.newResourceID("output")
	}

	app.OutputDescriptions = append(app.OutputDescriptions, outputs...)

	for i := range refDataSources {
		refDataSources[i].ReferenceID = b.newResourceID("ref")
	}

	app.ReferenceDataSourceDescriptions = append(app.ReferenceDataSourceDescriptions, refDataSources...)

	for i := range vpcConfigs {
		vpcConfigs[i].VpcConfigurationID = b.newResourceID("vpc")

		if vpcConfigs[i].SubnetIDs == nil {
			vpcConfigs[i].SubnetIDs = []string{}
		}

		if vpcConfigs[i].SecurityGroupIDs == nil {
			vpcConfigs[i].SecurityGroupIDs = []string{}
		}
	}

	app.VpcConfigurationDescriptions = append(app.VpcConfigurationDescriptions, vpcConfigs...)

	for i := range cwlOptions {
		cwlOptions[i].CloudWatchLoggingOptionID = b.newResourceID("cwl")
	}

	app.CloudWatchLoggingOptionDescs = append(app.CloudWatchLoggingOptionDescs, cwlOptions...)

	// Refresh the version-1 history snapshot recorded by CreateApplication so
	// DescribeApplicationVersion(name, 1) reflects the seeded configuration
	// too, not just the bare application.
	if vers := b.versionsStore(region)[name]; len(vers) > 0 {
		vers[len(vers)-1] = appCopy(app)
	}

	return nil
}

// DescribeApplication retrieves an application by name.
// Returns a deep copy so callers cannot mutate internal state.
func (b *InMemoryBackend) DescribeApplication(ctx context.Context, name string) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeApplication")
	defer b.mu.RUnlock()

	app, ok := b.findApplication(region, name)
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

	out := slices.Clone(b.applicationsByRegion.Get(region))

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

// UpdateApplication updates an application's description and service role,
// returning the OperationID of the recorded UpdateApplication operation (see
// recordOperation). currentVersionID implements the optimistic-concurrency
// check real AWS performs via CurrentApplicationVersionId (a zero/negative
// value skips the check, matching checkAndBumpVersion's convention for the
// Add*/Delete* config ops elsewhere in this file).
func (b *InMemoryBackend) UpdateApplication(
	ctx context.Context,
	name string,
	currentVersionID int64,
	serviceRole, description string,
) (*Application, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return nil, "", ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return nil, "", err
	}

	defer b.snapshotVersion(region, name, app)

	if serviceRole != "" {
		app.ServiceExecutionRole = serviceRole
	}

	if description != "" {
		app.ApplicationDescription = description
	}

	opID := b.recordOperation(region, name, "UpdateApplication")

	return app, opID, nil
}

// DeleteApplication deletes an application by name.
func (b *InMemoryBackend) DeleteApplication(ctx context.Context, name string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationKey(region, name)) {
		return ErrNotFound
	}

	snaps := slices.Clone(b.snapshotsByApp.Get(appParentKey(region, name)))
	for _, s := range snaps {
		b.snapshots.Delete(snapshotKey(region, name, s.SnapshotName))
	}

	b.applications.Delete(applicationKey(region, name))
	delete(b.versionsStore(region), name)
	if b.operations[region] != nil {
		delete(b.operations[region], name)
	}

	return nil
}

// StartApplication sets the application status to RUNNING and returns the
// OperationID of the recorded StartApplication operation (see recordOperation).
// Returns ResourceInUseException if the application is not in READY state,
// matching real AWS Kinesis Analytics v2 behavior.
func (b *InMemoryBackend) StartApplication(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("StartApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return "", ErrNotFound
	}

	if app.ApplicationStatus != ApplicationStatusReady {
		return "", ErrAlreadyExists
	}

	app.ApplicationStatus = ApplicationStatusRunning

	return b.recordOperation(region, name, "StartApplication"), nil
}

// StopApplication sets the application status to READY and returns the
// OperationID of the recorded StopApplication operation (see recordOperation).
// Returns ResourceInUseException if the application is not in RUNNING state,
// matching real AWS Kinesis Analytics v2 behavior.
func (b *InMemoryBackend) StopApplication(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("StopApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return "", ErrNotFound
	}

	if app.ApplicationStatus != ApplicationStatusRunning {
		return "", ErrAlreadyExists
	}

	app.ApplicationStatus = ApplicationStatusReady

	return b.recordOperation(region, name, "StopApplication"), nil
}

// CreateApplicationSnapshot creates a snapshot for an application.
func (b *InMemoryBackend) CreateApplicationSnapshot(
	ctx context.Context,
	appName, snapshotName string,
) (*Snapshot, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateApplicationSnapshot")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, appName)
	if !ok {
		return nil, ErrNotFound
	}

	// Real AWS requires application to be RUNNING before snapshot creation.
	if app.ApplicationStatus != ApplicationStatusRunning {
		return nil, ErrAlreadyExists
	}

	if b.snapshots.Has(snapshotKey(region, appName, snapshotName)) {
		return nil, ErrAlreadyExists
	}

	snap := &Snapshot{
		ApplicationARN:     app.ApplicationARN,
		SnapshotName:       snapshotName,
		SnapshotStatus:     "READY",
		Region:             region,
		AppName:            appName,
		ApplicationVersion: app.ApplicationVersionID,
		SnapshotCreation:   time.Now().UTC(),
	}
	b.snapshots.Put(snap)

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

	if !b.applications.Has(applicationKey(region, appName)) {
		return nil, ErrNotFound
	}

	s, ok := b.snapshots.Get(snapshotKey(region, appName, snapshotName))
	if !ok {
		return nil, ErrNotFound
	}

	return s, nil
}

// ListApplicationSnapshots returns snapshots for an application with optional pagination, sorted by creation time.
func (b *InMemoryBackend) ListApplicationSnapshots(
	ctx context.Context,
	appName, nextToken string,
) ([]*Snapshot, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListApplicationSnapshots")
	defer b.mu.RUnlock()

	if !b.applications.Has(applicationKey(region, appName)) {
		return nil, "", ErrNotFound
	}

	out := slices.Clone(b.snapshotsByApp.Get(appParentKey(region, appName)))

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

	if !b.applications.Has(applicationKey(region, appName)) {
		return ErrNotFound
	}

	if !b.snapshots.Delete(snapshotKey(region, appName, snapshotName)) {
		return ErrNotFound
	}

	return nil
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

// findByARN finds an application by its ARN using O(1) index lookup, scoped
// to region (matching the pre-Phase-3.3 per-region ARN map: an ARN whose
// embedded region does not match the caller-derived region is treated as not
// found). Must be called with lock held.
func (b *InMemoryBackend) findByARN(region, resourceARN string) *Application {
	for _, app := range b.applicationsByARN.Get(resourceARN) {
		if app.Region == region {
			return app
		}
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

	b.registry.ResetAll()
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
	cp.Region = region
	b.applications.Put(cp)
}

// newResourceID generates a unique resource ID. Must be called under b.mu.
func (b *InMemoryBackend) newResourceID(prefix string) string {
	b.nextID++

	return fmt.Sprintf("%s-%d", prefix, b.nextID)
}

// checkAndBumpVersion validates the current version and increments it.
// A zero/negative currentVersionID is treated as "skip version check".
// Callers must follow a successful call with `defer b.snapshotVersion(region,
// name, app)` (see below) to record the version history.
func checkAndBumpVersion(app *Application, currentVersionID int64) error {
	if currentVersionID > 0 && app.ApplicationVersionID != currentVersionID {
		return ErrConcurrentModification
	}

	app.ApplicationVersionID++

	return nil
}

// snapshotVersion appends a copy of app's current state to the (region,
// name) version history. Callers invoke this via `defer` immediately after a
// successful checkAndBumpVersion so it captures state *after* the caller's
// subsequent field mutations run, not the state at the moment of the version
// bump -- deferred calls execute at function return, once all of the
// function's own statements (including those mutations) have completed.
// Without this, DescribeApplicationVersion, ListApplicationVersions, and
// RollbackApplication would only ever see the single version-1 snapshot
// CreateApplication seeds -- every Add*/Delete* config op and
// UpdateApplication bumps ApplicationVersionID but previously left
// b.versions untouched, so RollbackApplication's "len(vers) < 2" guard could
// never be satisfied by real traffic and always failed with ErrValidation.
// Must run under b.mu.
func (b *InMemoryBackend) snapshotVersion(region, name string, app *Application) {
	b.versionsStore(region)[name] = append(b.versionsStore(region)[name], appCopy(app))
}

// recordOperation appends a completed ApplicationOperation record for
// (region, appName) and returns its OperationID, so DescribeApplicationOperation
// and ListApplicationOperations have real data to serve instead of being
// permanently empty. Must be called under b.mu.
func (b *InMemoryBackend) recordOperation(region, appName, opType string) string {
	now := time.Now().UTC()
	opID := b.newResourceID("op")

	op := &ApplicationOperation{
		OperationID:     opID,
		ApplicationName: appName,
		Operation:       opType,
		OperationStatus: OperationStatusSuccessful,
		StartTimestamp:  now,
		EndTimestamp:    now,
	}

	if b.operations[region] == nil {
		b.operations[region] = make(map[string][]*ApplicationOperation)
	}

	b.operations[region][appName] = append(b.operations[region][appName], op)

	return opID
}

// AddApplicationCloudWatchLoggingOption adds a CloudWatch logging option to an application.
func (b *InMemoryBackend) AddApplicationCloudWatchLoggingOption(
	ctx context.Context,
	name string, currentVersionID int64, logStreamARN, roleARN string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddApplicationCloudWatchLoggingOption")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
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

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
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

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
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

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
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

	defer b.snapshotVersion(region, name, app)

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
		SnapshotCreationTimestamp: awstime.Epoch(s.SnapshotCreation),
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

	app, ok := b.findApplication(region, name)
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

	defer b.snapshotVersion(region, name, app)

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

	app, ok := b.findApplication(region, name)
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

	defer b.snapshotVersion(region, name, app)

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

	if !b.applications.Has(applicationKey(region, name)) {
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

	if !b.applications.Has(applicationKey(region, name)) {
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

	if !b.applications.Has(applicationKey(region, name)) {
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

	if !b.applications.Has(applicationKey(region, name)) {
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

// RollbackApplication rolls back an application to its previous version,
// returning the OperationID of the recorded RollbackApplication operation
// (see recordOperation).
func (b *InMemoryBackend) RollbackApplication(
	ctx context.Context,
	name string,
	currentVersionID int64,
) (*Application, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("RollbackApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return nil, "", ErrNotFound
	}

	if currentVersionID > 0 && app.ApplicationVersionID != currentVersionID {
		return nil, "", ErrConcurrentModification
	}

	const minVersionsForRollback = 2
	vers := b.versions[region][name]
	if len(vers) < minVersionsForRollback {
		return nil, "", ErrValidation
	}

	// Roll back to the second-to-last stored version.
	prev := appCopy(vers[len(vers)-2])
	prev.ApplicationVersionID = app.ApplicationVersionID + 1
	b.applications.Put(prev)
	b.versions[region][name] = append(b.versions[region][name], appCopy(prev))

	opID := b.recordOperation(region, name, "RollbackApplication")

	return appCopy(prev), opID, nil
}

// UpdateApplicationMaintenanceConfiguration sets the maintenance window start time.
func (b *InMemoryBackend) UpdateApplicationMaintenanceConfiguration(
	ctx context.Context,
	name string, maintenanceWindowStartTime string,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateApplicationMaintenanceConfiguration")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
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
