package kinesisanalyticsv2

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const kav2DefaultPageSize = 50

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrConcurrentModification is returned when the application version does not match.
	ErrConcurrentModification = awserr.New("ConcurrentModificationException", awserr.ErrInvalidParameter)
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
type InMemoryBackend struct {
	applications    map[string]*Application // key: applicationName
	applicationARNs map[string]string       // application ARN → applicationName
	snapshots       map[string][]*Snapshot  // key: applicationName → snapshots
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
	nextID          int64
}

// NewInMemoryBackend creates a new in-memory Kinesis Data Analytics v2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:    make(map[string]*Application),
		applicationARNs: make(map[string]string),
		snapshots:       make(map[string][]*Snapshot),
		mu:              lockmetrics.New("kinesisanalyticsv2"),
		accountID:       accountID,
		region:          region,
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
	b.applications[name] = app
	b.applicationARNs[appARN] = name

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

// ListApplications returns applications with optional pagination.
func (b *InMemoryBackend) ListApplications(nextToken string) ([]*Application, string) {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]*Application, 0, len(b.applications))
	for _, app := range b.applications {
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

	app, ok := b.applications[name]
	if !ok {
		return ErrNotFound
	}

	delete(b.applicationARNs, app.ApplicationARN)
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

// ListApplicationSnapshots returns snapshots for an application with optional pagination.
func (b *InMemoryBackend) ListApplicationSnapshots(appName, nextToken string) ([]*Snapshot, string, error) {
	b.mu.RLock("ListApplicationSnapshots")
	defer b.mu.RUnlock()

	if _, ok := b.applications[appName]; !ok {
		return nil, "", ErrNotFound
	}

	snaps := b.snapshots[appName]
	out := make([]*Snapshot, len(snaps))
	copy(out, snaps)

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

// findByARN finds an application by its ARN using O(1) index lookup.
// Must be called with lock held.
func (b *InMemoryBackend) findByARN(resourceARN string) *Application {
	if name, ok := b.applicationARNs[resourceARN]; ok {
		return b.applications[name]
	}

	return nil
}

// GenerateApplicationARN exposes the ARN builder for testing.
func (b *InMemoryBackend) GenerateApplicationARN(name string) string {
	return b.applicationARN(name)
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
	name string, currentVersionID int64, logStreamARN, roleARN string,
) error {
	b.mu.Lock("AddApplicationCloudWatchLoggingOption")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	app.CloudWatchLoggingOptionDescs = append(app.CloudWatchLoggingOptionDescs, CloudWatchLoggingOptionDesc{
		CloudWatchLoggingOptionID: b.newResourceID("cwl"),
		LogStreamARN:              logStreamARN,
		RoleARN:                   roleARN,
	})

	return nil
}

// AddApplicationInput adds an input configuration to an application.
func (b *InMemoryBackend) AddApplicationInput(
	name string, currentVersionID int64, input InputDescription,
) error {
	b.mu.Lock("AddApplicationInput")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
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
	name string, currentVersionID int64, inputID string, config *InputProcessingConfigurationDesc,
) error {
	b.mu.Lock("AddApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
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
	name string, currentVersionID int64, output OutputDescription,
) error {
	b.mu.Lock("AddApplicationOutput")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
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
	name string, currentVersionID int64, ref ReferenceDataSourceDescription,
) error {
	b.mu.Lock("AddApplicationReferenceDataSource")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
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
	name string, currentVersionID int64, vpc VpcConfigurationDescription,
) error {
	b.mu.Lock("AddApplicationVpcConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, currentVersionID); err != nil {
		return err
	}

	vpc.VpcConfigurationID = b.newResourceID("vpc")
	app.VpcConfigurationDescriptions = append(app.VpcConfigurationDescriptions, vpc)

	return nil
}

// DeleteApplicationCloudWatchLoggingOption removes a CloudWatch logging option from an application.
func (b *InMemoryBackend) DeleteApplicationCloudWatchLoggingOption(
	name string, currentVersionID int64, loggingOptionID string,
) error {
	b.mu.Lock("DeleteApplicationCloudWatchLoggingOption")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
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
	name string, currentVersionID int64, inputID string,
) error {
	b.mu.Lock("DeleteApplicationInputProcessingConfiguration")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
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
	name string, currentVersionID int64, outputID string,
) error {
	b.mu.Lock("DeleteApplicationOutput")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
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
