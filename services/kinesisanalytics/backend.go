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
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
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
	AddApplicationCloudWatchLoggingOption(name string, versionID int64, option CloudWatchLoggingOptionDesc) error
	AddApplicationInput(name string, versionID int64, input InputDescription) error
	AddApplicationInputProcessingConfiguration(
		name string,
		versionID int64,
		inputID string,
		config *InputProcessingConfigurationDesc,
	) error
	AddApplicationOutput(name string, versionID int64, output OutputDescription) error
	AddApplicationReferenceDataSource(name string, versionID int64, ref ReferenceDataSourceDescription) error
	DeleteApplicationCloudWatchLoggingOption(name string, versionID int64, loggingOptionID string) error
	DeleteApplicationInputProcessingConfiguration(name string, versionID int64, inputID string) error
	DeleteApplicationOutput(name string, versionID int64, outputID string) error
	DeleteApplicationReferenceDataSource(name string, versionID int64, referenceID string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	apps      map[string]*Application
	appsByARN map[string]*Application // application ARN → Application
	region    string
	accountID string
	nextID    int64
	mu        sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Kinesis Analytics backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		apps:      make(map[string]*Application),
		appsByARN: make(map[string]*Application),
		region:    region,
		accountID: accountID,
	}
}

// Reset clears all state and resets the ID counter.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.apps = make(map[string]*Application)
	b.appsByARN = make(map[string]*Application)
	b.nextID = 0
}

// applicationARN builds the ARN for a Kinesis Analytics application.
func applicationARN(region, accountID, name string) string {
	return arn.Build("kinesisanalytics", region, accountID, "application/"+name)
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
		ApplicationName:          name,
		ApplicationARN:           applicationARN(region, accountID, name),
		ApplicationDescription:   description,
		ApplicationCode:          code,
		ApplicationStatus:        statusReady,
		ApplicationVersionID:     1,
		CreateTimestamp:          &now,
		LastUpdateTimestamp:      &now,
		Tags:                     t,
		CloudWatchLoggingOptions: []CloudWatchLoggingOptionDesc{},
		Inputs:                   []InputDescription{},
		Outputs:                  []OutputDescription{},
		ReferenceDataSources:     []ReferenceDataSourceDescription{},
	}

	b.apps[name] = app
	b.appsByARN[app.ApplicationARN] = app

	return appCopy(app), nil
}

// DeleteApplication deletes a Kinesis Analytics application.
func (b *InMemoryBackend) DeleteApplication(name string, _ *time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	delete(b.appsByARN, app.ApplicationARN)
	delete(b.apps, name)

	return nil
}

// DescribeApplication returns a copy of the details for a Kinesis Analytics application.
func (b *InMemoryBackend) DescribeApplication(name string) (*Application, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	app, exists := b.apps[name]
	if !exists {
		return nil, ErrNotFound
	}

	return appCopy(app), nil
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

	app, ok := b.appsByARN[resourceARN]
	if !ok {
		return nil, ErrNotFound
	}

	result := make(map[string]string, len(app.Tags))
	maps.Copy(result, app.Tags)

	return result, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.appsByARN[resourceARN]
	if !ok {
		return ErrNotFound
	}

	if app.Tags == nil {
		app.Tags = make(map[string]string)
	}

	maps.Copy(app.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.appsByARN[resourceARN]
	if !ok {
		return ErrNotFound
	}

	for _, k := range tagKeys {
		delete(app.Tags, k)
	}

	return nil
}

// newResourceID generates a new unique resource ID. Must be called under b.mu.
func (b *InMemoryBackend) newResourceID(prefix string) string {
	b.nextID++

	return fmt.Sprintf("%s-%d", prefix, b.nextID)
}

// appCopy returns a deep copy of the Application, safe to hand to callers.
func appCopy(src *Application) *Application {
	cp := *src

	cp.Tags = maps.Clone(src.Tags)
	cp.CloudWatchLoggingOptions = copyCloudWatchOptions(src.CloudWatchLoggingOptions)
	cp.Inputs = copyInputDescs(src.Inputs)
	cp.Outputs = copyOutputDescs(src.Outputs)
	cp.ReferenceDataSources = copyRefDataSources(src.ReferenceDataSources)

	return &cp
}

func copyCloudWatchOptions(src []CloudWatchLoggingOptionDesc) []CloudWatchLoggingOptionDesc {
	dst := make([]CloudWatchLoggingOptionDesc, len(src))
	copy(dst, src)

	return dst
}

func copyInputDescs(src []InputDescription) []InputDescription {
	dst := make([]InputDescription, len(src))

	for i, inp := range src {
		dup := inp
		if inp.InputProcessingConfigurationDescription != nil {
			c := *inp.InputProcessingConfigurationDescription
			if c.InputLambdaProcessor != nil {
				lp := *c.InputLambdaProcessor
				c.InputLambdaProcessor = &lp
			}
			dup.InputProcessingConfigurationDescription = &c
		}
		dst[i] = dup
	}

	return dst
}

func copyOutputDescs(src []OutputDescription) []OutputDescription {
	dst := make([]OutputDescription, len(src))

	for i, out := range src {
		dup := out
		if out.KinesisStreamsOutputDescription != nil {
			s := *out.KinesisStreamsOutputDescription
			dup.KinesisStreamsOutputDescription = &s
		}
		if out.KinesisFirehoseOutputDescription != nil {
			f := *out.KinesisFirehoseOutputDescription
			dup.KinesisFirehoseOutputDescription = &f
		}
		if out.LambdaOutputDescription != nil {
			l := *out.LambdaOutputDescription
			dup.LambdaOutputDescription = &l
		}
		if out.DestinationSchema != nil {
			ds := *out.DestinationSchema
			dup.DestinationSchema = &ds
		}
		dst[i] = dup
	}

	return dst
}

func copyRefDataSources(src []ReferenceDataSourceDescription) []ReferenceDataSourceDescription {
	dst := make([]ReferenceDataSourceDescription, len(src))

	for i, ref := range src {
		dup := ref
		if ref.S3ReferenceDataSourceDescription != nil {
			s3 := *ref.S3ReferenceDataSourceDescription
			dup.S3ReferenceDataSourceDescription = &s3
		}
		if ref.ReferenceSchema != nil {
			rs := *ref.ReferenceSchema
			rs.RecordColumns = make([]RecordColumn, len(ref.ReferenceSchema.RecordColumns))
			copy(rs.RecordColumns, ref.ReferenceSchema.RecordColumns)
			if ref.ReferenceSchema.RecordFormat.MappingParameters != nil {
				mp := *ref.ReferenceSchema.RecordFormat.MappingParameters
				rs.RecordFormat.MappingParameters = &mp
			}
			dup.ReferenceSchema = &rs
		}
		dst[i] = dup
	}

	return dst
}

// checkAndBumpVersion validates the version and increments it. Must be called under b.mu.
func checkAndBumpVersion(app *Application, currentVersionID int64) error {
	if app.ApplicationVersionID != currentVersionID {
		return ErrConcurrentUpdate
	}

	now := time.Now().UTC()
	app.ApplicationVersionID++
	app.LastUpdateTimestamp = &now

	return nil
}

// AddApplicationInternal is a test-only seed helper that stores an application directly.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := appCopy(app)
	b.apps[cp.ApplicationName] = cp
	b.appsByARN[cp.ApplicationARN] = cp
}

// AddApplicationCloudWatchLoggingOption adds a CloudWatch logging option to an application.
func (b *InMemoryBackend) AddApplicationCloudWatchLoggingOption(
	name string, versionID int64, option CloudWatchLoggingOptionDesc,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	option.CloudWatchLoggingOptionID = b.newResourceID("cwl")
	app.CloudWatchLoggingOptions = append(app.CloudWatchLoggingOptions, option)

	return nil
}

// AddApplicationInput adds an input configuration to an application.
func (b *InMemoryBackend) AddApplicationInput(
	name string, versionID int64, input InputDescription,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	input.InputID = b.newResourceID("input")
	app.Inputs = append(app.Inputs, input)

	return nil
}

// AddApplicationInputProcessingConfiguration sets a processing configuration on an existing input.
func (b *InMemoryBackend) AddApplicationInputProcessingConfiguration(
	name string, versionID int64, inputID string, config *InputProcessingConfigurationDesc,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	// Find the input before bumping the version to avoid side-effects on NotFound.
	idx := -1

	for i := range app.Inputs {
		if app.Inputs[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	app.Inputs[idx].InputProcessingConfigurationDescription = config

	return nil
}

// AddApplicationOutput adds an output configuration to an application.
func (b *InMemoryBackend) AddApplicationOutput(
	name string, versionID int64, output OutputDescription,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	output.OutputID = b.newResourceID("output")
	app.Outputs = append(app.Outputs, output)

	return nil
}

// AddApplicationReferenceDataSource adds a reference data source to an application.
func (b *InMemoryBackend) AddApplicationReferenceDataSource(
	name string, versionID int64, ref ReferenceDataSourceDescription,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	ref.ReferenceID = b.newResourceID("ref")
	app.ReferenceDataSources = append(app.ReferenceDataSources, ref)

	return nil
}

// DeleteApplicationCloudWatchLoggingOption removes a CloudWatch logging option from an application.
func (b *InMemoryBackend) DeleteApplicationCloudWatchLoggingOption(
	name string, versionID int64, loggingOptionID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	// Find before bumping to avoid a phantom version increment on NotFound.
	idx := -1

	for i, opt := range app.CloudWatchLoggingOptions {
		if opt.CloudWatchLoggingOptionID == loggingOptionID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	app.CloudWatchLoggingOptions = append(
		app.CloudWatchLoggingOptions[:idx],
		app.CloudWatchLoggingOptions[idx+1:]...,
	)

	return nil
}

// DeleteApplicationInputProcessingConfiguration removes the processing config from an input.
func (b *InMemoryBackend) DeleteApplicationInputProcessingConfiguration(
	name string, versionID int64, inputID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i := range app.Inputs {
		if app.Inputs[i].InputID == inputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	app.Inputs[idx].InputProcessingConfigurationDescription = nil

	return nil
}

// DeleteApplicationOutput removes an output configuration from an application.
func (b *InMemoryBackend) DeleteApplicationOutput(
	name string, versionID int64, outputID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i, out := range app.Outputs {
		if out.OutputID == outputID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	app.Outputs = append(app.Outputs[:idx], app.Outputs[idx+1:]...)

	return nil
}

// DeleteApplicationReferenceDataSource removes a reference data source from an application.
func (b *InMemoryBackend) DeleteApplicationReferenceDataSource(
	name string, versionID int64, referenceID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	// Find before bumping.
	idx := -1

	for i, ref := range app.ReferenceDataSources {
		if ref.ReferenceID == referenceID {
			idx = i

			break
		}
	}

	if idx < 0 {
		return ErrNotFound
	}

	if err := checkAndBumpVersion(app, versionID); err != nil {
		return err
	}

	app.ReferenceDataSources = append(app.ReferenceDataSources[:idx], app.ReferenceDataSources[idx+1:]...)

	return nil
}
