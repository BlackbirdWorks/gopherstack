package kinesisanalytics

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
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
	// ErrLimitExceeded is returned when a resource limit is reached.
	ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)
	// ErrResourceInUse is returned when the app is in an incompatible state for the requested operation.
	ErrResourceInUse = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
)

const (
	statusReady         = "READY"
	statusRunning       = "RUNNING"
	statusStarting      = "STARTING"
	statusStopping      = "STOPPING"
	statusUpdating      = "UPDATING"
	statusDeleting      = "DELETING"
	statusAutoScaling   = "AUTOSCALING"    //nolint:deadcode // AWS status constant
	statusForceStopping = "FORCE_STOPPING" //nolint:deadcode // AWS status constant
	statusMaintenance   = "MAINTENANCE"    //nolint:deadcode // AWS status constant
	statusRollingBack   = "ROLLING_BACK"   //nolint:deadcode // AWS status constant
	statusRolledBack    = "ROLLED_BACK"    //nolint:deadcode // AWS status constant

	runtimeEnvironmentV1 = "SQL-1_0"

	maxApplicationsPerRegion = 50
	maxInputs                = 1
	maxOutputs               = 3
	maxRefSources            = 1
	maxCWLOptions            = 50
	maxTagsPerResource       = 200
	maxTagKeyLen             = 128
	maxTagValueLen           = 256

	maxAppNameLen = 128
	maxAppDescLen = 1024
	maxAppCodeLen = 102400

	// recordFormatJSON is the JSON record format type constant.
	recordFormatJSON = "JSON"

	maxInputParallelism = 64
	minInputParallelism = 1

	// transitionDelay is the simulated time in transient lifecycle states.
	transitionDelay = 50 * time.Millisecond
)

var appNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

// StorageBackend is the interface for the Kinesis Analytics in-memory backend.
type StorageBackend interface {
	CreateApplication(region, accountID, name, description, code, serviceRole string,
		inputs []InputDescription, outputs []OutputDescription,
		cwlOptions []CloudWatchLoggingOptionDesc, tags map[string]string) (*Application, error)
	DeleteApplication(name string, createTimestamp *time.Time) error
	DescribeApplication(name string) (*Application, error)
	ListApplications(exclusiveStart string, limit int) ([]*Application, bool, error)
	StartApplication(name string, inputConfigs []inputConfiguration) error
	StopApplication(name string) error
	UpdateApplication(name string, currentVersionID int64, update *applicationUpdate) (*Application, error)
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
	apps        map[string]*Application
	appsByARN   map[string]*Application       // application ARN → Application
	cancelFuncs map[string]context.CancelFunc // application name → lifecycle goroutine cancel
	region      string
	accountID   string
	nextID      int64
	mu          sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Kinesis Analytics backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		apps:        make(map[string]*Application),
		appsByARN:   make(map[string]*Application),
		cancelFuncs: make(map[string]context.CancelFunc),
		region:      region,
		accountID:   accountID,
	}
}

// Reset clears all state and resets the ID counter.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, cancel := range b.cancelFuncs {
		cancel()
	}

	b.apps = make(map[string]*Application)
	b.appsByARN = make(map[string]*Application)
	b.cancelFuncs = make(map[string]context.CancelFunc)
	b.nextID = 0
}

// applicationARN builds the ARN for a Kinesis Analytics application.
func applicationARN(region, accountID, name string) string {
	return arn.Build("kinesisanalytics", region, accountID, "application/"+name)
}

// validateApplicationName returns an error if the name violates AWS rules.
func validateApplicationName(name string) error {
	if len(name) == 0 || len(name) > maxAppNameLen {
		return fmt.Errorf("%w: ApplicationName must be 1-%d characters", ErrValidation, maxAppNameLen)
	}

	if !appNameRegexp.MatchString(name) {
		return fmt.Errorf("%w: ApplicationName must match [a-zA-Z0-9_.\\-]+", ErrValidation)
	}

	return nil
}

// validateApplicationDescription returns an error if the description violates AWS rules.
func validateApplicationDescription(desc string) error {
	if len(desc) > maxAppDescLen {
		return fmt.Errorf("%w: ApplicationDescription must be 0-%d characters", ErrValidation, maxAppDescLen)
	}

	return nil
}

// validateApplicationCode returns an error if the code violates AWS rules.
func validateApplicationCode(code string) error {
	if len(code) > maxAppCodeLen {
		return fmt.Errorf("%w: ApplicationCode must be 0-%d characters", ErrValidation, maxAppCodeLen)
	}

	return nil
}

// validateTagKey returns an error if the tag key violates AWS rules.
func validateTagKey(key string) error {
	if len(key) == 0 || len(key) > maxTagKeyLen {
		return fmt.Errorf("%w: tag key must be 1-%d characters", ErrValidation, maxTagKeyLen)
	}

	if strings.HasPrefix(key, "aws:") {
		return fmt.Errorf("%w: tag key must not start with \"aws:\"", ErrValidation)
	}

	return nil
}

// validateTagValue returns an error if the tag value violates AWS rules.
func validateTagValue(value string) error {
	if len(value) > maxTagValueLen {
		return fmt.Errorf("%w: tag value must be 0-%d characters", ErrValidation, maxTagValueLen)
	}

	return nil
}

// validateAndMergeTags validates incoming tags and checks the per-resource cap.
func validateAndMergeTags(existing, incoming map[string]string) error {
	for k, v := range incoming {
		if err := validateTagKey(k); err != nil {
			return err
		}

		if err := validateTagValue(v); err != nil {
			return err
		}
	}

	total := len(existing)

	for k := range incoming {
		if _, alreadyPresent := existing[k]; !alreadyPresent {
			total++
		}
	}

	if total > maxTagsPerResource {
		return fmt.Errorf("%w: resource may not have more than %d tags", ErrLimitExceeded, maxTagsPerResource)
	}

	return nil
}

// validateARNShape verifies that an ARN refers to a kinesisanalytics application in this backend.
// ARN format: arn:{partition}:kinesisanalytics:{region}:{accountID}:application/{name}.
func (b *InMemoryBackend) validateARNShape(resourceARN string) error {
	const arnFieldCount = 6

	parts := strings.SplitN(resourceARN, ":", arnFieldCount)
	if len(parts) != arnFieldCount ||
		parts[0] != "arn" ||
		parts[2] != "kinesisanalytics" ||
		!strings.HasPrefix(parts[5], "application/") {
		return fmt.Errorf("%w: ResourceARN is not a valid kinesisanalytics application ARN", ErrValidation)
	}

	if parts[3] != b.region || parts[4] != b.accountID {
		return fmt.Errorf("%w: ResourceARN region/account does not match this endpoint", ErrValidation)
	}

	return nil
}

// inAppStreamNames derives the in-application stream names from a NamePrefix and parallelism count.
func inAppStreamNames(namePrefix string, count int) []string {
	if namePrefix == "" || count <= 0 {
		return nil
	}

	names := make([]string, count)

	for i := range count {
		names[i] = fmt.Sprintf("%s_%03d", namePrefix, i+1)
	}

	return names
}

// convertInputConfig converts a request input config to an InputDescription.
func convertInputConfig(cfg *applicationInputConfig) (InputDescription, error) {
	var desc InputDescription

	if cfg == nil {
		return desc, nil
	}

	desc.NamePrefix = cfg.NamePrefix

	// Enforce mutual exclusion of input source types.
	sourceCount := 0
	if cfg.KinesisStreamsInput != nil {
		sourceCount++
	}

	if cfg.KinesisFirehoseInput != nil {
		sourceCount++
	}

	if sourceCount > 1 {
		return desc, fmt.Errorf(
			"%w: exactly one of KinesisStreamsInput or KinesisFirehoseInput must be specified", ErrValidation)
	}

	if cfg.KinesisStreamsInput != nil {
		desc.KinesisStreamsInputDescription = &KinesisStreamsInputDesc{
			ResourceARN: cfg.KinesisStreamsInput.ResourceARN,
			RoleARN:     cfg.KinesisStreamsInput.RoleARN,
		}
	}

	if cfg.KinesisFirehoseInput != nil {
		desc.KinesisFirehoseInputDescription = &KinesisFirehoseInputDesc{
			ResourceARN: cfg.KinesisFirehoseInput.ResourceARN,
			RoleARN:     cfg.KinesisFirehoseInput.RoleARN,
		}
	}

	parallelism := 1
	if cfg.InputParallelism != nil {
		parallelism = cfg.InputParallelism.Count
	}

	if parallelism < minInputParallelism || parallelism > maxInputParallelism {
		return desc, fmt.Errorf("%w: InputParallelism.Count must be %d-%d",
			ErrValidation, minInputParallelism, maxInputParallelism)
	}

	desc.InputParallelism = &InputParallelism{Count: parallelism}
	desc.InAppStreamNames = inAppStreamNames(cfg.NamePrefix, parallelism)

	if cfg.InputSchema != nil {
		schema := convertSourceSchema(cfg.InputSchema)
		desc.InputSchema = &schema
	}

	if cfg.InputProcessingConfiguration != nil && cfg.InputProcessingConfiguration.InputLambdaProcessor != nil {
		desc.InputProcessingConfigurationDescription = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: cfg.InputProcessingConfiguration.InputLambdaProcessor.ResourceARN,
				RoleARN:     cfg.InputProcessingConfiguration.InputLambdaProcessor.RoleARN,
			},
		}
	}

	return desc, nil
}

// convertOutputConfig converts a request output config to an OutputDescription.
func convertOutputConfig(cfg *applicationOutputConfig) (OutputDescription, error) {
	var desc OutputDescription

	if cfg == nil {
		return desc, fmt.Errorf("%w: Output is required", ErrValidation)
	}

	desc.Name = cfg.Name

	// Enforce mutual exclusion of output destination types.
	destCount := 0
	if cfg.KinesisStreamsOutput != nil {
		destCount++
	}

	if cfg.KinesisFirehoseOutput != nil {
		destCount++
	}

	if cfg.LambdaOutput != nil {
		destCount++
	}

	if destCount > 1 {
		return desc, fmt.Errorf(
			"%w: exactly one of KinesisStreamsOutput, KinesisFirehoseOutput, or LambdaOutput must be specified",
			ErrValidation)
	}

	if cfg.KinesisStreamsOutput != nil {
		desc.KinesisStreamsOutputDescription = &KinesisStreamsOutputDesc{
			ResourceARN: cfg.KinesisStreamsOutput.ResourceARN,
			RoleARN:     cfg.KinesisStreamsOutput.RoleARN,
		}
	}

	if cfg.KinesisFirehoseOutput != nil {
		desc.KinesisFirehoseOutputDescription = &KinesisFirehoseOutputDesc{
			ResourceARN: cfg.KinesisFirehoseOutput.ResourceARN,
			RoleARN:     cfg.KinesisFirehoseOutput.RoleARN,
		}
	}

	if cfg.LambdaOutput != nil {
		desc.LambdaOutputDescription = &LambdaOutputDesc{
			ResourceARN: cfg.LambdaOutput.ResourceARN,
			RoleARN:     cfg.LambdaOutput.RoleARN,
		}
	}

	if cfg.DestinationSchema == nil {
		return desc, fmt.Errorf("%w: DestinationSchema is required", ErrValidation)
	}

	ft := cfg.DestinationSchema.RecordFormatType
	if ft != recordFormatJSON && ft != "CSV" {
		return desc, fmt.Errorf(
			"%w: DestinationSchema.RecordFormatType must be JSON or CSV", ErrValidation)
	}

	desc.DestinationSchema = &DestinationSchemaDesc{RecordFormatType: ft}

	return desc, nil
}

// convertSourceSchema converts a request source schema input to a SourceSchema.
func convertSourceSchema(s *sourceSchemaInput) SourceSchema {
	return SourceSchema{
		RecordEncoding: s.RecordEncoding,
		RecordColumns:  s.RecordColumns,
		RecordFormat: RecordFormat{
			RecordFormatType:  s.RecordFormat.RecordFormatType,
			MappingParameters: s.RecordFormat.MappingParameters,
		},
	}
}

// CreateApplication creates a new Kinesis Analytics application.
func (b *InMemoryBackend) CreateApplication(
	region, accountID, name, description, code, serviceRole string,
	inputs []InputDescription,
	outputs []OutputDescription,
	cwlOptions []CloudWatchLoggingOptionDesc,
	tags map[string]string,
) (*Application, error) {
	if err := validateApplicationName(name); err != nil {
		return nil, err
	}

	if err := validateApplicationDescription(description); err != nil {
		return nil, err
	}

	if err := validateApplicationCode(code); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.apps) >= maxApplicationsPerRegion {
		return nil, fmt.Errorf(
			"%w: maximum of %d applications per account/region",
			ErrLimitExceeded, maxApplicationsPerRegion,
		)
	}

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
		RuntimeEnvironment:       runtimeEnvironmentV1,
		ServiceExecutionRole:     serviceRole,
		Tags:                     t,
		CloudWatchLoggingOptions: make([]CloudWatchLoggingOptionDesc, 0),
		Inputs:                   make([]InputDescription, 0),
		Outputs:                  make([]OutputDescription, 0),
		ReferenceDataSources:     make([]ReferenceDataSourceDescription, 0),
	}

	for _, cwl := range cwlOptions {
		cwl.CloudWatchLoggingOptionID = b.newResourceID("cwl")
		app.CloudWatchLoggingOptions = append(app.CloudWatchLoggingOptions, cwl)
	}

	for _, inp := range inputs {
		inp.InputID = b.newResourceID("input")
		app.Inputs = append(app.Inputs, inp)
	}

	for _, out := range outputs {
		out.OutputID = b.newResourceID("output")
		app.Outputs = append(app.Outputs, out)
	}

	b.apps[name] = app
	b.appsByARN[app.ApplicationARN] = app

	return appCopy(app), nil
}

// DeleteApplication marks an application for deletion (DELETING state) then removes it after a delay.
func (b *InMemoryBackend) DeleteApplication(name string, createTimestamp *time.Time) error {
	if createTimestamp == nil {
		return fmt.Errorf("%w: CreateTimestamp is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if app.CreateTimestamp != nil && createTimestamp.Unix() != app.CreateTimestamp.Unix() {
		return fmt.Errorf(
			"%w: CreateTimestamp does not match stored value",
			awserr.New("ConcurrentModificationException", awserr.ErrConflict),
		)
	}

	// Cancel any in-flight lifecycle goroutine.
	if cancel, ok := b.cancelFuncs[name]; ok {
		cancel()
		delete(b.cancelFuncs, name)
	}

	now := time.Now().UTC()
	app.ApplicationStatus = statusDeleting
	app.LastUpdateTimestamp = &now

	appARN := app.ApplicationARN
	ctx, cancel := context.WithCancel(context.Background())
	b.cancelFuncs[name] = cancel

	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-time.After(transitionDelay):
		}

		b.mu.Lock()
		defer b.mu.Unlock()

		delete(b.cancelFuncs, name)
		delete(b.apps, name)
		delete(b.appsByARN, appARN)
	}()

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

// ListApplications returns paginated applications.
func (b *InMemoryBackend) ListApplications(exclusiveStart string, limit int) ([]*Application, bool, error) {
	if limit < 0 {
		return nil, false, fmt.Errorf("%w: Limit must be >= 0", ErrValidation)
	}

	if limit > maxApplicationsPerRegion {
		return nil, false, fmt.Errorf("%w: Limit must not exceed %d", ErrValidation, maxApplicationsPerRegion)
	}

	if limit == 0 {
		limit = maxApplicationsPerRegion
	}

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

	if len(all) > limit {
		return all[:limit], true, nil
	}

	return all, false, nil
}

// validateAndApplyInputConfigs checks that all input configurations reference known input IDs
// and applies starting position configurations. Must be called under b.mu.
func validateAndApplyInputConfigs(app *Application, inputConfigs []inputConfiguration) error {
	for _, ic := range inputConfigs {
		if ic.ID == "" {
			return fmt.Errorf("%w: InputConfigurations[].Id is required", ErrValidation)
		}

		idx := findInputIndex(app.Inputs, ic.ID)
		if idx < 0 {
			return fmt.Errorf("%w: input ID %q not found on application", ErrNotFound, ic.ID)
		}

		if ic.InputStartingPositionConfiguration != nil {
			app.Inputs[idx].InputStartingPositionConfiguration = ic.InputStartingPositionConfiguration
		}
	}

	return nil
}

// findInputIndex returns the index of the input with the given ID, or -1 if not found.
func findInputIndex(inputs []InputDescription, inputID string) int {
	for i := range inputs {
		if inputs[i].InputID == inputID {
			return i
		}
	}

	return -1
}

// StartApplication transitions an application to RUNNING via a STARTING transient state.
func (b *InMemoryBackend) StartApplication(name string, inputConfigs []inputConfiguration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if app.ApplicationStatus != statusReady {
		return fmt.Errorf("%w: application must be in %s state to start (current: %s)",
			ErrResourceInUse, statusReady, app.ApplicationStatus)
	}

	if err := validateAndApplyInputConfigs(app, inputConfigs); err != nil {
		return err
	}

	now := time.Now().UTC()
	app.ApplicationStatus = statusStarting
	app.LastUpdateTimestamp = &now

	b.launchTransition(name, statusRunning)

	return nil
}

// StopApplication transitions an application to READY via a STOPPING transient state.
func (b *InMemoryBackend) StopApplication(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if app.ApplicationStatus != statusRunning {
		return fmt.Errorf("%w: application must be in %s state to stop (current: %s)",
			ErrResourceInUse, statusRunning, app.ApplicationStatus)
	}

	now := time.Now().UTC()
	app.ApplicationStatus = statusStopping
	app.LastUpdateTimestamp = &now

	b.launchTransition(name, statusReady)

	return nil
}

// launchTransition starts a goroutine that moves app to targetStatus after transitionDelay.
// Must be called under b.mu.
func (b *InMemoryBackend) launchTransition(name, targetStatus string) {
	if cancel, ok := b.cancelFuncs[name]; ok {
		cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancelFuncs[name] = cancel

	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-time.After(transitionDelay):
		}

		b.mu.Lock()
		defer b.mu.Unlock()

		app, exists := b.apps[name]
		if !exists {
			return
		}

		now := time.Now().UTC()
		app.ApplicationStatus = targetStatus
		app.LastUpdateTimestamp = &now
		delete(b.cancelFuncs, name)
	}()
}

// applyInputUpdates applies input update operations to the application.
func applyInputUpdates(app *Application, updates []inputUpdate) error {
	for _, iu := range updates {
		idx := findInputIndex(app.Inputs, iu.InputID)
		if idx < 0 {
			return fmt.Errorf("%w: InputId %q not found", ErrNotFound, iu.InputID)
		}

		applyOneInputUpdate(&app.Inputs[idx], &iu)
	}

	return nil
}

// applyOneInputUpdate applies a single input update to an InputDescription.
func applyOneInputUpdate(inp *InputDescription, iu *inputUpdate) {
	if iu.NamePrefixUpdate != "" {
		inp.NamePrefix = iu.NamePrefixUpdate
	}

	if iu.KinesisStreamsInputUpdate != nil {
		inp.KinesisStreamsInputDescription = &KinesisStreamsInputDesc{
			ResourceARN: iu.KinesisStreamsInputUpdate.ResourceARN,
			RoleARN:     iu.KinesisStreamsInputUpdate.RoleARN,
		}
		inp.KinesisFirehoseInputDescription = nil
	}

	if iu.KinesisFirehoseInputUpdate != nil {
		inp.KinesisFirehoseInputDescription = &KinesisFirehoseInputDesc{
			ResourceARN: iu.KinesisFirehoseInputUpdate.ResourceARN,
			RoleARN:     iu.KinesisFirehoseInputUpdate.RoleARN,
		}
		inp.KinesisStreamsInputDescription = nil
	}

	if iu.InputSchemaUpdate != nil {
		schema := convertSourceSchema(iu.InputSchemaUpdate)
		inp.InputSchema = &schema
	}

	if iu.InputStartingPositionConfiguration != nil {
		inp.InputStartingPositionConfiguration = iu.InputStartingPositionConfiguration
	}

	if iu.InputProcessingConfigurationUpdate != nil &&
		iu.InputProcessingConfigurationUpdate.InputLambdaProcessor != nil {
		inp.InputProcessingConfigurationDescription = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: iu.InputProcessingConfigurationUpdate.InputLambdaProcessor.ResourceARN,
				RoleARN:     iu.InputProcessingConfigurationUpdate.InputLambdaProcessor.RoleARN,
			},
		}
	}

	if inp.InputParallelism != nil {
		inp.InAppStreamNames = inAppStreamNames(inp.NamePrefix, inp.InputParallelism.Count)
	}
}

// applyOutputUpdates applies output update operations to the application.
func applyOutputUpdates(app *Application, updates []outputUpdate) error {
	for _, ou := range updates {
		idx := findOutputIndex(app.Outputs, ou.OutputID)
		if idx < 0 {
			return fmt.Errorf("%w: OutputId %q not found", ErrNotFound, ou.OutputID)
		}

		if err := applyOneOutputUpdate(&app.Outputs[idx], &ou); err != nil {
			return err
		}
	}

	return nil
}

// findOutputIndex returns the index of the output with the given ID, or -1 if not found.
func findOutputIndex(outputs []OutputDescription, outputID string) int {
	for i := range outputs {
		if outputs[i].OutputID == outputID {
			return i
		}
	}

	return -1
}

// applyOneOutputUpdate applies a single output update to an OutputDescription.
func applyOneOutputUpdate(out *OutputDescription, ou *outputUpdate) error {
	if ou.NameUpdate != "" {
		out.Name = ou.NameUpdate
	}

	if ou.KinesisStreamsOutputUpdate != nil {
		out.KinesisStreamsOutputDescription = &KinesisStreamsOutputDesc{
			ResourceARN: ou.KinesisStreamsOutputUpdate.ResourceARN,
			RoleARN:     ou.KinesisStreamsOutputUpdate.RoleARN,
		}
		out.KinesisFirehoseOutputDescription = nil
		out.LambdaOutputDescription = nil
	}

	if ou.KinesisFirehoseOutputUpdate != nil {
		out.KinesisFirehoseOutputDescription = &KinesisFirehoseOutputDesc{
			ResourceARN: ou.KinesisFirehoseOutputUpdate.ResourceARN,
			RoleARN:     ou.KinesisFirehoseOutputUpdate.RoleARN,
		}
		out.KinesisStreamsOutputDescription = nil
		out.LambdaOutputDescription = nil
	}

	if ou.LambdaOutputUpdate != nil {
		out.LambdaOutputDescription = &LambdaOutputDesc{
			ResourceARN: ou.LambdaOutputUpdate.ResourceARN,
			RoleARN:     ou.LambdaOutputUpdate.RoleARN,
		}
		out.KinesisStreamsOutputDescription = nil
		out.KinesisFirehoseOutputDescription = nil
	}

	if ou.DestinationSchemaUpdate != nil {
		ft := ou.DestinationSchemaUpdate.RecordFormatType
		if ft != recordFormatJSON && ft != "CSV" {
			return fmt.Errorf(
				"%w: DestinationSchema.RecordFormatType must be JSON or CSV",
				ErrValidation,
			)
		}

		out.DestinationSchema = &DestinationSchemaDesc{RecordFormatType: ft}
	}

	return nil
}

// applyReferenceDataSourceUpdates applies reference data source updates to the application.
func applyReferenceDataSourceUpdates(
	app *Application,
	updates []referenceDataSourceUpdate,
) error {
	for _, ru := range updates {
		idx := findReferenceIndex(app.ReferenceDataSources, ru.ReferenceID)
		if idx < 0 {
			return fmt.Errorf("%w: ReferenceId %q not found", ErrNotFound, ru.ReferenceID)
		}

		ref := &app.ReferenceDataSources[idx]

		if ru.TableNameUpdate != "" {
			ref.TableName = ru.TableNameUpdate
		}

		if ru.S3ReferenceDataSourceUpdate != nil {
			ref.S3ReferenceDataSourceDescription = &S3ReferenceDataSourceDesc{
				BucketARN: ru.S3ReferenceDataSourceUpdate.BucketARN,
				FileKey:   ru.S3ReferenceDataSourceUpdate.FileKey,
				RoleARN:   ru.S3ReferenceDataSourceUpdate.RoleARN,
			}
		}

		if ru.ReferenceSchemaUpdate != nil {
			schema := convertSourceSchema(ru.ReferenceSchemaUpdate)
			ref.ReferenceSchema = &schema
		}
	}

	return nil
}

// findReferenceIndex returns the index of the reference with the given ID, or -1.
func findReferenceIndex(refs []ReferenceDataSourceDescription, refID string) int {
	for i := range refs {
		if refs[i].ReferenceID == refID {
			return i
		}
	}

	return -1
}

// applyCWLOptionUpdates applies CloudWatch logging option updates to the application.
func applyCWLOptionUpdates(
	app *Application,
	updates []cwlOptionUpdate,
) error {
	for _, cu := range updates {
		idx := findCWLOptionIndex(
			app.CloudWatchLoggingOptions,
			cu.CloudWatchLoggingOptionID,
		)
		if idx < 0 {
			return fmt.Errorf(
				"%w: CloudWatchLoggingOptionId %q not found",
				ErrNotFound, cu.CloudWatchLoggingOptionID,
			)
		}

		opt := &app.CloudWatchLoggingOptions[idx]

		if cu.LogStreamARNUpdate != "" {
			opt.LogStreamARN = cu.LogStreamARNUpdate
		}

		if cu.RoleARNUpdate != "" {
			opt.RoleARN = cu.RoleARNUpdate
		}
	}

	return nil
}

// findCWLOptionIndex returns the index of the CWL option with the given ID, or -1.
func findCWLOptionIndex(opts []CloudWatchLoggingOptionDesc, optID string) int {
	for i := range opts {
		if opts[i].CloudWatchLoggingOptionID == optID {
			return i
		}
	}

	return -1
}

// applyUpdate applies the full application update payload. Must be called under b.mu.
func applyUpdate(app *Application, update *applicationUpdate) error {
	if update == nil {
		return nil
	}

	if update.ApplicationCodeUpdate != "" {
		app.ApplicationCode = update.ApplicationCodeUpdate
	}

	if err := applyInputUpdates(app, update.InputUpdates); err != nil {
		return err
	}

	if err := applyOutputUpdates(app, update.OutputUpdates); err != nil {
		return err
	}

	if err := applyReferenceDataSourceUpdates(app, update.ReferenceDataSourceUpdates); err != nil {
		return err
	}

	return applyCWLOptionUpdates(app, update.CloudWatchLoggingOptionUpdates)
}

// UpdateApplication updates the application with the full update payload and bumps the version.
func (b *InMemoryBackend) UpdateApplication(
	name string,
	currentVersionID int64,
	update *applicationUpdate,
) (*Application, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return nil, ErrNotFound
	}

	if app.ApplicationStatus != statusReady && app.ApplicationStatus != statusRunning {
		return nil, fmt.Errorf(
			"%w: application must be in READY or RUNNING state to update (current: %s)",
			ErrResourceInUse, app.ApplicationStatus,
		)
	}

	if app.ApplicationVersionID != currentVersionID {
		return nil, ErrConcurrentUpdate
	}

	if err := applyUpdate(app, update); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	app.ApplicationVersionID++
	app.LastUpdateTimestamp = &now

	return appCopy(app), nil
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	if err := b.validateARNShape(resourceARN); err != nil {
		return nil, err
	}

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
	if err := b.validateARNShape(resourceARN); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.appsByARN[resourceARN]
	if !ok {
		return ErrNotFound
	}

	if app.Tags == nil {
		app.Tags = make(map[string]string)
	}

	if err := validateAndMergeTags(app.Tags, tags); err != nil {
		return err
	}

	maps.Copy(app.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	if err := b.validateARNShape(resourceARN); err != nil {
		return err
	}

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

func copyInputParallelism(p *InputParallelism) *InputParallelism {
	if p == nil {
		return nil
	}

	cp := *p

	return &cp
}

func copyInputStartingPosition(s *InputStartingPositionConfiguration) *InputStartingPositionConfiguration {
	if s == nil {
		return nil
	}

	cp := *s

	return &cp
}

func copySourceSchema(s *SourceSchema) *SourceSchema {
	if s == nil {
		return nil
	}

	cp := *s
	cp.RecordColumns = make([]RecordColumn, len(s.RecordColumns))
	copy(cp.RecordColumns, s.RecordColumns)

	if s.RecordFormat.MappingParameters != nil {
		mp := *s.RecordFormat.MappingParameters
		cp.RecordFormat.MappingParameters = &mp
	}

	return &cp
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

		dup.InputParallelism = copyInputParallelism(inp.InputParallelism)
		dup.InputStartingPositionConfiguration = copyInputStartingPosition(inp.InputStartingPositionConfiguration)
		dup.InputSchema = copySourceSchema(inp.InputSchema)

		if inp.InAppStreamNames != nil {
			dup.InAppStreamNames = make([]string, len(inp.InAppStreamNames))
			copy(dup.InAppStreamNames, inp.InAppStreamNames)
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

		dup.ReferenceSchema = copySourceSchema(ref.ReferenceSchema)
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
	if option.LogStreamARN == "" {
		return fmt.Errorf("%w: CloudWatchLoggingOption.LogStreamARN is required", ErrValidation)
	}

	if option.RoleARN == "" {
		return fmt.Errorf("%w: CloudWatchLoggingOption.RoleARN is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps[name]
	if !exists {
		return ErrNotFound
	}

	if len(app.CloudWatchLoggingOptions) >= maxCWLOptions {
		return fmt.Errorf(
			"%w: maximum of %d CloudWatch logging options per application",
			ErrLimitExceeded,
			maxCWLOptions,
		)
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

	if len(app.Inputs) >= maxInputs {
		return fmt.Errorf("%w: maximum of %d input(s) per application", ErrLimitExceeded, maxInputs)
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

	if len(app.Outputs) >= maxOutputs {
		return fmt.Errorf("%w: maximum of %d outputs per application", ErrLimitExceeded, maxOutputs)
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

	if len(app.ReferenceDataSources) >= maxRefSources {
		return fmt.Errorf("%w: maximum of %d reference data source(s) per application", ErrLimitExceeded, maxRefSources)
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
