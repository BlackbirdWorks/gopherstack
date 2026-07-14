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
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// KinesisAnalytics resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

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
	// ErrTooManyTags is returned when tagging an application would exceed the maximum tag
	// count. AWS models this as a dedicated TooManyTagsException on CreateApplication and
	// TagResource, distinct from the generic LimitExceededException (verified against
	// aws-sdk-go-v2/service/kinesisanalytics deserializers.go per-operation error lists).
	ErrTooManyTags = errors.New("TooManyTagsException: application tag limit exceeded")
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
	maxTagKeyLen             = 128
	maxTagValueLen           = 256
	// maxTagsPerResource is the maximum number of user-defined tags per application. Per AWS
	// docs: "the maximum number of application tags includes system tags. The maximum number
	// of user-defined application tags is 50" -- this is a KDA-specific limit, not the generic
	// 200 used by many other services.
	maxTagsPerResource = 50

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
	CreateApplication(ctx context.Context, name, description, code, serviceRole string,
		inputs []InputDescription, outputs []OutputDescription,
		cwlOptions []CloudWatchLoggingOptionDesc, tags map[string]string) (*Application, error)
	DeleteApplication(ctx context.Context, name string, createTimestamp *time.Time) error
	DescribeApplication(ctx context.Context, name string) (*Application, error)
	ListApplications(ctx context.Context, exclusiveStart string, limit int) ([]*Application, bool, error)
	StartApplication(ctx context.Context, name string, inputConfigs []inputConfiguration) error
	StopApplication(ctx context.Context, name string) error
	UpdateApplication(
		ctx context.Context,
		name string,
		currentVersionID int64,
		update *applicationUpdate,
	) (*Application, error)
	ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error)
	TagResource(ctx context.Context, resourceARN string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	AddApplicationCloudWatchLoggingOption(
		ctx context.Context,
		name string,
		versionID int64,
		option CloudWatchLoggingOptionDesc,
	) error
	AddApplicationInput(ctx context.Context, name string, versionID int64, input InputDescription) error
	AddApplicationInputProcessingConfiguration(
		ctx context.Context,
		name string,
		versionID int64,
		inputID string,
		config *InputProcessingConfigurationDesc,
	) error
	AddApplicationOutput(ctx context.Context, name string, versionID int64, output OutputDescription) error
	AddApplicationReferenceDataSource(
		ctx context.Context,
		name string,
		versionID int64,
		ref ReferenceDataSourceDescription,
	) error
	DeleteApplicationCloudWatchLoggingOption(
		ctx context.Context,
		name string,
		versionID int64,
		loggingOptionID string,
	) error
	DeleteApplicationInputProcessingConfiguration(
		ctx context.Context,
		name string,
		versionID int64,
		inputID string,
	) error
	DeleteApplicationOutput(ctx context.Context, name string, versionID int64, outputID string) error
	DeleteApplicationReferenceDataSource(ctx context.Context, name string, versionID int64, referenceID string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// Applications are keyed by a composite "region#name" key (see
// applicationKey) inside a single flat [store.Table], with secondary
// [store.Index]es grouping them by region (byRegion, replacing the old
// per-region map iteration) and by ARN (byARN, replacing the old
// map[region]map[arn]*Application reverse index) -- see store_setup.go for
// the full rationale.
type InMemoryBackend struct {
	svcCtx        context.Context
	apps          *store.Table[Application]
	appsByRegion  *store.Index[Application]
	appsByARN     *store.Index[Application]
	registry      *store.Registry
	cancelFuncs   map[string]context.CancelFunc
	defaultRegion string
	accountID     string
	nextID        int64
	mu            sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Kinesis Analytics backend with a background service context.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), region, accountID)
}

// NewInMemoryBackendWithContext creates a new in-memory Kinesis Analytics backend whose
// background goroutines are bounded by svcCtx. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, region, accountID string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		cancelFuncs:   make(map[string]context.CancelFunc),
		defaultRegion: region,
		accountID:     accountID,
		svcCtx:        svcCtx,
		registry:      store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// cancelKey returns the composite key used for the cancelFuncs map.
func cancelKey(region, name string) string {
	return region + ":" + name
}

// Reset clears all state and resets the ID counter.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, cancel := range b.cancelFuncs {
		cancel()
	}

	b.registry.ResetAll()
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
		return fmt.Errorf("%w: resource may not have more than %d tags", ErrTooManyTags, maxTagsPerResource)
	}

	return nil
}

// validateARNShape verifies that an ARN refers to a kinesisanalytics application in this backend.
// ARN format: arn:{partition}:kinesisanalytics:{region}:{accountID}:application/{name}.
func (b *InMemoryBackend) validateARNShape(resourceARN, region string) error {
	const arnFieldCount = 6

	parts := strings.SplitN(resourceARN, ":", arnFieldCount)
	if len(parts) != arnFieldCount ||
		parts[0] != "arn" ||
		parts[2] != "kinesisanalytics" ||
		!strings.HasPrefix(parts[5], "application/") {
		return fmt.Errorf("%w: ResourceARN is not a valid kinesisanalytics application ARN", ErrValidation)
	}

	if parts[3] != region || parts[4] != b.accountID {
		return fmt.Errorf("%w: ResourceARN region/account does not match this endpoint", ErrValidation)
	}

	return nil
}

// appByARN looks up the application registered under resourceARN via the
// byARN index. An ApplicationARN embeds its owning region (see
// applicationARN), so it uniquely identifies at most one application across
// every region -- callers must still validate the ARN's shape/region/account
// via validateARNShape before calling this, since the index performs no
// validation of its own. Must be called under b.mu held for reading or
// writing.
func (b *InMemoryBackend) appByARN(resourceARN string) (*Application, bool) {
	matches := b.appsByARN.Get(resourceARN)
	if len(matches) == 0 {
		return nil, false
	}

	return matches[0], true
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

	if cfg.NamePrefix == "" {
		return desc, fmt.Errorf("%w: Input.NamePrefix is required", ErrValidation)
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
			"%w: exactly one of KinesisStreamsInput or KinesisFirehoseInput must be specified", ErrValidation,
		)
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

	if cfg.Name == "" {
		return desc, fmt.Errorf("%w: Output.Name is required", ErrValidation)
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
			ErrValidation,
		)
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
			"%w: DestinationSchema.RecordFormatType must be JSON or CSV", ErrValidation,
		)
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
	ctx context.Context,
	name, description, code, serviceRole string,
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

	// CreateApplication is a modeled source of TooManyTagsException / tag validation errors
	// (same as TagResource), so initial Tags must go through the same validation as a later
	// TagResource call -- previously this was skipped entirely, letting CreateApplication
	// silently accept invalid keys/values and an unbounded tag count.
	if err := validateAndMergeTags(nil, tags); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.appsByRegion.Get(region)) >= maxApplicationsPerRegion {
		return nil, fmt.Errorf(
			"%w: maximum of %d applications per account/region",
			ErrLimitExceeded, maxApplicationsPerRegion,
		)
	}

	if b.apps.Has(applicationKey(region, name)) {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	t := make(map[string]string)
	maps.Copy(t, tags)

	app := &Application{
		ApplicationName:          name,
		ApplicationARN:           applicationARN(region, b.accountID, name),
		ApplicationDescription:   description,
		ApplicationCode:          code,
		ApplicationStatus:        statusReady,
		ApplicationVersionID:     1,
		CreateTimestamp:          &now,
		LastUpdateTimestamp:      &now,
		RuntimeEnvironment:       runtimeEnvironmentV1,
		ServiceExecutionRole:     serviceRole,
		Region:                   region,
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

	b.apps.Put(app)

	return appCopy(app), nil
}

// DeleteApplication marks an application for deletion (DELETING state) then removes it after a delay.
func (b *InMemoryBackend) DeleteApplication(ctx context.Context, name string, createTimestamp *time.Time) error {
	if createTimestamp == nil {
		return fmt.Errorf("%w: CreateTimestamp is required", ErrValidation)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	appKey := applicationKey(region, name)

	app, exists := b.apps.Get(appKey)
	if !exists {
		return ErrNotFound
	}

	if app.CreateTimestamp != nil && createTimestamp.Unix() != app.CreateTimestamp.Unix() {
		return fmt.Errorf("%w: CreateTimestamp does not match stored value", ErrConcurrentUpdate)
	}

	// Cancel any in-flight lifecycle goroutine.
	key := cancelKey(region, name)
	if cancel, ok := b.cancelFuncs[key]; ok {
		cancel()
		delete(b.cancelFuncs, key)
	}

	now := time.Now().UTC()
	app.ApplicationStatus = statusDeleting
	app.LastUpdateTimestamp = &now

	cancelCtx, cancel := context.WithCancel(b.svcCtx)
	b.cancelFuncs[key] = cancel

	go func() {
		defer cancel()

		select {
		case <-cancelCtx.Done():
			return
		case <-time.After(transitionDelay):
		}

		b.mu.Lock()
		defer b.mu.Unlock()

		delete(b.cancelFuncs, key)
		b.apps.Delete(appKey)
	}()

	return nil
}

// DescribeApplication returns a copy of the details for a Kinesis Analytics application.
func (b *InMemoryBackend) DescribeApplication(ctx context.Context, name string) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock()
	defer b.mu.RUnlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return nil, ErrNotFound
	}

	return appCopy(app), nil
}

// ListApplications returns paginated applications.
func (b *InMemoryBackend) ListApplications(
	ctx context.Context,
	exclusiveStart string,
	limit int,
) ([]*Application, bool, error) {
	if limit < 0 {
		return nil, false, fmt.Errorf("%w: Limit must be >= 0", ErrValidation)
	}

	if limit > maxApplicationsPerRegion {
		return nil, false, fmt.Errorf("%w: Limit must not exceed %d", ErrValidation, maxApplicationsPerRegion)
	}

	if limit == 0 {
		limit = maxApplicationsPerRegion
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock()
	defer b.mu.RUnlock()

	regionApps := b.appsByRegion.Get(region)

	all := make([]*Application, 0, len(regionApps))
	all = append(all, regionApps...)

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
func (b *InMemoryBackend) StartApplication(ctx context.Context, name string, inputConfigs []inputConfiguration) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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

	b.launchTransition(region, name, statusRunning)

	return nil
}

// StopApplication transitions an application to READY via a STOPPING transient state.
func (b *InMemoryBackend) StopApplication(ctx context.Context, name string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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

	b.launchTransition(region, name, statusReady)

	return nil
}

// launchTransition starts a goroutine that moves app to targetStatus after transitionDelay.
// Must be called under b.mu.
func (b *InMemoryBackend) launchTransition(region, name, targetStatus string) {
	key := cancelKey(region, name)

	if cancel, ok := b.cancelFuncs[key]; ok {
		cancel()
	}

	ctx, cancel := context.WithCancel(b.svcCtx)
	b.cancelFuncs[key] = cancel

	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-time.After(transitionDelay):
		}

		b.mu.Lock()
		defer b.mu.Unlock()

		app, exists := b.apps.Get(applicationKey(region, name))
		if !exists {
			return
		}

		now := time.Now().UTC()
		app.ApplicationStatus = targetStatus
		app.LastUpdateTimestamp = &now
		delete(b.cancelFuncs, key)
	}()
}

// applyInputUpdates applies input update operations to the application.
func applyInputUpdates(app *Application, updates []inputUpdate) error {
	for _, iu := range updates {
		idx := findInputIndex(app.Inputs, iu.InputID)
		if idx < 0 {
			return fmt.Errorf("%w: InputId %q not found", ErrNotFound, iu.InputID)
		}

		if err := applyOneInputUpdate(&app.Inputs[idx], &iu); err != nil {
			return err
		}
	}

	return nil
}

// applyOneInputUpdate applies a single input update to an InputDescription.
func applyOneInputUpdate(inp *InputDescription, iu *inputUpdate) error {
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

	applyInputSchemaUpdate(inp, iu.InputSchemaUpdate)

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

	if iu.InputParallelismUpdate != nil {
		count := iu.InputParallelismUpdate.Count
		if count < minInputParallelism || count > maxInputParallelism {
			return fmt.Errorf("%w: InputParallelismUpdate.CountUpdate must be %d-%d",
				ErrValidation, minInputParallelism, maxInputParallelism)
		}

		inp.InputParallelism = &InputParallelism{Count: count}
	}

	if inp.InputParallelism != nil {
		inp.InAppStreamNames = inAppStreamNames(inp.NamePrefix, inp.InputParallelism.Count)
	}

	return nil
}

// applyInputSchemaUpdate merges an InputSchemaUpdate payload into an input's schema.
// Unlike ReferenceSchemaUpdate (a whole-object replace using the full SourceSchema shape),
// InputSchemaUpdate carries its own "Update"-suffixed sub-fields and AWS applies it as a
// partial patch: only the sub-fields the caller supplied are overwritten.
func applyInputSchemaUpdate(inp *InputDescription, update *inputSchemaUpdateInput) {
	if update == nil {
		return
	}

	if inp.InputSchema == nil {
		inp.InputSchema = &SourceSchema{}
	}

	if update.RecordFormat != nil {
		inp.InputSchema.RecordFormat = RecordFormat{
			RecordFormatType:  update.RecordFormat.RecordFormatType,
			MappingParameters: update.RecordFormat.MappingParameters,
		}
	}

	if update.RecordEncoding != "" {
		inp.InputSchema.RecordEncoding = update.RecordEncoding
	}

	if update.RecordColumns != nil {
		inp.InputSchema.RecordColumns = update.RecordColumns
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
				BucketARN:        ru.S3ReferenceDataSourceUpdate.BucketARN,
				FileKey:          ru.S3ReferenceDataSourceUpdate.FileKey,
				ReferenceRoleARN: ru.S3ReferenceDataSourceUpdate.ReferenceRoleARN,
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
	ctx context.Context,
	name string,
	currentVersionID int64,
	update *applicationUpdate,
) (*Application, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.defaultRegion)

	if err := b.validateARNShape(resourceARN, region); err != nil {
		return nil, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	app, ok := b.appByARN(resourceARN)
	if !ok {
		return nil, ErrNotFound
	}

	result := make(map[string]string, len(app.Tags))
	maps.Copy(result, app.Tags)

	return result, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags map[string]string) error {
	region := getRegion(ctx, b.defaultRegion)

	if err := b.validateARNShape(resourceARN, region); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.appByARN(resourceARN)
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
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.defaultRegion)

	if err := b.validateARNShape(resourceARN, region); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	app, ok := b.appByARN(resourceARN)
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
// The region is extracted from the application ARN.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := appCopy(app)

	// Extract region from ARN (format: arn:partition:service:region:account:resource).
	region := b.defaultRegion
	const arnFieldCount = 6

	parts := strings.SplitN(cp.ApplicationARN, ":", arnFieldCount)

	if len(parts) == arnFieldCount && parts[3] != "" {
		region = parts[3]
	}

	cp.Region = region

	b.apps.Put(cp)
}

// AddApplicationCloudWatchLoggingOption adds a CloudWatch logging option to an application.
func (b *InMemoryBackend) AddApplicationCloudWatchLoggingOption(
	ctx context.Context, name string, versionID int64, option CloudWatchLoggingOptionDesc,
) error {
	if option.LogStreamARN == "" {
		return fmt.Errorf("%w: CloudWatchLoggingOption.LogStreamARN is required", ErrValidation)
	}

	if option.RoleARN == "" {
		return fmt.Errorf("%w: CloudWatchLoggingOption.RoleARN is required", ErrValidation)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationCloudWatchLoggingOption's modeled error set (verified against
	// aws-sdk-go-v2/service/kinesisanalytics deserializers.go) has no LimitExceededException --
	// this per-application cap violation surfaces as InvalidArgumentException instead.
	if len(app.CloudWatchLoggingOptions) >= maxCWLOptions {
		return fmt.Errorf(
			"%w: maximum of %d CloudWatch logging options per application",
			ErrValidation,
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
	ctx context.Context, name string, versionID int64, input InputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationInput's modeled error set has no LimitExceededException -- the hard
	// architectural cap of one input per SQL application surfaces as InvalidArgumentException.
	if len(app.Inputs) >= maxInputs {
		return fmt.Errorf("%w: maximum of %d input(s) per application", ErrValidation, maxInputs)
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
	ctx context.Context, name string, versionID int64, inputID string, config *InputProcessingConfigurationDesc,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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
	ctx context.Context, name string, versionID int64, output OutputDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationOutput's modeled error set has no LimitExceededException -- the cap of
	// three outputs per application surfaces as InvalidArgumentException.
	if len(app.Outputs) >= maxOutputs {
		return fmt.Errorf("%w: maximum of %d outputs per application", ErrValidation, maxOutputs)
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
	ctx context.Context, name string, versionID int64, ref ReferenceDataSourceDescription,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
	if !exists {
		return ErrNotFound
	}

	// AddApplicationReferenceDataSource's modeled error set has no LimitExceededException --
	// the cap of one reference data source per application surfaces as InvalidArgumentException.
	if len(app.ReferenceDataSources) >= maxRefSources {
		return fmt.Errorf("%w: maximum of %d reference data source(s) per application", ErrValidation, maxRefSources)
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
	ctx context.Context, name string, versionID int64, loggingOptionID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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
	ctx context.Context, name string, versionID int64, inputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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
	ctx context.Context, name string, versionID int64, outputID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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
	ctx context.Context, name string, versionID int64, referenceID string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	app, exists := b.apps.Get(applicationKey(region, name))
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

// Region returns the default region for this backend.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }
