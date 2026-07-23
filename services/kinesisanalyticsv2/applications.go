package kinesisanalyticsv2

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

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
	now := time.Now().UTC()
	app := &Application{
		ApplicationARN:                    appARN,
		ApplicationName:                   name,
		ApplicationStatus:                 ApplicationStatusReady,
		RuntimeEnvironment:                runtimeEnv,
		ServiceExecutionRole:              serviceRole,
		ApplicationDescription:            description,
		ApplicationMode:                   mode,
		Region:                            region,
		ApplicationVersionID:              1,
		Tags:                              cloneTags(tags),
		CreatedAt:                         now,
		ApplicationVersionCreateTimestamp: now,
		CloudWatchLoggingOptionDescs:      []CloudWatchLoggingOptionDesc{},
		InputDescriptions:                 []InputDescription{},
		OutputDescriptions:                []OutputDescription{},
		ReferenceDataSourceDescriptions:   []ReferenceDataSourceDescription{},
		VpcConfigurationDescriptions:      []VpcConfigurationDescription{},
	}
	b.applications.Put(app)
	b.versionsStore(region)[name] = []*Application{appCopy(app)}

	return app, nil
}

// SeedApplicationConfiguration sets a newly created application's initial
// configuration (SQL inputs/outputs/reference-data-sources, VPC
// configurations, CloudWatch logging options, and the Flink/Code/
// Environment/Snapshot/Rollback/Encryption portions of
// ApplicationConfiguration) in one step, without bumping ApplicationVersionId
// or appending a second version-history entry -- this mirrors real AWS,
// where CreateApplication's inline ApplicationConfiguration is part of the
// application's first version (ApplicationVersionId stays 1), unlike the
// separately-versioned Add* operations. Callers (handleCreateApplication)
// must invoke this immediately after CreateApplication succeeds, before the
// new application is exposed to any other caller. Returns ErrNotFound if
// name doesn't exist.
func (b *InMemoryBackend) SeedApplicationConfiguration(ctx context.Context, name string, cfg SeedConfig) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("SeedApplicationConfiguration")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	b.seedSQLConfig(app, cfg)
	b.seedVpcConfig(app, cfg.VpcConfigs)
	b.seedCWLOptions(app, cfg.CWLOptions)
	seedExtendedConfig(app, cfg)

	// Refresh the version-1 history snapshot recorded by CreateApplication so
	// DescribeApplicationVersion(name, 1) reflects the seeded configuration
	// too, not just the bare application.
	if vers := b.versionsStore(region)[name]; len(vers) > 0 {
		vers[len(vers)-1] = appCopy(app)
	}

	return nil
}

// seedSQLConfig appends the inline SqlApplicationConfiguration
// inputs/outputs/reference-data-sources, assigning fresh resource IDs. Must
// be called under b.mu.
func (b *InMemoryBackend) seedSQLConfig(app *Application, cfg SeedConfig) {
	for i := range cfg.Inputs {
		cfg.Inputs[i].InputID = b.newResourceID("input")
	}

	app.InputDescriptions = append(app.InputDescriptions, cfg.Inputs...)

	for i := range cfg.Outputs {
		cfg.Outputs[i].OutputID = b.newResourceID("output")
	}

	app.OutputDescriptions = append(app.OutputDescriptions, cfg.Outputs...)

	for i := range cfg.ReferenceDataSources {
		cfg.ReferenceDataSources[i].ReferenceID = b.newResourceID("ref")
	}

	app.ReferenceDataSourceDescriptions = append(app.ReferenceDataSourceDescriptions, cfg.ReferenceDataSources...)
}

// seedVpcConfig appends inline VpcConfigurations, assigning fresh resource
// IDs and normalizing nil subnet/security-group slices. Must be called under b.mu.
func (b *InMemoryBackend) seedVpcConfig(app *Application, vpcConfigs []VpcConfigurationDescription) {
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
}

// seedCWLOptions appends inline CloudWatchLoggingOptions, assigning fresh
// resource IDs. Must be called under b.mu.
func (b *InMemoryBackend) seedCWLOptions(app *Application, cwlOptions []CloudWatchLoggingOptionDesc) {
	for i := range cwlOptions {
		cwlOptions[i].CloudWatchLoggingOptionID = b.newResourceID("cwl")
	}

	app.CloudWatchLoggingOptionDescs = append(app.CloudWatchLoggingOptionDescs, cwlOptions...)
}

// seedExtendedConfig copies the Flink/Code/Environment/Snapshot/Rollback/
// Encryption portions of an inline ApplicationConfiguration onto app,
// replacing (not merging) any previously seeded value -- CreateApplication
// only ever calls this once, so replace-vs-merge is unobservable here, but
// matches applyApplicationConfigurationUpdate's replace semantics for
// consistency. No locking requirement beyond the caller's b.mu.
func seedExtendedConfig(app *Application, cfg SeedConfig) {
	if cfg.CodeConfig != nil {
		app.CodeConfig = cfg.CodeConfig
	}

	if cfg.FlinkConfig != nil {
		app.FlinkConfig = cfg.FlinkConfig
	}

	if len(cfg.EnvironmentPropertyGroups) > 0 {
		app.EnvironmentPropertyGroups = cfg.EnvironmentPropertyGroups
	}

	if cfg.SnapshotsEnabled != nil {
		app.SnapshotsEnabled = cfg.SnapshotsEnabled
	}

	if cfg.RollbackEnabled != nil {
		app.RollbackEnabled = cfg.RollbackEnabled
	}

	if cfg.EncryptionConfig != nil {
		app.EncryptionConfig = cfg.EncryptionConfig
	}
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

// UpdateApplication updates an application, returning the OperationID of the
// recorded UpdateApplication operation (see recordOperation).
// params.CurrentApplicationVersionID/params.ConditionalToken implement the
// two alternative optimistic-concurrency checks real AWS performs (see
// checkAndBumpVersionOrToken). References inside
// params.CloudWatchLoggingOptionUpdates/ApplicationConfigurationUpdate to
// sub-resource IDs that don't exist are validated *before* the version is
// bumped, so a rejected request never leaves a phantom version-history entry
// (matching the Add*/Delete* config ops' "find before bumping" convention
// elsewhere in this package).
func (b *InMemoryBackend) UpdateApplication(
	ctx context.Context,
	params UpdateApplicationParams,
) (*Application, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, params.Name)
	if !ok {
		return nil, "", ErrNotFound
	}

	if err := validateUpdateReferences(app, params); err != nil {
		return nil, "", err
	}

	if err := checkAndBumpVersionOrToken(app, params.CurrentApplicationVersionID, params.ConditionalToken); err != nil {
		return nil, "", err
	}

	defer b.snapshotVersion(region, params.Name, app)

	applyApplicationUpdate(app, params)
	app.LastUpdateTimestamp = time.Now().UTC()

	return app, b.recordOperation(region, params.Name, "UpdateApplication"), nil
}

// DeleteApplication deletes an application by name. createTimestampSeconds,
// when non-nil, is validated against the application's actual CreateTimestamp
// (real AWS's DeleteApplicationInput.CreateTimestamp is a required safety
// check retrieved from a prior DescribeApplication) -- a mismatch returns
// ErrValidation instead of deleting.
func (b *InMemoryBackend) DeleteApplication(ctx context.Context, name string, createTimestampSeconds *float64) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return ErrNotFound
	}

	if createTimestampSeconds != nil {
		const epsilon = 1e-6
		if diff := *createTimestampSeconds - awstime.Epoch(app.CreatedAt); diff > epsilon || diff < -epsilon {
			return ErrValidation
		}
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
// matching real AWS Kinesis Analytics v2 behavior. runConfig, when non-nil,
// is stored as the application's RunConfigurationDescription -- real AWS
// clients (Terraform, CloudFormation) commonly start a Flink application
// with ApplicationRestoreConfiguration set to restore from a snapshot, and
// expect DescribeApplication to echo it back afterward.
func (b *InMemoryBackend) StartApplication(
	ctx context.Context,
	name string,
	runConfig *RunConfigInput,
) (string, error) {
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
	applyRunConfigInput(app, runConfig)

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
