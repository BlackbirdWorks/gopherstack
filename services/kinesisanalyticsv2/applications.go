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

	if cfg.ZeppelinConfig != nil {
		app.ZeppelinConfig = cfg.ZeppelinConfig
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
		// terraform-provider-aws round-trips CreateTimestamp through Terraform
		// state as time.RFC3339 (internal/service/kinesisanalyticsv2/
		// application.go: resourceApplicationRead's d.Set("create_timestamp",
		// ...Format(time.RFC3339)), which carries no fractional-second
		// component at all, and resourceApplicationDelete parses that string
		// back with time.Parse(time.RFC3339, ...) before sending it here. That
		// truncates to the whole second, not the millisecond the wire format
		// itself preserves, so the tolerance must cover a full second or every
		// legitimate provider-driven DeleteApplication call would be rejected.
		const epsilon = 1.0
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
	sqlRunConfigs []SQLRunConfigInput,
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

	for _, src := range sqlRunConfigs {
		if findInputIndex(app, src.InputID) < 0 {
			return "", ErrNotFound
		}
	}

	app.ApplicationStatus = ApplicationStatusRunning
	applyRunConfigInput(app, runConfig)
	applySQLRunConfigurations(app, sqlRunConfigs)

	return b.recordOperation(region, name, "StartApplication"), nil
}

// applySQLRunConfigurations stores each SqlRunConfiguration's starting
// position on the matching InputDescription, echoed back via
// DescribeApplication -- real AWS's RunConfigurationDescription has no
// SqlRunConfigurations field (botocore kinesisanalyticsv2/2018-05-23/
// service-2.json.gz shape "RunConfigurationDescription"), it only surfaces
// per-input via InputDescription.InputStartingPositionConfiguration. Every
// InputID was already confirmed to exist by StartApplication's caller loop.
func applySQLRunConfigurations(app *Application, configs []SQLRunConfigInput) {
	for _, src := range configs {
		idx := findInputIndex(app, src.InputID)
		if idx < 0 {
			continue
		}

		app.InputDescriptions[idx].InputStartingPositionConfiguration = src.InputStartingPositionConfiguration
	}
}

// runtimeEnvironmentSQL is the real RuntimeEnvironment enum value that
// identifies a SQL-based application (as opposed to any Flink/Zeppelin
// family member) -- botocore kinesisanalyticsv2/2018-05-23/service-2.json.gz
// shape "RuntimeEnvironment".
const runtimeEnvironmentSQL = "SQL-1_0"

// StopApplication sets the application status to READY and returns the
// OperationID of the recorded StopApplication operation (see recordOperation).
// Returns ResourceInUseException if the application is not in RUNNING state,
// matching real AWS Kinesis Analytics v2 behavior. force mirrors the real
// StopApplicationInput.Force field: real AWS forbids force-stopping a
// SQL-based application ("You can only force stop a Managed Service for
// Apache Flink application" -- api_op_StopApplication.go doc comment,
// aws-sdk-go-v2/service/kinesisanalyticsv2@v1.41.4), so that combination
// returns InvalidArgumentException. Force's other documented effects --
// permitting stop from STARTING/UPDATING/STOPPING/AUTOSCALING, and skipping
// the pre-stop snapshot -- have no observable effect here: this backend's
// ApplicationStatus is only ever READY/RUNNING (synchronous lifecycle, same
// structural gap as DeleteApplication's unused ApplicationStatusDeleting),
// and it never auto-snapshots on stop regardless of Force (see PARITY.md).
func (b *InMemoryBackend) StopApplication(ctx context.Context, name string, force bool) (string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("StopApplication")
	defer b.mu.Unlock()

	app, ok := b.findApplication(region, name)
	if !ok {
		return "", ErrNotFound
	}

	if force && app.RuntimeEnvironment == runtimeEnvironmentSQL {
		return "", ErrValidation
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

// discoveredColumnSQLType is the placeholder RecordColumn.SqlType used by
// DiscoverInputSchema's synthetic column1/column2/column3 columns.
const discoveredColumnSQLType = "VARCHAR(64)"

// DiscoverInputSchema returns a synthetic discovered schema for a resource
// ARN: this backend has no live stream to sample, so RecordColumns/
// ParsedInputRecords are a fixed placeholder (unchanged from before this
// pass -- see PARITY.md). serviceExecutionRole is required on the real
// DiscoverInputSchemaRequest (botocore kinesisanalyticsv2/2018-05-23/
// service-2.json.gz shape "DiscoverInputSchemaRequest") but was previously
// accepted under the wrong wire key and never validated (see
// discoverInputSchemaInput's ServiceExecutionRole fix in handler_applications.go).
func (b *InMemoryBackend) DiscoverInputSchema(
	_ context.Context,
	resourceARN, serviceExecutionRole, _ /* inputStartingPosition */ string,
) (*DiscoveredSchema, error) {
	if resourceARN == "" || serviceExecutionRole == "" {
		return nil, ErrValidation
	}

	return &DiscoveredSchema{
		RecordFormat:   "JSON",
		RecordEncoding: "UTF-8",
		RecordColumns: []RecordColumnDesc{
			{Name: "column1", SQLType: discoveredColumnSQLType},
			{Name: "column2", SQLType: discoveredColumnSQLType},
			{Name: "column3", SQLType: discoveredColumnSQLType},
		},
		ParsedInputRecords: [][]string{
			{"column1", "column2", "column3"},
		},
	}, nil
}
