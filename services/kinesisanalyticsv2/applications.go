package kinesisanalyticsv2

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"time"
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
// Add*/Delete* config ops elsewhere in this package).
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
