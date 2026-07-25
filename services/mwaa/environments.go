package mwaa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateEnvironment creates a new MWAA environment in the region resolved from ctx.
func (b *InMemoryBackend) CreateEnvironment(
	ctx context.Context,
	name string,
	req *createEnvironmentRequest,
) (*Environment, error) {
	if err := validateEnvironmentName(name); err != nil {
		return nil, err
	}

	if err := validateCreateRequest(req); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	if b.environments.Has(regionKey(region, name)) {
		return nil, ErrEnvironmentAlreadyExists
	}

	defaults := resolveCreateDefaults(req)
	if defaults.minWorkers > defaults.maxWorkers {
		return nil, fmt.Errorf(
			"%w: MinWorkers (%d) must be <= MaxWorkers (%d)",
			ErrInvalidParameter, defaults.minWorkers, defaults.maxWorkers,
		)
	}

	env := buildEnvironment(region, b.accountID, name, req, defaults)
	env.region = region

	b.environments.Put(env)

	return env, nil
}

// createDefaults gathers the defaulted values for CreateEnvironment.
type createDefaults struct {
	airflowVersion string
	envClass       string
	accessMode     string
	endpointMgmt   string
	maxWorkers     int32
	minWorkers     int32
	maxWebservers  int32
	minWebservers  int32
	schedulers     int32
}

// resolveCreateDefaults applies AWS defaults for unset fields.
func resolveCreateDefaults(req *createEnvironmentRequest) createDefaults {
	d := createDefaults{
		airflowVersion: req.AirflowVersion,
		envClass:       req.EnvironmentClass,
		accessMode:     req.WebserverAccessMode,
		endpointMgmt:   req.EndpointManagement,
		maxWorkers:     req.MaxWorkers,
		minWorkers:     req.MinWorkers,
		maxWebservers:  req.MaxWebservers,
		minWebservers:  req.MinWebservers,
		schedulers:     req.Schedulers,
	}

	if d.airflowVersion == "" {
		d.airflowVersion = defaultAirflowVersion
	}

	if d.envClass == "" {
		d.envClass = defaultEnvironmentClass
	}

	if d.accessMode == "" {
		d.accessMode = defaultWebserverAccessMode
	}

	if d.endpointMgmt == "" {
		d.endpointMgmt = endpointManagementService
	}

	if d.maxWorkers == 0 {
		d.maxWorkers = defaultMaxWorkers
	}

	if d.minWorkers == 0 {
		d.minWorkers = defaultMinWorkers
	}

	if d.maxWebservers == 0 {
		d.maxWebservers = defaultMaxWebservers
	}

	if d.minWebservers == 0 {
		d.minWebservers = defaultMinWebservers
	}

	if d.schedulers == 0 {
		if strings.HasPrefix(d.airflowVersion, "1.") {
			d.schedulers = defaultSchedulersV1
		} else {
			d.schedulers = defaultSchedulersV2
		}
	}

	return d
}

// buildEnvironment constructs the Environment value from the validated request and defaults.
func buildEnvironment(
	region, accountID, name string,
	req *createEnvironmentRequest,
	d createDefaults,
) *Environment {
	envARN := arn.Build("airflow", region, accountID, "environment/"+name)

	sum := sha256.Sum256([]byte(name))
	uniqueID := hex.EncodeToString(sum[:8])

	tags := make(map[string]string)
	maps.Copy(tags, req.Tags)

	airflowConfig := make(map[string]string)
	maps.Copy(airflowConfig, req.AirflowConfigurationOptions)

	return &Environment{
		Name:                         name,
		ARN:                          envARN,
		Status:                       envStatusCreating,
		DagS3Path:                    req.DagS3Path,
		ExecutionRoleArn:             req.ExecutionRoleArn,
		SourceBucketArn:              req.SourceBucketArn,
		AirflowVersion:               d.airflowVersion,
		EnvironmentClass:             d.envClass,
		MaxWorkers:                   d.maxWorkers,
		MinWorkers:                   d.minWorkers,
		MaxWebservers:                d.maxWebservers,
		MinWebservers:                d.minWebservers,
		Schedulers:                   d.schedulers,
		WebserverURL:                 fmt.Sprintf("https://%s.airflow.%s.amazonaws.com", uniqueID, region),
		WebserverAccessMode:          d.accessMode,
		NetworkConfiguration:         req.NetworkConfiguration,
		Tags:                         tags,
		CreatedAt:                    epochSecondsNow(),
		KmsKey:                       req.KmsKey,
		AirflowConfigurationOptions:  airflowConfig,
		LoggingConfiguration:         req.LoggingConfiguration,
		PluginsS3Path:                req.PluginsS3Path,
		PluginsS3ObjectVersion:       req.PluginsS3ObjectVersion,
		RequirementsS3Path:           req.RequirementsS3Path,
		RequirementsS3ObjectVersion:  req.RequirementsS3ObjectVersion,
		StartupScriptS3Path:          req.StartupScriptS3Path,
		StartupScriptS3ObjectVersion: req.StartupScriptS3ObjectVersion,
		EndpointManagement:           d.endpointMgmt,
		WeeklyMaintenanceWindowStart: req.WeeklyMaintenanceWindowStart,
		ServiceRoleArn: arn.Build("iam", "", accountID,
			"role/aws-service-role/airflow.amazonaws.com/AWSServiceRoleForAmazonMWAA"),
		CeleryExecutorQueue: fmt.Sprintf(
			"https://sqs.%s.amazonaws.com/%s/airflow-celery-%s",
			region, accountID, uniqueID,
		),
		DatabaseVpcEndpointService:  fmt.Sprintf("com.amazonaws.%s.airflow.db.%s", region, uniqueID),
		WebserverVpcEndpointService: fmt.Sprintf("com.amazonaws.%s.airflow.api.%s", region, uniqueID),
	}
}

// GetEnvironment retrieves a deep copy of an MWAA environment by name.
func (b *InMemoryBackend) GetEnvironment(ctx context.Context, name string) (*Environment, error) {
	region := getRegion(ctx, b.region)

	// Full write lock: GetEnvironment may promote a transient lifecycle status
	// (UPDATING → AVAILABLE) on the stored environment via promoteTransientStatus.
	b.mu.Lock("GetEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments.Get(regionKey(region, name))
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	cp := cloneEnvironment(env)
	promoteTransientStatus(env)

	return cp, nil
}

// promoteTransientStatus advances mock-only transient lifecycle states (CREATING,
// UPDATING, CREATING_SNAPSHOT, ROLLING_BACK, PENDING) back to AVAILABLE
// so callers can observe the transition once and then see the steady state.
func promoteTransientStatus(env *Environment) {
	if env == nil {
		return
	}

	switch env.Status {
	case envStatusCreating, envStatusUpdating, envStatusCreatingSnapshot, envStatusRollingBack, envStatusPending:
		env.Status = envStatusAvailable
	}
}

// DeleteEnvironment deletes an MWAA environment by name and cascades to metrics.
func (b *InMemoryBackend) DeleteEnvironment(ctx context.Context, name string) (*Environment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments.Get(regionKey(region, name))
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	b.environments.Delete(regionKey(region, name))
	delete(b.metricsStore(region), name)

	return env, nil
}

// UpdateEnvironment updates an existing MWAA environment.
func (b *InMemoryBackend) UpdateEnvironment(
	ctx context.Context,
	name string,
	req *updateEnvironmentRequest,
) (*Environment, error) {
	if err := validateUpdateRequest(req); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments.Get(regionKey(region, name))
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	if env.Status != envStatusAvailable {
		return nil, fmt.Errorf(
			"%w: cannot update environment %q in state %s",
			ErrInvalidParameter, name, env.Status,
		)
	}

	applyUpdateScalars(env, req)
	applyUpdateS3Paths(env, req)

	if env.MinWorkers > env.MaxWorkers {
		return nil, fmt.Errorf(
			"%w: MinWorkers (%d) must be <= MaxWorkers (%d)",
			ErrInvalidParameter,
			env.MinWorkers,
			env.MaxWorkers,
		)
	}

	if req.WebserverAccessMode != "" {
		env.WebserverAccessMode = req.WebserverAccessMode
	}

	if req.NetworkConfiguration != nil {
		if err := validateNetworkConfigUpdate(req.NetworkConfiguration); err != nil {
			return nil, err
		}

		// AWS's UpdateEnvironment can only replace SecurityGroupIds; SubnetIds
		// are immutable after creation and are not part of the update wire
		// shape (see UpdateNetworkConfig), so the existing NetworkConfiguration
		// is merged in place rather than replaced wholesale.
		if env.NetworkConfiguration == nil {
			env.NetworkConfiguration = &NetworkConfig{}
		}

		env.NetworkConfiguration.SecurityGroupIDs = req.NetworkConfiguration.SecurityGroupIDs
	}

	if req.AirflowConfigurationOptions != nil {
		env.AirflowConfigurationOptions = make(map[string]string, len(req.AirflowConfigurationOptions))
		maps.Copy(env.AirflowConfigurationOptions, req.AirflowConfigurationOptions)
	}

	if req.LoggingConfiguration != nil {
		env.LoggingConfiguration = req.LoggingConfiguration
	}

	env.LastUpdate = &LastUpdate{
		CreatedAt:                 epochSecondsNow(),
		Status:                    "SUCCESS",
		Source:                    "USER",
		WorkerReplacementStrategy: req.WorkerReplacementStrategy,
	}

	// Reflect the AWS lifecycle: UpdateEnvironment puts the env into UPDATING,
	// then it returns to AVAILABLE on the next observation.
	env.Status = envStatusUpdating

	return env, nil
}

// applyUpdateScalars applies basic scalar field updates from req to env in place.
func applyUpdateScalars(env *Environment, req *updateEnvironmentRequest) {
	if req.DagS3Path != "" {
		env.DagS3Path = req.DagS3Path
	}

	if req.ExecutionRoleArn != "" {
		env.ExecutionRoleArn = req.ExecutionRoleArn
	}

	if req.SourceBucketArn != "" {
		env.SourceBucketArn = req.SourceBucketArn
	}

	if req.AirflowVersion != "" {
		env.AirflowVersion = req.AirflowVersion
	}

	if req.EnvironmentClass != "" {
		env.EnvironmentClass = req.EnvironmentClass
	}

	if req.MaxWorkers != 0 {
		env.MaxWorkers = req.MaxWorkers
	}

	if req.MinWorkers != 0 {
		env.MinWorkers = req.MinWorkers
	}

	if req.MaxWebservers != 0 {
		env.MaxWebservers = req.MaxWebservers
	}

	if req.MinWebservers != 0 {
		env.MinWebservers = req.MinWebservers
	}

	if req.Schedulers != 0 {
		env.Schedulers = req.Schedulers
	}

	if req.WeeklyMaintenanceWindowStart != "" {
		env.WeeklyMaintenanceWindowStart = req.WeeklyMaintenanceWindowStart
	}

	// WorkerReplacementStrategy is intentionally not applied to any top-level
	// Environment field here: AWS's Environment response shape has no such
	// member at all -- it is recorded only on LastUpdate.WorkerReplacementStrategy
	// (set unconditionally below in UpdateEnvironment), reflecting just the most
	// recent update call rather than a persistent environment-level setting.
}

// applyUpdateS3Paths copies the optional S3 path/version pairs from req to env.
func applyUpdateS3Paths(env *Environment, req *updateEnvironmentRequest) {
	if req.PluginsS3Path != "" {
		env.PluginsS3Path = req.PluginsS3Path
		env.PluginsS3ObjectVersion = req.PluginsS3ObjectVersion
	}

	if req.RequirementsS3Path != "" {
		env.RequirementsS3Path = req.RequirementsS3Path
		env.RequirementsS3ObjectVersion = req.RequirementsS3ObjectVersion
	}

	if req.StartupScriptS3Path != "" {
		env.StartupScriptS3Path = req.StartupScriptS3Path
		env.StartupScriptS3ObjectVersion = req.StartupScriptS3ObjectVersion
	}
}

// ListEnvironmentsPage returns a paginated, sorted list of environment names.
// pageSize is clamped to [1, listEnvMaxPageSize]; 0 falls back to listEnvDefaultPageSize.
// nextToken is the name of the first environment to include in this page (exclusive
// start cursor of the previous page); empty starts at the beginning.
func (b *InMemoryBackend) ListEnvironmentsPage(
	ctx context.Context,
	nextToken string,
	pageSize int,
) ([]string, string, error) {
	if pageSize <= 0 {
		pageSize = listEnvDefaultPageSize
	}

	if pageSize > listEnvMaxPageSize {
		pageSize = listEnvMaxPageSize
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListEnvironmentsPage")
	defer b.mu.RUnlock()

	envs := b.environmentsByRegion.Get(region)
	all := make([]string, len(envs))

	for i, e := range envs {
		all[i] = e.Name
	}

	sort.Strings(all)

	startIdx := 0
	if nextToken != "" {
		startIdx = sort.SearchStrings(all, nextToken)
	}

	if startIdx >= len(all) {
		return []string{}, "", nil
	}

	end := startIdx + pageSize

	var outToken string
	if end < len(all) {
		outToken = all[end]
	} else {
		end = len(all)
	}

	page := make([]string, end-startIdx)
	copy(page, all[startIdx:end])

	return page, outToken, nil
}

// ListEnvironments returns a sorted list of environment names.
func (b *InMemoryBackend) ListEnvironments(ctx context.Context) ([]string, error) {
	names, _, err := b.ListEnvironmentsPage(ctx, "", listEnvMaxPageSize)

	return names, err
}

// cloneEnvironment returns a deep copy of the given environment.
func cloneEnvironment(env *Environment) *Environment {
	clone := *env

	if env.Tags != nil {
		clone.Tags = make(map[string]string, len(env.Tags))
		maps.Copy(clone.Tags, env.Tags)
	}

	if env.NetworkConfiguration != nil {
		nc := *env.NetworkConfiguration
		clone.NetworkConfiguration = &nc
	}

	if env.AirflowConfigurationOptions != nil {
		clone.AirflowConfigurationOptions = make(map[string]string, len(env.AirflowConfigurationOptions))
		maps.Copy(clone.AirflowConfigurationOptions, env.AirflowConfigurationOptions)
	}

	if env.LoggingConfiguration != nil {
		lc := *env.LoggingConfiguration
		clone.LoggingConfiguration = &lc
	}

	if env.LastUpdate != nil {
		lu := *env.LastUpdate
		clone.LastUpdate = &lu
	}

	return &clone
}
