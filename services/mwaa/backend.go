package mwaa

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultAirflowVersion      = "2.10.3"
	defaultEnvironmentClass    = "mw1.small"
	defaultMaxWorkers          = int32(10)
	defaultMinWorkers          = int32(1)
	defaultMaxWebservers       = int32(2)
	defaultMinWebservers       = int32(2)
	minWebserversAllowed       = int32(1)
	maxWebserversAllowed       = int32(5)
	defaultSchedulersV2        = int32(2)
	defaultSchedulersV1        = int32(1)
	minSchedulersV2            = int32(2)
	maxSchedulersV2            = int32(5)
	defaultWebserverAccessMode = "PUBLIC_ONLY"
	restAPISuccessCode         = int32(200)
	maxMetricsPerEnv           = 1000
	listEnvDefaultPageSize     = 25
	listEnvMaxPageSize         = 100

	// Environment status constants.
	envStatusAvailable        = "AVAILABLE"
	envStatusCreating         = "CREATING"
	envStatusCreatingSnapshot = "CREATING_SNAPSHOT"
	envStatusDeleting         = "DELETING"
	envStatusDeleted          = "DELETED"
	envStatusUpdating         = "UPDATING"
	envStatusUpdateRollback   = "UPDATE_ROLLING_BACK"
	envStatusUpdateFailed     = "UPDATE_FAILED"
	envStatusPending          = "PENDING"
	envStatusCreateFailed     = "CREATE_FAILED"
	envStatusUnavailable      = "UNAVAILABLE"
	envStatusError            = "ERROR"

	// EndpointManagement constants.
	endpointManagementService  = "SERVICE"
	endpointManagementCustomer = "CUSTOMER"

	// WebserverAccessMode constants.
	accessModePublic  = "PUBLIC_ONLY"
	accessModePrivate = "PRIVATE_ONLY"

	// Worker limits.
	maxWorkersAllowed = int32(25)

	// Tag limit per resource.
	maxTagsPerResource = 50

	// Environment name length bounds.
	minEnvNameLen = 1
	maxEnvNameLen = 80

	// WorkerReplacementStrategy constants.
	workerStrategyForced = "FORCED"
	workerStrategyDrain  = "TERMINATION_WITH_DRAIN"
)

// validEnvironmentClasses returns the set of valid environment class values.
func validEnvironmentClasses() map[string]struct{} {
	return map[string]struct{}{
		"mw1.small":   {},
		"mw1.medium":  {},
		"mw1.large":   {},
		"mw1.xlarge":  {},
		"mw1.2xlarge": {},
	}
}

// validLogLevels returns the set of valid Airflow log level values, initialised once.
var validLogLevels = sync.OnceValue(func() map[string]struct{} {
	return map[string]struct{}{
		"CRITICAL": {},
		"ERROR":    {},
		"WARNING":  {},
		"INFO":     {},
		"DEBUG":    {},
	}
})

// validAirflowVersions returns the set of supported Airflow versions.
func validAirflowVersions() map[string]struct{} {
	return map[string]struct{}{
		"2.10.3":  {},
		"2.9.2":   {},
		"2.8.1":   {},
		"2.7.2":   {},
		"2.6.3":   {},
		"2.5.1":   {},
		"2.4.3":   {},
		"2.2.2":   {},
		"1.10.12": {},
	}
}

// isLetter reports whether r is an ASCII letter.
func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// isValidEnvNameChar reports whether r is a valid non-first character for an MWAA environment name.
func isValidEnvNameChar(r rune) bool {
	return isLetter(r) || (r >= '0' && r <= '9') || r == '-' || r == '_'
}

// validateModuleLogging validates a single module logging configuration.
func validateModuleLogging(field string, mlc *ModuleLoggingConfiguration) error {
	if mlc == nil {
		return nil
	}

	if mlc.LogLevel == "" {
		return nil
	}

	if _, ok := validLogLevels()[mlc.LogLevel]; !ok {
		return fmt.Errorf(
			"%w: %s.LogLevel must be one of CRITICAL/ERROR/WARNING/INFO/DEBUG, got %q",
			ErrInvalidParameter, field, mlc.LogLevel,
		)
	}

	return nil
}

// validateLoggingConfiguration validates the logging configuration for all five Airflow modules.
func validateLoggingConfiguration(lc *LoggingConfiguration) error {
	if lc == nil {
		return nil
	}

	type moduleEntry struct {
		mlc  *ModuleLoggingConfiguration
		name string
	}

	modules := []moduleEntry{
		{lc.DagProcessingLogs, "DagProcessingLogs"},
		{lc.SchedulerLogs, "SchedulerLogs"},
		{lc.TaskLogs, "TaskLogs"},
		{lc.WebserverLogs, "WebserverLogs"},
		{lc.WorkerLogs, "WorkerLogs"},
	}

	for _, m := range modules {
		if err := validateModuleLogging(m.name, m.mlc); err != nil {
			return err
		}
	}

	return nil
}

// validateEnvironmentName enforces AWS MWAA naming rules for environment names:
// 1-80 chars, must start with a letter, followed by alphanumeric/hyphen/underscore.
func validateEnvironmentName(name string) error {
	n := len(name)
	if n < minEnvNameLen || n > maxEnvNameLen {
		return fmt.Errorf("%w: environment name must be 1-80 characters", ErrInvalidParameter)
	}

	if !isLetter(rune(name[0])) {
		return fmt.Errorf("%w: environment name must start with a letter", ErrInvalidParameter)
	}

	for _, r := range name[1:] {
		if !isValidEnvNameChar(r) {
			return fmt.Errorf(
				"%w: environment name must contain only alphanumeric characters, hyphens, or underscores",
				ErrInvalidParameter,
			)
		}
	}

	return nil
}

// validateWorkerReplacementStrategy validates the WorkerReplacementStrategy field.
func validateWorkerReplacementStrategy(strategy string) error {
	if strategy == "" {
		return nil
	}

	if strategy != workerStrategyForced && strategy != workerStrategyDrain {
		return fmt.Errorf(
			"%w: WorkerReplacementStrategy must be %s or %s, got %q",
			ErrInvalidParameter, workerStrategyForced, workerStrategyDrain, strategy,
		)
	}

	return nil
}

// generateMWAAToken produces a JWT-shaped token for CLI and web login operations.
// The token is deterministic for a given (envName, kind) pair so tests can reason
// about its structure; it is NOT a cryptographically valid JWT.
func generateMWAAToken(envName, kind string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(
		[]byte(`{"env":"` + envName + `","type":"` + kind + `"}`),
	)

	sum := sha256.Sum256([]byte(envName + ":" + kind))
	sig := base64.RawURLEncoding.EncodeToString(sum[:])

	return header + "." + payload + "." + sig
}

// Errors used by the backend.
var (
	// ErrEnvironmentNotFound is returned when an environment does not exist.
	ErrEnvironmentNotFound = awserr.New("ResourceNotFoundException: environment not found", awserr.ErrNotFound)
	// ErrEnvironmentAlreadyExists is returned when an environment already exists.
	ErrEnvironmentAlreadyExists = awserr.New(
		"AlreadyExistsException: environment already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrInvalidParameter is returned when an invalid or missing parameter is provided.
	ErrInvalidParameter = awserr.New("ValidationException: invalid parameter", awserr.ErrInvalidParameter)
)

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	environments map[string]*Environment
	arnIndex     map[string]string
	metrics      map[string][]MetricDatum
	mu           *lockmetrics.RWMutex
	region       string
	accountID    string
}

// NewInMemoryBackend creates a new MWAA in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		region:       region,
		accountID:    accountID,
		environments: make(map[string]*Environment),
		arnIndex:     make(map[string]string),
		metrics:      make(map[string][]MetricDatum),
		mu:           lockmetrics.New("mwaa"),
	}
}

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset closes the current mutex and reinitialises all maps.
func (b *InMemoryBackend) Reset() {
	b.mu.Close()
	b.mu = lockmetrics.New("mwaa")
	b.environments = make(map[string]*Environment)
	b.arnIndex = make(map[string]string)
	b.metrics = make(map[string][]MetricDatum)
}

// AddEnvironmentInternal creates an environment with minimal defaults, bypassing
// validation, intended for use in tests only.
func (b *InMemoryBackend) AddEnvironmentInternal(name string) *Environment {
	b.mu.Lock("AddEnvironmentInternal")
	defer b.mu.Unlock()

	envARN := arn.Build("airflow", b.region, b.accountID, "environment/"+name)
	env := &Environment{
		Name:      name,
		ARN:       envARN,
		Status:    envStatusAvailable,
		Tags:      make(map[string]string),
		CreatedAt: epochSecondsNow(),
	}

	b.environments[name] = env
	b.arnIndex[envARN] = name

	return env
}

// validateCreateRequest validates required fields and enumerated values for CreateEnvironment.
func validateCreateRequest(req *createEnvironmentRequest) error {
	if err := validateCreateRequiredFields(req); err != nil {
		return err
	}

	if err := validateCreateEnums(req); err != nil {
		return err
	}

	if err := validateCreateS3Paths(req); err != nil {
		return err
	}

	if len(req.Tags) > maxTagsPerResource {
		return fmt.Errorf("%w: cannot specify more than %d tags", ErrInvalidParameter, maxTagsPerResource)
	}

	if err := validateLoggingConfiguration(req.LoggingConfiguration); err != nil {
		return err
	}

	return validateCreateSizing(req)
}

// validateCreateRequiredFields checks that required CreateEnvironment fields are set.
func validateCreateRequiredFields(req *createEnvironmentRequest) error {
	if req.DagS3Path == "" {
		return fmt.Errorf("%w: DagS3Path is required", ErrInvalidParameter)
	}

	if req.ExecutionRoleArn == "" {
		return fmt.Errorf("%w: ExecutionRoleArn is required", ErrInvalidParameter)
	}

	if req.SourceBucketArn == "" {
		return fmt.Errorf("%w: SourceBucketArn is required", ErrInvalidParameter)
	}

	return nil
}

// validateCreateEnums validates enumerated string fields and ARN-shaped fields.
func validateCreateEnums(req *createEnvironmentRequest) error {
	if req.WebserverAccessMode != "" &&
		req.WebserverAccessMode != accessModePublic &&
		req.WebserverAccessMode != accessModePrivate {
		return fmt.Errorf(
			"%w: WebserverAccessMode must be %s or %s",
			ErrInvalidParameter, accessModePublic, accessModePrivate,
		)
	}

	if req.EnvironmentClass != "" {
		if _, ok := validEnvironmentClasses()[req.EnvironmentClass]; !ok {
			return fmt.Errorf("%w: invalid EnvironmentClass %q", ErrInvalidParameter, req.EnvironmentClass)
		}
	}

	if req.AirflowVersion != "" {
		if _, ok := validAirflowVersions()[req.AirflowVersion]; !ok {
			return fmt.Errorf("%w: unsupported AirflowVersion %q", ErrInvalidParameter, req.AirflowVersion)
		}
	}

	if req.EndpointManagement != "" &&
		req.EndpointManagement != endpointManagementService &&
		req.EndpointManagement != endpointManagementCustomer {
		return fmt.Errorf(
			"%w: EndpointManagement must be %s or %s",
			ErrInvalidParameter, endpointManagementService, endpointManagementCustomer,
		)
	}

	if req.KmsKey != "" && !strings.HasPrefix(req.KmsKey, "arn:") {
		return fmt.Errorf("%w: KmsKey must be a KMS ARN", ErrInvalidParameter)
	}

	return nil
}

// validateCreateS3Paths validates the three optional S3 path/version pairs and the
// weekly maintenance window format.
func validateCreateS3Paths(req *createEnvironmentRequest) error {
	if err := validateS3PathVersionPair("PluginsS3Path", req.PluginsS3Path, req.PluginsS3ObjectVersion); err != nil {
		return err
	}

	if err := validateS3PathVersionPair(
		"RequirementsS3Path", req.RequirementsS3Path, req.RequirementsS3ObjectVersion,
	); err != nil {
		return err
	}

	if err := validateS3PathVersionPair(
		"StartupScriptS3Path", req.StartupScriptS3Path, req.StartupScriptS3ObjectVersion,
	); err != nil {
		return err
	}

	if req.WeeklyMaintenanceWindowStart != "" {
		if err := validateWeeklyMaintenanceWindowStart(req.WeeklyMaintenanceWindowStart); err != nil {
			return err
		}
	}

	return nil
}

// validateCreateSizing validates webserver, worker, and scheduler bounds.
func validateCreateSizing(req *createEnvironmentRequest) error {
	if req.MaxWorkers != 0 && req.MaxWorkers > maxWorkersAllowed {
		return fmt.Errorf(
			"%w: MaxWorkers cannot exceed %d",
			ErrInvalidParameter, maxWorkersAllowed,
		)
	}

	if req.MaxWebservers != 0 || req.MinWebservers != 0 {
		if err := validateWebservers(req.MinWebservers, req.MaxWebservers); err != nil {
			return err
		}
	}

	if req.Schedulers != 0 {
		if err := validateSchedulers(req.AirflowVersion, req.Schedulers); err != nil {
			return err
		}
	}

	return nil
}

// validateS3PathVersionPair enforces that an S3 object version is provided whenever
// the corresponding S3 path is set, matching the AWS contract. The version field
// name is derived by replacing the trailing "Path" with "ObjectVersion" (e.g.
// "PluginsS3Path" → "PluginsS3ObjectVersion").
func validateS3PathVersionPair(field, path, version string) error {
	if path != "" && version == "" {
		versionField := strings.TrimSuffix(field, "Path") + "ObjectVersion"

		return fmt.Errorf("%w: %s is required when %s is set", ErrInvalidParameter, versionField, field)
	}

	return nil
}

// validateWeeklyMaintenanceWindowStart accepts the AWS "DAY:HH:MM" format
// (e.g. "MON:03:30"). Day must be one of MON-SUN; HH 00-23; MM 00-59.
func validateWeeklyMaintenanceWindowStart(value string) error {
	parts := strings.Split(value, ":")

	const expectedParts = 3
	if len(parts) != expectedParts {
		return fmt.Errorf(
			"%w: WeeklyMaintenanceWindowStart must be DAY:HH:MM, got %q",
			ErrInvalidParameter, value,
		)
	}

	days := map[string]struct{}{
		"MON": {}, "TUE": {}, "WED": {}, "THU": {}, "FRI": {}, "SAT": {}, "SUN": {},
	}
	if _, ok := days[parts[0]]; !ok {
		return fmt.Errorf(
			"%w: WeeklyMaintenanceWindowStart day must be MON-SUN (uppercase), got %q",
			ErrInvalidParameter,
			parts[0],
		)
	}

	hh, err := strconv.Atoi(parts[1])

	const maxHour = 23
	if err != nil || hh < 0 || hh > maxHour {
		return fmt.Errorf(
			"%w: WeeklyMaintenanceWindowStart hour must be 00-23, got %q",
			ErrInvalidParameter, parts[1],
		)
	}

	mm, err := strconv.Atoi(parts[2])

	const maxMinute = 59
	if err != nil || mm < 0 || mm > maxMinute {
		return fmt.Errorf(
			"%w: WeeklyMaintenanceWindowStart minute must be 00-59, got %q",
			ErrInvalidParameter, parts[2],
		)
	}

	return nil
}

// validateWebservers enforces AWS bounds on the webserver autoscaling range.
func validateWebservers(minVal, maxVal int32) error {
	if minVal == 0 {
		minVal = defaultMinWebservers
	}

	if maxVal == 0 {
		maxVal = defaultMaxWebservers
	}

	if minVal < minWebserversAllowed || maxVal > maxWebserversAllowed {
		return fmt.Errorf(
			"%w: webservers must be between %d and %d",
			ErrInvalidParameter, minWebserversAllowed, maxWebserversAllowed,
		)
	}

	if minVal > maxVal {
		return fmt.Errorf(
			"%w: MinWebservers (%d) must be <= MaxWebservers (%d)",
			ErrInvalidParameter, minVal, maxVal,
		)
	}

	return nil
}

// validateSchedulers enforces AWS bounds on the schedulers count given the airflow version.
// Airflow v1 supports exactly 1 scheduler; v2+ supports 2-5.
func validateSchedulers(airflowVersion string, count int32) error {
	if strings.HasPrefix(airflowVersion, "1.") {
		if count != defaultSchedulersV1 {
			return fmt.Errorf(
				"%w: Schedulers must be %d for Airflow v1",
				ErrInvalidParameter, defaultSchedulersV1,
			)
		}

		return nil
	}

	if count < minSchedulersV2 || count > maxSchedulersV2 {
		return fmt.Errorf(
			"%w: Schedulers must be between %d and %d for Airflow v2+",
			ErrInvalidParameter, minSchedulersV2, maxSchedulersV2,
		)
	}

	return nil
}

// CreateEnvironment creates a new MWAA environment.
func (b *InMemoryBackend) CreateEnvironment(
	region, accountID, name string,
	req *createEnvironmentRequest,
) (*Environment, error) {
	if err := validateEnvironmentName(name); err != nil {
		return nil, err
	}

	if err := validateCreateRequest(req); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	if _, exists := b.environments[name]; exists {
		return nil, ErrEnvironmentAlreadyExists
	}

	defaults := resolveCreateDefaults(req)
	if defaults.minWorkers > defaults.maxWorkers {
		return nil, fmt.Errorf(
			"%w: MinWorkers (%d) must be <= MaxWorkers (%d)",
			ErrInvalidParameter, defaults.minWorkers, defaults.maxWorkers,
		)
	}

	env := buildEnvironment(region, accountID, name, req, defaults)

	b.environments[name] = env
	b.arnIndex[env.ARN] = name

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
func (b *InMemoryBackend) GetEnvironment(name string) (*Environment, error) {
	// Full write lock: GetEnvironment may promote a transient lifecycle status
	// (UPDATING → AVAILABLE) on the stored environment via promoteTransientStatus.
	b.mu.Lock("GetEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	cp := cloneEnvironment(env)
	promoteTransientStatus(env)

	return cp, nil
}

// promoteTransientStatus advances mock-only transient lifecycle states (CREATING,
// UPDATING, CREATING_SNAPSHOT, UPDATE_ROLLING_BACK, PENDING) back to AVAILABLE
// so callers can observe the transition once and then see the steady state.
func promoteTransientStatus(env *Environment) {
	if env == nil {
		return
	}

	switch env.Status {
	case envStatusCreating, envStatusUpdating, envStatusCreatingSnapshot, envStatusUpdateRollback, envStatusPending:
		env.Status = envStatusAvailable
	}
}

// DeleteEnvironment deletes an MWAA environment by name and cascades to metrics.
func (b *InMemoryBackend) DeleteEnvironment(name string) (*Environment, error) {
	b.mu.Lock("DeleteEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	delete(b.environments, name)
	delete(b.arnIndex, env.ARN)
	delete(b.metrics, name)

	return env, nil
}

// UpdateEnvironment updates an existing MWAA environment.
func (b *InMemoryBackend) UpdateEnvironment(name string, req *updateEnvironmentRequest) (*Environment, error) {
	if err := validateUpdateRequest(req); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
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

		env.NetworkConfiguration = req.NetworkConfiguration
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

// validateUpdateS3Paths validates the three optional S3 path/version pairs and maintenance window.
func validateUpdateS3Paths(req *updateEnvironmentRequest) error {
	if err := validateS3PathVersionPair("PluginsS3Path", req.PluginsS3Path, req.PluginsS3ObjectVersion); err != nil {
		return err
	}

	if err := validateS3PathVersionPair(
		"RequirementsS3Path", req.RequirementsS3Path, req.RequirementsS3ObjectVersion,
	); err != nil {
		return err
	}

	if err := validateS3PathVersionPair(
		"StartupScriptS3Path", req.StartupScriptS3Path, req.StartupScriptS3ObjectVersion,
	); err != nil {
		return err
	}

	if req.WeeklyMaintenanceWindowStart != "" {
		if err := validateWeeklyMaintenanceWindowStart(req.WeeklyMaintenanceWindowStart); err != nil {
			return err
		}
	}

	return nil
}

// validateUpdateEnums validates enumerated fields that can be changed via UpdateEnvironment.
func validateUpdateEnums(req *updateEnvironmentRequest) error {
	if req.AirflowVersion != "" {
		if _, ok := validAirflowVersions()[req.AirflowVersion]; !ok {
			return fmt.Errorf("%w: unsupported AirflowVersion %q", ErrInvalidParameter, req.AirflowVersion)
		}
	}

	if req.EnvironmentClass != "" {
		if _, ok := validEnvironmentClasses()[req.EnvironmentClass]; !ok {
			return fmt.Errorf("%w: invalid EnvironmentClass %q", ErrInvalidParameter, req.EnvironmentClass)
		}
	}

	if req.WebserverAccessMode != "" &&
		req.WebserverAccessMode != accessModePublic &&
		req.WebserverAccessMode != accessModePrivate {
		return fmt.Errorf(
			"%w: WebserverAccessMode must be %s or %s",
			ErrInvalidParameter, accessModePublic, accessModePrivate,
		)
	}

	return validateWorkerReplacementStrategy(req.WorkerReplacementStrategy)
}

// validateUpdateSizing validates sizing fields for UpdateEnvironment.
func validateUpdateSizing(req *updateEnvironmentRequest) error {
	if req.MaxWorkers != 0 && req.MaxWorkers > maxWorkersAllowed {
		return fmt.Errorf("%w: MaxWorkers cannot exceed %d", ErrInvalidParameter, maxWorkersAllowed)
	}

	if req.MaxWebservers != 0 || req.MinWebservers != 0 {
		if err := validateWebservers(req.MinWebservers, req.MaxWebservers); err != nil {
			return err
		}
	}

	if req.Schedulers != 0 {
		if err := validateSchedulers(req.AirflowVersion, req.Schedulers); err != nil {
			return err
		}
	}

	return nil
}

// validateUpdateRequest applies the same field-level rules as create where applicable.
func validateUpdateRequest(req *updateEnvironmentRequest) error {
	if err := validateUpdateS3Paths(req); err != nil {
		return err
	}

	if err := validateUpdateEnums(req); err != nil {
		return err
	}

	if err := validateLoggingConfiguration(req.LoggingConfiguration); err != nil {
		return err
	}

	return validateUpdateSizing(req)
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
func (b *InMemoryBackend) ListEnvironmentsPage(nextToken string, pageSize int) ([]string, string, error) {
	if pageSize <= 0 {
		pageSize = listEnvDefaultPageSize
	}

	if pageSize > listEnvMaxPageSize {
		pageSize = listEnvMaxPageSize
	}

	b.mu.RLock("ListEnvironmentsPage")
	defer b.mu.RUnlock()

	all := make([]string, 0, len(b.environments))
	for name := range b.environments {
		all = append(all, name)
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
func (b *InMemoryBackend) ListEnvironments() ([]string, error) {
	names, _, err := b.ListEnvironmentsPage("", listEnvMaxPageSize)

	return names, err
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	env := b.findByARN(resourceARN)
	if env == nil {
		return ErrEnvironmentNotFound
	}

	if env.Tags == nil {
		env.Tags = make(map[string]string)
	}

	// Project the merged tag count to enforce the per-resource limit.
	projected := maps.Clone(env.Tags)
	maps.Copy(projected, tags)

	if len(projected) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource would have %d tags, which exceeds the limit of %d",
			ErrInvalidParameter, len(projected), maxTagsPerResource,
		)
	}

	maps.Copy(env.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	env := b.findByARN(resourceARN)
	if env == nil {
		return ErrEnvironmentNotFound
	}

	for _, k := range tagKeys {
		delete(env.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	env := b.findByARN(resourceARN)
	if env == nil {
		return nil, ErrEnvironmentNotFound
	}

	result := make(map[string]string, len(env.Tags))
	maps.Copy(result, env.Tags)

	return result, nil
}

// findByARN looks up an environment by its ARN using the ARN index. Must be called with lock held.
func (b *InMemoryBackend) findByARN(resourceARN string) *Environment {
	name, ok := b.arnIndex[resourceARN]
	if !ok {
		return nil
	}

	return b.environments[name]
}

// InvokeRestAPI simulates calling the Apache Airflow REST API on the specified environment's webserver.
func (b *InMemoryBackend) InvokeRestAPI(envName string, req *invokeRestAPIRequest) (*InvokeRestAPIResponse, error) {
	b.mu.RLock("InvokeRestAPI")
	defer b.mu.RUnlock()

	if _, ok := b.environments[envName]; !ok {
		return nil, ErrEnvironmentNotFound
	}

	if req.Method == "" {
		return nil, fmt.Errorf("%w: Method is required", ErrInvalidParameter)
	}

	if req.Path == "" {
		return nil, fmt.Errorf("%w: Path is required", ErrInvalidParameter)
	}

	return &InvokeRestAPIResponse{
		RestAPIStatusCode: restAPISuccessCode,
		RestAPIResponse:   map[string]any{},
	}, nil
}

// PublishMetrics stores internal environment metrics for the specified environment.
// The total number of metrics per environment is capped at maxMetricsPerEnv.
func (b *InMemoryBackend) PublishMetrics(envName string, req *publishMetricsRequest) error {
	b.mu.Lock("PublishMetrics")
	defer b.mu.Unlock()

	if _, ok := b.environments[envName]; !ok {
		return ErrEnvironmentNotFound
	}

	b.metrics[envName] = append(b.metrics[envName], req.MetricData...)

	if len(b.metrics[envName]) > maxMetricsPerEnv {
		b.metrics[envName] = b.metrics[envName][len(b.metrics[envName])-maxMetricsPerEnv:]
	}

	return nil
}

// GetMetrics returns the stored metrics for the specified environment.
func (b *InMemoryBackend) GetMetrics(envName string) ([]MetricDatum, error) {
	b.mu.RLock("GetMetrics")
	defer b.mu.RUnlock()

	if _, ok := b.environments[envName]; !ok {
		return nil, ErrEnvironmentNotFound
	}

	data := b.metrics[envName]
	result := make([]MetricDatum, len(data))
	copy(result, data)

	return result, nil
}

// CreateCliToken validates that the environment exists and returns a JWT-shaped CLI token.
func (b *InMemoryBackend) CreateCliToken(envName string) (string, error) {
	b.mu.RLock("CreateCliToken")
	defer b.mu.RUnlock()

	if _, ok := b.environments[envName]; !ok {
		return "", ErrEnvironmentNotFound
	}

	return generateMWAAToken(envName, "cli"), nil
}

// CreateWebLoginToken validates that the environment exists and returns a JWT-shaped web login token.
func (b *InMemoryBackend) CreateWebLoginToken(envName string) (string, error) {
	b.mu.RLock("CreateWebLoginToken")
	defer b.mu.RUnlock()

	if _, ok := b.environments[envName]; !ok {
		return "", ErrEnvironmentNotFound
	}

	return generateMWAAToken(envName, "web"), nil
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

// validateNetworkConfigUpdate enforces basic shape rules on a network update.
// AWS rejects updates that drop subnets/security groups; we mirror that to
// surface obvious user errors early.
func validateNetworkConfigUpdate(nc *NetworkConfig) error {
	if nc == nil {
		return nil
	}

	if len(nc.SubnetIDs) == 0 {
		return fmt.Errorf("%w: NetworkConfiguration.SubnetIds must not be empty", ErrInvalidParameter)
	}

	if len(nc.SecurityGroupIDs) == 0 {
		return fmt.Errorf("%w: NetworkConfiguration.SecurityGroupIds must not be empty", ErrInvalidParameter)
	}

	return nil
}
