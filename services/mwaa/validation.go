package mwaa

import (
	"fmt"
	"strconv"
	"strings"
)

// validEnvironmentClasses returns the set of valid environment class values.
// AWS's documented set (aws-sdk-go-v2/service/mwaa/types.EnvironmentClass field
// comment on CreateEnvironmentInput) is mw1.micro, mw1.small, mw1.medium,
// mw1.large, mw1.xlarge, mw1.2xlarge -- gopherstack previously omitted
// mw1.micro entirely, incorrectly rejecting a real, valid class name.
func validEnvironmentClasses() map[string]struct{} {
	return map[string]struct{}{
		environmentClassMicro: {},
		"mw1.small":           {},
		"mw1.medium":          {},
		"mw1.large":           {},
		"mw1.xlarge":          {},
		"mw1.2xlarge":         {},
	}
}

// validLogLevels returns the set of valid Airflow log level values.
func validLogLevels() map[string]struct{} {
	return map[string]struct{}{
		"CRITICAL": {},
		"ERROR":    {},
		"WARNING":  {},
		"INFO":     {},
		"DEBUG":    {},
	}
}

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

	if strategy != workerStrategyForced && strategy != workerStrategyGraceful {
		return fmt.Errorf(
			"%w: WorkerReplacementStrategy must be %s or %s, got %q",
			ErrInvalidParameter, workerStrategyForced, workerStrategyGraceful, strategy,
		)
	}

	return nil
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

	return validateNetworkConfigCreate(req.NetworkConfiguration)
}

// validateNetworkConfigCreate enforces AWS's CreateEnvironmentInput.NetworkConfiguration
// contract: the member itself is required (aws-sdk-go-v2's generated client-side
// validator rejects a nil NetworkConfiguration before the request is ever sent), and its
// SubnetIds must contain exactly 2 entries (subnets are pinned to 2 AZs) while
// SecurityGroupIds must contain 1-5 entries. Confirmed against
// docs.aws.amazon.com/mwaa/latest/API/API_CreateEnvironment.html and
// API_NetworkConfiguration.html.
func validateNetworkConfigCreate(nc *NetworkConfig) error {
	if nc == nil {
		return fmt.Errorf("%w: NetworkConfiguration is required", ErrInvalidParameter)
	}

	if len(nc.SubnetIDs) != requiredSubnetIDs {
		return fmt.Errorf(
			"%w: NetworkConfiguration.SubnetIds must contain exactly %d entries",
			ErrInvalidParameter, requiredSubnetIDs,
		)
	}

	if len(nc.SecurityGroupIDs) < minSecurityGroupIDs || len(nc.SecurityGroupIDs) > maxSecurityGroupIDs {
		return fmt.Errorf(
			"%w: NetworkConfiguration.SecurityGroupIds must contain %d-%d entries",
			ErrInvalidParameter, minSecurityGroupIDs, maxSecurityGroupIDs,
		)
	}

	return nil
}

// validateWebserverAccessMode validates the WebserverAccessMode field, shared
// by both CreateEnvironment and UpdateEnvironment. AWS's WebserverAccessMode
// enum has three values (PUBLIC_ONLY | PRIVATE_ONLY | PUBLIC_AND_PRIVATE --
// confirmed via aws-sdk-go-v2/service/mwaa/types.WebserverAccessMode); a prior
// version of this file only accepted the first two, incorrectly rejecting
// real requests that set PUBLIC_AND_PRIVATE.
func validateWebserverAccessMode(mode string) error {
	if mode == "" || mode == accessModePublic || mode == accessModePrivate || mode == accessModePublicAndPrivate {
		return nil
	}

	return fmt.Errorf(
		"%w: WebserverAccessMode must be %s, %s, or %s",
		ErrInvalidParameter, accessModePublic, accessModePrivate, accessModePublicAndPrivate,
	)
}

// validateCreateEnums validates enumerated string fields and ARN-shaped fields.
func validateCreateEnums(req *createEnvironmentRequest) error {
	if err := validateWebserverAccessMode(req.WebserverAccessMode); err != nil {
		return err
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

	// No WorkerReplacementStrategy check here: it is not a member of
	// CreateEnvironmentInput in the real API (see createEnvironmentRequest's
	// doc comment in models.go) -- only validateUpdateEnums validates it.
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
		if err := validateWebserversForClass(req.EnvironmentClass, req.MinWebservers, req.MaxWebservers); err != nil {
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

// validateWebserversForClass enforces AWS's mw1.micro-specific webserver bounds:
// "For environments larger than mw1.micro, accepts values from 2 to 5. Defaults
// to 2 for all environment sizes except mw1.micro, which defaults to 1" (confirmed
// via aws-sdk-go-v2/service/mwaa@v1.43.4/types/types.go's MaxWebservers/MinWebservers
// doc comments, identical text in botocore's mwaa/2020-07-01/service-2.json). Since
// the 2-5 range is explicitly scoped to classes larger than mw1.micro, mw1.micro
// itself only accepts an explicit value of 1.
func validateWebserversForClass(envClass string, minVal, maxVal int32) error {
	if envClass == environmentClassMicro {
		if (minVal != 0 && minVal != microWebservers) || (maxVal != 0 && maxVal != microWebservers) {
			return fmt.Errorf(
				"%w: mw1.micro only supports MaxWebservers/MinWebservers of %d",
				ErrInvalidParameter, microWebservers,
			)
		}

		return nil
	}

	return validateWebservers(minVal, maxVal)
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

	if err := validateWebserverAccessMode(req.WebserverAccessMode); err != nil {
		return err
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

// validateNetworkConfigUpdate enforces AWS's UpdateNetworkConfigurationInput
// bounds: SecurityGroupIds is required and accepts 1-5 entries. Unlike
// CreateEnvironment's NetworkConfiguration, there is no SubnetIds member here
// -- subnets are immutable after environment creation.
func validateNetworkConfigUpdate(nc *UpdateNetworkConfig) error {
	if nc == nil {
		return nil
	}

	if len(nc.SecurityGroupIDs) == 0 {
		return fmt.Errorf("%w: NetworkConfiguration.SecurityGroupIds must not be empty", ErrInvalidParameter)
	}

	if len(nc.SecurityGroupIDs) > maxSecurityGroupIDs {
		return fmt.Errorf(
			"%w: NetworkConfiguration.SecurityGroupIds cannot exceed %d entries",
			ErrInvalidParameter, maxSecurityGroupIDs,
		)
	}

	return nil
}
