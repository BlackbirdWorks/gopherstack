package lambdaweb

import "maps"

func weightsToDTO(ws []RevisionWeight) []revisionWeightDTO {
	out := make([]revisionWeightDTO, 0, len(ws))
	for _, w := range ws {
		out = append(out, revisionWeightDTO(w))
	}

	return out
}

func weightsFromDTO(ws []revisionWeightDTO) []RevisionWeight {
	out := make([]RevisionWeight, 0, len(ws))
	for _, w := range ws {
		out = append(out, RevisionWeight(w))
	}

	return out
}

func scalingToDTO(e *Endpoint) *scalingConfigDTO {
	if e.MaxEnvironments == nil {
		return nil
	}

	v := *e.MaxEnvironments

	return &scalingConfigDTO{MaxEnvironments: &v}
}

func throttleToDTO(e *Endpoint) *throttleConfigDTO {
	if e.RateLimit == nil {
		return nil
	}

	v := *e.RateLimit

	return &throttleConfigDTO{RateLimit: &v}
}

func scalingFromDTO(d *scalingConfigDTO) *ScalingInput {
	if d == nil {
		return nil
	}

	return &ScalingInput{MaxEnvironments: d.MaxEnvironments}
}

func throttleFromDTO(d *throttleConfigDTO) *ThrottleInput {
	if d == nil {
		return nil
	}

	return &ThrottleInput{RateLimit: d.RateLimit}
}

func functionSummary(f *Function) functionSummaryDTO {
	return functionSummaryDTO{
		CreatedAt: fmtTime(f.CreatedAt), FunctionArn: f.ARN, FunctionName: f.Name, State: f.State,
		StateReason: f.StateReason, UpdatedAt: fmtTime(f.UpdatedAt),
	}
}

func revisionSummary(r *Revision) revisionSummaryDTO {
	return revisionSummaryDTO{
		CreatedAt: fmtTime(r.CreatedAt), Description: r.Description, RevisionArn: r.ARN, RevisionID: r.ID,
		State: r.State, StateReason: r.StateReason,
	}
}

func endpointSummary(e *Endpoint) endpointSummaryDTO {
	return endpointSummaryDTO{
		ScalingConfig: scalingToDTO(e), ThrottleConfig: throttleToDTO(e), AuthType: e.AuthType,
		AutoDeploymentMode: e.AutoDeploymentMode, CreatedAt: fmtTime(e.CreatedAt), Description: e.Description,
		DomainName: e.DomainName, EndpointArn: e.ARN, EndpointName: e.Name, EndpointType: e.Type,
		Regions: e.Regions, RevisionWeights: weightsToDTO(e.Weights), State: e.State, StateReason: e.StateReason,
		UpdateStatus: e.UpdateStatus, UpdateStatusReason: updateReason(e), UpdatedAt: fmtTime(e.UpdatedAt),
	}
}

func updateReason(e *Endpoint) string {
	if e.UpdateStatus == "" {
		return ""
	}

	return e.UpdateReason
}

func endpointResponseOf(e *Endpoint) endpointResponse {
	regional := make(map[string]regionalEndpointDTO, len(e.Regions))
	for _, r := range e.Regions {
		regional[r] = regionalEndpointDTO{
			ScalingConfig: scalingToDTO(e), ThrottleConfig: throttleToDTO(e), AuthType: e.AuthType,
			DomainName: e.RegionDomains[r], RevisionWeights: weightsToDTO(e.Weights), State: e.State,
			StateReason: e.StateReason, UpdateStatus: e.UpdateStatus, UpdateStatusReason: updateReason(e),
		}
	}

	return endpointResponse{
		ScalingConfig: scalingToDTO(e), ThrottleConfig: throttleToDTO(e), RegionalEndpoints: regional,
		AuthType: e.AuthType, AutoDeploymentMode: e.AutoDeploymentMode, CreatedAt: fmtTime(e.CreatedAt),
		Description: e.Description, DomainName: e.DomainName, EndpointArn: e.ARN, EndpointName: e.Name,
		EndpointType: e.Type, FunctionArn: functionARN(e.Region, e.AccountID, e.FunctionName), Regions: e.Regions,
		RevisionWeights: weightsToDTO(e.Weights), State: e.State, StateReason: e.StateReason,
		UpdateStatus: e.UpdateStatus, UpdateStatusReason: updateReason(e), UpdatedAt: fmtTime(e.UpdatedAt),
	}
}

func buildConfigOf(r *Revision) buildConfigDTO {
	s3 := s3ObjectDTO{Bucket: r.S3.Bucket, Key: r.S3.Key, VersionID: r.S3.VersionID}

	return buildConfigDTO{
		CodeConfig: &codeConfigDTO{S3Object: &s3}, RuntimeConfig: &runtimeConfigDTO{Runtime: r.Runtime},
	}
}

func serviceConfigOf(sc ServiceConfig) serviceConfigDTO {
	timeout, conc := sc.TimeoutSeconds, sc.MaxConcurrencyPerEnvironment
	out := serviceConfigDTO{
		EnvironmentVariables: maps.Clone(sc.EnvironmentVariables), ExecutionRoleArn: sc.ExecutionRoleArn,
		EgressNetworkConnectorArn: sc.EgressNetworkConnectorArn, TimeoutSeconds: &timeout,
		MaxConcurrencyPerEnvironment: &conc,
	}

	if sc.Logging != nil {
		out.TelemetryConfig = &telemetryConfigDTO{LoggingConfig: &loggingConfigDTO{
			ApplicationLogLevel: sc.Logging.ApplicationLogLevel, LogGroup: sc.Logging.LogGroup,
			SystemLogLevel: sc.Logging.SystemLogLevel,
		}}
	}

	return out
}

func revisionResponseOf(r *Revision) revisionResponse {
	errs := make([]revisionErrorDTO, 0, len(r.Errors))
	for _, e := range r.Errors {
		errs = append(errs, revisionErrorDTO(e))
	}

	if len(errs) == 0 {
		errs = nil
	}

	return revisionResponse{
		BuildConfig: buildConfigOf(r), ServiceConfig: serviceConfigOf(r.ServiceConfig), Errors: errs,
		CreatedAt: fmtTime(r.CreatedAt), Description: r.Description,
		FunctionArn: functionARN(r.Region, r.AccountID, r.FunctionName), KmsKeyArn: r.KmsKeyArn,
		RevisionArn: r.ARN, RevisionID: r.ID, State: r.State, StateReason: r.StateReason,
	}
}

func revisionInputFromDTO(d *revisionConfigDTO) RevisionInput {
	in := RevisionInput{Description: d.Description, KmsKeyArn: d.KmsKeyArn}

	if d.BuildConfig != nil {
		if d.BuildConfig.CodeConfig != nil && d.BuildConfig.CodeConfig.S3Object != nil {
			o := d.BuildConfig.CodeConfig.S3Object
			in.S3 = S3Object{Bucket: o.Bucket, Key: o.Key, VersionID: o.VersionID}
		}

		if d.BuildConfig.RuntimeConfig != nil {
			in.Runtime = d.BuildConfig.RuntimeConfig.Runtime
		}
	}

	if d.ServiceConfig != nil {
		in.ServiceConfig = serviceConfigFromDTO(d.ServiceConfig)
	}

	return in
}

func serviceConfigFromDTO(d *serviceConfigDTO) ServiceConfig {
	sc := ServiceConfig{
		ExecutionRoleArn: d.ExecutionRoleArn, EgressNetworkConnectorArn: d.EgressNetworkConnectorArn,
		EnvironmentVariables: maps.Clone(d.EnvironmentVariables),
	}

	if d.TimeoutSeconds != nil {
		sc.TimeoutSeconds = *d.TimeoutSeconds
	}

	if d.MaxConcurrencyPerEnvironment != nil {
		sc.MaxConcurrencyPerEnvironment = *d.MaxConcurrencyPerEnvironment
	}

	if d.TelemetryConfig != nil && d.TelemetryConfig.LoggingConfig != nil {
		lc := d.TelemetryConfig.LoggingConfig
		sc.Logging = &LoggingConfig{
			ApplicationLogLevel: lc.ApplicationLogLevel, LogGroup: lc.LogGroup, SystemLogLevel: lc.SystemLogLevel,
		}
	} else if d.TelemetryConfig != nil {
		sc.Logging = &LoggingConfig{}
	}

	return sc
}

func endpointInputFromDTO(d *endpointConfigDTO) EndpointInput {
	return EndpointInput{
		Name: d.EndpointName, Description: d.Description, Type: d.EndpointType, AuthType: d.AuthType,
		AutoDeploymentMode: d.AutoDeploymentMode, Regions: d.Regions, Scaling: scalingFromDTO(d.ScalingConfig),
		Throttle: throttleFromDTO(d.ThrottleConfig),
	}
}

func filtersFromDTO(fs []filterDTO) []Filter {
	out := make([]Filter, 0, len(fs))
	for _, f := range fs {
		out = append(out, Filter(f))
	}

	return out
}
