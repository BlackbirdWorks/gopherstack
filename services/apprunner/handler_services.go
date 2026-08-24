package apprunner

import (
	"context"
	"fmt"
)

// --- wire input shapes -----------------------------------------------------
//
// These mirror aws-sdk-go-v2/service/apprunner/types' InstanceConfiguration,
// SourceConfiguration (+ ImageRepository/CodeRepository/
// AuthenticationConfiguration and their nested Image/CodeConfiguration
// shapes), NetworkConfiguration (+ Egress/IngressConfiguration), and
// HealthCheckConfiguration/EncryptionConfiguration/
// ServiceObservabilityConfiguration field-for-field.

type imageConfigurationInput struct {
	RuntimeEnvironmentSecrets   map[string]string `json:"RuntimeEnvironmentSecrets,omitempty"`
	RuntimeEnvironmentVariables map[string]string `json:"RuntimeEnvironmentVariables,omitempty"`
	Port                        string            `json:"Port,omitempty"`
	StartCommand                string            `json:"StartCommand,omitempty"`
}

type imageRepositoryInput struct {
	ImageConfiguration  *imageConfigurationInput `json:"ImageConfiguration,omitempty"`
	ImageIdentifier     string                   `json:"ImageIdentifier"`
	ImageRepositoryType string                   `json:"ImageRepositoryType"`
}

type sourceCodeVersionInput struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

type codeConfigurationValuesInput struct {
	RuntimeEnvironmentSecrets   map[string]string `json:"RuntimeEnvironmentSecrets,omitempty"`
	RuntimeEnvironmentVariables map[string]string `json:"RuntimeEnvironmentVariables,omitempty"`
	Runtime                     string            `json:"Runtime"`
	BuildCommand                string            `json:"BuildCommand,omitempty"`
	Port                        string            `json:"Port,omitempty"`
	StartCommand                string            `json:"StartCommand,omitempty"`
}

type codeConfigurationInput struct {
	CodeConfigurationValues *codeConfigurationValuesInput `json:"CodeConfigurationValues,omitempty"`
	ConfigurationSource     string                        `json:"ConfigurationSource"`
}

type codeRepositoryInput struct {
	SourceCodeVersion *sourceCodeVersionInput `json:"SourceCodeVersion"`
	CodeConfiguration *codeConfigurationInput `json:"CodeConfiguration,omitempty"`
	RepositoryURL     string                  `json:"RepositoryUrl"`
	SourceDirectory   string                  `json:"SourceDirectory,omitempty"`
}

type authenticationConfigurationInput struct {
	AccessRoleArn string `json:"AccessRoleArn,omitempty"`
	ConnectionArn string `json:"ConnectionArn,omitempty"`
}

type sourceConfigurationInput struct {
	AuthenticationConfiguration *authenticationConfigurationInput `json:"AuthenticationConfiguration,omitempty"`
	AutoDeploymentsEnabled      *bool                             `json:"AutoDeploymentsEnabled,omitempty"`
	CodeRepository              *codeRepositoryInput              `json:"CodeRepository,omitempty"`
	ImageRepository             *imageRepositoryInput             `json:"ImageRepository,omitempty"`
}

type instanceConfigurationInput struct {
	CPU             string `json:"Cpu,omitempty"`
	InstanceRoleArn string `json:"InstanceRoleArn,omitempty"`
	Memory          string `json:"Memory,omitempty"`
}

type egressConfigurationInput struct {
	EgressType      string `json:"EgressType,omitempty"`
	VpcConnectorArn string `json:"VpcConnectorArn,omitempty"`
}

type ingressConfigurationInput struct {
	IsPubliclyAccessible *bool `json:"IsPubliclyAccessible,omitempty"`
}

type networkConfigurationInput struct {
	EgressConfiguration  *egressConfigurationInput  `json:"EgressConfiguration,omitempty"`
	IngressConfiguration *ingressConfigurationInput `json:"IngressConfiguration,omitempty"`
	IPAddressType        string                     `json:"IpAddressType,omitempty"`
}

type healthCheckConfigurationInput struct {
	Path               string `json:"Path,omitempty"`
	Protocol           string `json:"Protocol,omitempty"`
	HealthyThreshold   int32  `json:"HealthyThreshold,omitempty"`
	Interval           int32  `json:"Interval,omitempty"`
	Timeout            int32  `json:"Timeout,omitempty"`
	UnhealthyThreshold int32  `json:"UnhealthyThreshold,omitempty"`
}

type encryptionConfigurationInput struct {
	KmsKey string `json:"KmsKey"`
}

type serviceObservabilityConfigurationInput struct {
	ObservabilityConfigurationArn string `json:"ObservabilityConfigurationArn,omitempty"`
	ObservabilityEnabled          bool   `json:"ObservabilityEnabled"`
}

// --- wire input -> domain mapping ------------------------------------------

func imageSourceFromInput(in *imageRepositoryInput) *ImageSource {
	if in == nil {
		return nil
	}

	img := &ImageSource{
		ImageIdentifier:     in.ImageIdentifier,
		ImageRepositoryType: in.ImageRepositoryType,
	}

	if in.ImageConfiguration != nil {
		img.Port = in.ImageConfiguration.Port
		img.StartCommand = in.ImageConfiguration.StartCommand
		img.RuntimeEnvironmentVariables = in.ImageConfiguration.RuntimeEnvironmentVariables
		img.RuntimeEnvironmentSecrets = in.ImageConfiguration.RuntimeEnvironmentSecrets
	}

	return img
}

func codeSourceFromInput(in *codeRepositoryInput) *CodeSource {
	if in == nil {
		return nil
	}

	cs := &CodeSource{
		RepositoryURL:   in.RepositoryURL,
		SourceDirectory: in.SourceDirectory,
	}

	if in.SourceCodeVersion != nil {
		cs.SourceCodeVersionType = in.SourceCodeVersion.Type
		cs.SourceCodeVersionValue = in.SourceCodeVersion.Value
	}

	if in.CodeConfiguration != nil {
		cs.ConfigurationSource = in.CodeConfiguration.ConfigurationSource

		if v := in.CodeConfiguration.CodeConfigurationValues; v != nil {
			cs.Runtime = v.Runtime
			cs.BuildCommand = v.BuildCommand
			cs.StartCommand = v.StartCommand
			cs.Port = v.Port
			cs.RuntimeEnvironmentVariables = v.RuntimeEnvironmentVariables
			cs.RuntimeEnvironmentSecrets = v.RuntimeEnvironmentSecrets
		}
	}

	return cs
}

func sourceConfigFromInput(in *sourceConfigurationInput) SourceConfig {
	if in == nil {
		return SourceConfig{}
	}

	sc := SourceConfig{
		ImageRepository:        imageSourceFromInput(in.ImageRepository),
		CodeRepository:         codeSourceFromInput(in.CodeRepository),
		AutoDeploymentsEnabled: in.AutoDeploymentsEnabled,
	}

	if in.AuthenticationConfiguration != nil {
		sc.AccessRoleArn = in.AuthenticationConfiguration.AccessRoleArn
		sc.ConnectionArn = in.AuthenticationConfiguration.ConnectionArn
	}

	return sc
}

func instanceConfigFromInput(in *instanceConfigurationInput) InstanceConfig {
	if in == nil {
		return InstanceConfig{}
	}

	return InstanceConfig{CPU: in.CPU, Memory: in.Memory, InstanceRoleArn: in.InstanceRoleArn}
}

func networkConfigFromInput(in *networkConfigurationInput) *NetworkConfig {
	if in == nil {
		return nil
	}

	nc := &NetworkConfig{IPAddressType: in.IPAddressType}

	if in.EgressConfiguration != nil {
		nc.EgressType = in.EgressConfiguration.EgressType
		nc.EgressVpcConnectorArn = in.EgressConfiguration.VpcConnectorArn
	}

	if in.IngressConfiguration != nil {
		nc.IsPubliclyAccessible = in.IngressConfiguration.IsPubliclyAccessible
	}

	return nc
}

func healthCheckConfigFromInput(in *healthCheckConfigurationInput) *HealthCheckConfig {
	if in == nil {
		return nil
	}

	return &HealthCheckConfig{
		Protocol:           in.Protocol,
		Path:               in.Path,
		Interval:           in.Interval,
		Timeout:            in.Timeout,
		HealthyThreshold:   in.HealthyThreshold,
		UnhealthyThreshold: in.UnhealthyThreshold,
	}
}

func observabilityFromInput(in *serviceObservabilityConfigurationInput) *ServiceObservability {
	if in == nil {
		return nil
	}

	return &ServiceObservability{Enabled: in.ObservabilityEnabled, ConfigurationArn: in.ObservabilityConfigurationArn}
}

// --- wire output shapes -----------------------------------------------------

type imageConfigurationOutput struct {
	RuntimeEnvironmentSecrets   map[string]string `json:"RuntimeEnvironmentSecrets,omitempty"`
	RuntimeEnvironmentVariables map[string]string `json:"RuntimeEnvironmentVariables,omitempty"`
	Port                        string            `json:"Port,omitempty"`
	StartCommand                string            `json:"StartCommand,omitempty"`
}

type imageRepositoryOutput struct {
	ImageConfiguration  *imageConfigurationOutput `json:"ImageConfiguration,omitempty"`
	ImageIdentifier     string                    `json:"ImageIdentifier"`
	ImageRepositoryType string                    `json:"ImageRepositoryType"`
}

type sourceCodeVersionOutput struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

type codeConfigurationValuesOutput struct {
	RuntimeEnvironmentSecrets   map[string]string `json:"RuntimeEnvironmentSecrets,omitempty"`
	RuntimeEnvironmentVariables map[string]string `json:"RuntimeEnvironmentVariables,omitempty"`
	Runtime                     string            `json:"Runtime"`
	BuildCommand                string            `json:"BuildCommand,omitempty"`
	Port                        string            `json:"Port,omitempty"`
	StartCommand                string            `json:"StartCommand,omitempty"`
}

type codeConfigurationOutput struct {
	CodeConfigurationValues *codeConfigurationValuesOutput `json:"CodeConfigurationValues,omitempty"`
	ConfigurationSource     string                         `json:"ConfigurationSource"`
}

type codeRepositoryOutput struct {
	SourceCodeVersion *sourceCodeVersionOutput `json:"SourceCodeVersion,omitempty"`
	CodeConfiguration *codeConfigurationOutput `json:"CodeConfiguration,omitempty"`
	RepositoryURL     string                   `json:"RepositoryUrl"`
	SourceDirectory   string                   `json:"SourceDirectory,omitempty"`
}

type authenticationConfigurationOutput struct {
	AccessRoleArn string `json:"AccessRoleArn,omitempty"`
	ConnectionArn string `json:"ConnectionArn,omitempty"`
}

type sourceConfigurationOutput struct {
	AuthenticationConfiguration *authenticationConfigurationOutput `json:"AuthenticationConfiguration,omitempty"`
	CodeRepository              *codeRepositoryOutput              `json:"CodeRepository,omitempty"`
	ImageRepository             *imageRepositoryOutput             `json:"ImageRepository,omitempty"`
	AutoDeploymentsEnabled      bool                               `json:"AutoDeploymentsEnabled"`
}

type instanceConfigurationOutput struct {
	CPU             string `json:"Cpu,omitempty"`
	InstanceRoleArn string `json:"InstanceRoleArn,omitempty"`
	Memory          string `json:"Memory,omitempty"`
}

type egressConfigurationOutput struct {
	EgressType      string `json:"EgressType,omitempty"`
	VpcConnectorArn string `json:"VpcConnectorArn,omitempty"`
}

type ingressConfigurationOutput struct {
	IsPubliclyAccessible bool `json:"IsPubliclyAccessible"`
}

type networkConfigurationOutput struct {
	EgressConfiguration  *egressConfigurationOutput  `json:"EgressConfiguration,omitempty"`
	IngressConfiguration *ingressConfigurationOutput `json:"IngressConfiguration,omitempty"`
	IPAddressType        string                      `json:"IpAddressType,omitempty"`
}

type healthCheckConfigurationOutput struct {
	Path               string `json:"Path,omitempty"`
	Protocol           string `json:"Protocol,omitempty"`
	HealthyThreshold   int32  `json:"HealthyThreshold,omitempty"`
	Interval           int32  `json:"Interval,omitempty"`
	Timeout            int32  `json:"Timeout,omitempty"`
	UnhealthyThreshold int32  `json:"UnhealthyThreshold,omitempty"`
}

type encryptionConfigurationOutput struct {
	KmsKey string `json:"KmsKey"`
}

type serviceObservabilityConfigurationOutput struct {
	ObservabilityConfigurationArn string `json:"ObservabilityConfigurationArn,omitempty"`
	ObservabilityEnabled          bool   `json:"ObservabilityEnabled"`
}

type serviceOutput struct {
	ObservabilityConfiguration      *serviceObservabilityConfigurationOutput `json:"ObservabilityConfiguration,omitempty"`
	EncryptionConfiguration         *encryptionConfigurationOutput           `json:"EncryptionConfiguration,omitempty"`
	HealthCheckConfiguration        *healthCheckConfigurationOutput          `json:"HealthCheckConfiguration,omitempty"`
	DeletedAt                       *int64                                   `json:"DeletedAt,omitempty"`
	InstanceConfiguration           instanceConfigurationOutput              `json:"InstanceConfiguration"`
	NetworkConfiguration            networkConfigurationOutput               `json:"NetworkConfiguration"`
	SourceConfiguration             sourceConfigurationOutput                `json:"SourceConfiguration"`
	ServiceArn                      string                                   `json:"ServiceArn"`
	ServiceID                       string                                   `json:"ServiceId"`
	ServiceName                     string                                   `json:"ServiceName"`
	ServiceURL                      string                                   `json:"ServiceUrl,omitempty"`
	Status                          string                                   `json:"Status"`
	AutoScalingConfigurationSummary autoScalingConfigurationSummaryOutput    `json:"AutoScalingConfigurationSummary"`
	CreatedAt                       int64                                    `json:"CreatedAt"`
	UpdatedAt                       int64                                    `json:"UpdatedAt"`
}

type createServiceInput struct {
	SourceConfiguration         *sourceConfigurationInput               `json:"SourceConfiguration"`
	InstanceConfiguration       *instanceConfigurationInput             `json:"InstanceConfiguration,omitempty"`
	NetworkConfiguration        *networkConfigurationInput              `json:"NetworkConfiguration,omitempty"`
	HealthCheckConfiguration    *healthCheckConfigurationInput          `json:"HealthCheckConfiguration,omitempty"`
	EncryptionConfiguration     *encryptionConfigurationInput           `json:"EncryptionConfiguration,omitempty"`
	ObservabilityConfiguration  *serviceObservabilityConfigurationInput `json:"ObservabilityConfiguration,omitempty"`
	ServiceName                 string                                  `json:"ServiceName"`
	AutoScalingConfigurationArn string                                  `json:"AutoScalingConfigurationArn,omitempty"`
	Tags                        []tagInput                              `json:"Tags"`
}

type createServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

// toServiceOutput builds the wire shape for a Service, joining in the live
// AutoScalingConfigurationSummary of svc.AutoScalingConfigurationArn (a
// method on Handler, not a free function, because that join needs
// h.Backend).
func (h *Handler) toServiceOutput(svc *Service) serviceOutput {
	out := serviceOutput{
		ServiceArn:  svc.ServiceArn,
		ServiceID:   svc.ServiceID,
		ServiceName: svc.ServiceName,
		ServiceURL:  svc.ServiceURL,
		Status:      svc.Status,
		CreatedAt:   svc.CreatedAt.Unix(),
		UpdatedAt:   svc.UpdatedAt.Unix(),
		InstanceConfiguration: instanceConfigurationOutput{
			CPU:             svc.Instance.CPU,
			Memory:          svc.Instance.Memory,
			InstanceRoleArn: svc.Instance.InstanceRoleArn,
		},
		SourceConfiguration:             toSourceConfigurationOutput(svc.Source),
		NetworkConfiguration:            toNetworkConfigurationOutput(svc.Network),
		HealthCheckConfiguration:        toHealthCheckConfigurationOutput(svc.HealthCheck),
		AutoScalingConfigurationSummary: h.autoScalingSummaryFor(svc.AutoScalingConfigurationArn),
		ObservabilityConfiguration: &serviceObservabilityConfigurationOutput{
			ObservabilityEnabled:          svc.Observability.Enabled,
			ObservabilityConfigurationArn: svc.Observability.ConfigurationArn,
		},
	}

	if svc.EncryptionKmsKey != "" {
		out.EncryptionConfiguration = &encryptionConfigurationOutput{KmsKey: svc.EncryptionKmsKey}
	}

	if !svc.DeletedAt.IsZero() {
		deletedAt := svc.DeletedAt.Unix()
		out.DeletedAt = &deletedAt
	}

	return out
}

// autoScalingSummaryFor joins asgArn to its live AutoScalingConfiguration.
// If the configuration was since deleted, degrades gracefully to just the
// ARN instead of failing the whole Service response.
func (h *Handler) autoScalingSummaryFor(asgArn string) autoScalingConfigurationSummaryOutput {
	cfg, err := h.Backend.DescribeAutoScalingConfiguration(asgArn)
	if err != nil {
		return autoScalingConfigurationSummaryOutput{AutoScalingConfigurationArn: asgArn}
	}

	return autoScalingConfigurationSummaryOutput{
		AutoScalingConfigurationArn:      cfg.AutoScalingConfigurationArn,
		AutoScalingConfigurationName:     cfg.AutoScalingConfigurationName,
		AutoScalingConfigurationRevision: cfg.AutoScalingConfigurationRevision,
		Status:                           cfg.Status,
		IsDefault:                        cfg.IsDefault,
		HasAssociatedService:             cfg.HasAssociatedService,
		CreatedAt:                        cfg.CreatedAt.Unix(),
	}
}

func toImageRepositoryOutput(img *ImageSource) *imageRepositoryOutput {
	out := &imageRepositoryOutput{
		ImageIdentifier:     img.ImageIdentifier,
		ImageRepositoryType: img.ImageRepositoryType,
	}

	if img.Port != "" || img.StartCommand != "" || len(img.RuntimeEnvironmentVariables) > 0 ||
		len(img.RuntimeEnvironmentSecrets) > 0 {
		out.ImageConfiguration = &imageConfigurationOutput{
			Port:                        img.Port,
			StartCommand:                img.StartCommand,
			RuntimeEnvironmentVariables: img.RuntimeEnvironmentVariables,
			RuntimeEnvironmentSecrets:   img.RuntimeEnvironmentSecrets,
		}
	}

	return out
}

func toCodeRepositoryOutput(cs *CodeSource) *codeRepositoryOutput {
	out := &codeRepositoryOutput{
		RepositoryURL:   cs.RepositoryURL,
		SourceDirectory: cs.SourceDirectory,
	}

	if cs.SourceCodeVersionType != "" {
		out.SourceCodeVersion = &sourceCodeVersionOutput{
			Type:  cs.SourceCodeVersionType,
			Value: cs.SourceCodeVersionValue,
		}
	}

	if cs.ConfigurationSource != "" {
		cfgOut := &codeConfigurationOutput{ConfigurationSource: cs.ConfigurationSource}

		if cs.Runtime != "" {
			cfgOut.CodeConfigurationValues = &codeConfigurationValuesOutput{
				Runtime:                     cs.Runtime,
				BuildCommand:                cs.BuildCommand,
				StartCommand:                cs.StartCommand,
				Port:                        cs.Port,
				RuntimeEnvironmentVariables: cs.RuntimeEnvironmentVariables,
				RuntimeEnvironmentSecrets:   cs.RuntimeEnvironmentSecrets,
			}
		}

		out.CodeConfiguration = cfgOut
	}

	return out
}

func toSourceConfigurationOutput(s SourceConfig) sourceConfigurationOutput {
	out := sourceConfigurationOutput{
		AutoDeploymentsEnabled: s.AutoDeploymentsEnabled != nil && *s.AutoDeploymentsEnabled,
	}

	if s.AccessRoleArn != "" || s.ConnectionArn != "" {
		out.AuthenticationConfiguration = &authenticationConfigurationOutput{
			AccessRoleArn: s.AccessRoleArn,
			ConnectionArn: s.ConnectionArn,
		}
	}

	if s.ImageRepository != nil {
		out.ImageRepository = toImageRepositoryOutput(s.ImageRepository)
	}

	if s.CodeRepository != nil {
		out.CodeRepository = toCodeRepositoryOutput(s.CodeRepository)
	}

	return out
}

func toNetworkConfigurationOutput(n NetworkConfig) networkConfigurationOutput {
	isPublic := true
	if n.IsPubliclyAccessible != nil {
		isPublic = *n.IsPubliclyAccessible
	}

	return networkConfigurationOutput{
		EgressConfiguration: &egressConfigurationOutput{
			EgressType:      n.EgressType,
			VpcConnectorArn: n.EgressVpcConnectorArn,
		},
		IngressConfiguration: &ingressConfigurationOutput{IsPubliclyAccessible: isPublic},
		IPAddressType:        n.IPAddressType,
	}
}

func toHealthCheckConfigurationOutput(h HealthCheckConfig) *healthCheckConfigurationOutput {
	return &healthCheckConfigurationOutput{
		Protocol:           h.Protocol,
		Path:               h.Path,
		Interval:           h.Interval,
		Timeout:            h.Timeout,
		HealthyThreshold:   h.HealthyThreshold,
		UnhealthyThreshold: h.UnhealthyThreshold,
	}
}

func createServiceParamsFromInput(in *createServiceInput) CreateServiceParams {
	params := CreateServiceParams{
		Name:                        in.ServiceName,
		Instance:                    instanceConfigFromInput(in.InstanceConfiguration),
		Source:                      sourceConfigFromInput(in.SourceConfiguration),
		AutoScalingConfigurationArn: in.AutoScalingConfigurationArn,
		Network:                     networkConfigFromInput(in.NetworkConfiguration),
		HealthCheck:                 healthCheckConfigFromInput(in.HealthCheckConfiguration),
		Observability:               observabilityFromInput(in.ObservabilityConfiguration),
		Tags:                        tagsFromInput(in.Tags),
	}

	if in.EncryptionConfiguration != nil {
		params.EncryptionKmsKey = in.EncryptionConfiguration.KmsKey
	}

	return params
}

func (h *Handler) handleCreateService(
	_ context.Context,
	in *createServiceInput,
) (*createServiceOutput, error) {
	if in.ServiceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", errInvalidRequest)
	}

	svc, err := h.Backend.CreateService(createServiceParamsFromInput(in))
	if err != nil {
		return nil, err
	}

	return &createServiceOutput{
		Service:     h.toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type describeServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type describeServiceOutput struct {
	Service serviceOutput `json:"Service"`
}

func (h *Handler) handleDescribeService(
	_ context.Context,
	in *describeServiceInput,
) (*describeServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.DescribeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &describeServiceOutput{Service: h.toServiceOutput(svc)}, nil
}

type updateServiceInput struct {
	SourceConfiguration         *sourceConfigurationInput               `json:"SourceConfiguration,omitempty"`
	InstanceConfiguration       *instanceConfigurationInput             `json:"InstanceConfiguration,omitempty"`
	NetworkConfiguration        *networkConfigurationInput              `json:"NetworkConfiguration,omitempty"`
	HealthCheckConfiguration    *healthCheckConfigurationInput          `json:"HealthCheckConfiguration,omitempty"`
	ObservabilityConfiguration  *serviceObservabilityConfigurationInput `json:"ObservabilityConfiguration,omitempty"`
	ServiceArn                  string                                  `json:"ServiceArn"`
	AutoScalingConfigurationArn string                                  `json:"AutoScalingConfigurationArn,omitempty"`
}

type updateServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func updateServiceParamsFromInput(in *updateServiceInput) UpdateServiceParams {
	params := UpdateServiceParams{
		ServiceArn:                  in.ServiceArn,
		AutoScalingConfigurationArn: in.AutoScalingConfigurationArn,
		Network:                     networkConfigFromInput(in.NetworkConfiguration),
		HealthCheck:                 healthCheckConfigFromInput(in.HealthCheckConfiguration),
		Observability:               observabilityFromInput(in.ObservabilityConfiguration),
	}

	if in.InstanceConfiguration != nil {
		ic := instanceConfigFromInput(in.InstanceConfiguration)
		params.Instance = &ic
	}

	if in.SourceConfiguration != nil {
		sc := sourceConfigFromInput(in.SourceConfiguration)
		params.Source = &sc
	}

	return params
}

func (h *Handler) handleUpdateService(
	_ context.Context,
	in *updateServiceInput,
) (*updateServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.UpdateService(updateServiceParamsFromInput(in))
	if err != nil {
		return nil, err
	}

	return &updateServiceOutput{
		Service:     h.toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type deleteServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type deleteServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleDeleteService(
	_ context.Context,
	in *deleteServiceInput,
) (*deleteServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.DeleteService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &deleteServiceOutput{
		Service:     h.toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type listServicesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type serviceSummaryOutput struct {
	ServiceArn  string `json:"ServiceArn"`
	ServiceID   string `json:"ServiceId"`
	ServiceName string `json:"ServiceName"`
	ServiceURL  string `json:"ServiceUrl"`
	Status      string `json:"Status"`
	CreatedAt   int64  `json:"CreatedAt"`
	UpdatedAt   int64  `json:"UpdatedAt"`
}

type listServicesOutput struct {
	NextToken          string                 `json:"NextToken,omitempty"`
	ServiceSummaryList []serviceSummaryOutput `json:"ServiceSummaryList"`
}

func (h *Handler) handleListServices(
	_ context.Context,
	in *listServicesInput,
) (*listServicesOutput, error) {
	services, nextToken, err := h.Backend.ListServices(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]serviceSummaryOutput, 0, len(services))
	for _, s := range services {
		out = append(out, serviceSummaryOutput{
			ServiceArn:  s.ServiceArn,
			ServiceID:   s.ServiceID,
			ServiceName: s.ServiceName,
			ServiceURL:  s.ServiceURL,
			Status:      s.Status,
			CreatedAt:   s.CreatedAt.Unix(),
			UpdatedAt:   s.UpdatedAt.Unix(),
		})
	}

	return &listServicesOutput{ServiceSummaryList: out, NextToken: nextToken}, nil
}

type pauseServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type pauseServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handlePauseService(
	_ context.Context,
	in *pauseServiceInput,
) (*pauseServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.PauseService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &pauseServiceOutput{
		Service:     h.toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type resumeServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type resumeServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleResumeService(
	_ context.Context,
	in *resumeServiceInput,
) (*resumeServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.ResumeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &resumeServiceOutput{
		Service:     h.toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type startDeploymentInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type startDeploymentOutput struct {
	OperationID string `json:"OperationId"`
}

func (h *Handler) handleStartDeployment(
	_ context.Context,
	in *startDeploymentInput,
) (*startDeploymentOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	opID, err := h.Backend.StartDeployment(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &startDeploymentOutput{OperationID: opID}, nil
}
