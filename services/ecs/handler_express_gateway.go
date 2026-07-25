package ecs

import (
	"context"
	"strings"
)

// describeExpressGatewayIncludeTags is the AWS-defined `include` value that
// requests resource tags be returned alongside the Express service (see also
// describeClusterIncludeTags for the equivalent DescribeClusters option).
const describeExpressGatewayIncludeTags = "TAGS"

// ----- Shared Express service revision-configuration wire types -----
//
// These mirror the real SDK's types.ExpressGatewayServiceConfiguration and
// its nested shapes exactly, and are shared by CreateExpressGatewayService,
// UpdateExpressGatewayService, and DescribeExpressGatewayService (both for
// request-side configuration fields and response-side
// ActiveConfigurations/TargetConfiguration).

type expressGatewayNetworkConfigurationInput struct {
	SecurityGroups []string `json:"securityGroups,omitempty"`
	Subnets        []string `json:"subnets,omitempty"`
}

type expressGatewayAwsLogsConfigurationInput struct {
	LogGroup        string `json:"logGroup"`
	LogStreamPrefix string `json:"logStreamPrefix"`
}

type expressGatewayRepositoryCredentialsInput struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

type expressGatewayContainerInput struct {
	AwsLogsConfiguration  *expressGatewayAwsLogsConfigurationInput  `json:"awsLogsConfiguration,omitempty"`
	RepositoryCredentials *expressGatewayRepositoryCredentialsInput `json:"repositoryCredentials,omitempty"`
	ContainerPort         *int                                      `json:"containerPort,omitempty"`
	Image                 string                                    `json:"image"`
	Command               []string                                  `json:"command,omitempty"`
	Environment           []KeyValuePair                            `json:"environment,omitempty"`
	Secrets               []SecretReference                         `json:"secrets,omitempty"`
}

type expressGatewayScalingTargetInput struct {
	AutoScalingTargetValue *int   `json:"autoScalingTargetValue,omitempty"`
	MaxTaskCount           *int   `json:"maxTaskCount,omitempty"`
	MinTaskCount           *int   `json:"minTaskCount,omitempty"`
	AutoScalingMetric      string `json:"autoScalingMetric,omitempty"`
}

// toExpressGatewayNetworkConfiguration converts handler input to backend type.
func toExpressGatewayNetworkConfiguration(
	in *expressGatewayNetworkConfigurationInput,
) *ExpressGatewayServiceNetworkConfiguration {
	if in == nil {
		return nil
	}

	return &ExpressGatewayServiceNetworkConfiguration{
		SecurityGroups: in.SecurityGroups,
		Subnets:        in.Subnets,
	}
}

// toExpressGatewayContainer converts handler input to backend type.
func toExpressGatewayContainer(in *expressGatewayContainerInput) *ExpressGatewayContainer {
	if in == nil {
		return nil
	}

	out := &ExpressGatewayContainer{
		Image:         in.Image,
		Command:       in.Command,
		ContainerPort: in.ContainerPort,
		Environment:   in.Environment,
		Secrets:       in.Secrets,
	}

	if in.AwsLogsConfiguration != nil {
		out.AwsLogsConfiguration = &ExpressGatewayServiceAwsLogsConfiguration{
			LogGroup:        in.AwsLogsConfiguration.LogGroup,
			LogStreamPrefix: in.AwsLogsConfiguration.LogStreamPrefix,
		}
	}

	if in.RepositoryCredentials != nil {
		out.RepositoryCredentials = &ExpressGatewayRepositoryCredentials{
			CredentialsParameter: in.RepositoryCredentials.CredentialsParameter,
		}
	}

	return out
}

// toExpressGatewayScalingTarget converts handler input to backend type.
func toExpressGatewayScalingTarget(
	in *expressGatewayScalingTargetInput,
) *ExpressGatewayScalingTarget {
	if in == nil {
		return nil
	}

	return &ExpressGatewayScalingTarget{
		AutoScalingMetric:      in.AutoScalingMetric,
		AutoScalingTargetValue: in.AutoScalingTargetValue,
		MaxTaskCount:           in.MaxTaskCount,
		MinTaskCount:           in.MinTaskCount,
	}
}

type expressGatewayNetworkConfigurationView struct {
	SecurityGroups []string `json:"securityGroups,omitempty"`
	Subnets        []string `json:"subnets,omitempty"`
}

type expressGatewayAwsLogsConfigurationView struct {
	LogGroup        string `json:"logGroup"`
	LogStreamPrefix string `json:"logStreamPrefix"`
}

type expressGatewayRepositoryCredentialsView struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

type expressGatewayContainerView struct {
	AwsLogsConfiguration  *expressGatewayAwsLogsConfigurationView  `json:"awsLogsConfiguration,omitempty"`
	RepositoryCredentials *expressGatewayRepositoryCredentialsView `json:"repositoryCredentials,omitempty"`
	ContainerPort         *int                                     `json:"containerPort,omitempty"`
	Image                 string                                   `json:"image"`
	Command               []string                                 `json:"command,omitempty"`
	Environment           []KeyValuePair                           `json:"environment,omitempty"`
	Secrets               []SecretReference                        `json:"secrets,omitempty"`
}

type expressGatewayScalingTargetView struct {
	AutoScalingTargetValue *int   `json:"autoScalingTargetValue,omitempty"`
	MaxTaskCount           *int   `json:"maxTaskCount,omitempty"`
	MinTaskCount           *int   `json:"minTaskCount,omitempty"`
	AutoScalingMetric      string `json:"autoScalingMetric,omitempty"`
}

type ingressPathSummaryView struct {
	AccessType string `json:"accessType,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
}

type expressGatewayServiceConfigurationView struct {
	NetworkConfiguration *expressGatewayNetworkConfigurationView `json:"networkConfiguration,omitempty"`
	PrimaryContainer     *expressGatewayContainerView            `json:"primaryContainer,omitempty"`
	ScalingTarget        *expressGatewayScalingTargetView        `json:"scalingTarget,omitempty"`
	ServiceRevisionArn   string                                  `json:"serviceRevisionArn,omitempty"`
	CPU                  string                                  `json:"cpu,omitempty"`
	Memory               string                                  `json:"memory,omitempty"`
	HealthCheckPath      string                                  `json:"healthCheckPath,omitempty"`
	ExecutionRoleArn     string                                  `json:"executionRoleArn,omitempty"`
	TaskRoleArn          string                                  `json:"taskRoleArn,omitempty"`
	TaskDefinitionArn    string                                  `json:"taskDefinitionArn,omitempty"`
	IngressPaths         []ingressPathSummaryView                `json:"ingressPaths,omitempty"`
	CreatedAt            float64                                 `json:"createdAt"`
}

func toExpressGatewayServiceConfigurationView(
	cfg ExpressGatewayServiceConfiguration,
) expressGatewayServiceConfigurationView {
	v := expressGatewayServiceConfigurationView{
		CreatedAt:          float64Unix(cfg.CreatedAt),
		ServiceRevisionArn: cfg.ServiceRevisionArn,
		CPU:                cfg.CPU,
		Memory:             cfg.Memory,
		HealthCheckPath:    cfg.HealthCheckPath,
		ExecutionRoleArn:   cfg.ExecutionRoleArn,
		TaskRoleArn:        cfg.TaskRoleArn,
		TaskDefinitionArn:  cfg.TaskDefinitionArn,
	}

	if cfg.NetworkConfiguration != nil {
		v.NetworkConfiguration = &expressGatewayNetworkConfigurationView{
			SecurityGroups: cfg.NetworkConfiguration.SecurityGroups,
			Subnets:        cfg.NetworkConfiguration.Subnets,
		}
	}

	if cfg.PrimaryContainer != nil {
		v.PrimaryContainer = toExpressGatewayContainerView(cfg.PrimaryContainer)
	}

	if cfg.ScalingTarget != nil {
		v.ScalingTarget = &expressGatewayScalingTargetView{
			AutoScalingMetric:      cfg.ScalingTarget.AutoScalingMetric,
			AutoScalingTargetValue: cfg.ScalingTarget.AutoScalingTargetValue,
			MaxTaskCount:           cfg.ScalingTarget.MaxTaskCount,
			MinTaskCount:           cfg.ScalingTarget.MinTaskCount,
		}
	}

	for _, p := range cfg.IngressPaths {
		v.IngressPaths = append(v.IngressPaths, ingressPathSummaryView(p))
	}

	return v
}

func toExpressGatewayContainerView(c *ExpressGatewayContainer) *expressGatewayContainerView {
	v := &expressGatewayContainerView{
		Image:         c.Image,
		Command:       c.Command,
		ContainerPort: c.ContainerPort,
		Environment:   c.Environment,
		Secrets:       c.Secrets,
	}

	if c.AwsLogsConfiguration != nil {
		v.AwsLogsConfiguration = &expressGatewayAwsLogsConfigurationView{
			LogGroup:        c.AwsLogsConfiguration.LogGroup,
			LogStreamPrefix: c.AwsLogsConfiguration.LogStreamPrefix,
		}
	}

	if c.RepositoryCredentials != nil {
		v.RepositoryCredentials = &expressGatewayRepositoryCredentialsView{
			CredentialsParameter: c.RepositoryCredentials.CredentialsParameter,
		}
	}

	return v
}

// expressGatewayServiceStatusView mirrors types.ExpressGatewayServiceStatus:
// real AWS nests the status as {statusCode, statusReason} rather than a bare
// string.
type expressGatewayServiceStatusView struct {
	StatusCode   string `json:"statusCode,omitempty"`
	StatusReason string `json:"statusReason,omitempty"`
}

// ----- Handler: UpdateExpressGatewayService -----

type updateExpressGatewayServiceInput struct {
	NetworkConfiguration  *expressGatewayNetworkConfigurationInput `json:"networkConfiguration,omitempty"`
	PrimaryContainer      *expressGatewayContainerInput            `json:"primaryContainer,omitempty"`
	ScalingTarget         *expressGatewayScalingTargetInput        `json:"scalingTarget,omitempty"`
	ServiceArn            string                                   `json:"serviceArn"`
	CPU                   string                                   `json:"cpu,omitempty"`
	Memory                string                                   `json:"memory,omitempty"`
	HealthCheckPath       string                                   `json:"healthCheckPath,omitempty"`
	ExecutionRoleArn      string                                   `json:"executionRoleArn,omitempty"`
	InfrastructureRoleArn string                                   `json:"infrastructureRoleArn,omitempty"`
	TaskDefinitionArn     string                                   `json:"taskDefinitionArn,omitempty"`
	TaskRoleArn           string                                   `json:"taskRoleArn,omitempty"`
}

// updatedExpressGatewayServiceView mirrors types.UpdatedExpressGatewayService,
// which is a DIFFERENT (narrower) shape than ECSExpressGatewayService: no
// Tags, no ActiveConfigurations list -- instead a single TargetConfiguration
// describing the revision the update is rolling out to.
type updatedExpressGatewayServiceView struct {
	TargetConfiguration *expressGatewayServiceConfigurationView `json:"targetConfiguration,omitempty"`
	Status              *expressGatewayServiceStatusView        `json:"status,omitempty"`
	ServiceArn          string                                  `json:"serviceArn"`
	ServiceName         string                                  `json:"serviceName"`
	Cluster             string                                  `json:"cluster"`
	CreatedAt           float64                                 `json:"createdAt"`
	UpdatedAt           float64                                 `json:"updatedAt"`
}

func toUpdatedExpressGatewayServiceView(svc ExpressGatewayService) updatedExpressGatewayServiceView {
	v := updatedExpressGatewayServiceView{
		ServiceArn:  svc.ServiceArn,
		ServiceName: svc.ServiceName,
		Cluster:     svc.Cluster,
		CreatedAt:   float64Unix(svc.CreatedAt),
		UpdatedAt:   float64Unix(svc.UpdatedAt),
		Status:      &expressGatewayServiceStatusView{StatusCode: svc.Status, StatusReason: svc.StatusReason},
	}

	if len(svc.ActiveConfigurations) > 0 {
		cfg := toExpressGatewayServiceConfigurationView(svc.ActiveConfigurations[0])
		v.TargetConfiguration = &cfg
	}

	return v
}

type updateExpressGatewayServiceOutput struct {
	Service updatedExpressGatewayServiceView `json:"service"`
}

func (h *Handler) handleUpdateExpressGatewayService(
	_ context.Context,
	in *updateExpressGatewayServiceInput,
) (*updateExpressGatewayServiceOutput, error) {
	svc, err := h.Backend.UpdateExpressGatewayService(UpdateExpressGatewayServiceInput{
		ServiceArn:            in.ServiceArn,
		CPU:                   in.CPU,
		Memory:                in.Memory,
		HealthCheckPath:       in.HealthCheckPath,
		ExecutionRoleArn:      in.ExecutionRoleArn,
		InfrastructureRoleArn: in.InfrastructureRoleArn,
		TaskDefinitionArn:     in.TaskDefinitionArn,
		TaskRoleArn:           in.TaskRoleArn,
		NetworkConfiguration:  toExpressGatewayNetworkConfiguration(in.NetworkConfiguration),
		PrimaryContainer:      toExpressGatewayContainer(in.PrimaryContainer),
		ScalingTarget:         toExpressGatewayScalingTarget(in.ScalingTarget),
	})
	if err != nil {
		return nil, err
	}

	return &updateExpressGatewayServiceOutput{Service: toUpdatedExpressGatewayServiceView(*svc)}, nil
}

// ----- ExpressGatewayService view types -----

type expressGatewayServiceView struct {
	Status                *expressGatewayServiceStatusView         `json:"status,omitempty"`
	ServiceArn            string                                   `json:"serviceArn"`
	ServiceName           string                                   `json:"serviceName"`
	Cluster               string                                   `json:"cluster"`
	InfrastructureRoleArn string                                   `json:"infrastructureRoleArn"`
	CurrentDeployment     string                                   `json:"currentDeployment,omitempty"`
	Tags                  []Tag                                    `json:"tags,omitempty"`
	ActiveConfigurations  []expressGatewayServiceConfigurationView `json:"activeConfigurations,omitempty"`
	CreatedAt             float64                                  `json:"createdAt"`
	UpdatedAt             float64                                  `json:"updatedAt"`
}

func toExpressGatewayServiceView(svc ExpressGatewayService) expressGatewayServiceView {
	v := expressGatewayServiceView{
		ServiceArn:            svc.ServiceArn,
		ServiceName:           svc.ServiceName,
		Cluster:               svc.Cluster,
		InfrastructureRoleArn: svc.InfrastructureRoleArn,
		CurrentDeployment:     svc.CurrentDeployment,
		Tags:                  svc.Tags,
		CreatedAt:             float64Unix(svc.CreatedAt),
		UpdatedAt:             float64Unix(svc.UpdatedAt),
		Status:                &expressGatewayServiceStatusView{StatusCode: svc.Status, StatusReason: svc.StatusReason},
	}

	for _, cfg := range svc.ActiveConfigurations {
		v.ActiveConfigurations = append(v.ActiveConfigurations, toExpressGatewayServiceConfigurationView(cfg))
	}

	return v
}

// ----- Handler: CreateExpressGatewayService -----

type createExpressGatewayServiceInput struct {
	NetworkConfiguration  *expressGatewayNetworkConfigurationInput `json:"networkConfiguration,omitempty"`
	PrimaryContainer      *expressGatewayContainerInput            `json:"primaryContainer,omitempty"`
	ScalingTarget         *expressGatewayScalingTargetInput        `json:"scalingTarget,omitempty"`
	ExecutionRoleArn      string                                   `json:"executionRoleArn"`
	InfrastructureRoleArn string                                   `json:"infrastructureRoleArn"`
	Cluster               string                                   `json:"cluster,omitempty"`
	ServiceName           string                                   `json:"serviceName,omitempty"`
	CPU                   string                                   `json:"cpu,omitempty"`
	Memory                string                                   `json:"memory,omitempty"`
	HealthCheckPath       string                                   `json:"healthCheckPath,omitempty"`
	TaskDefinitionArn     string                                   `json:"taskDefinitionArn,omitempty"`
	TaskRoleArn           string                                   `json:"taskRoleArn,omitempty"`
	Tags                  []tagInput                               `json:"tags,omitempty"`
}

type createExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleCreateExpressGatewayService(
	_ context.Context,
	in *createExpressGatewayServiceInput,
) (*createExpressGatewayServiceOutput, error) {
	tags := make([]Tag, 0, len(in.Tags))
	for _, t := range in.Tags {
		tags = append(tags, Tag(t))
	}

	svc, err := h.Backend.CreateExpressGatewayService(CreateExpressGatewayServiceInput{
		ExecutionRoleArn:      in.ExecutionRoleArn,
		InfrastructureRoleArn: in.InfrastructureRoleArn,
		Cluster:               in.Cluster,
		ServiceName:           in.ServiceName,
		CPU:                   in.CPU,
		Memory:                in.Memory,
		HealthCheckPath:       in.HealthCheckPath,
		TaskDefinitionArn:     in.TaskDefinitionArn,
		TaskRoleArn:           in.TaskRoleArn,
		NetworkConfiguration:  toExpressGatewayNetworkConfiguration(in.NetworkConfiguration),
		PrimaryContainer:      toExpressGatewayContainer(in.PrimaryContainer),
		ScalingTarget:         toExpressGatewayScalingTarget(in.ScalingTarget),
		Tags:                  tags,
	})
	if err != nil {
		return nil, err
	}

	return &createExpressGatewayServiceOutput{Service: toExpressGatewayServiceView(*svc)}, nil
}

// ----- Handler: DeleteExpressGatewayService -----

type deleteExpressGatewayServiceInput struct {
	ServiceArn string `json:"serviceArn"`
}

type deleteExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleDeleteExpressGatewayService(
	_ context.Context,
	in *deleteExpressGatewayServiceInput,
) (*deleteExpressGatewayServiceOutput, error) {
	svc, err := h.Backend.DeleteExpressGatewayService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &deleteExpressGatewayServiceOutput{Service: toExpressGatewayServiceView(*svc)}, nil
}

// ----- Handler: DescribeExpressGatewayService -----

type describeExpressGatewayServiceInput struct {
	ServiceArn string   `json:"serviceArn"`
	Include    []string `json:"include,omitempty"`
}

type describeExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleDescribeExpressGatewayService(
	_ context.Context,
	in *describeExpressGatewayServiceInput,
) (*describeExpressGatewayServiceOutput, error) {
	svc, err := h.Backend.DescribeExpressGatewayService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	view := toExpressGatewayServiceView(*svc)

	wantTags := false

	for _, opt := range in.Include {
		if strings.EqualFold(opt, describeExpressGatewayIncludeTags) {
			wantTags = true

			break
		}
	}

	if !wantTags {
		view.Tags = nil
	}

	return &describeExpressGatewayServiceOutput{Service: view}, nil
}
