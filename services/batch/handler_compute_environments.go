package batch

import "context"

// --- Input / Output types ---

type launchTemplateOverrideInput struct {
	LaunchTemplateName  string   `json:"launchTemplateName,omitempty"`
	LaunchTemplateID    string   `json:"launchTemplateId,omitempty"`
	Version             string   `json:"version,omitempty"`
	TargetInstanceTypes []string `json:"targetInstanceTypes,omitempty"`
}

type launchTemplateInput struct {
	LaunchTemplateName string                        `json:"launchTemplateName,omitempty"`
	LaunchTemplateID   string                        `json:"launchTemplateId,omitempty"`
	Version            string                        `json:"version,omitempty"`
	Overrides          []launchTemplateOverrideInput `json:"overrides,omitempty"`
}

type ec2ConfigurationInput struct {
	ImageType              string `json:"imageType"`
	ImageIDOverride        string `json:"imageIdOverride,omitempty"`
	ImageKubernetesVersion string `json:"imageKubernetesVersion,omitempty"`
}

type computeResourcesInput struct {
	Type               string                  `json:"type,omitempty"`
	AllocationStrategy string                  `json:"allocationStrategy,omitempty"`
	InstanceRole       string                  `json:"instanceRole,omitempty"`
	Ec2KeyPair         string                  `json:"ec2KeyPair,omitempty"`
	ImageID            string                  `json:"imageId,omitempty"`
	PlacementGroup     string                  `json:"placementGroup,omitempty"`
	SpotIamFleetRole   string                  `json:"spotIamFleetRole,omitempty"`
	InstanceTypes      []string                `json:"instanceTypes,omitempty"`
	Subnets            []string                `json:"subnets,omitempty"`
	SecurityGroupIDs   []string                `json:"securityGroupIds,omitempty"`
	Tags               map[string]string       `json:"tags,omitempty"`
	LaunchTemplate     *launchTemplateInput    `json:"launchTemplate,omitempty"`
	Ec2Configuration   []ec2ConfigurationInput `json:"ec2Configuration,omitempty"`
	MinvCpus           int32                   `json:"minvCpus,omitempty"`
	MaxvCpus           int32                   `json:"maxvCpus,omitempty"`
	DesiredvCpus       int32                   `json:"desiredvCpus,omitempty"`
	BidPercentage      int32                   `json:"bidPercentage,omitempty"`
}

type eksConfigurationInput struct {
	EksClusterArn       string `json:"eksClusterArn"`
	KubernetesNamespace string `json:"kubernetesNamespace"`
}

type updatePolicyInput struct {
	TerminateJobsOnUpdate      bool  `json:"terminateJobsOnUpdate,omitempty"`
	JobExecutionTimeoutMinutes int64 `json:"jobExecutionTimeoutMinutes,omitempty"`
}

type createComputeEnvironmentInput struct {
	Tags                   map[string]string      `json:"tags"`
	ComputeResources       *computeResourcesInput `json:"computeResources,omitempty"`
	EksConfiguration       *eksConfigurationInput `json:"eksConfiguration,omitempty"`
	UpdatePolicy           *updatePolicyInput     `json:"updatePolicy,omitempty"`
	ComputeEnvironmentName string                 `json:"computeEnvironmentName"`
	Type                   string                 `json:"type"`
	State                  string                 `json:"state"`
	ServiceRole            string                 `json:"serviceRole,omitempty"`
}

type createComputeEnvironmentOutput struct {
	ComputeEnvironmentArn  string `json:"computeEnvironmentArn"`
	ComputeEnvironmentName string `json:"computeEnvironmentName"`
}

func computeResourcesFromInput(in *computeResourcesInput) *ComputeResources {
	if in == nil {
		return nil
	}

	cr := &ComputeResources{
		Type:               in.Type,
		AllocationStrategy: in.AllocationStrategy,
		InstanceRole:       in.InstanceRole,
		Ec2KeyPair:         in.Ec2KeyPair,
		ImageID:            in.ImageID,
		PlacementGroup:     in.PlacementGroup,
		SpotIamFleetRole:   in.SpotIamFleetRole,
		InstanceTypes:      in.InstanceTypes,
		Subnets:            in.Subnets,
		SecurityGroupIDs:   in.SecurityGroupIDs,
		Tags:               in.Tags,
		MinvCpus:           in.MinvCpus,
		MaxvCpus:           in.MaxvCpus,
		DesiredvCpus:       in.DesiredvCpus,
		BidPercentage:      in.BidPercentage,
	}

	if in.LaunchTemplate != nil {
		lt := &LaunchTemplate{
			LaunchTemplateName: in.LaunchTemplate.LaunchTemplateName,
			LaunchTemplateID:   in.LaunchTemplate.LaunchTemplateID,
			Version:            in.LaunchTemplate.Version,
		}

		for _, o := range in.LaunchTemplate.Overrides {
			lt.Overrides = append(lt.Overrides, LaunchTemplateOverride(o))
		}

		cr.LaunchTemplate = lt
	}

	for _, ec2 := range in.Ec2Configuration {
		cr.Ec2Configuration = append(cr.Ec2Configuration, Ec2Configuration(ec2))
	}

	return cr
}

func eksConfigFromInput(in *eksConfigurationInput) *EksConfiguration {
	if in == nil {
		return nil
	}

	return &EksConfiguration{
		EksClusterArn:       in.EksClusterArn,
		KubernetesNamespace: in.KubernetesNamespace,
	}
}

func updatePolicyFromInput(in *updatePolicyInput) *UpdatePolicy {
	if in == nil {
		return nil
	}

	return &UpdatePolicy{
		TerminateJobsOnUpdate:      in.TerminateJobsOnUpdate,
		JobExecutionTimeoutMinutes: in.JobExecutionTimeoutMinutes,
	}
}

func (h *Handler) handleCreateComputeEnvironment(
	ctx context.Context,
	in *createComputeEnvironmentInput,
) (*createComputeEnvironmentOutput, error) {
	state := in.State
	if state == "" {
		state = stateEnabled
	}

	ce, err := h.Backend.CreateComputeEnvironment(
		ctx,
		in.ComputeEnvironmentName, in.Type, state, in.Tags, in.ServiceRole,
		computeResourcesFromInput(in.ComputeResources),
		eksConfigFromInput(in.EksConfiguration),
		updatePolicyFromInput(in.UpdatePolicy),
	)
	if err != nil {
		return nil, err
	}

	return &createComputeEnvironmentOutput{
		ComputeEnvironmentArn:  ce.ComputeEnvironmentArn,
		ComputeEnvironmentName: ce.ComputeEnvironmentName,
	}, nil
}

type describeComputeEnvironmentsInput struct {
	MaxResults          *int32   `json:"maxResults,omitempty"`
	NextToken           *string  `json:"nextToken,omitempty"`
	ComputeEnvironments []string `json:"computeEnvironments"`
}

type describeComputeEnvironmentsOutput struct {
	NextToken           *string               `json:"nextToken,omitempty"`
	ComputeEnvironments []*ComputeEnvironment `json:"computeEnvironments"`
}

func (h *Handler) handleDescribeComputeEnvironments(
	ctx context.Context,
	in *describeComputeEnvironmentsInput,
) (*describeComputeEnvironmentsOutput, error) {
	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	ces, outToken := h.Backend.DescribeComputeEnvironments(ctx, in.ComputeEnvironments, maxResults, nextToken)
	out := &describeComputeEnvironmentsOutput{ComputeEnvironments: ces}

	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type updateComputeEnvironmentInput struct {
	ComputeResources   *computeResourcesInput `json:"computeResources,omitempty"`
	UpdatePolicy       *updatePolicyInput     `json:"updatePolicy,omitempty"`
	ComputeEnvironment string                 `json:"computeEnvironment"`
	State              string                 `json:"state"`
	ServiceRole        string                 `json:"serviceRole,omitempty"`
}

type updateComputeEnvironmentOutput struct {
	ComputeEnvironmentArn  string `json:"computeEnvironmentArn"`
	ComputeEnvironmentName string `json:"computeEnvironmentName"`
}

func (h *Handler) handleUpdateComputeEnvironment(
	ctx context.Context,
	in *updateComputeEnvironmentInput,
) (*updateComputeEnvironmentOutput, error) {
	ce, err := h.Backend.UpdateComputeEnvironment(
		ctx,
		in.ComputeEnvironment, in.State, in.ServiceRole,
		computeResourcesFromInput(in.ComputeResources),
		updatePolicyFromInput(in.UpdatePolicy),
	)
	if err != nil {
		return nil, err
	}

	return &updateComputeEnvironmentOutput{
		ComputeEnvironmentArn:  ce.ComputeEnvironmentArn,
		ComputeEnvironmentName: ce.ComputeEnvironmentName,
	}, nil
}

type deleteComputeEnvironmentInput struct {
	ComputeEnvironment string `json:"computeEnvironment"`
}

func (h *Handler) handleDeleteComputeEnvironment(
	ctx context.Context,
	in *deleteComputeEnvironmentInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteComputeEnvironment(ctx, in.ComputeEnvironment); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
