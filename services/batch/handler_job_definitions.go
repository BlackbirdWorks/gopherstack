package batch

import "context"

type jobDefinitionTimeout struct {
	AttemptDurationSeconds int32 `json:"attemptDurationSeconds,omitempty"`
}

type resourceRequirementInput struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type keyValuePairInput struct {
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

type mountPointInput struct {
	ContainerPath string `json:"containerPath,omitempty"`
	SourceVolume  string `json:"sourceVolume,omitempty"`
	ReadOnly      bool   `json:"readOnly,omitempty"`
}

type hostVolumeInput struct {
	SourcePath string `json:"sourcePath,omitempty"`
}

type volumeInput struct {
	Host *hostVolumeInput `json:"host,omitempty"`
	Name string           `json:"name"`
}

type ulimitInput struct {
	Name      string `json:"name"`
	SoftLimit int32  `json:"softLimit"`
	HardLimit int32  `json:"hardLimit"`
}

type logConfigurationInput struct {
	Options   map[string]string `json:"options,omitempty"`
	LogDriver string            `json:"logDriver"`
}

type networkConfigurationInput struct {
	AssignPublicIP string `json:"assignPublicIp,omitempty"`
}

type fargatePlatformConfigInput struct {
	PlatformVersion string `json:"platformVersion,omitempty"`
}

type ephemeralStorageInput struct {
	SizeInGiB int32 `json:"sizeInGiB"`
}

type runtimePlatformInput struct {
	OperatingSystemFamily string `json:"operatingSystemFamily,omitempty"`
	CPUArchitecture       string `json:"cpuArchitecture,omitempty"`
}

type repositoryCredentialsInput struct {
	CredentialsParameter string `json:"credentialsParameter"`
}

type secretInput struct {
	Name      string `json:"name"`
	ValueFrom string `json:"valueFrom"`
}

type deviceInput struct {
	HostPath      string   `json:"hostPath"`
	ContainerPath string   `json:"containerPath,omitempty"`
	Permissions   []string `json:"permissions,omitempty"`
}

type tmpfsInput struct {
	ContainerPath string   `json:"containerPath"`
	MountOptions  []string `json:"mountOptions,omitempty"`
	Size          int32    `json:"size"`
}

type linuxParametersInput struct {
	Devices            []deviceInput `json:"devices,omitempty"`
	Tmpfs              []tmpfsInput  `json:"tmpfs,omitempty"`
	InitProcessEnabled bool          `json:"initProcessEnabled,omitempty"`
	SharedMemorySize   int32         `json:"sharedMemorySize,omitempty"`
	MaxSwap            int32         `json:"maxSwap,omitempty"`
	Swappiness         int32         `json:"swappiness,omitempty"`
}

type containerPropertiesInput struct {
	LinuxParameters              *linuxParametersInput       `json:"linuxParameters,omitempty"`
	RepositoryCredentials        *repositoryCredentialsInput `json:"repositoryCredentials,omitempty"`
	RuntimePlatform              *runtimePlatformInput       `json:"runtimePlatform,omitempty"`
	EphemeralStorage             *ephemeralStorageInput      `json:"ephemeralStorage,omitempty"`
	FargatePlatformConfiguration *fargatePlatformConfigInput `json:"fargatePlatformConfiguration,omitempty"`
	NetworkConfiguration         *networkConfigurationInput  `json:"networkConfiguration,omitempty"`
	LogConfiguration             *logConfigurationInput      `json:"logConfiguration,omitempty"`
	JobRoleArn                   string                      `json:"jobRoleArn,omitempty"`
	ExecutionRoleArn             string                      `json:"executionRoleArn,omitempty"`
	User                         string                      `json:"user,omitempty"`
	InstanceType                 string                      `json:"instanceType,omitempty"`
	Image                        string                      `json:"image,omitempty"`
	Command                      []string                    `json:"command,omitempty"`
	Secrets                      []secretInput               `json:"secrets,omitempty"`
	ResourceRequirements         []resourceRequirementInput  `json:"resourceRequirements,omitempty"`
	Ulimits                      []ulimitInput               `json:"ulimits,omitempty"`
	MountPoints                  []mountPointInput           `json:"mountPoints,omitempty"`
	Volumes                      []volumeInput               `json:"volumes,omitempty"`
	Environment                  []keyValuePairInput         `json:"environment,omitempty"`
	Vcpus                        int32                       `json:"vcpus,omitempty"`
	Memory                       int32                       `json:"memory,omitempty"`
	ReadonlyRootFilesystem       bool                        `json:"readonlyRootFilesystem,omitempty"`
	Privileged                   bool                        `json:"privileged,omitempty"`
}

type consumableResourcePropertyInput struct {
	ConsumableResource string  `json:"consumableResource"`
	Quantity           float64 `json:"quantity"`
}

// consumableResourcePropertiesInput mirrors aws-sdk-go-v2/service/batch/
// types.ConsumableResourceProperties: the requirement list is nested under
// "consumableResourceList", not serialized as a bare array.
type consumableResourcePropertiesInput struct {
	ConsumableResourceList []consumableResourcePropertyInput `json:"consumableResourceList,omitempty"`
}

type nodeRangePropertyInput struct {
	ContainerProperties *containerPropertiesInput `json:"containerProperties,omitempty"`
	TargetNodes         string                    `json:"targetNodes"`
}

type nodePropertiesInput struct {
	NodeRangeProperties []nodeRangePropertyInput `json:"nodeRangeProperties"`
	NumNodes            int32                    `json:"numNodes"`
	MainNode            int32                    `json:"mainNode"`
}

type registerJobDefinitionInput struct {
	Tags                         map[string]string                  `json:"tags"`
	Parameters                   map[string]string                  `json:"parameters,omitempty"`
	Timeout                      *jobDefinitionTimeout              `json:"timeout,omitempty"`
	ContainerProperties          *containerPropertiesInput          `json:"containerProperties,omitempty"`
	NodeProperties               *nodePropertiesInput               `json:"nodeProperties,omitempty"`
	RuntimePlatform              *runtimePlatformInput              `json:"runtimePlatform,omitempty"`
	ConsumableResourceProperties *consumableResourcePropertiesInput `json:"consumableResourceProperties,omitempty"`
	JobDefinitionName            string                             `json:"jobDefinitionName"`
	Type                         string                             `json:"type"`
	PlatformCapabilities         []string                           `json:"platformCapabilities,omitempty"`
	SchedulingPriority           int32                              `json:"schedulingPriority,omitempty"`
	PropagateTags                bool                               `json:"propagateTags,omitempty"`
}

// registerJobDefinitionOutput mirrors aws-sdk-go-v2/service/batch's
// RegisterJobDefinitionOutput exactly: only these three fields are returned.
type registerJobDefinitionOutput struct {
	JobDefinitionArn  string `json:"jobDefinitionArn"`
	JobDefinitionName string `json:"jobDefinitionName"`
	Revision          int32  `json:"revision"`
}

// linuxParametersFromInput converts the wire-shaped linuxParametersInput,
// including its Devices and Tmpfs lists, to a *LinuxParameters.
func linuxParametersFromInput(in *linuxParametersInput) *LinuxParameters {
	if in == nil {
		return nil
	}

	lp := &LinuxParameters{
		InitProcessEnabled: in.InitProcessEnabled,
		SharedMemorySize:   in.SharedMemorySize,
		MaxSwap:            in.MaxSwap,
		Swappiness:         in.Swappiness,
	}

	for _, d := range in.Devices {
		lp.Devices = append(
			lp.Devices,
			Device(d),
		)
	}

	for _, t := range in.Tmpfs {
		lp.Tmpfs = append(
			lp.Tmpfs,
			Tmpfs(t),
		)
	}

	return lp
}

func containerPropertiesFromInput(in *containerPropertiesInput) *ContainerProperties {
	if in == nil {
		return nil
	}

	cp := &ContainerProperties{
		Image:                  in.Image,
		JobRoleArn:             in.JobRoleArn,
		ExecutionRoleArn:       in.ExecutionRoleArn,
		User:                   in.User,
		InstanceType:           in.InstanceType,
		Command:                in.Command,
		Vcpus:                  in.Vcpus,
		Memory:                 in.Memory,
		ReadonlyRootFilesystem: in.ReadonlyRootFilesystem,
		Privileged:             in.Privileged,
	}

	for _, e := range in.Environment {
		cp.Environment = append(cp.Environment, KeyValuePair(e))
	}

	for _, v := range in.Volumes {
		vol := Volume{Name: v.Name}
		if v.Host != nil {
			vol.Host = &HostVolume{SourcePath: v.Host.SourcePath}
		}

		cp.Volumes = append(cp.Volumes, vol)
	}

	for _, m := range in.MountPoints {
		cp.MountPoints = append(cp.MountPoints, MountPoint(m))
	}

	for _, u := range in.Ulimits {
		cp.Ulimits = append(cp.Ulimits, Ulimit(u))
	}

	for _, rr := range in.ResourceRequirements {
		cp.ResourceRequirements = append(cp.ResourceRequirements, ResourceRequirement(rr))
	}

	for _, s := range in.Secrets {
		cp.Secrets = append(cp.Secrets, Secret(s))
	}

	cp.LinuxParameters = linuxParametersFromInput(in.LinuxParameters)

	if in.LogConfiguration != nil {
		cp.LogConfiguration = &LogConfiguration{
			LogDriver: in.LogConfiguration.LogDriver,
			Options:   in.LogConfiguration.Options,
		}
	}

	if in.NetworkConfiguration != nil {
		cp.NetworkConfiguration = &NetworkConfiguration{AssignPublicIP: in.NetworkConfiguration.AssignPublicIP}
	}

	if in.FargatePlatformConfiguration != nil {
		cp.FargatePlatformConfiguration = &FargatePlatformConfiguration{
			PlatformVersion: in.FargatePlatformConfiguration.PlatformVersion,
		}
	}

	if in.EphemeralStorage != nil {
		cp.EphemeralStorage = &EphemeralStorage{SizeInGiB: in.EphemeralStorage.SizeInGiB}
	}

	if in.RuntimePlatform != nil {
		cp.RuntimePlatform = &RuntimePlatform{
			OperatingSystemFamily: in.RuntimePlatform.OperatingSystemFamily,
			CPUArchitecture:       in.RuntimePlatform.CPUArchitecture,
		}
	}

	if in.RepositoryCredentials != nil {
		cp.RepositoryCredentials = &RepositoryCredentials{
			CredentialsParameter: in.RepositoryCredentials.CredentialsParameter,
		}
	}

	return cp
}

func nodePropertiesFromInput(in *nodePropertiesInput) *NodeProperties {
	if in == nil {
		return nil
	}

	np := &NodeProperties{
		NumNodes: in.NumNodes,
		MainNode: in.MainNode,
	}

	for _, nrp := range in.NodeRangeProperties {
		np.NodeRangeProperties = append(np.NodeRangeProperties, NodeRangeProperty{
			TargetNodes:         nrp.TargetNodes,
			ContainerProperties: containerPropertiesFromInput(nrp.ContainerProperties),
		})
	}

	return np
}

func runtimePlatformFromInput(in *runtimePlatformInput) *RuntimePlatform {
	if in == nil {
		return nil
	}

	return &RuntimePlatform{
		OperatingSystemFamily: in.OperatingSystemFamily,
		CPUArchitecture:       in.CPUArchitecture,
	}
}

func consumableResourcePropertiesFromInput(in *consumableResourcePropertiesInput) []ConsumableResourceProperty {
	if in == nil || len(in.ConsumableResourceList) == 0 {
		return nil
	}

	out := make([]ConsumableResourceProperty, len(in.ConsumableResourceList))
	for i, c := range in.ConsumableResourceList {
		out[i] = ConsumableResourceProperty(c)
	}

	return out
}

func (h *Handler) handleRegisterJobDefinition(
	ctx context.Context,
	in *registerJobDefinitionInput,
) (*registerJobDefinitionOutput, error) {
	var timeoutSeconds int32
	if in.Timeout != nil {
		timeoutSeconds = in.Timeout.AttemptDurationSeconds
	}

	jd, err := h.Backend.RegisterJobDefinition(
		ctx,
		in.JobDefinitionName,
		in.Type,
		in.Tags,
		in.PlatformCapabilities,
		timeoutSeconds,
		in.SchedulingPriority,
		containerPropertiesFromInput(in.ContainerProperties),
		nodePropertiesFromInput(in.NodeProperties),
		nil, // EksProperties not yet wired through handler input
		runtimePlatformFromInput(in.RuntimePlatform),
		consumableResourcePropertiesFromInput(in.ConsumableResourceProperties),
		in.Parameters,
		in.PropagateTags,
	)
	if err != nil {
		return nil, err
	}

	return &registerJobDefinitionOutput{
		JobDefinitionArn:  jd.JobDefinitionArn,
		JobDefinitionName: jd.JobDefinitionName,
		Revision:          jd.Revision,
	}, nil
}

type describeJobDefinitionsInput struct {
	MaxResults        *int32   `json:"maxResults,omitempty"`
	NextToken         *string  `json:"nextToken,omitempty"`
	JobDefinitionName string   `json:"jobDefinitionName,omitempty"`
	Status            string   `json:"status,omitempty"`
	JobDefinitions    []string `json:"jobDefinitions"`
}

type describeJobDefinitionsOutput struct {
	NextToken      *string          `json:"nextToken,omitempty"`
	JobDefinitions []*JobDefinition `json:"jobDefinitions"`
}

func (h *Handler) handleDescribeJobDefinitions(
	ctx context.Context,
	in *describeJobDefinitionsInput,
) (*describeJobDefinitionsOutput, error) {
	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	jds, outToken := h.Backend.DescribeJobDefinitions(
		ctx,
		in.JobDefinitions,
		in.Status,
		in.JobDefinitionName,
		maxResults,
		nextToken,
	)

	out := &describeJobDefinitionsOutput{JobDefinitions: jds}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type deregisterJobDefinitionInput struct {
	JobDefinition string `json:"jobDefinition"`
}

func (h *Handler) handleDeregisterJobDefinition(
	ctx context.Context,
	in *deregisterJobDefinitionInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeregisterJobDefinition(ctx, in.JobDefinition); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
