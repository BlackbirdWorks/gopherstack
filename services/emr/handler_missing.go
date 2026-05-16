package emr

import (
	"context"
	"time"
)

// --- DescribeJobFlows ---

type describeJobFlowsInput struct {
	CreatedAfter  *float64 `json:"CreatedAfter"`
	CreatedBefore *float64 `json:"CreatedBefore"`
	JobFlowIDs    []string `json:"JobFlowIds"`
	JobFlowStates []string `json:"JobFlowStates"`
}

type describeJobFlowsOutput struct {
	JobFlows []JobFlow `json:"JobFlows"`
}

func (h *Handler) handleDescribeJobFlows(
	_ context.Context, in *describeJobFlowsInput,
) (*describeJobFlowsOutput, error) {
	var createdAfter, createdBefore *time.Time

	if in.CreatedAfter != nil {
		t := time.UnixMilli(int64(*in.CreatedAfter))
		createdAfter = &t
	}

	if in.CreatedBefore != nil {
		t := time.UnixMilli(int64(*in.CreatedBefore))
		createdBefore = &t
	}

	flows := h.Backend.DescribeJobFlows(in.JobFlowIDs, in.JobFlowStates, createdAfter, createdBefore)

	return &describeJobFlowsOutput{JobFlows: flows}, nil
}

// --- DescribeNotebookExecution ---

type describeNotebookExecutionInput struct {
	NotebookExecutionID string `json:"NotebookExecutionId"`
}

type describeNotebookExecutionOutput struct {
	NotebookExecution *NotebookExecution `json:"NotebookExecution"`
}

func (h *Handler) handleDescribeNotebookExecution(
	_ context.Context,
	in *describeNotebookExecutionInput,
) (*describeNotebookExecutionOutput, error) {
	ne, err := h.Backend.DescribeNotebookExecution(in.NotebookExecutionID)
	if err != nil {
		return nil, err
	}

	return &describeNotebookExecutionOutput{NotebookExecution: ne}, nil
}

// --- DescribePersistentAppUI ---

type describePersistentAppUIInput struct {
	PersistentAppUIId string `json:"PersistentAppUIId"`
}

type describePersistentAppUIOutput struct {
	PersistentAppUI *PersistentAppUI `json:"PersistentAppUI"`
}

func (h *Handler) handleDescribePersistentAppUI(
	_ context.Context,
	in *describePersistentAppUIInput,
) (*describePersistentAppUIOutput, error) {
	ui, err := h.Backend.DescribePersistentAppUI(in.PersistentAppUIId)
	if err != nil {
		return nil, err
	}

	return &describePersistentAppUIOutput{PersistentAppUI: ui}, nil
}

// --- DescribeReleaseLabel ---

type describeReleaseLabelInput struct {
	ReleaseLabel string `json:"ReleaseLabel"`
}

type describeReleaseLabelOutput struct {
	ReleaseLabel string                    `json:"ReleaseLabel"`
	Applications []ReleaseLabelApplication `json:"Applications,omitempty"`
}

func (h *Handler) handleDescribeReleaseLabel(
	_ context.Context,
	in *describeReleaseLabelInput,
) (*describeReleaseLabelOutput, error) {
	rl, err := h.Backend.DescribeReleaseLabel(in.ReleaseLabel)
	if err != nil {
		return nil, err
	}

	return &describeReleaseLabelOutput{
		ReleaseLabel: rl.ReleaseLabel,
		Applications: rl.Applications,
	}, nil
}

// --- DescribeStep ---

type describeStepInput struct {
	ClusterID string `json:"ClusterId"`
	StepID    string `json:"StepId"`
}

type describeStepOutput struct {
	Step *Step `json:"Step"`
}

func (h *Handler) handleDescribeStep(_ context.Context, in *describeStepInput) (*describeStepOutput, error) {
	step, err := h.Backend.DescribeStep(in.ClusterID, in.StepID)
	if err != nil {
		return nil, err
	}

	return &describeStepOutput{Step: step}, nil
}

// --- DescribeStudio ---

type describeStudioInput struct {
	StudioID string `json:"StudioId"`
}

type describeStudioOutput struct {
	Studio *Studio `json:"Studio"`
}

func (h *Handler) handleDescribeStudio(_ context.Context, in *describeStudioInput) (*describeStudioOutput, error) {
	studio, err := h.Backend.DescribeStudio(in.StudioID)
	if err != nil {
		return nil, err
	}

	return &describeStudioOutput{Studio: studio}, nil
}

// --- GetBlockPublicAccessConfiguration ---

type getBlockPublicAccessConfigurationInput struct{}

type getBlockPublicAccessConfigurationOutput struct {
	BlockPublicAccessConfigurationMetadata blockPublicAccessConfigurationMetadata `json:"BlockPublicAccessConfigurationMetadata"` //nolint:lll // AWS JSON key is unavoidably long
	BlockPublicAccessConfiguration         BlockPublicAccessConfiguration         `json:"BlockPublicAccessConfiguration"`
}

type blockPublicAccessConfigurationMetadata struct {
	CreationDateTime string `json:"CreationDateTime"`
	CreatedByArn     string `json:"CreatedByArn,omitempty"`
}

func (h *Handler) handleGetBlockPublicAccessConfiguration(
	_ context.Context,
	_ *getBlockPublicAccessConfigurationInput,
) (*getBlockPublicAccessConfigurationOutput, error) {
	cfg, meta := h.Backend.GetBlockPublicAccessConfiguration()

	return &getBlockPublicAccessConfigurationOutput{
		BlockPublicAccessConfiguration: cfg,
		BlockPublicAccessConfigurationMetadata: blockPublicAccessConfigurationMetadata{
			CreationDateTime: meta.CreationDateTime.UTC().Format("2006-01-02T15:04:05Z"),
			CreatedByArn:     meta.CreatedByArn,
		},
	}, nil
}

// --- GetClusterSessionCredentials ---

type getClusterSessionCredentialsInput struct {
	ClusterID        string `json:"ClusterId"`
	ExecutionRoleArn string `json:"ExecutionRoleArn"`
}

type getClusterSessionCredentialsOutput struct {
	Credentials map[string]any `json:"Credentials"`
	ExpiresAt   string         `json:"ExpiresAt"`
}

func (h *Handler) handleGetClusterSessionCredentials(
	_ context.Context,
	in *getClusterSessionCredentialsInput,
) (*getClusterSessionCredentialsOutput, error) {
	creds, expiry, err := h.Backend.GetClusterSessionCredentials(in.ClusterID, in.ExecutionRoleArn)
	if err != nil {
		return nil, err
	}

	return &getClusterSessionCredentialsOutput{
		Credentials: creds,
		ExpiresAt:   expiry.UTC().Format("2006-01-02T15:04:05Z"),
	}, nil
}

// --- GetOnClusterAppUIPresignedURL ---

type getOnClusterAppUIPresignedURLInput struct {
	ClusterID string `json:"ClusterId"`
}

type getOnClusterAppUIPresignedURLOutput struct {
	URL string `json:"URL,omitempty"`
}

func (h *Handler) handleGetOnClusterAppUIPresignedURL(
	_ context.Context,
	in *getOnClusterAppUIPresignedURLInput,
) (*getOnClusterAppUIPresignedURLOutput, error) {
	url := h.Backend.GetPresignedURL(in.ClusterID, h.Backend.region)

	return &getOnClusterAppUIPresignedURLOutput{URL: url}, nil
}

// --- GetPersistentAppUIPresignedURL ---

type getPersistentAppUIPresignedURLInput struct {
	PersistentAppUIId string `json:"PersistentAppUIId"`
}

type getPersistentAppUIPresignedURLOutput struct {
	PresignedURL string `json:"PresignedURL,omitempty"`
}

func (h *Handler) handleGetPersistentAppUIPresignedURL(
	_ context.Context,
	in *getPersistentAppUIPresignedURLInput,
) (*getPersistentAppUIPresignedURLOutput, error) {
	url := h.Backend.GetPresignedURL(in.PersistentAppUIId, h.Backend.region)

	return &getPersistentAppUIPresignedURLOutput{PresignedURL: url}, nil
}

// --- GetStudioSessionMapping ---

type getStudioSessionMappingInput struct {
	StudioID     string `json:"StudioId"`
	IdentityType string `json:"IdentityType"`
	IdentityID   string `json:"IdentityId,omitempty"`
	IdentityName string `json:"IdentityName,omitempty"`
}

type getStudioSessionMappingOutput struct {
	SessionMapping *StudioSessionMapping `json:"SessionMapping"`
}

func (h *Handler) handleGetStudioSessionMapping(
	_ context.Context,
	in *getStudioSessionMappingInput,
) (*getStudioSessionMappingOutput, error) {
	mapping, err := h.Backend.GetStudioSessionMapping(in.StudioID, in.IdentityType, in.IdentityID, in.IdentityName)
	if err != nil {
		return nil, err
	}

	return &getStudioSessionMappingOutput{SessionMapping: mapping}, nil
}

// --- ListInstances ---

type listInstancesInput struct {
	ClusterID          string   `json:"ClusterId"`
	InstanceGroupID    string   `json:"InstanceGroupId"`
	InstanceFleetID    string   `json:"InstanceFleetId"`
	Marker             string   `json:"Marker"`
	InstanceGroupTypes []string `json:"InstanceGroupTypes"`
	InstanceStates     []string `json:"InstanceStates"`
}

type listInstancesOutput struct {
	Marker    string            `json:"Marker,omitempty"`
	Instances []ClusterInstance `json:"Instances"`
}

func (h *Handler) handleListInstances(_ context.Context, in *listInstancesInput) (*listInstancesOutput, error) {
	params := ListInstancesParams{
		InstanceGroupID:    in.InstanceGroupID,
		InstanceFleetID:    in.InstanceFleetID,
		InstanceGroupTypes: in.InstanceGroupTypes,
		InstanceStates:     in.InstanceStates,
		Marker:             in.Marker,
	}

	instances, nextMarker := h.Backend.ListInstances(in.ClusterID, params)

	return &listInstancesOutput{Instances: instances, Marker: nextMarker}, nil
}

// --- ListNotebookExecutions ---

type listNotebookExecutionsInput struct {
	EditorID string `json:"EditorId,omitempty"`
	Status   string `json:"Status,omitempty"`
	Marker   string `json:"Marker,omitempty"`
}

type listNotebookExecutionsOutput struct {
	Marker             string              `json:"Marker,omitempty"`
	NotebookExecutions []NotebookExecution `json:"NotebookExecutions"`
}

func (h *Handler) handleListNotebookExecutions(
	_ context.Context,
	in *listNotebookExecutionsInput,
) (*listNotebookExecutionsOutput, error) {
	list, marker := h.Backend.ListNotebookExecutions(ListNotebookExecutionsParams{
		EditorID: in.EditorID,
		Status:   in.Status,
		Marker:   in.Marker,
	})

	return &listNotebookExecutionsOutput{NotebookExecutions: list, Marker: marker}, nil
}

// --- ListReleaseLabels ---

type listReleaseLabelsInput struct {
	Filters    listReleaseLabelFilters `json:"Filters"`
	Marker     string                  `json:"Marker"`
	MaxResults int                     `json:"MaxResults"`
}

type listReleaseLabelFilters struct {
	Prefix      string `json:"Prefix"`
	Application string `json:"Application"`
}

type listReleaseLabelsOutput struct {
	NextToken     string   `json:"NextToken,omitempty"`
	ReleaseLabels []string `json:"ReleaseLabels"`
}

func (h *Handler) handleListReleaseLabels(
	_ context.Context,
	in *listReleaseLabelsInput,
) (*listReleaseLabelsOutput, error) {
	labels, next := h.Backend.ListReleaseLabels(in.Filters.Prefix, in.Filters.Application, in.Marker)

	return &listReleaseLabelsOutput{ReleaseLabels: labels, NextToken: next}, nil
}

// --- ListSecurityConfigurations ---

type listSecurityConfigurationsInput struct {
	Marker string `json:"Marker"`
}

type listSecurityConfigurationsOutput struct {
	Marker                 string                  `json:"Marker,omitempty"`
	SecurityConfigurations []SecurityConfigSummary `json:"SecurityConfigurations"`
}

func (h *Handler) handleListSecurityConfigurations(
	_ context.Context,
	in *listSecurityConfigurationsInput,
) (*listSecurityConfigurationsOutput, error) {
	configs, nextMarker := h.Backend.ListSecurityConfigurations(in.Marker)

	return &listSecurityConfigurationsOutput{
		SecurityConfigurations: configs,
		Marker:                 nextMarker,
	}, nil
}

// --- ListStudioSessionMappings ---

type listStudioSessionMappingsInput struct {
	StudioID     string `json:"StudioId"`
	IdentityType string `json:"IdentityType"`
}

type listStudioSessionMappingsOutput struct {
	SessionMappings []StudioSessionMapping `json:"SessionMappings"`
}

func (h *Handler) handleListStudioSessionMappings(
	_ context.Context,
	in *listStudioSessionMappingsInput,
) (*listStudioSessionMappingsOutput, error) {
	mappings := h.Backend.ListStudioSessionMappings(in.StudioID, in.IdentityType)

	return &listStudioSessionMappingsOutput{SessionMappings: mappings}, nil
}

// --- ListStudios ---

type listStudiosInput struct {
	Marker string `json:"Marker"`
}

type listStudiosOutput struct {
	Marker  string          `json:"Marker,omitempty"`
	Studios []StudioSummary `json:"Studios"`
}

func (h *Handler) handleListStudios(_ context.Context, in *listStudiosInput) (*listStudiosOutput, error) {
	studios, nextMarker := h.Backend.ListStudios(in.Marker)

	return &listStudiosOutput{Studios: studios, Marker: nextMarker}, nil
}

// --- ListSupportedInstanceTypes ---

type listSupportedInstanceTypesInput struct {
	ReleaseLabel string `json:"ReleaseLabel"`
	Marker       string `json:"Marker"`
}

type listSupportedInstanceTypesOutput struct {
	Marker                 string                  `json:"Marker,omitempty"`
	SupportedInstanceTypes []SupportedInstanceType `json:"SupportedInstanceTypes"`
}

func (h *Handler) handleListSupportedInstanceTypes(
	_ context.Context,
	in *listSupportedInstanceTypesInput,
) (*listSupportedInstanceTypesOutput, error) {
	types, nextMarker := h.Backend.ListSupportedInstanceTypes(in.ReleaseLabel, in.Marker)

	return &listSupportedInstanceTypesOutput{
		SupportedInstanceTypes: types,
		Marker:                 nextMarker,
	}, nil
}

// --- ModifyCluster ---

type modifyClusterInput struct {
	ClusterID            string `json:"ClusterId"`
	StepConcurrencyLevel int    `json:"StepConcurrencyLevel"`
}

type modifyClusterOutput struct {
	StepConcurrencyLevel int `json:"StepConcurrencyLevel"`
}

func (h *Handler) handleModifyCluster(_ context.Context, in *modifyClusterInput) (*modifyClusterOutput, error) {
	level, err := h.Backend.ModifyCluster(in.ClusterID, in.StepConcurrencyLevel)
	if err != nil {
		return nil, err
	}

	return &modifyClusterOutput{StepConcurrencyLevel: level}, nil
}

// --- ModifyInstanceFleet ---

type modifyInstanceFleetInput struct {
	ClusterID     string                    `json:"ClusterId"`
	InstanceFleet InstanceFleetModification `json:"InstanceFleet"`
}

type modifyInstanceFleetOutput struct{}

func (h *Handler) handleModifyInstanceFleet(
	_ context.Context,
	in *modifyInstanceFleetInput,
) (*modifyInstanceFleetOutput, error) {
	if err := h.Backend.ModifyInstanceFleet(in.ClusterID, in.InstanceFleet); err != nil {
		return nil, err
	}

	return &modifyInstanceFleetOutput{}, nil
}

// --- ModifyInstanceGroups ---

type modifyInstanceGroupsInput struct {
	ClusterID      string                      `json:"ClusterId"`
	InstanceGroups []InstanceGroupModification `json:"InstanceGroups"`
}

type modifyInstanceGroupsOutput struct{}

func (h *Handler) handleModifyInstanceGroups(
	_ context.Context,
	in *modifyInstanceGroupsInput,
) (*modifyInstanceGroupsOutput, error) {
	if err := h.Backend.ModifyInstanceGroups(in.ClusterID, in.InstanceGroups); err != nil {
		return nil, err
	}

	return &modifyInstanceGroupsOutput{}, nil
}

// --- PutAutoScalingPolicy ---

type putAutoScalingPolicyInput struct {
	ClusterID         string                `json:"ClusterId"`
	InstanceGroupID   string                `json:"InstanceGroupId"`
	AutoScalingPolicy AutoScalingPolicySpec `json:"AutoScalingPolicy"`
}

type putAutoScalingPolicyOutput struct {
	AutoScalingPolicy *AutoScalingPolicyDetail `json:"AutoScalingPolicy"`
	ClusterARN        string                   `json:"ClusterArn"`
	ClusterID         string                   `json:"ClusterId"`
	InstanceGroupID   string                   `json:"InstanceGroupId"`
}

func (h *Handler) handlePutAutoScalingPolicy(
	_ context.Context,
	in *putAutoScalingPolicyInput,
) (*putAutoScalingPolicyOutput, error) {
	detail, clusterARN, groupID, err := h.Backend.PutAutoScalingPolicy(
		in.ClusterID, in.InstanceGroupID, in.AutoScalingPolicy,
	)
	if err != nil {
		return nil, err
	}

	return &putAutoScalingPolicyOutput{
		ClusterARN:        clusterARN,
		ClusterID:         in.ClusterID,
		InstanceGroupID:   groupID,
		AutoScalingPolicy: detail,
	}, nil
}

// --- PutAutoTerminationPolicy ---

type putAutoTerminationPolicyInput struct {
	ClusterID             string                `json:"ClusterId"`
	AutoTerminationPolicy AutoTerminationPolicy `json:"AutoTerminationPolicy"`
}

type putAutoTerminationPolicyOutput struct{}

func (h *Handler) handlePutAutoTerminationPolicy(
	_ context.Context,
	in *putAutoTerminationPolicyInput,
) (*putAutoTerminationPolicyOutput, error) {
	if err := h.Backend.PutAutoTerminationPolicy(in.ClusterID, in.AutoTerminationPolicy); err != nil {
		return nil, err
	}

	return &putAutoTerminationPolicyOutput{}, nil
}

// --- PutBlockPublicAccessConfiguration ---

type putBlockPublicAccessConfigurationInput struct {
	BlockPublicAccessConfiguration BlockPublicAccessConfiguration `json:"BlockPublicAccessConfiguration"`
}

type putBlockPublicAccessConfigurationOutput struct{}

func (h *Handler) handlePutBlockPublicAccessConfiguration(
	_ context.Context,
	in *putBlockPublicAccessConfigurationInput,
) (*putBlockPublicAccessConfigurationOutput, error) {
	if err := h.Backend.PutBlockPublicAccessConfiguration(in.BlockPublicAccessConfiguration); err != nil {
		return nil, err
	}

	return &putBlockPublicAccessConfigurationOutput{}, nil
}

// --- PutManagedScalingPolicy ---

type putManagedScalingPolicyInput struct {
	ClusterID            string               `json:"ClusterId"`
	ManagedScalingPolicy ManagedScalingPolicy `json:"ManagedScalingPolicy"`
}

type putManagedScalingPolicyOutput struct{}

func (h *Handler) handlePutManagedScalingPolicy(
	_ context.Context,
	in *putManagedScalingPolicyInput,
) (*putManagedScalingPolicyOutput, error) {
	if err := h.Backend.PutManagedScalingPolicy(in.ClusterID, in.ManagedScalingPolicy); err != nil {
		return nil, err
	}

	return &putManagedScalingPolicyOutput{}, nil
}

// --- RemoveAutoScalingPolicy ---

type removeAutoScalingPolicyInput struct {
	ClusterID       string `json:"ClusterId"`
	InstanceGroupID string `json:"InstanceGroupId"`
}

type removeAutoScalingPolicyOutput struct{}

func (h *Handler) handleRemoveAutoScalingPolicy(
	_ context.Context,
	in *removeAutoScalingPolicyInput,
) (*removeAutoScalingPolicyOutput, error) {
	if err := h.Backend.RemoveAutoScalingPolicy(in.ClusterID, in.InstanceGroupID); err != nil {
		return nil, err
	}

	return &removeAutoScalingPolicyOutput{}, nil
}

// --- RemoveAutoTerminationPolicy ---

type removeAutoTerminationPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type removeAutoTerminationPolicyOutput struct{}

func (h *Handler) handleRemoveAutoTerminationPolicy(
	_ context.Context,
	in *removeAutoTerminationPolicyInput,
) (*removeAutoTerminationPolicyOutput, error) {
	if err := h.Backend.RemoveAutoTerminationPolicy(in.ClusterID); err != nil {
		return nil, err
	}

	return &removeAutoTerminationPolicyOutput{}, nil
}

// --- RemoveManagedScalingPolicy ---

type removeManagedScalingPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type removeManagedScalingPolicyOutput struct{}

func (h *Handler) handleRemoveManagedScalingPolicy(
	_ context.Context,
	in *removeManagedScalingPolicyInput,
) (*removeManagedScalingPolicyOutput, error) {
	if err := h.Backend.RemoveManagedScalingPolicy(in.ClusterID); err != nil {
		return nil, err
	}

	return &removeManagedScalingPolicyOutput{}, nil
}

// --- SetKeepJobFlowAliveWhenNoSteps ---

type setKeepJobFlowAliveWhenNoStepsInput struct {
	JobFlowIDs                  []string `json:"JobFlowIds"`
	KeepJobFlowAliveWhenNoSteps bool     `json:"KeepJobFlowAliveWhenNoSteps"`
}

type setKeepJobFlowAliveWhenNoStepsOutput struct{}

func (h *Handler) handleSetKeepJobFlowAliveWhenNoSteps(
	_ context.Context,
	in *setKeepJobFlowAliveWhenNoStepsInput,
) (*setKeepJobFlowAliveWhenNoStepsOutput, error) {
	if err := h.Backend.SetKeepJobFlowAliveWhenNoSteps(in.JobFlowIDs, in.KeepJobFlowAliveWhenNoSteps); err != nil {
		return nil, err
	}

	return &setKeepJobFlowAliveWhenNoStepsOutput{}, nil
}

// --- SetTerminationProtection ---

type setTerminationProtectionInput struct {
	JobFlowIDs           []string `json:"JobFlowIds"`
	TerminationProtected bool     `json:"TerminationProtected"`
}

type setTerminationProtectionOutput struct{}

func (h *Handler) handleSetTerminationProtection(
	_ context.Context,
	in *setTerminationProtectionInput,
) (*setTerminationProtectionOutput, error) {
	if err := h.Backend.SetTerminationProtection(in.JobFlowIDs, in.TerminationProtected); err != nil {
		return nil, err
	}

	return &setTerminationProtectionOutput{}, nil
}

// --- SetUnhealthyNodeReplacement ---

type setUnhealthyNodeReplacementInput struct {
	JobFlowIDs               []string `json:"JobFlowIds"`
	UnhealthyNodeReplacement bool     `json:"UnhealthyNodeReplacement"`
}

type setUnhealthyNodeReplacementOutput struct{}

func (h *Handler) handleSetUnhealthyNodeReplacement(
	_ context.Context,
	in *setUnhealthyNodeReplacementInput,
) (*setUnhealthyNodeReplacementOutput, error) {
	if err := h.Backend.SetUnhealthyNodeReplacement(in.JobFlowIDs, in.UnhealthyNodeReplacement); err != nil {
		return nil, err
	}

	return &setUnhealthyNodeReplacementOutput{}, nil
}

// --- SetVisibleToAllUsers ---

type setVisibleToAllUsersInput struct {
	JobFlowIDs        []string `json:"JobFlowIds"`
	VisibleToAllUsers bool     `json:"VisibleToAllUsers"`
}

type setVisibleToAllUsersOutput struct{}

func (h *Handler) handleSetVisibleToAllUsers(
	_ context.Context,
	in *setVisibleToAllUsersInput,
) (*setVisibleToAllUsersOutput, error) {
	if err := h.Backend.SetVisibleToAllUsers(in.JobFlowIDs, in.VisibleToAllUsers); err != nil {
		return nil, err
	}

	return &setVisibleToAllUsersOutput{}, nil
}

// --- StartNotebookExecution ---

type startNotebookExecutionInput struct {
	EditorID              string `json:"EditorId,omitempty"`
	NotebookExecutionName string `json:"NotebookExecutionName,omitempty"`
	NotebookParams        string `json:"NotebookParams,omitempty"`
	ExecutionEngineConfig struct {
		ID string `json:"Id,omitempty"`
	} `json:"ExecutionEngineConfig"`
	Tags []Tag `json:"Tags,omitempty"`
}

type startNotebookExecutionOutput struct {
	NotebookExecutionID string `json:"NotebookExecutionId"`
}

func (h *Handler) handleStartNotebookExecution(
	_ context.Context,
	in *startNotebookExecutionInput,
) (*startNotebookExecutionOutput, error) {
	ne, err := h.Backend.StartNotebookExecution(
		in.EditorID,
		in.NotebookExecutionName,
		in.NotebookParams,
		in.ExecutionEngineConfig.ID,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &startNotebookExecutionOutput{NotebookExecutionID: ne.NotebookExecutionID}, nil
}

// --- StopNotebookExecution ---

type stopNotebookExecutionInput struct {
	NotebookExecutionID string `json:"NotebookExecutionId"`
}

type stopNotebookExecutionOutput struct{}

func (h *Handler) handleStopNotebookExecution(
	_ context.Context,
	in *stopNotebookExecutionInput,
) (*stopNotebookExecutionOutput, error) {
	if err := h.Backend.StopNotebookExecution(in.NotebookExecutionID); err != nil {
		return nil, err
	}

	return &stopNotebookExecutionOutput{}, nil
}

// --- UpdateStudio ---

type updateStudioInput struct {
	StudioID          string   `json:"StudioId"`
	Name              string   `json:"Name"`
	Description       string   `json:"Description"`
	DefaultS3Location string   `json:"DefaultS3Location"`
	SubnetIDs         []string `json:"SubnetIds"`
}

type updateStudioOutput struct{}

func (h *Handler) handleUpdateStudio(_ context.Context, in *updateStudioInput) (*updateStudioOutput, error) {
	if err := h.Backend.UpdateStudio(in.StudioID, in.Name, in.Description, in.DefaultS3Location, ""); err != nil {
		return nil, err
	}

	return &updateStudioOutput{}, nil
}

// --- UpdateStudioSessionMapping ---

type updateStudioSessionMappingInput struct {
	StudioID         string `json:"StudioId"`
	IdentityType     string `json:"IdentityType"`
	IdentityID       string `json:"IdentityId,omitempty"`
	IdentityName     string `json:"IdentityName,omitempty"`
	SessionPolicyArn string `json:"SessionPolicyArn"`
}

type updateStudioSessionMappingOutput struct{}

func (h *Handler) handleUpdateStudioSessionMapping(
	_ context.Context,
	in *updateStudioSessionMappingInput,
) (*updateStudioSessionMappingOutput, error) {
	if err := h.Backend.UpdateStudioSessionMapping(
		in.StudioID,
		in.IdentityType,
		in.IdentityID,
		in.IdentityName,
		in.SessionPolicyArn,
	); err != nil {
		return nil, err
	}

	return &updateStudioSessionMappingOutput{}, nil
}
