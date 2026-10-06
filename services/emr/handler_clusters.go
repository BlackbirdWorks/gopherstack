package emr

import (
	"context"
)

// --- RunJobFlow ---

type runJobFlowInput struct {
	KerberosAttributes      *KerberosAttributes      `json:"KerberosAttributes"`
	MonitoringConfiguration *MonitoringConfiguration `json:"MonitoringConfiguration"`
	AutoTerminationPolicy   *AutoTerminationPolicy   `json:"AutoTerminationPolicy"`
	ManagedScalingPolicy    *ManagedScalingPolicy    `json:"ManagedScalingPolicy"`
	VisibleToAllUsers       *bool                    `json:"VisibleToAllUsers"`
	ReleaseLabel            string                   `json:"ReleaseLabel"`
	SecurityConfiguration   string                   `json:"SecurityConfiguration"`
	Name                    string                   `json:"Name"`
	ScaleDownBehavior       string                   `json:"ScaleDownBehavior"`
	CustomAmiID             string                   `json:"CustomAmiId"`
	JobFlowRole             string                   `json:"JobFlowRole"`
	RepoUpgradeOnBoot       string                   `json:"RepoUpgradeOnBoot"`
	LogEncryptionKmsKeyID   string                   `json:"LogEncryptionKmsKeyId"`
	StepExecutionRoleArn    string                   `json:"StepExecutionRoleArn"`
	AmiVersion              string                   `json:"AmiVersion"`
	OSReleaseLabel          string                   `json:"OSReleaseLabel"`
	ServiceRole             string                   `json:"ServiceRole"`
	LogURI                  string                   `json:"LogUri"`
	AutoScalingRole         string                   `json:"AutoScalingRole"`
	PlacementGroupConfigs   []PlacementGroupConfig   `json:"PlacementGroupConfigs"`
	Steps                   []StepSpec               `json:"Steps"`
	Configurations          []Configuration          `json:"Configurations"`
	Applications            []Application            `json:"Applications"`
	Tags                    []Tag                    `json:"Tags"`
	BootstrapActions        []BootstrapActionConfig  `json:"BootstrapActions"`
	Instances               RunJobFlowInstances      `json:"Instances"`
	StepConcurrencyLevel    int                      `json:"StepConcurrencyLevel"`
	EbsRootVolumeSize       int                      `json:"EbsRootVolumeSize"`
	EbsRootVolumeIops       int                      `json:"EbsRootVolumeIops"`
	EbsRootVolumeThroughput int                      `json:"EbsRootVolumeThroughput"`
	SessionEnabled          bool                     `json:"SessionEnabled"`
}

type runJobFlowOutput struct {
	JobFlowID  string `json:"JobFlowId"`
	ClusterArn string `json:"ClusterArn"`
}

func (h *Handler) handleRunJobFlow(ctx context.Context, in *runJobFlowInput) (*runJobFlowOutput, error) {
	cluster, err := h.Backend.RunJobFlow(ctx, RunJobFlowParams{
		Name:                    in.Name,
		ReleaseLabel:            in.ReleaseLabel,
		OSReleaseLabel:          in.OSReleaseLabel,
		Tags:                    in.Tags,
		Applications:            in.Applications,
		Configurations:          in.Configurations,
		Steps:                   in.Steps,
		BootstrapActions:        in.BootstrapActions,
		KerberosAttributes:      in.KerberosAttributes,
		PlacementGroupConfigs:   in.PlacementGroupConfigs,
		ManagedScalingPolicy:    in.ManagedScalingPolicy,
		AutoTerminationPolicy:   in.AutoTerminationPolicy,
		MonitoringConfiguration: in.MonitoringConfiguration,
		Instances:               in.Instances,
		LogURI:                  in.LogURI,
		LogEncryptionKmsKeyID:   in.LogEncryptionKmsKeyID,
		RepoUpgradeOnBoot:       in.RepoUpgradeOnBoot,
		AmiVersion:              in.AmiVersion,
		ServiceRole:             in.ServiceRole,
		JobFlowRole:             in.JobFlowRole,
		AutoScalingRole:         in.AutoScalingRole,
		ScaleDownBehavior:       in.ScaleDownBehavior,
		SecurityConfiguration:   in.SecurityConfiguration,
		CustomAmiID:             in.CustomAmiID,
		StepExecutionRoleArn:    in.StepExecutionRoleArn,
		StepConcurrencyLevel:    in.StepConcurrencyLevel,
		EbsRootVolumeSize:       in.EbsRootVolumeSize,
		EbsRootVolumeIops:       in.EbsRootVolumeIops,
		EbsRootVolumeThroughput: in.EbsRootVolumeThroughput,
		VisibleToAllUsers:       in.VisibleToAllUsers == nil || *in.VisibleToAllUsers,
		SessionEnabled:          in.SessionEnabled,
	})
	if err != nil {
		return nil, err
	}

	return &runJobFlowOutput{
		JobFlowID:  cluster.ID,
		ClusterArn: cluster.ARN,
	}, nil
}

// --- DescribeCluster ---

type describeClusterInput struct {
	ClusterID string `json:"ClusterId"`
}

type describeClusterOutput struct {
	Cluster *Cluster `json:"Cluster"`
}

func (h *Handler) handleDescribeCluster(ctx context.Context, in *describeClusterInput) (*describeClusterOutput, error) {
	cluster, err := h.Backend.DescribeCluster(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &describeClusterOutput{Cluster: cluster}, nil
}

// --- ListClusters ---

type listClustersInput struct {
	CreatedAfter  *float64 `json:"CreatedAfter"`
	CreatedBefore *float64 `json:"CreatedBefore"`
	Marker        string   `json:"Marker"`
	ClusterStates []string `json:"ClusterStates"`
}

type listClustersOutput struct {
	Marker   string           `json:"Marker,omitempty"`
	Clusters []ClusterSummary `json:"Clusters"`
}

func (h *Handler) handleListClusters(ctx context.Context, in *listClustersInput) (*listClustersOutput, error) {
	params := ListClustersParams{
		ClusterStates: in.ClusterStates,
		Marker:        in.Marker,
	}

	if in.CreatedAfter != nil {
		t := epochSecondsToTime(*in.CreatedAfter)
		params.CreatedAfter = &t
	}

	if in.CreatedBefore != nil {
		t := epochSecondsToTime(*in.CreatedBefore)
		params.CreatedBefore = &t
	}

	clusters, nextMarker := h.Backend.ListClusters(ctx, params)

	return &listClustersOutput{Clusters: clusters, Marker: nextMarker}, nil
}

// --- TerminateJobFlows ---

type terminateJobFlowsInput struct {
	JobFlowIDs []string `json:"JobFlowIds"`
}

func (h *Handler) handleTerminateJobFlows(
	ctx context.Context,
	in *terminateJobFlowsInput,
) (*emptyOutput, error) {
	if err := h.Backend.TerminateJobFlows(ctx, in.JobFlowIDs); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
