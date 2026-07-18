package emr

import (
	"context"
)

// --- RunJobFlow ---

type runJobFlowInput struct {
	SecurityConfiguration   string                  `json:"SecurityConfiguration"`
	ReleaseLabel            string                  `json:"ReleaseLabel"`
	OSReleaseLabel          string                  `json:"OSReleaseLabel"`
	LogURI                  string                  `json:"LogUri"`
	ServiceRole             string                  `json:"ServiceRole"`
	AutoScalingRole         string                  `json:"AutoScalingRole"`
	Name                    string                  `json:"Name"`
	ScaleDownBehavior       string                  `json:"ScaleDownBehavior"`
	CustomAmiID             string                  `json:"CustomAmiId"`
	Tags                    []Tag                   `json:"Tags"`
	Applications            []Application           `json:"Applications"`
	Configurations          []Configuration         `json:"Configurations"`
	Steps                   []StepSpec              `json:"Steps"`
	BootstrapActions        []BootstrapActionConfig `json:"BootstrapActions"`
	Instances               RunJobFlowInstances     `json:"Instances"`
	StepConcurrencyLevel    int                     `json:"StepConcurrencyLevel"`
	EbsRootVolumeSize       int                     `json:"EbsRootVolumeSize"`
	EbsRootVolumeIops       int                     `json:"EbsRootVolumeIops"`
	EbsRootVolumeThroughput int                     `json:"EbsRootVolumeThroughput"`
	VisibleToAllUsers       bool                    `json:"VisibleToAllUsers"`
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
		Instances:               in.Instances,
		LogURI:                  in.LogURI,
		ServiceRole:             in.ServiceRole,
		AutoScalingRole:         in.AutoScalingRole,
		ScaleDownBehavior:       in.ScaleDownBehavior,
		SecurityConfiguration:   in.SecurityConfiguration,
		CustomAmiID:             in.CustomAmiID,
		StepConcurrencyLevel:    in.StepConcurrencyLevel,
		EbsRootVolumeSize:       in.EbsRootVolumeSize,
		EbsRootVolumeIops:       in.EbsRootVolumeIops,
		EbsRootVolumeThroughput: in.EbsRootVolumeThroughput,
		VisibleToAllUsers:       in.VisibleToAllUsers,
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
