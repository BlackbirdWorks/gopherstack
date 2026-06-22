package ecs

// handler_stubs.go contains handler implementations for ECS operations that
// were previously stub-only: Daemon CRUD, DiscoverPollEndpoint, Submit state
// changes, and DescribeServiceRevisions.

import (
	"context"
	"fmt"
	"time"
)

// ackResponse is the acknowledgment string returned for state-change submissions.
const ackResponse = "ACK"

// ---- Daemon types (API shapes) ----

type daemonView struct {
	DaemonArn      string `json:"daemonArn,omitempty"`
	DaemonName     string `json:"daemonName,omitempty"`
	ClusterArn     string `json:"clusterArn,omitempty"`
	TaskDefinition string `json:"taskDefinition,omitempty"`
	Status         string `json:"status,omitempty"`
	CreatedAt      string `json:"createdAt,omitempty"`
	UpdatedAt      string `json:"updatedAt,omitempty"`
}

type daemonOutput struct {
	Daemon daemonView `json:"daemon"`
}

type daemonsOutput struct {
	Daemons []daemonView `json:"daemons"`
}

type daemonTaskDefinitionView struct {
	DaemonTaskDefinitionArn string `json:"daemonTaskDefinitionArn,omitempty"`
	DaemonArn               string `json:"daemonArn,omitempty"`
	Family                  string `json:"family,omitempty"`
	TaskDefinitionArn       string `json:"taskDefinitionArn,omitempty"`
	Status                  string `json:"status,omitempty"`
	Revision                int    `json:"revision,omitempty"`
}

type daemonTaskDefinitionOutput struct {
	DaemonTaskDefinition daemonTaskDefinitionView `json:"daemonTaskDefinition"`
}

type daemonTaskDefinitionsOutput struct {
	DaemonTaskDefinitions []daemonTaskDefinitionView `json:"daemonTaskDefinitions"`
}

type daemonDeploymentView struct {
	ID           string `json:"id,omitempty"`
	DaemonArn    string `json:"daemonArn,omitempty"`
	Status       string `json:"status,omitempty"`
	CreatedAt    string `json:"createdAt,omitempty"`
	UpdatedAt    string `json:"updatedAt,omitempty"`
	FailedTasks  int    `json:"failedTasks"`
	PendingTasks int    `json:"pendingTasks"`
	RunningTasks int    `json:"runningTasks"`
}

type daemonDeploymentsOutput struct {
	Deployments []daemonDeploymentView `json:"deployments"`
}

type daemonDeploymentIDsOutput struct {
	DeploymentIDs []string `json:"deploymentIds"`
}

type daemonRevisionView struct {
	DaemonRevisionArn string `json:"daemonRevisionArn,omitempty"`
	DaemonArn         string `json:"daemonArn,omitempty"`
	TaskDefinitionArn string `json:"taskDefinitionArn,omitempty"`
	Revision          int    `json:"revision,omitempty"`
}

type daemonRevisionsOutput struct {
	Revisions []daemonRevisionView `json:"revisions"`
}

type emptyECSOutput struct{}

// ---- Conversions ----

func toDaemonView(d Daemon) daemonView {
	return daemonView{
		DaemonArn:      d.DaemonArn,
		DaemonName:     d.DaemonName,
		ClusterArn:     d.ClusterArn,
		TaskDefinition: d.TaskDefinition,
		Status:         d.Status,
		CreatedAt:      d.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      d.UpdatedAt.Format(time.RFC3339),
	}
}

func toDaemonTaskDefinitionView(def DaemonTaskDefinition) daemonTaskDefinitionView {
	return daemonTaskDefinitionView{
		DaemonTaskDefinitionArn: def.DaemonTaskDefinitionArn,
		DaemonArn:               def.DaemonArn,
		Family:                  def.Family,
		TaskDefinitionArn:       def.TaskDefinitionArn,
		Status:                  def.Status,
		Revision:                def.Revision,
	}
}

func toDaemonDeploymentView(dep DaemonDeployment) daemonDeploymentView {
	return daemonDeploymentView{
		ID:           dep.ID,
		DaemonArn:    dep.DaemonArn,
		Status:       dep.Status,
		FailedTasks:  dep.FailedTasks,
		PendingTasks: dep.PendingTasks,
		RunningTasks: dep.RunningTasks,
		CreatedAt:    dep.CreatedAt.Format(time.RFC3339),
		UpdatedAt:    dep.UpdatedAt.Format(time.RFC3339),
	}
}

func toDaemonRevisionView(r DaemonRevision) daemonRevisionView {
	return daemonRevisionView{
		DaemonRevisionArn: r.DaemonRevisionArn,
		DaemonArn:         r.DaemonArn,
		TaskDefinitionArn: r.TaskDefinitionArn,
		Revision:          r.Revision,
	}
}

// ---- CreateDaemon ----

type createDaemonInput struct {
	Cluster        string `json:"cluster"`
	DaemonName     string `json:"daemonName"`
	TaskDefinition string `json:"taskDefinition,omitempty"`
}

func (h *Handler) handleCreateDaemon(
	_ context.Context,
	in *createDaemonInput,
) (*daemonOutput, error) {
	d, err := h.Backend.CreateDaemon(CreateDaemonInput{
		ClusterName:    in.Cluster,
		DaemonName:     in.DaemonName,
		TaskDefinition: in.TaskDefinition,
	})
	if err != nil {
		return nil, err
	}

	return &daemonOutput{Daemon: toDaemonView(*d)}, nil
}

// ---- DeleteDaemon ----

type deleteDaemonInput struct {
	Cluster    string `json:"cluster"`
	DaemonName string `json:"daemonName"`
}

func (h *Handler) handleDeleteDaemon(
	_ context.Context,
	in *deleteDaemonInput,
) (*daemonOutput, error) {
	d, err := h.Backend.DeleteDaemon(in.Cluster, in.DaemonName)
	if err != nil {
		return nil, err
	}

	return &daemonOutput{Daemon: toDaemonView(*d)}, nil
}

// ---- DescribeDaemon ----

type describeDaemonInput struct {
	Cluster    string `json:"cluster"`
	DaemonName string `json:"daemonName"`
}

func (h *Handler) handleDescribeDaemon(
	_ context.Context,
	in *describeDaemonInput,
) (*daemonOutput, error) {
	d, err := h.Backend.DescribeDaemon(in.Cluster, in.DaemonName)
	if err != nil {
		return nil, err
	}

	return &daemonOutput{Daemon: toDaemonView(*d)}, nil
}

// ---- ListDaemons ----

type listDaemonsInput struct {
	Cluster string `json:"cluster"`
}

func (h *Handler) handleListDaemons(
	_ context.Context,
	in *listDaemonsInput,
) (*daemonsOutput, error) {
	daemons, err := h.Backend.ListDaemons(in.Cluster)
	if err != nil {
		return nil, err
	}

	views := make([]daemonView, 0, len(daemons))
	for _, d := range daemons {
		views = append(views, toDaemonView(d))
	}

	return &daemonsOutput{Daemons: views}, nil
}

// ---- UpdateDaemon ----

type updateDaemonInput struct {
	Cluster        string `json:"cluster"`
	DaemonName     string `json:"daemonName"`
	TaskDefinition string `json:"taskDefinition,omitempty"`
}

func (h *Handler) handleUpdateDaemon(
	_ context.Context,
	in *updateDaemonInput,
) (*daemonOutput, error) {
	d, err := h.Backend.UpdateDaemon(UpdateDaemonInput{
		ClusterName:    in.Cluster,
		DaemonName:     in.DaemonName,
		TaskDefinition: in.TaskDefinition,
	})
	if err != nil {
		return nil, err
	}

	return &daemonOutput{Daemon: toDaemonView(*d)}, nil
}

// ---- RegisterDaemonTaskDefinition ----

type registerDaemonTaskDefinitionInput struct {
	Daemon            string `json:"daemon"`
	Family            string `json:"family,omitempty"`
	TaskDefinitionArn string `json:"taskDefinitionArn,omitempty"`
}

func (h *Handler) handleRegisterDaemonTaskDefinition(
	_ context.Context,
	in *registerDaemonTaskDefinitionInput,
) (*daemonTaskDefinitionOutput, error) {
	def, err := h.Backend.RegisterDaemonTaskDefinition(RegisterDaemonTaskDefinitionInput{
		DaemonName:        in.Daemon,
		Family:            in.Family,
		TaskDefinitionArn: in.TaskDefinitionArn,
	})
	if err != nil {
		return nil, err
	}

	return &daemonTaskDefinitionOutput{DaemonTaskDefinition: toDaemonTaskDefinitionView(*def)}, nil
}

// ---- DeleteDaemonTaskDefinition ----

type deleteDaemonTaskDefinitionInput struct {
	DaemonTaskDefinitionArn string `json:"daemonTaskDefinitionArn"`
}

func (h *Handler) handleDeleteDaemonTaskDefinition(
	_ context.Context,
	in *deleteDaemonTaskDefinitionInput,
) (*emptyECSOutput, error) {
	if err := h.Backend.DeleteDaemonTaskDefinition(in.DaemonTaskDefinitionArn); err != nil {
		return nil, err
	}

	return &emptyECSOutput{}, nil
}

// ---- DescribeDaemonTaskDefinition ----

type describeDaemonTaskDefinitionInput struct {
	Daemon string `json:"daemon"`
}

func (h *Handler) handleDescribeDaemonTaskDefinition(
	_ context.Context,
	in *describeDaemonTaskDefinitionInput,
) (*daemonTaskDefinitionOutput, error) {
	def, err := h.Backend.DescribeDaemonTaskDefinition(in.Daemon)
	if err != nil {
		return nil, err
	}

	return &daemonTaskDefinitionOutput{DaemonTaskDefinition: toDaemonTaskDefinitionView(*def)}, nil
}

// ---- ListDaemonTaskDefinitions ----

type listDaemonTaskDefinitionsInput struct {
	Daemon string `json:"daemon"`
}

func (h *Handler) handleListDaemonTaskDefinitions(
	_ context.Context,
	in *listDaemonTaskDefinitionsInput,
) (*daemonTaskDefinitionsOutput, error) {
	defs, err := h.Backend.ListDaemonTaskDefinitions(in.Daemon)
	if err != nil {
		return nil, err
	}

	views := make([]daemonTaskDefinitionView, 0, len(defs))
	for _, d := range defs {
		views = append(views, toDaemonTaskDefinitionView(d))
	}

	return &daemonTaskDefinitionsOutput{DaemonTaskDefinitions: views}, nil
}

// ---- DescribeDaemonDeployments ----

type describeDaemonDeploymentsInput struct {
	DaemonArn     string   `json:"daemonArn"`
	DeploymentIDs []string `json:"deploymentIds,omitempty"`
}

func (h *Handler) handleDescribeDaemonDeployments(
	_ context.Context,
	in *describeDaemonDeploymentsInput,
) (*daemonDeploymentsOutput, error) {
	if in.DaemonArn == "" {
		return nil, fmt.Errorf("%w: daemonArn is required", ErrInvalidParameter)
	}

	deployments, err := h.Backend.DescribeDaemonDeployments(in.DaemonArn, in.DeploymentIDs)
	if err != nil {
		return nil, err
	}

	views := make([]daemonDeploymentView, 0, len(deployments))
	for _, dep := range deployments {
		views = append(views, toDaemonDeploymentView(dep))
	}

	return &daemonDeploymentsOutput{Deployments: views}, nil
}

// ---- ListDaemonDeployments ----

type listDaemonDeploymentsInput struct {
	Cluster    string `json:"cluster"`
	DaemonName string `json:"daemonName"`
}

func (h *Handler) handleListDaemonDeployments(
	_ context.Context,
	in *listDaemonDeploymentsInput,
) (*daemonDeploymentIDsOutput, error) {
	ids, err := h.Backend.ListDaemonDeployments(in.Cluster, in.DaemonName)
	if err != nil {
		return nil, err
	}

	return &daemonDeploymentIDsOutput{DeploymentIDs: ids}, nil
}

// ---- DescribeDaemonRevisions ----

type describeDaemonRevisionsInput struct {
	Daemon    string `json:"daemon"`
	Revisions []int  `json:"revisions,omitempty"`
}

func (h *Handler) handleDescribeDaemonRevisions(
	_ context.Context,
	in *describeDaemonRevisionsInput,
) (*daemonRevisionsOutput, error) {
	revisions, err := h.Backend.DescribeDaemonRevisions(in.Daemon, in.Revisions)
	if err != nil {
		return nil, err
	}

	views := make([]daemonRevisionView, 0, len(revisions))
	for _, r := range revisions {
		views = append(views, toDaemonRevisionView(r))
	}

	return &daemonRevisionsOutput{Revisions: views}, nil
}

// ---- DescribeServiceRevisions ----

type describeServiceRevisionsInput struct {
	ServiceRevisionArns []string `json:"serviceRevisionArns"`
}

type serviceRevisionView struct {
	ServiceRevisionArn       string                         `json:"serviceRevisionArn,omitempty"`
	ServiceArn               string                         `json:"serviceArn,omitempty"`
	ClusterArn               string                         `json:"clusterArn,omitempty"`
	TaskDefinition           string                         `json:"taskDefinition,omitempty"`
	LaunchType               string                         `json:"launchType,omitempty"`
	PlatformVersion          string                         `json:"platformVersion,omitempty"`
	PlatformFamily           string                         `json:"platformFamily,omitempty"`
	CreatedAt                string                         `json:"createdAt,omitempty"`
	CapacityProviderStrategy []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	LoadBalancers            []LoadBalancer                 `json:"loadBalancers,omitempty"`
	ServiceRegistries        []ServiceRegistry              `json:"serviceRegistries,omitempty"`
}

type serviceRevisionsOutput struct {
	ServiceRevisions []serviceRevisionView `json:"serviceRevisions"`
	Failures         []failureView         `json:"failures"`
}

func toServiceRevisionView(r ServiceRevision) serviceRevisionView {
	return serviceRevisionView{
		ServiceRevisionArn:       r.ServiceRevisionArn,
		ServiceArn:               r.ServiceArn,
		ClusterArn:               r.ClusterArn,
		TaskDefinition:           r.TaskDefinition,
		LaunchType:               r.LaunchType,
		PlatformVersion:          r.PlatformVersion,
		PlatformFamily:           r.PlatformFamily,
		CreatedAt:                r.CreatedAt.Format(time.RFC3339),
		CapacityProviderStrategy: r.CapacityProviderStrategy,
		LoadBalancers:            r.LoadBalancers,
		ServiceRegistries:        r.ServiceRegistries,
	}
}

func (h *Handler) handleDescribeServiceRevisions(
	_ context.Context,
	in *describeServiceRevisionsInput,
) (*serviceRevisionsOutput, error) {
	if len(in.ServiceRevisionArns) == 0 {
		return &serviceRevisionsOutput{
			ServiceRevisions: []serviceRevisionView{},
			Failures:         []failureView{},
		}, nil
	}

	revisions, failures, err := h.Backend.DescribeServiceRevisions(in.ServiceRevisionArns)
	if err != nil {
		return nil, err
	}

	views := make([]serviceRevisionView, 0, len(revisions))
	for _, r := range revisions {
		views = append(views, toServiceRevisionView(r))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &serviceRevisionsOutput{
		ServiceRevisions: views,
		Failures:         failViews,
	}, nil
}

// ---- DiscoverPollEndpoint ----
// Returns a region-specific ECS agent endpoint. Real AWS returns endpoints
// based on the cluster's control-plane; we return a well-formed regional URL.

type discoverPollEndpointInput struct {
	ClusterArn           string `json:"clusterArn"`
	ContainerInstanceArn string `json:"containerInstanceArn"`
}

type discoverPollEndpointOutput struct {
	Endpoint               string `json:"endpoint"`
	TelemetryEndpoint      string `json:"telemetryEndpoint,omitempty"`
	ServiceConnectEndpoint string `json:"serviceConnectEndpoint,omitempty"`
}

func (h *Handler) handleDiscoverPollEndpoint(
	_ context.Context,
	_ *discoverPollEndpointInput,
) (*discoverPollEndpointOutput, error) {
	region := h.region
	if region == "" {
		region = "us-east-1"
	}

	return &discoverPollEndpointOutput{
		Endpoint:          fmt.Sprintf("https://ecs-a-1.%s.amazonaws.com/", region),
		TelemetryEndpoint: fmt.Sprintf("https://ecs-t-1.%s.amazonaws.com/", region),
	}, nil
}

// ---- SubmitAttachmentStateChanges ----

type attachmentStateChangeItem struct {
	AttachmentArn string `json:"attachmentArn"`
	Status        string `json:"status"`
}

type submitAttachmentStateChangesInput struct {
	Cluster     string                      `json:"cluster"`
	Attachments []attachmentStateChangeItem `json:"attachments"`
}

type submitAttachmentStateChangesOutput struct {
	Acknowledgment string `json:"acknowledgment"`
}

func (h *Handler) handleSubmitAttachmentStateChanges(
	_ context.Context,
	in *submitAttachmentStateChangesInput,
) (*submitAttachmentStateChangesOutput, error) {
	changes := make([]AttachmentStateChange, 0, len(in.Attachments))
	for _, a := range in.Attachments {
		changes = append(changes, AttachmentStateChange(a))
	}

	if err := h.Backend.SubmitAttachmentStateChanges(in.Cluster, changes); err != nil {
		return nil, err
	}

	return &submitAttachmentStateChangesOutput{Acknowledgment: ackResponse}, nil
}

// ---- SubmitContainerStateChange ----

type submitContainerStateChangeInput struct {
	ExitCode      *int   `json:"exitCode,omitempty"`
	Cluster       string `json:"cluster"`
	Task          string `json:"task"`
	ContainerName string `json:"containerName"`
	RuntimeID     string `json:"runtimeId,omitempty"`
	Status        string `json:"status,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

func (h *Handler) handleSubmitContainerStateChange(
	_ context.Context,
	in *submitContainerStateChangeInput,
) (*submitAttachmentStateChangesOutput, error) {
	if err := h.Backend.SubmitContainerStateChange(SubmitContainerStateChangeInput{
		Cluster:       in.Cluster,
		Task:          in.Task,
		ContainerName: in.ContainerName,
		RuntimeID:     in.RuntimeID,
		Status:        in.Status,
		Reason:        in.Reason,
		ExitCode:      in.ExitCode,
	}); err != nil {
		return nil, err
	}

	return &submitAttachmentStateChangesOutput{Acknowledgment: ackResponse}, nil
}

// ---- SubmitTaskStateChange ----

type submitTaskStateChangeInput struct {
	PullStartedAt      *time.Time `json:"pullStartedAt,omitempty"`
	PullStoppedAt      *time.Time `json:"pullStoppedAt,omitempty"`
	ExecutionStoppedAt *time.Time `json:"executionStoppedAt,omitempty"`
	Cluster            string     `json:"cluster"`
	Task               string     `json:"task"`
	Status             string     `json:"status,omitempty"`
	Reason             string     `json:"reason,omitempty"`
}

func (h *Handler) handleSubmitTaskStateChange(
	_ context.Context,
	in *submitTaskStateChangeInput,
) (*submitAttachmentStateChangesOutput, error) {
	if err := h.Backend.SubmitTaskStateChange(SubmitTaskStateChangeInput{
		Cluster:            in.Cluster,
		Task:               in.Task,
		Status:             in.Status,
		Reason:             in.Reason,
		PullStartedAt:      in.PullStartedAt,
		PullStoppedAt:      in.PullStoppedAt,
		ExecutionStoppedAt: in.ExecutionStoppedAt,
	}); err != nil {
		return nil, err
	}

	return &submitAttachmentStateChangesOutput{Acknowledgment: ackResponse}, nil
}
