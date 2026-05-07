package ecs

// handler_stubs.go registers stub handlers for ECS SDK operations that are not
// yet fully implemented.  Each stub returns a minimal valid empty response.

import "context"

// ackResponse is the acknowledgment string returned for state-change submissions.
const ackResponse = "ACK"

// --- Daemon stubs ---

type daemonStub struct {
	DaemonName string `json:"daemonName,omitempty"`
	Status     string `json:"status,omitempty"`
}

type daemonOutput struct {
	Daemon daemonStub `json:"daemon"`
}

type daemonsOutput struct {
	Daemons []daemonStub `json:"daemons"`
}

type daemonTaskDefinitionStub struct {
	Family string `json:"family,omitempty"`
}

type daemonTaskDefinitionOutput struct {
	DaemonTaskDefinition daemonTaskDefinitionStub `json:"daemonTaskDefinition"`
}

type daemonTaskDefinitionsOutput struct {
	DaemonTaskDefinitions []daemonTaskDefinitionStub `json:"daemonTaskDefinitions"`
}

type daemonDeploymentStub struct {
	ID string `json:"id,omitempty"`
}

type daemonDeploymentsOutput struct {
	Deployments []daemonDeploymentStub `json:"deployments"`
}

type daemonRevisionStub struct {
	Revision int `json:"revision,omitempty"`
}

type daemonRevisionsOutput struct {
	Revisions []daemonRevisionStub `json:"revisions"`
}

type emptyECSOutput struct{}

func (h *Handler) handleCreateDaemon(_ context.Context, _ *emptyECSOutput) (*daemonOutput, error) {
	return &daemonOutput{Daemon: daemonStub{Status: statusActive}}, nil
}

func (h *Handler) handleDeleteDaemon(
	_ context.Context,
	_ *emptyECSOutput,
) (*emptyECSOutput, error) {
	return &emptyECSOutput{}, nil
}

func (h *Handler) handleDescribeDaemon(
	_ context.Context,
	_ *emptyECSOutput,
) (*daemonOutput, error) {
	return &daemonOutput{Daemon: daemonStub{Status: statusActive}}, nil
}

func (h *Handler) handleDescribeDaemonDeployments(
	_ context.Context,
	_ *emptyECSOutput,
) (*daemonDeploymentsOutput, error) {
	return &daemonDeploymentsOutput{Deployments: []daemonDeploymentStub{}}, nil
}

func (h *Handler) handleDescribeDaemonRevisions(
	_ context.Context,
	_ *emptyECSOutput,
) (*daemonRevisionsOutput, error) {
	return &daemonRevisionsOutput{Revisions: []daemonRevisionStub{}}, nil
}

func (h *Handler) handleDescribeDaemonTaskDefinition(
	_ context.Context,
	_ *emptyECSOutput,
) (*daemonTaskDefinitionOutput, error) {
	return &daemonTaskDefinitionOutput{DaemonTaskDefinition: daemonTaskDefinitionStub{}}, nil
}

func (h *Handler) handleDeleteDaemonTaskDefinition(
	_ context.Context,
	_ *emptyECSOutput,
) (*emptyECSOutput, error) {
	return &emptyECSOutput{}, nil
}

func (h *Handler) handleListDaemons(_ context.Context, _ *emptyECSOutput) (*daemonsOutput, error) {
	return &daemonsOutput{Daemons: []daemonStub{}}, nil
}

func (h *Handler) handleListDaemonDeployments(
	_ context.Context,
	_ *emptyECSOutput,
) (*daemonDeploymentsOutput, error) {
	return &daemonDeploymentsOutput{Deployments: []daemonDeploymentStub{}}, nil
}

func (h *Handler) handleListDaemonTaskDefinitions(
	_ context.Context,
	_ *emptyECSOutput,
) (*daemonTaskDefinitionsOutput, error) {
	return &daemonTaskDefinitionsOutput{DaemonTaskDefinitions: []daemonTaskDefinitionStub{}}, nil
}

func (h *Handler) handleRegisterDaemonTaskDefinition(
	_ context.Context,
	_ *emptyECSOutput,
) (*daemonTaskDefinitionOutput, error) {
	return &daemonTaskDefinitionOutput{DaemonTaskDefinition: daemonTaskDefinitionStub{}}, nil
}

func (h *Handler) handleUpdateDaemon(_ context.Context, _ *emptyECSOutput) (*daemonOutput, error) {
	return &daemonOutput{Daemon: daemonStub{Status: statusActive}}, nil
}

// --- DescribeServiceRevisions stub ---

type serviceRevisionStub struct {
	ServiceRevisionArn string `json:"serviceRevisionArn,omitempty"`
}

type serviceRevisionsOutput struct {
	ServiceRevisions []serviceRevisionStub `json:"serviceRevisions"`
	Failures         []any                 `json:"failures"`
}

func (h *Handler) handleDescribeServiceRevisions(
	_ context.Context,
	_ *emptyECSOutput,
) (*serviceRevisionsOutput, error) {
	return &serviceRevisionsOutput{
		ServiceRevisions: []serviceRevisionStub{},
		Failures:         []any{},
	}, nil
}

// --- DiscoverPollEndpoint stub ---

type discoverPollEndpointInput struct {
	ClusterArn           string `json:"clusterArn"`
	ContainerInstanceArn string `json:"containerInstanceArn"`
}

type discoverPollEndpointOutput struct {
	Endpoint          string `json:"endpoint"`
	TelemetryEndpoint string `json:"telemetryEndpoint,omitempty"`
}

func (h *Handler) handleDiscoverPollEndpoint(
	_ context.Context,
	_ *discoverPollEndpointInput,
) (*discoverPollEndpointOutput, error) {
	return &discoverPollEndpointOutput{
		Endpoint: "https://ecs-a-1.us-east-1.amazonaws.com/",
	}, nil
}

// --- Submit state change stubs ---

type submitAttachmentStateChangesOutput struct {
	Acknowledgment string `json:"acknowledgment"`
}

func (h *Handler) handleSubmitAttachmentStateChanges(
	_ context.Context,
	_ *emptyECSOutput,
) (*submitAttachmentStateChangesOutput, error) {
	return &submitAttachmentStateChangesOutput{Acknowledgment: ackResponse}, nil
}

func (h *Handler) handleSubmitContainerStateChange(
	_ context.Context,
	_ *emptyECSOutput,
) (*submitAttachmentStateChangesOutput, error) {
	return &submitAttachmentStateChangesOutput{Acknowledgment: ackResponse}, nil
}

func (h *Handler) handleSubmitTaskStateChange(
	_ context.Context,
	_ *emptyECSOutput,
) (*submitAttachmentStateChangesOutput, error) {
	return &submitAttachmentStateChangesOutput{Acknowledgment: ackResponse}, nil
}
