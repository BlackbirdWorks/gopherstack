package ecs

// handler_stubs.go registers stub handlers for ECS SDK operations that are not
// yet fully implemented.  Each stub returns a minimal valid empty response.

import "context"

// ackResponse is the acknowledgment string returned for state-change submissions.
const ackResponse = "ACK"

type emptyECSOutput struct{}

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
