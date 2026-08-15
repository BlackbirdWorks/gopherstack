package ecs

// handler_agent_ops.go registers handlers for ECS operations that describe
// service revisions and support the internal ECS agent contract (poll
// endpoint discovery and task/container/attachment state-change submissions).

import "context"

// ackResponse is the acknowledgment string returned for state-change submissions.
const ackResponse = "ACK"

// ackOutput is the shared output shape for the three Submit*StateChange(s)
// operations, all of which return only an acknowledgment string.
type ackOutput struct {
	Acknowledgment string `json:"acknowledgment"`
}

// --- DescribeServiceRevisions ---

type describeServiceRevisionsInput struct {
	ServiceRevisionArns []string `json:"serviceRevisionArns"`
}

type serviceRevisionsOutput struct {
	ServiceRevisions []ServiceRevision `json:"serviceRevisions"`
	Failures         []Failure         `json:"failures"`
}

func (h *Handler) handleDescribeServiceRevisions(
	_ context.Context,
	in *describeServiceRevisionsInput,
) (*serviceRevisionsOutput, error) {
	revisions, failures := h.Backend.DescribeServiceRevisions(in.ServiceRevisionArns)

	return &serviceRevisionsOutput{
		ServiceRevisions: revisions,
		Failures:         failures,
	}, nil
}

// --- DiscoverPollEndpoint ---

// discoverPollEndpointInput mirrors DiscoverPollEndpointInput (ecs v1.90.0,
// api_op_DiscoverPollEndpoint.go): Cluster/ContainerInstance, wire keys
// "cluster"/"containerInstance" (serializers.go:10302-10314) — not
// ClusterArn/ContainerInstanceArn, and not necessarily ARNs (the short name
// is also accepted). Currently unused: the handler discards its input.
type discoverPollEndpointInput struct {
	Cluster           string `json:"cluster"`
	ContainerInstance string `json:"containerInstance"`
}

type discoverPollEndpointOutput struct {
	Endpoint               string `json:"endpoint"`
	ServiceConnectEndpoint string `json:"serviceConnectEndpoint,omitempty"`
	TelemetryEndpoint      string `json:"telemetryEndpoint,omitempty"`
}

func (h *Handler) handleDiscoverPollEndpoint(
	_ context.Context,
	_ *discoverPollEndpointInput,
) (*discoverPollEndpointOutput, error) {
	endpoint, serviceConnectEndpoint, telemetryEndpoint := h.Backend.DiscoverPollEndpoint()

	return &discoverPollEndpointOutput{
		Endpoint:               endpoint,
		ServiceConnectEndpoint: serviceConnectEndpoint,
		TelemetryEndpoint:      telemetryEndpoint,
	}, nil
}

// --- Submit state-change operations ---

func (h *Handler) handleSubmitAttachmentStateChanges(
	_ context.Context,
	in *submitAttachmentStateChangesInput,
) (*ackOutput, error) {
	if err := h.Backend.SubmitAttachmentStateChanges(in.Cluster, in.Attachments); err != nil {
		return nil, err
	}

	return &ackOutput{Acknowledgment: ackResponse}, nil
}

type submitAttachmentStateChangesInput struct {
	Cluster     string                  `json:"cluster,omitempty"`
	Attachments []AttachmentStateChange `json:"attachments"`
}

func (h *Handler) handleSubmitContainerStateChange(
	_ context.Context,
	in *SubmitContainerStateChangeInput,
) (*ackOutput, error) {
	if err := h.Backend.SubmitContainerStateChange(*in); err != nil {
		return nil, err
	}

	return &ackOutput{Acknowledgment: ackResponse}, nil
}

func (h *Handler) handleSubmitTaskStateChange(
	_ context.Context,
	in *SubmitTaskStateChangeInput,
) (*ackOutput, error) {
	if err := h.Backend.SubmitTaskStateChange(*in); err != nil {
		return nil, err
	}

	return &ackOutput{Acknowledgment: ackResponse}, nil
}
