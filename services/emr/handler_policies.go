package emr

import (
	"context"
)

// --- GetAutoTerminationPolicy ---

type getAutoTerminationPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

// getAutoTerminationPolicyOutput mirrors GetAutoTerminationPolicyOutput.
// AutoTerminationPolicy is a pointer with omitempty: real AWS omits the
// field entirely (rather than sending a zero-valued object) when no policy
// is attached, so a real client's output.AutoTerminationPolicy is nil in
// that case -- sending {"IdleTimeout":0} would make it non-nil instead.
type getAutoTerminationPolicyOutput struct {
	AutoTerminationPolicy *AutoTerminationPolicy `json:"AutoTerminationPolicy,omitempty"`
}

func (h *Handler) handleGetAutoTerminationPolicy(
	ctx context.Context,
	in *getAutoTerminationPolicyInput,
) (*getAutoTerminationPolicyOutput, error) {
	policy, err := h.Backend.GetAutoTerminationPolicy(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &getAutoTerminationPolicyOutput{AutoTerminationPolicy: policy}, nil
}

// --- GetManagedScalingPolicy ---

type getManagedScalingPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

// getManagedScalingPolicyOutput mirrors GetManagedScalingPolicyOutput; see
// getAutoTerminationPolicyOutput for why ManagedScalingPolicy must be a
// pointer with omitempty rather than a zero-valued struct.
type getManagedScalingPolicyOutput struct {
	ManagedScalingPolicy *ManagedScalingPolicy `json:"ManagedScalingPolicy,omitempty"`
}

func (h *Handler) handleGetManagedScalingPolicy(
	ctx context.Context,
	in *getManagedScalingPolicyInput,
) (*getManagedScalingPolicyOutput, error) {
	policy, err := h.Backend.GetManagedScalingPolicy(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &getManagedScalingPolicyOutput{ManagedScalingPolicy: policy}, nil
}

// --- PutManagedScalingPolicy ---

type putManagedScalingPolicyInput struct {
	ClusterID            string               `json:"ClusterId"`
	ManagedScalingPolicy ManagedScalingPolicy `json:"ManagedScalingPolicy"`
}

type putManagedScalingPolicyOutput struct{}

func (h *Handler) handlePutManagedScalingPolicy(
	ctx context.Context,
	in *putManagedScalingPolicyInput,
) (*putManagedScalingPolicyOutput, error) {
	if err := h.Backend.PutManagedScalingPolicy(ctx, in.ClusterID, in.ManagedScalingPolicy); err != nil {
		return nil, err
	}

	return &putManagedScalingPolicyOutput{}, nil
}

// --- RemoveManagedScalingPolicy ---

type removeManagedScalingPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type removeManagedScalingPolicyOutput struct{}

func (h *Handler) handleRemoveManagedScalingPolicy(
	ctx context.Context,
	in *removeManagedScalingPolicyInput,
) (*removeManagedScalingPolicyOutput, error) {
	if err := h.Backend.RemoveManagedScalingPolicy(ctx, in.ClusterID); err != nil {
		return nil, err
	}

	return &removeManagedScalingPolicyOutput{}, nil
}

// --- PutAutoTerminationPolicy ---

type putAutoTerminationPolicyInput struct {
	ClusterID             string                `json:"ClusterId"`
	AutoTerminationPolicy AutoTerminationPolicy `json:"AutoTerminationPolicy"`
}

type putAutoTerminationPolicyOutput struct{}

func (h *Handler) handlePutAutoTerminationPolicy(
	ctx context.Context,
	in *putAutoTerminationPolicyInput,
) (*putAutoTerminationPolicyOutput, error) {
	if err := h.Backend.PutAutoTerminationPolicy(ctx, in.ClusterID, in.AutoTerminationPolicy); err != nil {
		return nil, err
	}

	return &putAutoTerminationPolicyOutput{}, nil
}

// --- RemoveAutoTerminationPolicy ---

type removeAutoTerminationPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type removeAutoTerminationPolicyOutput struct{}

func (h *Handler) handleRemoveAutoTerminationPolicy(
	ctx context.Context,
	in *removeAutoTerminationPolicyInput,
) (*removeAutoTerminationPolicyOutput, error) {
	if err := h.Backend.RemoveAutoTerminationPolicy(ctx, in.ClusterID); err != nil {
		return nil, err
	}

	return &removeAutoTerminationPolicyOutput{}, nil
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
	ctx context.Context,
	in *putAutoScalingPolicyInput,
) (*putAutoScalingPolicyOutput, error) {
	detail, clusterARN, groupID, err := h.Backend.PutAutoScalingPolicy(
		ctx,
		in.ClusterID,
		in.InstanceGroupID,
		in.AutoScalingPolicy,
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

// --- RemoveAutoScalingPolicy ---

type removeAutoScalingPolicyInput struct {
	ClusterID       string `json:"ClusterId"`
	InstanceGroupID string `json:"InstanceGroupId"`
}

type removeAutoScalingPolicyOutput struct{}

func (h *Handler) handleRemoveAutoScalingPolicy(
	ctx context.Context,
	in *removeAutoScalingPolicyInput,
) (*removeAutoScalingPolicyOutput, error) {
	if err := h.Backend.RemoveAutoScalingPolicy(ctx, in.ClusterID, in.InstanceGroupID); err != nil {
		return nil, err
	}

	return &removeAutoScalingPolicyOutput{}, nil
}

// --- GetBlockPublicAccessConfiguration ---

type getBlockPublicAccessConfigurationInput struct{}

type getBlockPublicAccessConfigurationOutput struct {
	BlockPublicAccessConfigurationMetadata blockPublicAccessConfigurationMetadata `json:"BlockPublicAccessConfigurationMetadata"` //nolint:lll // AWS JSON key is unavoidably long
	BlockPublicAccessConfiguration         BlockPublicAccessConfiguration         `json:"BlockPublicAccessConfiguration"`
}

// blockPublicAccessConfigurationMetadata mirrors
// BlockPublicAccessConfigurationMetadata; CreationDateTime is epoch seconds
// (float64), matching the EMR awsjson1.1 wire format -- the real SDK
// deserializer parses it with smithytime.ParseEpochSeconds and rejects
// RFC3339 strings.
type blockPublicAccessConfigurationMetadata struct {
	CreatedByArn     string  `json:"CreatedByArn"`
	CreationDateTime float64 `json:"CreationDateTime"`
}

func (h *Handler) handleGetBlockPublicAccessConfiguration(
	ctx context.Context,
	_ *getBlockPublicAccessConfigurationInput,
) (*getBlockPublicAccessConfigurationOutput, error) {
	cfg, meta := h.Backend.GetBlockPublicAccessConfiguration(ctx)

	return &getBlockPublicAccessConfigurationOutput{
		BlockPublicAccessConfiguration: cfg,
		BlockPublicAccessConfigurationMetadata: blockPublicAccessConfigurationMetadata{
			CreationDateTime: meta.CreationDateTime,
			CreatedByArn:     meta.CreatedByArn,
		},
	}, nil
}

// --- PutBlockPublicAccessConfiguration ---

type putBlockPublicAccessConfigurationInput struct {
	BlockPublicAccessConfiguration BlockPublicAccessConfiguration `json:"BlockPublicAccessConfiguration"`
}

type putBlockPublicAccessConfigurationOutput struct{}

func (h *Handler) handlePutBlockPublicAccessConfiguration(
	ctx context.Context,
	in *putBlockPublicAccessConfigurationInput,
) (*putBlockPublicAccessConfigurationOutput, error) {
	if err := h.Backend.PutBlockPublicAccessConfiguration(ctx, in.BlockPublicAccessConfiguration); err != nil {
		return nil, err
	}

	return &putBlockPublicAccessConfigurationOutput{}, nil
}
