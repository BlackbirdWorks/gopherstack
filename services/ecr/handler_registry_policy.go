package ecr

import (
	"context"
)

// deleteRegistryPolicyInput is the (empty) request body for DeleteRegistryPolicy.
type deleteRegistryPolicyInput struct{}

func (h *Handler) handleDeleteRegistryPolicy(
	ctx context.Context,
	_ *deleteRegistryPolicyInput,
) (*RegistryPolicyResult, error) {
	return h.Backend.DeleteRegistryPolicy(ctx)
}

// emptyInput is the (empty) request body shared by operations that take no
// input parameters (DescribeRegistry, GetRegistryPolicy,
// GetRegistryScanningConfiguration, GetSigningConfiguration,
// DeleteSigningConfiguration, ListPullTimeUpdateExclusions).
type emptyInput struct{}

func (h *Handler) handleDescribeRegistry(
	ctx context.Context,
	_ *emptyInput,
) (*RegistryDescription, error) {
	return h.Backend.DescribeRegistry(ctx)
}

func (h *Handler) handleGetRegistryPolicy(
	ctx context.Context,
	_ *emptyInput,
) (*RegistryPolicyResult, error) {
	return h.Backend.GetRegistryPolicy(ctx)
}

type getRegistryScanningConfigurationOutput struct {
	ScanningConfiguration *RegistryScanningSettings `json:"scanningConfiguration"`
	RegistryID            string                    `json:"registryId"`
}

func (h *Handler) handleGetRegistryScanningConfiguration(
	ctx context.Context,
	_ *emptyInput,
) (*getRegistryScanningConfigurationOutput, error) {
	settings, err := h.Backend.GetRegistryScanningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &getRegistryScanningConfigurationOutput{ScanningConfiguration: settings}, nil
}

// putRegistryPolicyInput is the request body for PutRegistryPolicy.
type putRegistryPolicyInput struct {
	PolicyText string `json:"policyText"`
}

func (h *Handler) handlePutRegistryPolicy(
	ctx context.Context,
	in *putRegistryPolicyInput,
) (*RegistryPolicyResult, error) {
	return h.Backend.PutRegistryPolicy(ctx, in.PolicyText)
}

func (h *Handler) handlePutRegistryScanningConfiguration(
	ctx context.Context,
	in *RegistryScanningSettings,
) (*getRegistryScanningConfigurationOutput, error) {
	settings, err := h.Backend.PutRegistryScanningConfiguration(ctx, in)
	if err != nil {
		return nil, err
	}

	return &getRegistryScanningConfigurationOutput{ScanningConfiguration: settings}, nil
}
