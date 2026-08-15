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
// DeleteSigningConfiguration).
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

	return &getRegistryScanningConfigurationOutput{
		ScanningConfiguration: settings,
		RegistryID:            h.Backend.AccountID(),
	}, nil
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

// putRegistryScanningConfigurationOutput is the response body for
// PutRegistryScanningConfiguration. Unlike GetRegistryScanningConfigurationOutput
// (wrapper key "scanningConfiguration" + a top-level "registryId"),
// PutRegistryScanningConfigurationOutput wraps the settings under
// "registryScanningConfiguration" and has NO registryId field at all — a
// genuinely different shape confirmed by direct diff of
// awsAwsjson11_deserializeOpDocumentPutRegistryScanningConfigurationOutput vs
// awsAwsjson11_deserializeOpDocumentGetRegistryScanningConfigurationOutput.
// Reusing getRegistryScanningConfigurationOutput here (as this handler
// previously did) emitted "scanningConfiguration", a key the real Put
// deserializer's switch has no case for — a real client would silently get a
// nil RegistryScanningConfiguration back despite a 200 response.
type putRegistryScanningConfigurationOutput struct {
	RegistryScanningConfiguration *RegistryScanningSettings `json:"registryScanningConfiguration"`
}

func (h *Handler) handlePutRegistryScanningConfiguration(
	ctx context.Context,
	in *RegistryScanningSettings,
) (*putRegistryScanningConfigurationOutput, error) {
	settings, err := h.Backend.PutRegistryScanningConfiguration(ctx, in)
	if err != nil {
		return nil, err
	}

	return &putRegistryScanningConfigurationOutput{
		RegistryScanningConfiguration: settings,
	}, nil
}
