package ecr

import (
	"context"
)

// signingConfigurationInput is both PutSigningConfiguration's request body
// and its response body: PutSigningConfigurationOutput's real shape is
// {signingConfiguration} with no registryId (confirmed against
// awsAwsjson11_deserializeOpDocumentPutSigningConfigurationOutput).
type signingConfigurationInput struct {
	SigningConfiguration *SigningSettings `json:"signingConfiguration"`
}

// signingConfigurationWithRegistryOutput is the response body shared by
// GetSigningConfiguration and DeleteSigningConfiguration. Unlike Put, their
// real output shapes both carry a top-level registryId alongside
// signingConfiguration (awsAwsjson11_deserializeOpDocumentGetSigningConfigurationOutput
// / ...Delete...Output) — a real client previously got a zero-value
// registryId here since this handler reused signingConfigurationInput
// (Put's registryId-less shape) for all three ops.
type signingConfigurationWithRegistryOutput struct {
	SigningConfiguration *SigningSettings `json:"signingConfiguration"`
	RegistryID           string           `json:"registryId"`
}

func (h *Handler) handleGetSigningConfiguration(
	ctx context.Context,
	_ *emptyInput,
) (*signingConfigurationWithRegistryOutput, error) {
	settings, err := h.Backend.GetSigningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationWithRegistryOutput{
		SigningConfiguration: settings,
		RegistryID:           h.Backend.AccountID(),
	}, nil
}

func (h *Handler) handlePutSigningConfiguration(
	ctx context.Context,
	in *signingConfigurationInput,
) (*signingConfigurationInput, error) {
	settings, err := h.Backend.PutSigningConfiguration(ctx, in.SigningConfiguration)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationInput{SigningConfiguration: settings}, nil
}

func (h *Handler) handleDeleteSigningConfiguration(
	ctx context.Context,
	_ *emptyInput,
) (*signingConfigurationWithRegistryOutput, error) {
	settings, err := h.Backend.DeleteSigningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationWithRegistryOutput{
		SigningConfiguration: settings,
		RegistryID:           h.Backend.AccountID(),
	}, nil
}

type describeImageSigningStatusOutput struct {
	ImageID         ImageIdentifier            `json:"imageId"`
	RegistryID      string                     `json:"registryId,omitempty"`
	RepositoryName  string                     `json:"repositoryName"`
	SigningStatuses []ImageSigningStatusRecord `json:"signingStatuses"`
}

func (h *Handler) handleDescribeImageSigningStatus(
	ctx context.Context,
	in *imageInput,
) (*describeImageSigningStatusOutput, error) {
	result, err := h.Backend.DescribeImageSigningStatus(ctx, in.RepositoryName, in.ImageID)
	if err != nil {
		return nil, err
	}

	return &describeImageSigningStatusOutput{
		ImageID:         result.ImageID,
		RegistryID:      result.RegistryID,
		RepositoryName:  result.RepositoryName,
		SigningStatuses: result.SigningStatuses,
	}, nil
}
