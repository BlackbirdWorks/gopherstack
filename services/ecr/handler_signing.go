package ecr

import (
	"context"
)

type signingConfigurationInput struct {
	SigningConfiguration *SigningSettings `json:"signingConfiguration"`
}

func (h *Handler) handleGetSigningConfiguration(
	ctx context.Context,
	_ *emptyInput,
) (*signingConfigurationInput, error) {
	settings, err := h.Backend.GetSigningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationInput{SigningConfiguration: settings}, nil
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
) (*signingConfigurationInput, error) {
	settings, err := h.Backend.DeleteSigningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationInput{SigningConfiguration: settings}, nil
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
