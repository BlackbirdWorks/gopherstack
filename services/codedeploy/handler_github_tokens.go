package codedeploy

import (
	"context"
	"fmt"
)

type deleteGitHubAccountTokenInput struct {
	TokenName string `json:"tokenName"`
}

type deleteGitHubAccountTokenOutput struct {
	TokenName string `json:"tokenName"`
}

func (h *Handler) handleDeleteGitHubAccountToken(
	_ context.Context,
	in *deleteGitHubAccountTokenInput,
) (*deleteGitHubAccountTokenOutput, error) {
	if in.TokenName == "" {
		return nil, fmt.Errorf("%w: tokenName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteGitHubAccountToken(in.TokenName); err != nil {
		return nil, err
	}

	return &deleteGitHubAccountTokenOutput{TokenName: in.TokenName}, nil
}

type listGitHubAccountTokenNamesInput struct{}

type listGitHubAccountTokenNamesOutput struct {
	TokenNameList []string `json:"tokenNameList"`
}

func (h *Handler) handleListGitHubAccountTokenNames(
	_ context.Context,
	_ *listGitHubAccountTokenNamesInput,
) (*listGitHubAccountTokenNamesOutput, error) {
	return &listGitHubAccountTokenNamesOutput{TokenNameList: h.Backend.ListGitHubAccountTokenNames()}, nil
}

type deleteResourcesByExternalIDInput struct {
	ExternalID string `json:"externalId"`
}

type deleteResourcesByExternalIDOutput struct{}

func (h *Handler) handleDeleteResourcesByExternalID(
	_ context.Context,
	_ *deleteResourcesByExternalIDInput,
) (*deleteResourcesByExternalIDOutput, error) {
	return &deleteResourcesByExternalIDOutput{}, nil
}
