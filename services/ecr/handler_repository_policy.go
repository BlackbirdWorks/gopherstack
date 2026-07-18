package ecr

import (
	"context"
)

type repositoryPolicyInput struct {
	PolicyText     string `json:"policyText,omitempty"`
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
	Force          bool   `json:"force,omitempty"`
}

func (h *Handler) handleGetRepositoryPolicy(
	ctx context.Context,
	in *repositoryPolicyInput,
) (*RepositoryPolicyResult, error) {
	return h.Backend.GetRepositoryPolicy(ctx, in.RepositoryName)
}

func (h *Handler) handleSetRepositoryPolicy(
	ctx context.Context,
	in *repositoryPolicyInput,
) (*RepositoryPolicyResult, error) {
	return h.Backend.SetRepositoryPolicy(ctx, in.RepositoryName, in.PolicyText)
}

func (h *Handler) handleDeleteRepositoryPolicy(
	ctx context.Context,
	in *repositoryPolicyInput,
) (*RepositoryPolicyResult, error) {
	return h.Backend.DeleteRepositoryPolicy(ctx, in.RepositoryName)
}
