package ecr

import (
	"context"
)

// deleteLifecyclePolicyInput is the request body for DeleteLifecyclePolicy.
type deleteLifecyclePolicyInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

func (h *Handler) handleDeleteLifecyclePolicy(
	ctx context.Context,
	in *deleteLifecyclePolicyInput,
) (*LifecyclePolicyResult, error) {
	return h.Backend.DeleteLifecyclePolicy(ctx, in.RepositoryName)
}

type getLifecyclePolicyInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

func (h *Handler) handleGetLifecyclePolicy(
	ctx context.Context,
	in *getLifecyclePolicyInput,
) (*LifecyclePolicyResult, error) {
	return h.Backend.GetLifecyclePolicy(ctx, in.RepositoryName)
}

func (h *Handler) handleGetLifecyclePolicyPreview(
	ctx context.Context,
	in *getLifecyclePolicyInput,
) (*LifecyclePolicyPreviewResult, error) {
	return h.Backend.GetLifecyclePolicyPreview(ctx, in.RepositoryName)
}

// putLifecyclePolicyInput is the request body for PutLifecyclePolicy.
type putLifecyclePolicyInput struct {
	RepositoryName      string `json:"repositoryName"`
	LifecyclePolicyText string `json:"lifecyclePolicyText"`
	RegistryID          string `json:"registryId,omitempty"`
}

func (h *Handler) handlePutLifecyclePolicy(
	ctx context.Context,
	in *putLifecyclePolicyInput,
) (*LifecyclePolicyResult, error) {
	return h.Backend.PutLifecyclePolicy(ctx, in.RepositoryName, in.LifecyclePolicyText)
}

func (h *Handler) handleStartLifecyclePolicyPreview(
	ctx context.Context,
	in *putLifecyclePolicyInput,
) (*LifecyclePolicyPreviewResult, error) {
	return h.Backend.StartLifecyclePolicyPreview(ctx, in.RepositoryName, in.LifecyclePolicyText)
}
