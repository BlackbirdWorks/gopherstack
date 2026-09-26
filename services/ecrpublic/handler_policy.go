package ecrpublic

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) buildPolicyOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetRepositoryPolicy":    service.WrapOp(h.handleGetRepositoryPolicy),
		"SetRepositoryPolicy":    service.WrapOp(h.handleSetRepositoryPolicy),
		"DeleteRepositoryPolicy": service.WrapOp(h.handleDeleteRepositoryPolicy),
	}
}

type getRepositoryPolicyInput struct {
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
}

type repositoryPolicyOutput struct {
	PolicyText     string `json:"policyText,omitempty"`
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName,omitempty"`
}

func (h *Handler) handleGetRepositoryPolicy(
	_ context.Context, in *getRepositoryPolicyInput,
) (*repositoryPolicyOutput, error) {
	text, err := h.Backend.GetRepositoryPolicy(in.RegistryID, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &repositoryPolicyOutput{
		PolicyText:     text,
		RegistryID:     h.registryIDOrDefault(in.RegistryID),
		RepositoryName: in.RepositoryName,
	}, nil
}

type setRepositoryPolicyInput struct {
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
	PolicyText     string `json:"policyText"`
	Force          bool   `json:"force,omitempty"`
}

func (h *Handler) handleSetRepositoryPolicy(
	_ context.Context, in *setRepositoryPolicyInput,
) (*repositoryPolicyOutput, error) {
	text, err := h.Backend.SetRepositoryPolicy(in.RegistryID, in.RepositoryName, in.PolicyText)
	if err != nil {
		return nil, err
	}

	return &repositoryPolicyOutput{
		PolicyText:     text,
		RegistryID:     h.registryIDOrDefault(in.RegistryID),
		RepositoryName: in.RepositoryName,
	}, nil
}

type deleteRepositoryPolicyInput struct {
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
}

func (h *Handler) handleDeleteRepositoryPolicy(
	_ context.Context, in *deleteRepositoryPolicyInput,
) (*repositoryPolicyOutput, error) {
	text, err := h.Backend.DeleteRepositoryPolicy(in.RegistryID, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &repositoryPolicyOutput{
		PolicyText:     text,
		RegistryID:     h.registryIDOrDefault(in.RegistryID),
		RepositoryName: in.RepositoryName,
	}, nil
}
