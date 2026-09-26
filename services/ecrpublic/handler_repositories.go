package ecrpublic

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) buildRepositoryOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateRepository":     service.WrapOp(h.handleCreateRepository),
		"DescribeRepositories": service.WrapOp(h.handleDescribeRepositories),
		"DeleteRepository":     service.WrapOp(h.handleDeleteRepository),
	}
}

type createRepositoryInput struct {
	CatalogData    *CatalogDataInputWire `json:"catalogData,omitempty"`
	RepositoryName string                `json:"repositoryName"`
	Tags           []TagWire             `json:"tags,omitempty"`
}

type createRepositoryOutput struct {
	Repository  RepositoryWire  `json:"repository"`
	CatalogData CatalogDataWire `json:"catalogData"`
}

func (h *Handler) handleCreateRepository(
	_ context.Context, in *createRepositoryInput,
) (*createRepositoryOutput, error) {
	repo, err := h.Backend.CreateRepository(
		in.RepositoryName,
		toCatalogDataInput(in.CatalogData),
		tagsFromWire(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createRepositoryOutput{
		CatalogData: toCatalogDataWire(&repo.CatalogData),
		Repository:  toRepositoryWire(repo),
	}, nil
}

type describeRepositoriesInput struct {
	NextToken       string   `json:"nextToken,omitempty"`
	RegistryID      string   `json:"registryId,omitempty"`
	RepositoryNames []string `json:"repositoryNames,omitempty"`
	MaxResults      int32    `json:"maxResults,omitempty"`
}

type describeRepositoriesOutput struct {
	NextToken    string           `json:"nextToken,omitempty"`
	Repositories []RepositoryWire `json:"repositories"`
}

func (h *Handler) handleDescribeRepositories(
	_ context.Context, in *describeRepositoriesInput,
) (*describeRepositoriesOutput, error) {
	repos, err := h.Backend.DescribeRepositories(in.RegistryID, in.RepositoryNames)
	if err != nil {
		return nil, err
	}

	out := make([]RepositoryWire, 0, len(repos))
	for _, r := range repos {
		out = append(out, toRepositoryWire(r))
	}

	return &describeRepositoriesOutput{Repositories: out}, nil
}

type deleteRepositoryInput struct {
	RegistryID     string `json:"registryId,omitempty"`
	RepositoryName string `json:"repositoryName"`
	Force          bool   `json:"force,omitempty"`
}

type deleteRepositoryOutput struct {
	Repository RepositoryWire `json:"repository"`
}

func (h *Handler) handleDeleteRepository(
	_ context.Context, in *deleteRepositoryInput,
) (*deleteRepositoryOutput, error) {
	repo, err := h.Backend.DeleteRepository(in.RegistryID, in.RepositoryName, in.Force)
	if err != nil {
		return nil, err
	}

	return &deleteRepositoryOutput{Repository: toRepositoryWire(repo)}, nil
}
