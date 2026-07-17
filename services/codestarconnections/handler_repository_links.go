package codestarconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createRepositoryLinkInput struct {
	ConnectionArn    string `json:"ConnectionArn"`
	OwnerID          string `json:"OwnerId"`
	RepositoryName   string `json:"RepositoryName"`
	EncryptionKeyArn string `json:"EncryptionKeyArn"`
}

type repositoryLinkItem struct {
	ConnectionArn     string `json:"ConnectionArn"`
	EncryptionKeyArn  string `json:"EncryptionKeyArn,omitempty"`
	OwnerID           string `json:"OwnerId"`
	ProviderType      string `json:"ProviderType"`
	RepositoryLinkArn string `json:"RepositoryLinkArn"`
	RepositoryLinkID  string `json:"RepositoryLinkId"`
	RepositoryName    string `json:"RepositoryName"`
}

type createRepositoryLinkOutput struct {
	RepositoryLinkInfo repositoryLinkItem `json:"RepositoryLinkInfo"`
}

func (h *Handler) handleCreateRepositoryLink(
	ctx context.Context,
	in *createRepositoryLinkInput,
) (*createRepositoryLinkOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	if in.OwnerID == "" {
		return nil, fmt.Errorf("%w: OwnerId is required", errInvalidRequest)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: RepositoryName is required", errInvalidRequest)
	}

	link, err := h.Backend.CreateRepositoryLink(
		ctx, in.ConnectionArn, in.OwnerID, in.RepositoryName, in.EncryptionKeyArn,
	)
	if err != nil {
		return nil, err
	}

	return &createRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}

type getRepositoryLinkInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
}

type getRepositoryLinkOutput struct {
	RepositoryLinkInfo repositoryLinkItem `json:"RepositoryLinkInfo"`
}

func (h *Handler) handleGetRepositoryLink(
	ctx context.Context,
	in *getRepositoryLinkInput,
) (*getRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	link, err := h.Backend.GetRepositoryLink(ctx, in.RepositoryLinkID)
	if err != nil {
		return nil, err
	}

	return &getRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}

type deleteRepositoryLinkInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
}

type deleteRepositoryLinkOutput struct{}

func (h *Handler) handleDeleteRepositoryLink(
	ctx context.Context,
	in *deleteRepositoryLinkInput,
) (*deleteRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteRepositoryLink(ctx, in.RepositoryLinkID); err != nil {
		return nil, err
	}

	return &deleteRepositoryLinkOutput{}, nil
}

type listRepositoryLinksInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listRepositoryLinksOutput struct {
	NextToken       string               `json:"NextToken,omitempty"`
	RepositoryLinks []repositoryLinkItem `json:"RepositoryLinks"`
}

func (h *Handler) handleListRepositoryLinks(
	ctx context.Context,
	in *listRepositoryLinksInput,
) (*listRepositoryLinksOutput, error) {
	links := h.Backend.ListRepositoryLinks(ctx)

	items := make([]repositoryLinkItem, len(links))
	for i, link := range links {
		items[i] = repositoryLinkToItem(link)
	}

	p := page.New(items, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listRepositoryLinksOutput{RepositoryLinks: p.Data, NextToken: p.Next}, nil
}

func repositoryLinkToItem(link *RepositoryLink) repositoryLinkItem {
	return repositoryLinkItem{
		ConnectionArn:     link.ConnectionArn,
		OwnerID:           link.OwnerID,
		ProviderType:      link.ProviderType,
		RepositoryLinkArn: link.RepositoryLinkArn,
		RepositoryLinkID:  link.RepositoryLinkID,
		RepositoryName:    link.RepositoryName,
		EncryptionKeyArn:  link.EncryptionKeyArn,
	}
}

type updateRepositoryLinkInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	ConnectionArn    string `json:"ConnectionArn"`
	EncryptionKeyArn string `json:"EncryptionKeyArn"`
}

type updateRepositoryLinkOutput struct {
	RepositoryLinkInfo repositoryLinkItem `json:"RepositoryLinkInfo"`
}

func (h *Handler) handleUpdateRepositoryLink(
	ctx context.Context,
	in *updateRepositoryLinkInput,
) (*updateRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	link, err := h.Backend.UpdateRepositoryLink(ctx, in.RepositoryLinkID, in.ConnectionArn, in.EncryptionKeyArn)
	if err != nil {
		return nil, err
	}

	return &updateRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}
