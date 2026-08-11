package codeconnections

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
	Tags             []tag  `json:"Tags"`
}

// repositoryLinkItem is the wire shape of RepositoryLinkInfo
// (aws-sdk-go-v2/service/codeconnections@v1.13.4 types.RepositoryLinkInfo).
// It deliberately has NO Tags field: the real RepositoryLinkInfo struct has
// no Tags member at all -- tags for a repository link are only ever returned
// via ListTagsForResource. A previous audit pass added a Tags field here
// that does not exist on the real wire type; removed.
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
		return nil, fmt.Errorf("%w: ConnectionArn is required", ErrValidation)
	}

	if in.OwnerID == "" {
		return nil, fmt.Errorf("%w: OwnerId is required", ErrValidation)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: RepositoryName is required", ErrValidation)
	}

	link, err := h.Backend.CreateRepositoryLink(
		ctx,
		in.ConnectionArn,
		in.OwnerID,
		in.RepositoryName,
		in.EncryptionKeyArn,
		tagsFromArray(in.Tags),
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
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
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

func (h *Handler) handleDeleteRepositoryLink(
	ctx context.Context,
	in *deleteRepositoryLinkInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteRepositoryLink(ctx, in.RepositoryLinkID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
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

type listRepositoryLinksInput struct {
	NextToken  *string `json:"NextToken"`
	MaxResults *int32  `json:"MaxResults"`
}

type listRepositoryLinksOutput struct {
	NextToken       *string              `json:"NextToken,omitempty"`
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

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(items, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listRepositoryLinksOutput{RepositoryLinks: p.Data, NextToken: nextToken}, nil
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
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	link, err := h.Backend.UpdateRepositoryLink(
		ctx,
		in.RepositoryLinkID,
		in.ConnectionArn,
		in.EncryptionKeyArn,
	)
	if err != nil {
		return nil, err
	}

	return &updateRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}
