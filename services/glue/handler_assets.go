package glue

import (
	"context"
	"fmt"
)

// maxAssetListResults is this backend's MaxResults upper bound for the
// asset-catalog list/search operations, matching maxGlossaryListResults.
const maxAssetListResults = 100

type putAssetTypeInput struct {
	Forms map[string]AssetTypeFormReference `json:"Forms"`
	Name  string                            `json:"Name"`
}

func (h *Handler) handlePutAssetType(_ context.Context, in *putAssetTypeInput) (*AssetType, error) {
	return h.Backend.PutAssetType(in.Name, in.Forms)
}

type getAssetTypeInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleGetAssetType(_ context.Context, in *getAssetTypeInput) (*AssetType, error) {
	return h.Backend.GetAssetType(in.Identifier)
}

type deleteAssetTypeInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleDeleteAssetType(_ context.Context, in *deleteAssetTypeInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteAssetType(in.Identifier); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listAssetTypesInput struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listAssetTypesOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Items     []*AssetTypeItem `json:"Items"`
}

func (h *Handler) handleListAssetTypes(_ context.Context, in *listAssetTypesInput) (*listAssetTypesOutput, error) {
	types := h.Backend.ListAssetTypes()

	items := make([]*AssetTypeItem, len(types))
	for i, at := range types {
		items[i] = &AssetTypeItem{ID: at.ID, Name: at.Name}
	}

	limit := maxAssetListResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(items, in.NextToken, limit)

	return &listAssetTypesOutput{Items: page, NextToken: next}, nil
}

type putAssetInput struct {
	Forms       map[string]AssetFormEntry `json:"Forms"`
	AssetTypeID string                    `json:"AssetTypeId"`
	Identifier  string                    `json:"Identifier"`
	Name        string                    `json:"Name"`
	Description string                    `json:"Description,omitempty"`
}

// putAssetOutput matches PutAssetOutput's fields exactly -- notably lacking
// Asset's AssetTypeId/Attachments/GlossaryTerms/IterableForms/UpdatedAt, so it
// is built explicitly rather than reusing the Asset type.
type putAssetOutput struct {
	Forms       map[string]AssetFormEntry `json:"Forms,omitempty"`
	ID          string                    `json:"Id"`
	Name        string                    `json:"Name"`
	Description string                    `json:"Description,omitempty"`
	CreatedAt   float64                   `json:"CreatedAt,omitempty"`
}

func (h *Handler) handlePutAsset(_ context.Context, in *putAssetInput) (*putAssetOutput, error) {
	a, err := h.Backend.PutAsset(in.Identifier, in.Name, in.Description, in.AssetTypeID, in.Forms)
	if err != nil {
		return nil, err
	}

	return &putAssetOutput{
		ID: a.ID, Name: a.Name, Description: a.Description, Forms: a.Forms, CreatedAt: a.CreatedAt,
	}, nil
}

type getAssetInput struct {
	Identifier string `json:"Identifier"`
}

// getAssetOutput matches GetAssetOutput's fields exactly, field-diffed
// against the SDK's own type.
type getAssetOutput struct {
	Attachments   map[string]AssetFormEntry    `json:"Attachments,omitempty"`
	Forms         map[string]AssetFormEntry    `json:"Forms,omitempty"`
	IterableForms map[string]IterableFormEntry `json:"IterableForms,omitempty"`
	AssetTypeID   string                       `json:"AssetTypeId"`
	ID            string                       `json:"Id"`
	Name          string                       `json:"Name,omitempty"`
	Description   string                       `json:"Description,omitempty"`
	GlossaryTerms []string                     `json:"GlossaryTerms,omitempty"`
	CreatedAt     float64                      `json:"CreatedAt,omitempty"`
	UpdatedAt     float64                      `json:"UpdatedAt,omitempty"`
}

func (h *Handler) handleGetAsset(_ context.Context, in *getAssetInput) (*getAssetOutput, error) {
	a, err := h.Backend.GetAsset(in.Identifier)
	if err != nil {
		return nil, err
	}

	return &getAssetOutput{
		Attachments:   a.Attachments,
		Forms:         a.Forms,
		IterableForms: a.IterableForms,
		GlossaryTerms: a.GlossaryTerms,
		AssetTypeID:   a.AssetTypeID,
		ID:            a.ID,
		Name:          a.Name,
		Description:   a.Description,
		CreatedAt:     a.CreatedAt,
		UpdatedAt:     a.UpdatedAt,
	}, nil
}

type updateAssetInput struct {
	Name        *string `json:"Name,omitempty"`
	Description *string `json:"Description,omitempty"`
	Identifier  string  `json:"Identifier"`
}

// updateAssetOutput matches UpdateAssetOutput's fields exactly.
type updateAssetOutput struct {
	ID          string  `json:"Id"`
	Description string  `json:"Description,omitempty"`
	Name        string  `json:"Name,omitempty"`
	UpdatedAt   float64 `json:"UpdatedAt,omitempty"`
}

func (h *Handler) handleUpdateAsset(_ context.Context, in *updateAssetInput) (*updateAssetOutput, error) {
	a, err := h.Backend.UpdateAsset(in.Identifier, in.Name, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateAssetOutput{ID: a.ID, Description: a.Description, Name: a.Name, UpdatedAt: a.UpdatedAt}, nil
}

type deleteAssetInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleDeleteAsset(_ context.Context, in *deleteAssetInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteAsset(in.Identifier); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type searchAssetsInput struct {
	FilterClause *searchFilterClause `json:"FilterClause,omitempty"`
	Sort         *searchSort         `json:"Sort,omitempty"`
	MaxResults   *int32              `json:"MaxResults,omitempty"`
	NextToken    string              `json:"NextToken,omitempty"`
	SearchText   string              `json:"SearchText,omitempty"`
}

type searchAssetsOutput struct {
	NextToken string              `json:"NextToken,omitempty"`
	Items     []*SearchResultItem `json:"Items"`
}

func (h *Handler) handleSearchAssets(_ context.Context, in *searchAssetsInput) (*searchAssetsOutput, error) {
	if in.SearchText == "" && in.FilterClause == nil {
		return nil, fmt.Errorf("%w: at least one of SearchText or FilterClause must be provided", ErrValidation)
	}

	sortAttr, sortDesc := "", false
	if in.Sort != nil {
		sortAttr = in.Sort.Attribute
		sortDesc = in.Sort.Order == "DESCENDING"
	}

	assets := h.Backend.SearchAssets(in.SearchText, in.FilterClause, sortAttr, sortDesc)

	items := make([]*SearchResultItem, len(assets))
	for i, a := range assets {
		items[i] = &SearchResultItem{
			AssetDescription: a.Description,
			AssetName:        a.Name,
			AssetTypeID:      a.AssetTypeID,
			ID:               a.ID,
			UpdatedAt:        a.UpdatedAt,
		}
	}

	limit := maxAssetListResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(items, in.NextToken, limit)

	return &searchAssetsOutput{Items: page, NextToken: next}, nil
}

type putAttachmentInput struct {
	AssetIdentifier  string `json:"AssetIdentifier"`
	AttachmentName   string `json:"AttachmentName"`
	Content          string `json:"Content"`
	FormTypeID       string `json:"FormTypeId"`
	ItemIdentifier   string `json:"ItemIdentifier,omitempty"`
	IterableFormName string `json:"IterableFormName,omitempty"`
}

type putAttachmentOutput struct {
	AssetIdentifier  string `json:"AssetIdentifier,omitempty"`
	AttachmentName   string `json:"AttachmentName,omitempty"`
	FormTypeID       string `json:"FormTypeId,omitempty"`
	ItemIdentifier   string `json:"ItemIdentifier,omitempty"`
	IterableFormName string `json:"IterableFormName,omitempty"`
}

func (h *Handler) handlePutAttachment(_ context.Context, in *putAttachmentInput) (*putAttachmentOutput, error) {
	err := h.Backend.PutAttachment(
		in.AssetIdentifier, in.AttachmentName, in.FormTypeID, in.Content, in.IterableFormName, in.ItemIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return &putAttachmentOutput{
		AssetIdentifier:  in.AssetIdentifier,
		AttachmentName:   in.AttachmentName,
		FormTypeID:       in.FormTypeID,
		ItemIdentifier:   in.ItemIdentifier,
		IterableFormName: in.IterableFormName,
	}, nil
}

type deleteAttachmentInput struct {
	AssetIdentifier  string `json:"AssetIdentifier"`
	AttachmentName   string `json:"AttachmentName"`
	ItemIdentifier   string `json:"ItemIdentifier,omitempty"`
	IterableFormName string `json:"IterableFormName,omitempty"`
}

type deleteAttachmentOutput struct {
	AssetIdentifier string `json:"AssetIdentifier,omitempty"`
}

func (h *Handler) handleDeleteAttachment(
	_ context.Context,
	in *deleteAttachmentInput,
) (*deleteAttachmentOutput, error) {
	err := h.Backend.DeleteAttachment(in.AssetIdentifier, in.AttachmentName, in.IterableFormName, in.ItemIdentifier)
	if err != nil {
		return nil, err
	}

	return &deleteAttachmentOutput{AssetIdentifier: in.AssetIdentifier}, nil
}

type batchGetIterableFormsInput struct {
	AssetIdentifier  string   `json:"AssetIdentifier"`
	IterableFormName string   `json:"IterableFormName"`
	ItemIdentifiers  []string `json:"ItemIdentifiers"`
}

type batchGetIterableFormsOutput struct {
	Errors []ItemErrorDetail   `json:"Errors,omitempty"`
	Items  []*IterableFormItem `json:"Items"`
}

func (h *Handler) handleBatchGetIterableForms(
	_ context.Context,
	in *batchGetIterableFormsInput,
) (*batchGetIterableFormsOutput, error) {
	items, errs, err := h.Backend.BatchGetIterableForms(in.AssetIdentifier, in.IterableFormName, in.ItemIdentifiers)
	if err != nil {
		return nil, err
	}

	return &batchGetIterableFormsOutput{Items: items, Errors: errs}, nil
}

type listIterableFormsInput struct {
	MaxResults       *int32 `json:"MaxResults,omitempty"`
	AssetIdentifier  string `json:"AssetIdentifier"`
	IterableFormName string `json:"IterableFormName"`
	NextToken        string `json:"NextToken,omitempty"`
}

type listIterableFormsOutput struct {
	NextToken string                  `json:"NextToken,omitempty"`
	Items     []*IterableFormListItem `json:"Items"`
}

func (h *Handler) handleListIterableForms(
	_ context.Context,
	in *listIterableFormsInput,
) (*listIterableFormsOutput, error) {
	items, err := h.Backend.ListIterableForms(in.AssetIdentifier, in.IterableFormName)
	if err != nil {
		return nil, err
	}

	limit := maxAssetListResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(items, in.NextToken, limit)

	return &listIterableFormsOutput{Items: page, NextToken: next}, nil
}
