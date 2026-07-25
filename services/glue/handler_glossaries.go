package glue

import (
	"context"
	"fmt"
)

// maxGlossaryListResults is this backend's MaxResults upper bound for
// ListGlossaries/ListGlossaryTerms, matching the 100 already used for
// GetDatabases (see maxGetDatabasesResults in handler_databases.go) -- AWS
// does not document a fixed limit for these newer operations, so the
// existing package-wide default is reused rather than inventing a new one.
const maxGlossaryListResults = 100

type createGlossaryInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
}

func (h *Handler) handleCreateGlossary(_ context.Context, in *createGlossaryInput) (*Glossary, error) {
	return h.Backend.CreateGlossary(in.Name, in.Description)
}

type getGlossaryInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleGetGlossary(_ context.Context, in *getGlossaryInput) (*Glossary, error) {
	return h.Backend.GetGlossary(in.Identifier)
}

type updateGlossaryInput struct {
	Name        *string `json:"Name,omitempty"`
	Description *string `json:"Description,omitempty"`
	Identifier  string  `json:"Identifier"`
}

func (h *Handler) handleUpdateGlossary(_ context.Context, in *updateGlossaryInput) (*Glossary, error) {
	return h.Backend.UpdateGlossary(in.Identifier, in.Name, in.Description)
}

type deleteGlossaryInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleDeleteGlossary(_ context.Context, in *deleteGlossaryInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteGlossary(in.Identifier); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listGlossariesInput struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listGlossariesOutput struct {
	NextToken string      `json:"NextToken,omitempty"`
	Items     []*Glossary `json:"Items"`
}

func (h *Handler) handleListGlossaries(_ context.Context, in *listGlossariesInput) (*listGlossariesOutput, error) {
	limit := maxGlossaryListResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(h.Backend.ListGlossaries(), in.NextToken, limit)

	return &listGlossariesOutput{Items: page, NextToken: next}, nil
}

type createGlossaryTermInput struct {
	GlossaryIdentifier string `json:"GlossaryIdentifier"`
	Name               string `json:"Name"`
	ShortDescription   string `json:"ShortDescription,omitempty"`
	LongDescription    string `json:"LongDescription,omitempty"`
}

func (h *Handler) handleCreateGlossaryTerm(_ context.Context, in *createGlossaryTermInput) (*GlossaryTerm, error) {
	return h.Backend.CreateGlossaryTerm(in.GlossaryIdentifier, in.Name, in.ShortDescription, in.LongDescription)
}

type getGlossaryTermInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleGetGlossaryTerm(_ context.Context, in *getGlossaryTermInput) (*GlossaryTerm, error) {
	return h.Backend.GetGlossaryTerm(in.Identifier)
}

type updateGlossaryTermInput struct {
	Name             *string `json:"Name,omitempty"`
	ShortDescription *string `json:"ShortDescription,omitempty"`
	LongDescription  *string `json:"LongDescription,omitempty"`
	Identifier       string  `json:"Identifier"`
}

func (h *Handler) handleUpdateGlossaryTerm(_ context.Context, in *updateGlossaryTermInput) (*GlossaryTerm, error) {
	return h.Backend.UpdateGlossaryTerm(in.Identifier, in.Name, in.ShortDescription, in.LongDescription)
}

type deleteGlossaryTermInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleDeleteGlossaryTerm(_ context.Context, in *deleteGlossaryTermInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteGlossaryTerm(in.Identifier); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listGlossaryTermsInput struct {
	MaxResults         *int32 `json:"MaxResults,omitempty"`
	GlossaryIdentifier string `json:"GlossaryIdentifier"`
	NextToken          string `json:"NextToken,omitempty"`
}

type listGlossaryTermsOutput struct {
	NextToken string              `json:"NextToken,omitempty"`
	Items     []*GlossaryTermItem `json:"Items"`
}

func (h *Handler) handleListGlossaryTerms(
	_ context.Context,
	in *listGlossaryTermsInput,
) (*listGlossaryTermsOutput, error) {
	terms, err := h.Backend.ListGlossaryTerms(in.GlossaryIdentifier)
	if err != nil {
		return nil, err
	}

	items := make([]*GlossaryTermItem, len(terms))
	for i, t := range terms {
		items[i] = &GlossaryTermItem{ID: t.ID, Name: t.Name, ShortDescription: t.ShortDescription}
	}

	limit := maxGlossaryListResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(items, in.NextToken, limit)

	return &listGlossaryTermsOutput{Items: page, NextToken: next}, nil
}

type associateGlossaryTermsInput struct {
	AssetIdentifier         string   `json:"AssetIdentifier"`
	GlossaryTermIdentifiers []string `json:"GlossaryTermIdentifiers"`
}

type glossaryTermAssociationOutput struct {
	AssetIdentifier string   `json:"AssetIdentifier"`
	GlossaryTerms   []string `json:"GlossaryTerms"`
}

func (h *Handler) handleAssociateGlossaryTerms(
	_ context.Context,
	in *associateGlossaryTermsInput,
) (*glossaryTermAssociationOutput, error) {
	if in.AssetIdentifier == "" {
		return nil, fmt.Errorf("%w: AssetIdentifier is required", ErrValidation)
	}
	if len(in.GlossaryTermIdentifiers) == 0 {
		return nil, fmt.Errorf("%w: GlossaryTermIdentifiers is required", ErrValidation)
	}

	terms, err := h.Backend.AssociateGlossaryTerms(in.AssetIdentifier, in.GlossaryTermIdentifiers)
	if err != nil {
		return nil, err
	}

	return &glossaryTermAssociationOutput{AssetIdentifier: in.AssetIdentifier, GlossaryTerms: terms}, nil
}

type disassociateGlossaryTermsInput struct {
	AssetIdentifier         string   `json:"AssetIdentifier"`
	GlossaryTermIdentifiers []string `json:"GlossaryTermIdentifiers"`
}

func (h *Handler) handleDisassociateGlossaryTerms(
	_ context.Context,
	in *disassociateGlossaryTermsInput,
) (*glossaryTermAssociationOutput, error) {
	if in.AssetIdentifier == "" {
		return nil, fmt.Errorf("%w: AssetIdentifier is required", ErrValidation)
	}
	if len(in.GlossaryTermIdentifiers) == 0 {
		return nil, fmt.Errorf("%w: GlossaryTermIdentifiers is required", ErrValidation)
	}

	terms, err := h.Backend.DisassociateGlossaryTerms(in.AssetIdentifier, in.GlossaryTermIdentifiers)
	if err != nil {
		return nil, err
	}

	return &glossaryTermAssociationOutput{AssetIdentifier: in.AssetIdentifier, GlossaryTerms: terms}, nil
}
