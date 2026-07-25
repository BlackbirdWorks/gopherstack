package glue

import (
	"context"
)

// maxFormTypeListResults is this backend's MaxResults upper bound for
// ListFormTypes, matching maxGlossaryListResults.
const maxFormTypeListResults = 100

type putFormTypeInput struct {
	Name   string `json:"Name"`
	Schema string `json:"Schema"`
}

func (h *Handler) handlePutFormType(_ context.Context, in *putFormTypeInput) (*FormType, error) {
	return h.Backend.PutFormType(in.Name, in.Schema)
}

type getFormTypeInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleGetFormType(_ context.Context, in *getFormTypeInput) (*FormType, error) {
	return h.Backend.GetFormType(in.Identifier)
}

type deleteFormTypeInput struct {
	Identifier string `json:"Identifier"`
}

func (h *Handler) handleDeleteFormType(_ context.Context, in *deleteFormTypeInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteFormType(in.Identifier); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listFormTypesInput struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listFormTypesOutput struct {
	NextToken string          `json:"NextToken,omitempty"`
	Items     []*FormTypeItem `json:"Items"`
}

func (h *Handler) handleListFormTypes(_ context.Context, in *listFormTypesInput) (*listFormTypesOutput, error) {
	types := h.Backend.ListFormTypes()

	items := make([]*FormTypeItem, len(types))
	for i, ft := range types {
		items[i] = &FormTypeItem{ID: ft.ID, Name: ft.Name}
	}

	limit := maxFormTypeListResults
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
	}

	page, next := paginateSlice(items, in.NextToken, limit)

	return &listFormTypesOutput{Items: page, NextToken: next}, nil
}
