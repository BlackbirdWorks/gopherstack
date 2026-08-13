package glue

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

type batchGetCustomEntityTypesInput struct {
	Names []string `json:"Names"`
}

type batchGetCustomEntityTypesOutput struct {
	CustomEntityTypes         []*CustomEntityType `json:"CustomEntityTypes"`
	CustomEntityTypesNotFound []string            `json:"CustomEntityTypesNotFound"`
}

func (h *Handler) handleBatchGetCustomEntityTypes(
	_ context.Context,
	in *batchGetCustomEntityTypesInput,
) (*batchGetCustomEntityTypesOutput, error) {
	found, missing := h.Backend.BatchGetCustomEntityTypes(in.Names)

	return &batchGetCustomEntityTypesOutput{CustomEntityTypes: found, CustomEntityTypesNotFound: missing}, nil
}

// createCustomEntityTypeInput holds input for CreateCustomEntityType.
type createCustomEntityTypeInput struct {
	Name         string   `json:"Name"`
	RegexString  string   `json:"RegexString,omitempty"`
	ContextWords []string `json:"ContextWords,omitempty"`
}

// createCustomEntityTypeOutput holds the result for CreateCustomEntityType.
type createCustomEntityTypeOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateCustomEntityType(
	_ context.Context,
	in *createCustomEntityTypeInput,
) (*createCustomEntityTypeOutput, error) {
	if _, err := h.Backend.CreateCustomEntityType(in.Name, in.RegexString, in.ContextWords); err != nil {
		return nil, err
	}

	return &createCustomEntityTypeOutput{Name: in.Name}, nil
}

// deleteCustomEntityTypeInput holds input for DeleteCustomEntityType.
type deleteCustomEntityTypeInput struct {
	Name string `json:"Name"`
}

// deleteCustomEntityTypeOutput holds the result for DeleteCustomEntityType.
type deleteCustomEntityTypeOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteCustomEntityType(
	_ context.Context,
	in *deleteCustomEntityTypeInput,
) (*deleteCustomEntityTypeOutput, error) {
	if err := h.Backend.DeleteCustomEntityType(in.Name); err != nil {
		return nil, err
	}

	return &deleteCustomEntityTypeOutput{Name: in.Name}, nil
}

// getCustomEntityTypeInput holds input for GetCustomEntityType.
type getCustomEntityTypeInput struct {
	Name string `json:"Name"`
}

// getCustomEntityTypeOutput holds the result for GetCustomEntityType.
type getCustomEntityTypeOutput struct {
	Name         string   `json:"Name"`
	RegexString  string   `json:"RegexString"`
	ContextWords []string `json:"ContextWords"`
}

func (h *Handler) handleGetCustomEntityType(
	_ context.Context,
	in *getCustomEntityTypeInput,
) (*getCustomEntityTypeOutput, error) {
	found, missing := h.Backend.BatchGetCustomEntityTypes([]string{in.Name})
	if len(missing) > 0 {
		return nil, awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	}

	return &getCustomEntityTypeOutput{
		Name:         found[0].Name,
		RegexString:  found[0].RegexString,
		ContextWords: found[0].ContextWords,
	}, nil
}

// defaultListCustomEntityTypesLimit is used when ListCustomEntityTypesInput.MaxResults is unset.
const defaultListCustomEntityTypesLimit = 100

// listCustomEntityTypesInput holds input for ListCustomEntityTypes.
//
// Tags is not modeled: CustomEntityType (models.go) carries no Tags field --
// unlike crawlers/jobs/triggers/etc., custom entity types are never routed
// through tags.go's tagResource/GetTags/TaggedResources dispatch, so there is
// no tag state anywhere in this backend to filter on. Accepted on the wire
// and otherwise inert; see PARITY.md.
type listCustomEntityTypesInput struct {
	Tags       map[string]string `json:"Tags,omitempty"`
	NextToken  string            `json:"NextToken,omitempty"`
	MaxResults int32             `json:"MaxResults,omitempty"`
}

// listCustomEntityTypesOutput holds the result for ListCustomEntityTypes.
type listCustomEntityTypesOutput struct {
	NextToken         string              `json:"NextToken,omitempty"`
	CustomEntityTypes []*CustomEntityType `json:"CustomEntityTypes"`
}

func (h *Handler) handleListCustomEntityTypes(
	_ context.Context,
	in *listCustomEntityTypesInput,
) (*listCustomEntityTypesOutput, error) {
	all := h.Backend.ListCustomEntityTypes()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListCustomEntityTypesLimit
	}

	page, next := paginateSlice(all, in.NextToken, limit)

	return &listCustomEntityTypesOutput{CustomEntityTypes: page, NextToken: next}, nil
}
