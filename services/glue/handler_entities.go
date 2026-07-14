package glue

import (
	"context"
	"fmt"
)

// describeEntityInput holds input for DescribeEntity.
type describeEntityInput struct {
	ConnectionName      string `json:"ConnectionName"`
	EntityName          string `json:"EntityName"`
	CatalogID           string `json:"CatalogId,omitempty"`
	DataStoreAPIVersion string `json:"DataStoreApiVersion,omitempty"`
	NextToken           string `json:"NextToken,omitempty"`
}

// describeEntityOutput holds the result for DescribeEntity.
type describeEntityOutput struct {
	NextToken string        `json:"NextToken,omitempty"`
	Fields    []EntityField `json:"Fields"`
}

func (h *Handler) handleDescribeEntity(
	_ context.Context,
	in *describeEntityInput,
) (*describeEntityOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", ErrValidation)
	}

	if in.EntityName == "" {
		return nil, fmt.Errorf("%w: EntityName is required", ErrValidation)
	}

	fields, err := h.Backend.DescribeEntity(in.ConnectionName, in.EntityName)
	if err != nil {
		return nil, err
	}

	return &describeEntityOutput{Fields: fields}, nil
}

// getEntityRecordsInput holds input for GetEntityRecords.
type getEntityRecordsInput struct {
	ConnectionName      string `json:"ConnectionName"`
	EntityName          string `json:"EntityName"`
	CatalogID           string `json:"CatalogId,omitempty"`
	NextToken           string `json:"NextToken,omitempty"`
	DataStoreAPIVersion string `json:"DataStoreApiVersion,omitempty"`
	FilterPredicate     string `json:"FilterPredicate,omitempty"`
	OrderBy             string `json:"OrderBy,omitempty"`
	Limit               int    `json:"Limit,omitempty"`
}

// getEntityRecordsOutput holds the result for GetEntityRecords.
type getEntityRecordsOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Records   []map[string]any `json:"Records"`
}

func (h *Handler) handleGetEntityRecords(
	_ context.Context,
	in *getEntityRecordsInput,
) (*getEntityRecordsOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", ErrValidation)
	}

	if in.EntityName == "" {
		return nil, fmt.Errorf("%w: EntityName is required", ErrValidation)
	}

	records, nextToken, err := h.Backend.GetEntityRecords(
		in.ConnectionName, in.EntityName, in.Limit, in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &getEntityRecordsOutput{Records: records, NextToken: nextToken}, nil
}

// listEntitiesInput holds input for ListEntities.
type listEntitiesInput struct {
	ConnectionName   string `json:"ConnectionName"`
	CatalogID        string `json:"CatalogId,omitempty"`
	ParentEntityName string `json:"ParentEntityName,omitempty"`
	NextToken        string `json:"NextToken,omitempty"`
	DataStoreAPIVer  string `json:"DataStoreApiVersion,omitempty"`
}

// listEntitiesOutput holds the result for ListEntities.
type listEntitiesOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	Entities  []EntityDescriptor `json:"Entities"`
}

func (h *Handler) handleListEntities(
	_ context.Context,
	in *listEntitiesInput,
) (*listEntitiesOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", ErrValidation)
	}

	entities, err := h.Backend.ListEntities(in.ConnectionName)
	if err != nil {
		return nil, err
	}

	return &listEntitiesOutput{Entities: entities}, nil
}
