package verifiedpermissions

import (
	"context"
	"fmt"
)

type putSchemaDefinitionJSON struct {
	CedarJSON string `json:"cedarJson"`
}

type putSchemaInput struct {
	PolicyStoreID string                  `json:"policyStoreId"`
	Definition    putSchemaDefinitionJSON `json:"definition"`
}

type putSchemaOutput struct {
	PolicyStoreID   string   `json:"policyStoreId"`
	CreatedDate     string   `json:"createdDate"`
	LastUpdatedDate string   `json:"lastUpdatedDate"`
	Namespaces      []string `json:"namespaces"`
}

func (h *Handler) handlePutSchema(_ context.Context, in *putSchemaInput) (*putSchemaOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.Definition.CedarJSON == "" {
		return nil, fmt.Errorf("%w: definition.cedarJson is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	namespaces, err := h.Backend.PutSchema(resolvedID, in.Definition.CedarJSON)
	if err != nil {
		return nil, err
	}

	// Read back timestamps directly from stored schema.
	s, err := h.Backend.GetSchema(resolvedID)
	if err != nil {
		return nil, err
	}

	if namespaces == nil {
		namespaces = []string{}
	}

	return &putSchemaOutput{
		PolicyStoreID:   resolvedID,
		CreatedDate:     s.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: s.LastUpdated.UTC().Format(timeFormat),
		Namespaces:      namespaces,
	}, nil
}

type getSchemaInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type getSchemaOutput struct {
	PolicyStoreID   string   `json:"policyStoreId"`
	Schema          string   `json:"schema"`
	CreatedDate     string   `json:"createdDate"`
	LastUpdatedDate string   `json:"lastUpdatedDate"`
	Namespaces      []string `json:"namespaces,omitempty"`
}

func (h *Handler) handleGetSchema(_ context.Context, in *getSchemaInput) (*getSchemaOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	s, err := h.Backend.GetSchema(resolvedID)
	if err != nil {
		return nil, err
	}

	return &getSchemaOutput{
		PolicyStoreID:   resolvedID,
		Schema:          s.Schema,
		Namespaces:      s.Namespaces,
		CreatedDate:     s.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: s.LastUpdated.UTC().Format(timeFormat),
	}, nil
}
