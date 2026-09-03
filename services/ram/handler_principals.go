package ram

import (
	"context"
	"encoding/json"
	"fmt"
)

type principalObject struct {
	ID               string  `json:"id"`
	ResourceShareArn string  `json:"resourceShareArn"`
	External         bool    `json:"external"`
	CreationTime     float64 `json:"creationTime"`
	LastUpdatedTime  float64 `json:"lastUpdatedTime"`
}

func toPrincipalObject(a *ResourceShareAssociation) principalObject {
	return principalObject{
		ID:               a.AssociatedEntity,
		ResourceShareArn: a.ResourceShareARN,
		External:         a.External,
		CreationTime:     epochSeconds(a.CreationTime),
		LastUpdatedTime:  epochSeconds(a.LastUpdatedTime),
	}
}

type listPrincipalsRequest struct {
	MaxResults        *int32   `json:"maxResults,omitempty"`
	ResourceOwner     string   `json:"resourceOwner"`
	NextToken         string   `json:"nextToken"`
	ResourceShareArns []string `json:"resourceShareArns"`
}

type listPrincipalsResponse struct {
	NextToken  string            `json:"nextToken,omitempty"`
	Principals []principalObject `json:"principals"`
}

func (h *Handler) handleListPrincipals(_ context.Context, body []byte) ([]byte, error) {
	var req listPrincipalsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceOwner == "" {
		return nil, fmt.Errorf("%w: resourceOwner is required", errInvalidRequest)
	}

	assocs := h.Backend.ListPrincipals(req.ResourceOwner, req.ResourceShareArns)
	objs := make([]principalObject, 0, len(assocs))

	for _, a := range assocs {
		objs = append(objs, toPrincipalObject(a))
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listPrincipalsResponse{NextToken: nextToken, Principals: page})
}
