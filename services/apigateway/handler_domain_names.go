package apigateway

import (
	"encoding/json"
	"net/http"
	"strings"
)

const (
	opDeleteDomainNameAccessAssociation = "DeleteDomainNameAccessAssociation"
	opGetDomainNameAccessAssociations   = "GetDomainNameAccessAssociations"
	opRejectDomainNameAccessAssociation = "RejectDomainNameAccessAssociation"
)

// parseAPIGWDomainNameAccessAssociationsPath handles /domainnameaccessassociations/... paths.
// The association ARN (when present) may itself contain "/" once URL-decoded,
// so remaining segments are rejoined to reconstruct it, mirroring /tags/{arn}.
func parseAPIGWDomainNameAccessAssociationsPath(method string, segs []string, n int) (string, map[string]string, bool) {
	if n == pathDepth1 {
		switch method {
		case http.MethodGet:
			return opGetDomainNameAccessAssociations, nil, true
		case http.MethodPost:
			return opCreateDomainNameAccessAssociation, nil, true
		}

		return apiGWUnknownOp, nil, false
	}

	if method != http.MethodDelete {
		return apiGWUnknownOp, nil, false
	}

	arn := strings.Join(segs[1:], "/")

	return opDeleteDomainNameAccessAssociation, map[string]string{keyDomainNameAccessAssociationArn: arn}, true
}

type getDomainNamesPageInput struct {
	Position      string `json:"position"`
	ResourceOwner string `json:"resourceOwner"`
	Limit         int    `json:"limit"`
}

type getDomainNameInput struct {
	DomainName string `json:"domainName"`
}

type deleteDomainNameInput struct {
	DomainName string `json:"domainName"`
}

// parseAPIGWDomainNamesPath handles /domainnames/... paths.
func parseAPIGWDomainNamesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch n {
	case pathDepth1:
		switch method {
		case http.MethodGet:
			return opGetDomainNames, nil, true
		case http.MethodPost:
			return opCreateDomainName, nil, true
		}
	case pathDepth2:
		switch method {
		case http.MethodGet:
			return opGetDomainName, map[string]string{keyDomainName: segs[1]}, true
		case http.MethodDelete:
			return opDeleteDomainName, map[string]string{keyDomainName: segs[1]}, true
		case http.MethodPatch:
			return opUpdateDomainName, map[string]string{keyDomainName: segs[1]}, true
		}
	case pathDepth3:
		return parseAPIGWDomainNamesDepth3(method, segs)
	case pathDepth4:
		if segs[2] == apiGWSegBasePathMappings {
			return parseAPIGWDomainNamesBasePathMapping(method, segs)
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWDomainNamesDepth3 handles /domainnames/{name}/{sub} paths.
func parseAPIGWDomainNamesDepth3(method string, segs []string) (string, map[string]string, bool) {
	if segs[2] == apiGWSegBasePathMappings {
		switch method {
		case http.MethodGet:
			return opGetBasePathMappings, map[string]string{keyDomainName: segs[1]}, true
		case http.MethodPost:
			return opCreateBasePathMapping, map[string]string{keyDomainName: segs[1]}, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// domainNameActions returns the action map for domain name CRUD operations.
func (h *Handler) domainNameActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateDomainName: h.createDomainNameAction,
		opGetDomainName:    h.getDomainNameAction,
		opGetDomainNames:   h.getDomainNamesAction,
		opDeleteDomainName: h.deleteDomainNameAction,
		opUpdateDomainName: h.updateDomainNameAction,
	}
}

func (h *Handler) createDomainNameAction(b []byte) (int, any, error) {
	var input CreateDomainNameInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	dn, err := h.Backend.CreateDomainName(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, dn, nil
}

func (h *Handler) getDomainNameAction(b []byte) (int, any, error) {
	var input getDomainNameInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	dn, err := h.Backend.GetDomainName(input.DomainName)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, dn, nil
}

func (h *Handler) getDomainNamesAction(b []byte) (int, any, error) {
	var input getDomainNamesPageInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if input.ResourceOwner == resourceOwnerOther {
		return http.StatusOK, map[string]any{keyItem: []DomainName{}}, nil
	}
	if input.Limit == 0 && input.Position == "" {
		dns, err := h.Backend.GetDomainNames(input.ResourceOwner)
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, map[string]any{keyItem: dns}, nil
	}
	dns, position, err := h.Backend.GetDomainNamesPage(input.Limit, input.Position)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: dns, keyPosition: position}, nil
}

func (h *Handler) deleteDomainNameAction(b []byte) (int, any, error) {
	var input deleteDomainNameInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteDomainName(input.DomainName); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) updateDomainNameAction(b []byte) (int, any, error) {
	var input UpdateDomainNameInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.UpdateDomainName(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

// domainNameAccessAssociationActions returns real stateful handlers for the
// Create/Get/Delete/Reject domain name access association operations.
func (h *Handler) domainNameAccessAssociationActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateDomainNameAccessAssociation: func(b []byte) (int, any, error) {
			var input CreateDomainNameAccessAssociationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			assoc, err := h.Backend.CreateDomainNameAccessAssociation(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, assoc, nil
		},
		opGetDomainNameAccessAssociations: func(b []byte) (int, any, error) {
			var input struct {
				ResourceOwner string `json:"resourceOwner,omitempty"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			assocs, err := h.Backend.GetDomainNameAccessAssociations(input.ResourceOwner)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, &domainNameAccessAssociationsView{Items: assocs}, nil
		},
		opDeleteDomainNameAccessAssociation: func(b []byte) (int, any, error) {
			var input struct {
				Arn string `json:"domainNameAccessAssociationArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			if err := h.Backend.DeleteDomainNameAccessAssociation(input.Arn); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, nil, nil
		},
		opRejectDomainNameAccessAssociation: func(b []byte) (int, any, error) {
			var input struct {
				Arn           string `json:"domainNameAccessAssociationArn"`
				DomainNameArn string `json:"domainNameArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			if err := h.Backend.RejectDomainNameAccessAssociation(input.Arn, input.DomainNameArn); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, nil, nil
		},
	}
}

// domainNameAccessAssociationsView is the response for GetDomainNameAccessAssociations.
type domainNameAccessAssociationsView struct {
	Items []DomainNameAccessAssociation `json:"item"`
}
