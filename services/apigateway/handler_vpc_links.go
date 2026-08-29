package apigateway

import (
	"encoding/json"
	"net/http"
)

const (
	opCreateVpcLink = "CreateVpcLink"
	opDeleteVpcLink = "DeleteVpcLink"
	opGetVpcLink    = "GetVpcLink"
	opGetVpcLinks   = "GetVpcLinks"
	opUpdateVpcLink = "UpdateVpcLink"
)

type getVpcLinkInput struct {
	VpcLinkID string `json:"vpcLinkId"`
}

type getVpcLinksInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

type deleteVpcLinkInput struct {
	VpcLinkID string `json:"vpcLinkId"`
}

// parseAPIGWVpcLinksPath handles /vpclinks/... paths.
func parseAPIGWVpcLinksPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch n {
	case pathDepth1:
		switch method {
		case http.MethodGet:
			return opGetVpcLinks, nil, true
		case http.MethodPost:
			return opCreateVpcLink, nil, true
		}
	case pathDepth2:
		params := map[string]string{keyVpcLinkID: segs[1]}
		switch method {
		case http.MethodGet:
			return opGetVpcLink, params, true
		case http.MethodPatch:
			return opUpdateVpcLink, params, true
		case http.MethodDelete:
			return opDeleteVpcLink, params, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// vpcLinkActions returns real stateful action handlers for VPC Link operations.
func (h *Handler) vpcLinkActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateVpcLink: h.createVpcLinkAction,
		opDeleteVpcLink: h.deleteVpcLinkAction,
		opGetVpcLink:    h.getVpcLinkAction,
		opGetVpcLinks:   h.getVpcLinksAction,
		opUpdateVpcLink: h.updateVpcLinkAction,
	}
}

func (h *Handler) createVpcLinkAction(b []byte) (int, any, error) {
	var input CreateVpcLinkInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	link, err := h.Backend.CreateVpcLink(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, link, nil
}

func (h *Handler) deleteVpcLinkAction(b []byte) (int, any, error) {
	var input deleteVpcLinkInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	if err := h.Backend.DeleteVpcLink(input.VpcLinkID); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, nil, nil
}

func (h *Handler) getVpcLinkAction(b []byte) (int, any, error) {
	var input getVpcLinkInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	link, err := h.Backend.GetVpcLink(input.VpcLinkID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, link, nil
}

func (h *Handler) getVpcLinksAction(b []byte) (int, any, error) {
	var input getVpcLinksInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	links, err := h.Backend.GetVpcLinks()
	if err != nil {
		return 0, nil, err
	}
	if input.Limit == 0 && input.Position == "" {
		return http.StatusOK, map[string]any{keyItem: links}, nil
	}
	page, position := paginatePageByKey(links, input.Limit, input.Position,
		func(l VpcLink) string { return l.ID })
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
}

func (h *Handler) updateVpcLinkAction(b []byte) (int, any, error) {
	var input UpdateVpcLinkInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	link, err := h.Backend.UpdateVpcLink(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, link, nil
}
