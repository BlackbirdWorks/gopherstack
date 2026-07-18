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
		opCreateVpcLink: func(b []byte) (int, any, error) {
			var input CreateVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			link, err := h.Backend.CreateVpcLink(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, link, nil
		},
		opDeleteVpcLink: func(b []byte) (int, any, error) {
			var input deleteVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			if err := h.Backend.DeleteVpcLink(input.VpcLinkID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, nil, nil
		},
		opGetVpcLink: func(b []byte) (int, any, error) {
			var input getVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			link, err := h.Backend.GetVpcLink(input.VpcLinkID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, link, nil
		},
		opGetVpcLinks: func(_ []byte) (int, any, error) {
			links, err := h.Backend.GetVpcLinks()
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: links}, nil
		},
		opUpdateVpcLink: func(b []byte) (int, any, error) {
			var input UpdateVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			link, err := h.Backend.UpdateVpcLink(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, link, nil
		},
	}
}
