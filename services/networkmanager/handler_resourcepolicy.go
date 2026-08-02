package networkmanager

import (
	"context"
	"net/http"
)

// resourcePolicyRoutes wires PARITY.md family W (3 ops).
func (h *Handler) resourcePolicyRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segResourcePolicy, paramResourceArn},
			op:      "PutResourcePolicy",
			fn:      h.dispatchPutResourcePolicy,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segResourcePolicy, paramResourceArn},
			op:      "GetResourcePolicy",
			fn:      h.dispatchGetResourcePolicy,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segResourcePolicy, paramResourceArn},
			op:      "DeleteResourcePolicy",
			fn:      h.dispatchDeleteResourcePolicy,
		},
	}
}

func (h *Handler) dispatchPutResourcePolicy(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req putResourcePolicyReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.PutResourcePolicy(params["ResourceArn"], req.PolicyDocument); err != nil {
		return nil, err
	}

	return []byte("{}"), nil
}

func (h *Handler) dispatchGetResourcePolicy(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	doc := h.Backend.GetResourcePolicy(params["ResourceArn"])

	return marshalResponse(getResourcePolicyResponse{PolicyDocument: doc})
}

func (h *Handler) dispatchDeleteResourcePolicy(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	if err := h.Backend.DeleteResourcePolicy(params["ResourceArn"]); err != nil {
		return nil, err
	}

	return []byte("{}"), nil
}
