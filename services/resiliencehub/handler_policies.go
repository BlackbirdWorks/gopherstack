package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateResiliencyPolicy(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createResiliencyPolicyRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	p, err := h.Backend.CreateResiliencyPolicy(&req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(policyEnvelope{Policy: toResiliencyPolicyWire(p)})
}

func (h *Handler) handleDescribeResiliencyPolicy(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req policyArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	p, err := h.Backend.DescribeResiliencyPolicy(req.PolicyArn)
	if err != nil {
		return nil, err
	}

	return marshalResponse(policyEnvelope{Policy: toResiliencyPolicyWire(p)})
}

func (h *Handler) handleUpdateResiliencyPolicy(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		updateResiliencyPolicyRequest

		PolicyArn string `json:"policyArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	p, err := h.Backend.UpdateResiliencyPolicy(req.PolicyArn, &req.updateResiliencyPolicyRequest)
	if err != nil {
		return nil, err
	}

	return marshalResponse(policyEnvelope{Policy: toResiliencyPolicyWire(p)})
}

func (h *Handler) handleDeleteResiliencyPolicy(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req policyArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteResiliencyPolicy(req.PolicyArn); err != nil {
		return nil, err
	}

	return marshalResponse(policyArnRequest{PolicyArn: req.PolicyArn})
}

func (h *Handler) handleListResiliencyPolicies(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	p := h.Backend.ListResiliencyPolicies(q.Get("policyName"), q.Get("nextToken"), queryMaxResults(q))

	resp := listResiliencyPoliciesResponse{
		ResiliencyPolicies: make([]resiliencyPolicyWire, 0, len(p.Data)),
		NextToken:          p.Next,
	}
	for _, pol := range p.Data {
		resp.ResiliencyPolicies = append(resp.ResiliencyPolicies, *toResiliencyPolicyWire(pol))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleListSuggestedResiliencyPolicies(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	p := h.Backend.ListSuggestedResiliencyPolicies(q.Get("nextToken"), queryMaxResults(q))

	resp := listResiliencyPoliciesResponse{
		ResiliencyPolicies: make([]resiliencyPolicyWire, 0, len(p.Data)),
		NextToken:          p.Next,
	}
	for _, pol := range p.Data {
		resp.ResiliencyPolicies = append(resp.ResiliencyPolicies, *toResiliencyPolicyWire(pol))
	}

	return marshalResponse(resp)
}
