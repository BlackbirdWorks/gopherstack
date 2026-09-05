package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateRecommendationTemplate(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createRecommendationTemplateRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	t, err := h.Backend.CreateRecommendationTemplate(&req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(recommendationTemplateEnvelope{RecommendationTemplate: toRecommendationTemplateWire(t)})
}

func (h *Handler) handleDeleteRecommendationTemplate(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req recommendationTemplateArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	status, err := h.Backend.DeleteRecommendationTemplate(req.RecommendationTemplateArn)
	if err != nil {
		return nil, err
	}

	return marshalResponse(deleteRecommendationTemplateResponse{
		RecommendationTemplateArn: req.RecommendationTemplateArn, Status: status,
	})
}

func (h *Handler) handleListRecommendationTemplates(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	f := listTemplatesFilter{
		assessmentArn: q.Get("assessmentArn"), name: q.Get("name"), templateArn: q.Get("recommendationTemplateArn"),
		statuses: q["status"], reverseOrder: q.Get("reverseOrder") == queryValueTrue,
	}

	p := h.Backend.ListRecommendationTemplates(f, q.Get("nextToken"), queryMaxResults(q))

	resp := listRecommendationTemplatesResponse{
		RecommendationTemplates: make([]recommendationTemplateWire, 0, len(p.Data)), NextToken: p.Next,
	}
	for _, t := range p.Data {
		resp.RecommendationTemplates = append(resp.RecommendationTemplates, *toRecommendationTemplateWire(t))
	}

	return marshalResponse(resp)
}
