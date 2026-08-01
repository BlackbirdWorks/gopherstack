package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleStartResourceGroupingRecommendationTask(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req startResourceGroupingRecommendationTaskRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	t, err := h.Backend.StartResourceGroupingRecommendationTask(req.AppArn)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		groupingTaskResponse{
			AppArn:       t.AppArn,
			GroupingID:   t.GroupingID,
			Status:       t.Status,
			ErrorMessage: t.ErrorMessage,
		},
	)
}

func (h *Handler) handleDescribeResourceGroupingRecommendationTask(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req groupingIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	t, err := h.Backend.DescribeResourceGroupingRecommendationTask(req.AppArn, req.GroupingID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(struct {
		GroupingID   string `json:"groupingId"`
		Status       string `json:"status"`
		ErrorMessage string `json:"errorMessage,omitempty"`
	}{GroupingID: t.GroupingID, Status: t.Status, ErrorMessage: t.ErrorMessage})
}

func (h *Handler) handleListResourceGroupingRecommendations(
	_ context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	if err := h.Backend.ListResourceGroupingRecommendations(q.Get("appArn")); err != nil {
		return nil, err
	}

	return marshalResponse(listResourceGroupingRecommendationsResponse{GroupingRecommendations: []struct{}{}})
}

func (h *Handler) handleAcceptResourceGroupingRecommendations(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req acceptResourceGroupingRecommendationsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(req.Entries))
	for _, e := range req.Entries {
		ids = append(ids, e.GroupingRecommendationID)
	}

	failed, err := h.Backend.AcceptResourceGroupingRecommendations(req.AppArn, ids)
	if err != nil {
		return nil, err
	}

	return marshalResponse(groupingRecommendationsFailedResponse{AppArn: req.AppArn, FailedEntries: failed})
}

func (h *Handler) handleRejectResourceGroupingRecommendations(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req rejectResourceGroupingRecommendationsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(req.Entries))
	for _, e := range req.Entries {
		ids = append(ids, e.GroupingRecommendationID)
	}

	failed, err := h.Backend.RejectResourceGroupingRecommendations(req.AppArn, ids)
	if err != nil {
		return nil, err
	}

	return marshalResponse(groupingRecommendationsFailedResponse{AppArn: req.AppArn, FailedEntries: failed})
}
