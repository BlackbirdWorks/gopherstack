package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleListAlarmRecommendations(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListAlarmRecommendations(req.AssessmentArn); err != nil {
		return nil, err
	}

	return marshalResponse(listAlarmRecommendationsResponse{AlarmRecommendations: []struct{}{}})
}

func (h *Handler) handleListSopRecommendations(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListSopRecommendations(req.AssessmentArn); err != nil {
		return nil, err
	}

	return marshalResponse(listSopRecommendationsResponse{SopRecommendations: []struct{}{}})
}

func (h *Handler) handleListTestRecommendations(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListTestRecommendations(req.AssessmentArn); err != nil {
		return nil, err
	}

	return marshalResponse(listTestRecommendationsResponse{TestRecommendations: []struct{}{}})
}

func (h *Handler) handleListAppComponentRecommendations(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req assessmentArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.ListAppComponentRecommendations(req.AssessmentArn); err != nil {
		return nil, err
	}

	return marshalResponse(listAppComponentRecommendationsResponse{ComponentRecommendations: []struct{}{}})
}

func (h *Handler) handleBatchUpdateRecommendationStatus(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req batchUpdateRecommendationStatusRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, failed, err := h.Backend.BatchUpdateRecommendationStatus(req.AppArn, req.RequestEntries)
	if err != nil {
		return nil, err
	}

	return marshalResponse(batchUpdateRecommendationStatusResponse{
		AppArn: a.ARN, FailedEntries: failed, SuccessfulEntries: []batchUpdateRecommendationStatusSuccessfulEntryWire{},
	})
}
