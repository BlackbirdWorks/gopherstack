package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) faceLivenessOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateFaceLivenessSession":     service.WrapOp(h.handleCreateFaceLivenessSession),
		"GetFaceLivenessSessionResults": service.WrapOp(h.handleGetFaceLivenessSessionResults),
	}
}

// =============================================================================
// Face Liveness
// =============================================================================

type createFaceLivenessSessionReq struct {
	ClientRequestToken string `json:"ClientRequestToken"`
}

type createFaceLivenessSessionResp struct {
	SessionId string `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateFaceLivenessSession(
	_ context.Context, _ *createFaceLivenessSessionReq,
) (*createFaceLivenessSessionResp, error) {
	sessionID, err := h.Backend.CreateFaceLivenessSession()
	if err != nil {
		return nil, err
	}

	return &createFaceLivenessSessionResp{SessionId: sessionID}, nil
}

type getFaceLivenessSessionResultsReq struct {
	SessionId string `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
}

type getFaceLivenessSessionResultsResp struct {
	SessionId  string  `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
	Status     string  `json:"Status"`
	Confidence float32 `json:"Confidence"`
}

func (h *Handler) handleGetFaceLivenessSessionResults(
	_ context.Context, req *getFaceLivenessSessionResultsReq,
) (*getFaceLivenessSessionResultsResp, error) {
	if req.SessionId == "" {
		return nil, fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	result, err := h.Backend.GetFaceLivenessSessionResults(req.SessionId)
	if err != nil {
		return nil, err
	}

	return &getFaceLivenessSessionResultsResp{
		SessionId:  result.SessionID,
		Status:     result.Status,
		Confidence: result.Confidence,
	}, nil
}
