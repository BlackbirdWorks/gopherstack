package lightsail

import "context"

// taggingVpcMiscOps returns the dispatch table for family X+Y+AA+BB (10 ops).
func (h *Handler) taggingVpcMiscOps() map[string]opFunc {
	return map[string]opFunc{
		"PeerVpc":                       h.handlePeerVpc,
		"UnpeerVpc":                     h.handleUnpeerVpc,
		"IsVpcPeered":                   h.handleIsVpcPeered,
		"TagResource":                   h.handleTagResource,
		"UntagResource":                 h.handleUntagResource,
		"CreateGUISessionAccessDetails": h.handleCreateGUISessionAccessDetails,
		"StartGUISession":               h.handleStartGUISession,
		"StopGUISession":                h.handleStopGUISession,
		"GetActiveNames":                h.handleGetActiveNames,
		"GetCostEstimate":               h.handleGetCostEstimate,
	}
}

func (h *Handler) handlePeerVpc(_ context.Context, _ []byte) ([]byte, error) {
	op, err := h.Backend.PeerVpc()
	if err != nil {
		return nil, err
	}

	return marshalResponse(opEnvelope(op))
}

func (h *Handler) handleUnpeerVpc(_ context.Context, _ []byte) ([]byte, error) {
	op, err := h.Backend.UnpeerVpc()
	if err != nil {
		return nil, err
	}

	return marshalResponse(opEnvelope(op))
}

type isVpcPeeredResponse struct {
	IsPeered bool `json:"isPeered"`
}

func (h *Handler) handleIsVpcPeered(_ context.Context, _ []byte) ([]byte, error) {
	return marshalResponse(isVpcPeeredResponse{IsPeered: h.Backend.IsVpcPeered()})
}

type tagResourceRequest struct {
	ResourceArn  string    `json:"resourceArn,omitempty"`
	ResourceName string    `json:"resourceName"`
	Tags         []tagWire `json:"tags"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[tagResourceRequest](body)
	if err != nil {
		return nil, err
	}

	ops, tagErr := h.Backend.TagResource(req.ResourceName, req.ResourceArn, tagsFromWire(req.Tags))
	if tagErr != nil {
		return nil, tagErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type untagResourceRequest struct {
	ResourceArn  string   `json:"resourceArn,omitempty"`
	ResourceName string   `json:"resourceName"`
	TagKeys      []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[untagResourceRequest](body)
	if err != nil {
		return nil, err
	}

	ops, untagErr := h.Backend.UntagResource(req.ResourceName, req.ResourceArn, req.TagKeys)
	if untagErr != nil {
		return nil, untagErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type guiSessionWire struct {
	Name      string `json:"name,omitempty"`
	URL       string `json:"url,omitempty"`
	IsPrimary bool   `json:"isPrimary,omitempty"`
}

type createGUISessionResponse struct {
	FailureReason      string           `json:"failureReason,omitempty"`
	ResourceName       string           `json:"resourceName,omitempty"`
	Status             string           `json:"status,omitempty"`
	Sessions           []guiSessionWire `json:"sessions,omitempty"`
	PercentageComplete int32            `json:"percentageComplete,omitempty"`
}

func (h *Handler) handleCreateGUISessionAccessDetails(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resourceNameRequest](body)
	if err != nil {
		return nil, err
	}

	sess, createErr := h.Backend.CreateGUISessionAccessDetails(req.ResourceName)
	if createErr != nil {
		return nil, createErr
	}

	percentage := int32(0)
	if sess.Status == GUISessionStatusReady {
		percentage = 100
	}

	var sessions []guiSessionWire
	if sess.URL != "" {
		sessions = []guiSessionWire{{IsPrimary: true, Name: "primary", URL: sess.URL}}
	}

	return marshalResponse(createGUISessionResponse{
		PercentageComplete: percentage, ResourceName: sess.ResourceName, Sessions: sessions, Status: sess.Status,
	})
}

func (h *Handler) handleStartGUISession(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resourceNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, startErr := h.Backend.StartGUISession(req.ResourceName)
	if startErr != nil {
		return nil, startErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}

func (h *Handler) handleStopGUISession(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resourceNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, stopErr := h.Backend.StopGUISession(req.ResourceName)
	if stopErr != nil {
		return nil, stopErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}

type getActiveNamesResponse struct {
	NextPageToken string   `json:"nextPageToken,omitempty"`
	ActiveNames   []string `json:"activeNames,omitempty"`
}

func (h *Handler) handleGetActiveNames(_ context.Context, _ []byte) ([]byte, error) {
	return marshalResponse(getActiveNamesResponse{ActiveNames: h.Backend.GetActiveNames()})
}

type getCostEstimateRequest struct {
	EndTime      *float64 `json:"endTime,omitempty"`
	StartTime    *float64 `json:"startTime,omitempty"`
	ResourceName string   `json:"resourceName"`
}

type getCostEstimateResponse struct {
	ResourcesBudgetEstimate []struct{} `json:"resourcesBudgetEstimate"`
}

func (h *Handler) handleGetCostEstimate(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getCostEstimateRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetCostEstimate(req.ResourceName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(getCostEstimateResponse{ResourcesBudgetEstimate: []struct{}{}})
}
