package rekognition

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) moderationOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DetectModerationLabels":    service.WrapOp(h.handleDetectModerationLabels),
		"DetectProtectiveEquipment": service.WrapOp(h.handleDetectProtectiveEquipment),
		"StartContentModeration":    service.WrapOp(h.handleStartContentModeration),
		"GetContentModeration":      service.WrapOp(h.handleGetContentModeration),
	}
}

type detectModerationLabelsReq struct {
	Image         imageRef `json:"Image"`
	MinConfidence float32  `json:"MinConfidence"`
}

type moderationLabelEntry struct {
	Name       string  `json:"Name"`
	ParentName string  `json:"ParentName"`
	Confidence float32 `json:"Confidence"`
}

type detectModerationLabelsResp struct { //nolint:govet // existing issue.
	ModerationLabels       []moderationLabelEntry `json:"ModerationLabels"`
	ModerationModelVersion string                 `json:"ModerationModelVersion"`
}

func (h *Handler) handleDetectModerationLabels(
	_ context.Context, req *detectModerationLabelsReq,
) (*detectModerationLabelsResp, error) {
	// Default: content is clean. Only return labels if MinConfidence is very low,
	// which indicates the caller wants to see all possible labels.
	labels := []moderationLabelEntry{}
	if req.MinConfidence > 0 && req.MinConfidence <= 10.0 {
		const suggestiveConfidence = 7.5
		labels = []moderationLabelEntry{
			{Name: "Suggestive", ParentName: "", Confidence: suggestiveConfidence},
		}
	}

	return &detectModerationLabelsResp{
		ModerationLabels:       labels,
		ModerationModelVersion: "4.0",
	}, nil
}

type detectProtectiveEquipmentReq struct { //nolint:govet // existing issue.
	Image                   imageRef  `json:"Image"`
	SummarizationAttributes *struct { //nolint:govet // existing issue.
		MinConfidence          float32  `json:"MinConfidence"`
		RequiredEquipmentTypes []string `json:"RequiredEquipmentTypes"`
	} `json:"SummarizationAttributes"`
}

type protectiveEquipmentPersonEntry struct {
	Id         int32   `json:"Id"` //nolint:revive,staticcheck // existing issue.
	Confidence float32 `json:"Confidence"`
}

type detectProtectiveEquipmentResp struct { //nolint:govet // existing issue.
	Persons                         []protectiveEquipmentPersonEntry `json:"Persons"`
	ProtectiveEquipmentModelVersion string                           `json:"ProtectiveEquipmentModelVersion"`
}

func (h *Handler) handleDetectProtectiveEquipment(
	_ context.Context, _ *detectProtectiveEquipmentReq,
) (*detectProtectiveEquipmentResp, error) {
	return &detectProtectiveEquipmentResp{
		Persons:                         []protectiveEquipmentPersonEntry{},
		ProtectiveEquipmentModelVersion: "1.0",
	}, nil
}

// --- Async video jobs: content moderation ---

type startContentModerationReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	MinConfidence      float32  `json:"MinConfidence"`
}

func (h *Handler) handleStartContentModeration(
	_ context.Context, _ *startContentModerationReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("content_moderation", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type getContentModerationResp struct {
	getJobBaseResp
	ModerationLabels []struct{} `json:"ModerationLabels"`
}

func (h *Handler) handleGetContentModeration(
	_ context.Context, req *getJobReq,
) (*getContentModerationResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getContentModerationResp{
		getJobBaseResp:   *base,
		ModerationLabels: []struct{}{},
	}, nil
}
