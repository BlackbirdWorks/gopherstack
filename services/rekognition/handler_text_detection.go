package rekognition

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) textDetectionOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DetectText":         service.WrapOp(h.handleDetectText),
		"StartTextDetection": service.WrapOp(h.handleStartTextDetection),
		"GetTextDetection":   service.WrapOp(h.handleGetTextDetection),
	}
}

type detectTextReq struct { //nolint:govet // existing issue.
	Image   imageRef  `json:"Image"`
	Filters *struct{} `json:"Filters"`
}

type textDetectionEntry struct { //nolint:govet // existing issue.
	DetectedText string  `json:"DetectedText"`
	Confidence   float64 `json:"Confidence"`
	Type         string  `json:"Type"`
	Id           int32   `json:"Id"` //nolint:revive,staticcheck // existing issue.
}

type detectTextResp struct { //nolint:govet // existing issue.
	TextDetections   []textDetectionEntry `json:"TextDetections"`
	TextModelVersion string               `json:"TextModelVersion"`
}

func (h *Handler) handleDetectText(_ context.Context, req *detectTextReq) (*detectTextResp, error) {
	detections := plausibleTextDetections(req)

	return &detectTextResp{
		TextDetections:   detections,
		TextModelVersion: "3.1",
	}, nil
}

// plausibleTextDetections returns minimal text detection results derived from the image reference.
func plausibleTextDetections(req *detectTextReq) []textDetectionEntry {
	// Derive a plausible text value from the image S3 key when available.
	label := "SAMPLE TEXT"
	if req.Image.S3Object != nil && req.Image.S3Object.Name != "" {
		label = req.Image.S3Object.Name
	}

	const textConfidence = 97.2

	return []textDetectionEntry{
		{Id: 0, DetectedText: label, Type: "LINE", Confidence: textConfidence},
		{Id: 1, DetectedText: label, Type: "WORD", Confidence: textConfidence},
	}
}

// --- Async video jobs: text detection ---

type startTextDetectionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
}

func (h *Handler) handleStartTextDetection(
	_ context.Context, _ *startTextDetectionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("text_detection", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type getTextDetectionResp struct {
	getJobBaseResp
	TextDetections []struct{} `json:"TextDetections"`
}

func (h *Handler) handleGetTextDetection(
	_ context.Context, req *getJobReq,
) (*getTextDetectionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getTextDetectionResp{
		getJobBaseResp: *base,
		TextDetections: []struct{}{},
	}, nil
}
