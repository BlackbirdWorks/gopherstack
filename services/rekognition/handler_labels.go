package rekognition

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) labelOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DetectLabels":        service.WrapOp(h.handleDetectLabels),
		"StartLabelDetection": service.WrapOp(h.handleStartLabelDetection),
		"GetLabelDetection":   service.WrapOp(h.handleGetLabelDetection),
	}
}

type detectLabelsReq struct {
	Image         imageRef `json:"Image"`
	MaxLabels     int32    `json:"MaxLabels"`
	MinConfidence float64  `json:"MinConfidence"`
}

type labelEntry struct {
	Name       string  `json:"Name"`
	Confidence float64 `json:"Confidence"`
}

type detectLabelsResp struct { //nolint:govet // existing issue.
	Labels                []labelEntry `json:"Labels"`
	OrientationCorrection string       `json:"OrientationCorrection"`
}

func (h *Handler) handleDetectLabels(_ context.Context, req *detectLabelsReq) (*detectLabelsResp, error) {
	minConf := req.MinConfidence
	if minConf <= 0 {
		minConf = 50.0
	}
	labels := plausibleLabels(minConf, req.MaxLabels)

	return &detectLabelsResp{
		Labels:                labels,
		OrientationCorrection: orientationRotate0,
	}, nil
}

// Synthetic confidence scores for generic scene labels, in descending order.
const (
	confPerson     = 95.5
	confHuman      = 94.8
	confOutdoor    = 87.3
	confNature     = 73.2
	confSky        = 68.9
	confVegetation = 62.1
	confAnimal     = 55.4
)

// plausibleLabels returns a set of generic scene labels above the confidence threshold.
func plausibleLabels(minConfidence float64, maxLabels int32) []labelEntry {
	all := []labelEntry{
		{Name: "Person", Confidence: confPerson},
		{Name: "Human", Confidence: confHuman},
		{Name: "Outdoor", Confidence: confOutdoor},
		{Name: "Nature", Confidence: confNature},
		{Name: "Sky", Confidence: confSky},
		{Name: "Vegetation", Confidence: confVegetation},
		{Name: "Animal", Confidence: confAnimal},
	}
	out := make([]labelEntry, 0, len(all))
	for _, l := range all {
		if l.Confidence >= minConfidence {
			out = append(out, l)
		}
		if maxLabels > 0 && len(out) >= int(maxLabels) {
			break
		}
	}

	return out
}

// --- Async video jobs: label detection ---

type startLabelDetectionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	MinConfidence      float32  `json:"MinConfidence"`
}

func (h *Handler) handleStartLabelDetection(
	_ context.Context, _ *startLabelDetectionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("label_detection", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type getLabelDetectionResp struct {
	getJobBaseResp
	Labels []struct{} `json:"Labels"`
}

func (h *Handler) handleGetLabelDetection(
	_ context.Context, req *getJobReq,
) (*getLabelDetectionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getLabelDetectionResp{
		getJobBaseResp: *base,
		Labels:         []struct{}{},
	}, nil
}
