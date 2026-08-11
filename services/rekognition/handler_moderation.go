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

type detectModerationLabelsResp struct {
	ModerationModelVersion string                 `json:"ModerationModelVersion"`
	ModerationLabels       []moderationLabelEntry `json:"ModerationLabels"`
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

type detectProtectiveEquipmentReq struct {
	SummarizationAttributes *struct { //nolint:govet // existing issue.
		MinConfidence          float32  `json:"MinConfidence"`
		RequiredEquipmentTypes []string `json:"RequiredEquipmentTypes"`
	} `json:"SummarizationAttributes"`
	Image imageRef `json:"Image"`
}

type protectiveEquipmentPersonEntry struct {
	Id         int32   `json:"Id"` //nolint:revive,staticcheck // existing issue.
	Confidence float32 `json:"Confidence"`
}

type detectProtectiveEquipmentResp struct {
	ProtectiveEquipmentModelVersion string                           `json:"ProtectiveEquipmentModelVersion"`
	Persons                         []protectiveEquipmentPersonEntry `json:"Persons"`
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
	_ context.Context, req *startContentModerationReq,
) (*startJobResp, error) {
	bucket, name, version := videoRefS3(req.Video)

	jobID, err := h.Backend.StartAsyncJob(StartAsyncJobParams{
		JobType:        "content_moderation",
		JobTag:         req.JobTag,
		VideoS3Bucket:  bucket,
		VideoS3Name:    name,
		VideoS3Version: version,
	})
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type getContentModerationReq struct {
	JobId       string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	NextToken   string `json:"NextToken"`
	AggregateBy string `json:"AggregateBy"`
	SortBy      string `json:"SortBy"`
	MaxResults  int32  `json:"MaxResults"`
}

type getContentModerationResp struct {
	getJobBaseResp
	GetRequestMetadata *getRequestMetadataWire `json:"GetRequestMetadata,omitempty"`
	ModerationLabels   []struct{}              `json:"ModerationLabels"`
}

// contentModerationSortByDefault / contentModerationAggregateByDefault are
// GetContentModerationInput's documented defaults ("The default sort is by
// TIMESTAMP." / "Default aggregation option is TIMESTAMPS.",
// api_op_GetContentModeration.go).
const (
	contentModerationSortByDefault      = "TIMESTAMP"
	contentModerationAggregateByDefault = "TIMESTAMPS"
)

func (h *Handler) handleGetContentModeration(
	_ context.Context, req *getContentModerationReq,
) (*getContentModerationResp, error) {
	base, _, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getContentModerationResp{
		getJobBaseResp:   *base,
		ModerationLabels: []struct{}{},
		GetRequestMetadata: &getRequestMetadataWire{
			AggregateBy: orDefault(req.AggregateBy, contentModerationAggregateByDefault),
			SortBy:      orDefault(req.SortBy, contentModerationSortByDefault),
		},
	}, nil
}
