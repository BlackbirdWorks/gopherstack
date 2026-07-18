package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) mediaAnalysisOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"StartPersonTracking":   service.WrapOp(h.handleStartPersonTracking),
		"GetPersonTracking":     service.WrapOp(h.handleGetPersonTracking),
		"StartSegmentDetection": service.WrapOp(h.handleStartSegmentDetection),
		"GetSegmentDetection":   service.WrapOp(h.handleGetSegmentDetection),
		"StartMediaAnalysisJob": service.WrapOp(h.handleStartMediaAnalysisJob),
		"GetMediaAnalysisJob":   service.WrapOp(h.handleGetMediaAnalysisJob),
		"ListMediaAnalysisJobs": service.WrapOp(h.handleListMediaAnalysisJobs),
	}
}

// --- Async video jobs: person tracking / segment detection ---

type startPersonTrackingReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
}

func (h *Handler) handleStartPersonTracking(
	_ context.Context, _ *startPersonTrackingReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("person_tracking", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type getPersonTrackingResp struct {
	getJobBaseResp
	Persons []struct{} `json:"Persons"`
}

func (h *Handler) handleGetPersonTracking(
	_ context.Context, req *getJobReq,
) (*getPersonTrackingResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getPersonTrackingResp{
		getJobBaseResp: *base,
		Persons:        []struct{}{},
	}, nil
}

type startSegmentDetectionReq struct {
	Video              videoRef `json:"Video"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	JobTag             string   `json:"JobTag"`
	SegmentTypes       []string `json:"SegmentTypes"`
}

func (h *Handler) handleStartSegmentDetection(
	_ context.Context, _ *startSegmentDetectionReq,
) (*startJobResp, error) {
	jobID, err := h.Backend.StartAsyncJob("segment_detection", "")
	if err != nil {
		return nil, err
	}

	return &startJobResp{JobId: jobID}, nil
}

type getSegmentDetectionResp struct {
	getJobBaseResp
	Segments             []struct{} `json:"Segments"`
	SelectedSegmentTypes []struct{} `json:"SelectedSegmentTypes"`
}

func (h *Handler) handleGetSegmentDetection(
	_ context.Context, req *getJobReq,
) (*getSegmentDetectionResp, error) {
	base, err := h.getJobBase(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getSegmentDetectionResp{
		getJobBaseResp:       *base,
		Segments:             []struct{}{},
		SelectedSegmentTypes: []struct{}{},
	}, nil
}

// =============================================================================
// MediaAnalysis Jobs
// =============================================================================

type startMediaAnalysisJobReq struct {
	JobName            string `json:"JobName"`
	ClientRequestToken string `json:"ClientRequestToken"`
}

type startMediaAnalysisJobResp struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleStartMediaAnalysisJob(
	_ context.Context, req *startMediaAnalysisJobReq,
) (*startMediaAnalysisJobResp, error) {
	jobName := req.JobName
	if jobName == "" {
		jobName = "media-analysis-job"
	}

	jobID, err := h.Backend.StartMediaAnalysisJob(jobName)
	if err != nil {
		return nil, err
	}

	return &startMediaAnalysisJobResp{JobId: jobID}, nil
}

type getMediaAnalysisJobReq struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

type mediaAnalysisJobDescription struct {
	JobId             string  `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	JobName           string  `json:"JobName"`
	Status            string  `json:"Status"`
	CreationTimestamp float64 `json:"CreationTimestamp"`
}

type getMediaAnalysisJobResp struct {
	JobId             string  `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	JobName           string  `json:"JobName"`
	Status            string  `json:"Status"`
	CreationTimestamp float64 `json:"CreationTimestamp"`
}

func (h *Handler) handleGetMediaAnalysisJob(
	_ context.Context, req *getMediaAnalysisJobReq,
) (*getMediaAnalysisJobResp, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("%w: JobId is required", ErrValidation)
	}

	job, err := h.Backend.GetMediaAnalysisJob(req.JobId)
	if err != nil {
		return nil, err
	}

	return &getMediaAnalysisJobResp{
		JobId:             job.JobID,
		JobName:           job.JobName,
		Status:            job.Status,
		CreationTimestamp: epochSeconds(job.CreationTimestamp),
	}, nil
}

type listMediaAnalysisJobsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listMediaAnalysisJobsResp struct {
	NextToken         string                        `json:"NextToken,omitempty"`
	MediaAnalysisJobs []mediaAnalysisJobDescription `json:"MediaAnalysisJobs"`
}

func (h *Handler) handleListMediaAnalysisJobs(
	_ context.Context, req *listMediaAnalysisJobsReq,
) (*listMediaAnalysisJobsResp, error) {
	jobs, nextToken, err := h.Backend.ListMediaAnalysisJobs(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	descriptions := make([]mediaAnalysisJobDescription, 0, len(jobs))
	for _, j := range jobs {
		descriptions = append(descriptions, mediaAnalysisJobDescription{
			JobId:             j.JobID,
			JobName:           j.JobName,
			Status:            j.Status,
			CreationTimestamp: epochSeconds(j.CreationTimestamp),
		})
	}

	return &listMediaAnalysisJobsResp{
		MediaAnalysisJobs: descriptions,
		NextToken:         nextToken,
	}, nil
}
