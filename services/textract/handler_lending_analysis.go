package textract

import (
	"context"
	"fmt"
)

// getLendingAnalysisInput is the input for GetLendingAnalysis.
type getLendingAnalysisInput struct {
	JobID      string `json:"JobId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// getLendingAnalysisResponse is the response for GetLendingAnalysis.
type getLendingAnalysisResponse struct {
	AnalyzeLendingModelVersion string          `json:"AnalyzeLendingModelVersion"`
	JobStatus                  string          `json:"JobStatus"`
	StatusMessage              string          `json:"StatusMessage,omitempty"`
	Warnings                   []WarningBlock  `json:"Warnings,omitempty"`
	Results                    []LendingResult `json:"Results"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetLendingAnalysis(
	ctx context.Context,
	in *getLendingAnalysisInput,
) (*getLendingAnalysisResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetLendingAnalysis(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getLendingAnalysisResponse{
		AnalyzeLendingModelVersion: modelVersion10,
		JobStatus:                  job.JobStatus,
		StatusMessage:              job.StatusMessage,
		Warnings:                   job.Warnings,
		Results:                    job.Results,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// getLendingAnalysisSummaryInput is the input for GetLendingAnalysisSummary.
type getLendingAnalysisSummaryInput struct {
	JobID string `json:"JobId"`
}

// getLendingAnalysisSummaryResponse is the response for GetLendingAnalysisSummary.
type getLendingAnalysisSummaryResponse struct {
	Summary                    *LendingSummary `json:"Summary,omitempty"`
	AnalyzeLendingModelVersion string          `json:"AnalyzeLendingModelVersion"`
	JobStatus                  string          `json:"JobStatus"`
	StatusMessage              string          `json:"StatusMessage,omitempty"`
	Warnings                   []WarningBlock  `json:"Warnings,omitempty"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetLendingAnalysisSummary(
	ctx context.Context,
	in *getLendingAnalysisSummaryInput,
) (*getLendingAnalysisSummaryResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetLendingAnalysisSummary(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getLendingAnalysisSummaryResponse{
		AnalyzeLendingModelVersion: modelVersion10,
		JobStatus:                  job.JobStatus,
		StatusMessage:              job.StatusMessage,
		Warnings:                   job.Warnings,
		Summary:                    job.Summary,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// startLendingAnalysisInput is the input for StartLendingAnalysis.
type startLendingAnalysisInput struct {
	DocumentLocation struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
	} `json:"DocumentLocation"`
	NotificationChannel *NotificationChannel `json:"NotificationChannel"`
	OutputConfig        *OutputConfig        `json:"OutputConfig"`
	JobTag              string               `json:"JobTag"`
	ClientRequestToken  string               `json:"ClientRequestToken"`
}

func (h *Handler) handleStartLendingAnalysis(
	ctx context.Context,
	in *startLendingAnalysisInput,
) (*startJobResponse, error) {
	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	var job *LendingJob
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		job, err = b.StartLendingAnalysisWithOptions(
			ctx,
			uri,
			in.OutputConfig,
			in.NotificationChannel,
			in.JobTag,
			in.ClientRequestToken,
		)
	} else {
		job, err = h.Backend.StartLendingAnalysis(ctx, uri)
	}

	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}
