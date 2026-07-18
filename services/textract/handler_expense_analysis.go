package textract

import (
	"context"
	"fmt"
)

// analyzeExpenseInput is the input for AnalyzeExpense.
type analyzeExpenseInput struct {
	Document struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
		Bytes []byte `json:"Bytes"`
	} `json:"Document"`
}

// analyzeExpenseResponse is the response for AnalyzeExpense.
type analyzeExpenseResponse struct {
	ExpenseDocuments []ExpenseDocument `json:"ExpenseDocuments"`
	DocumentMetadata struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleAnalyzeExpense(
	ctx context.Context,
	in *analyzeExpenseInput,
) (*analyzeExpenseResponse, error) {
	uri := documentURI(in.Document.S3Object.Bucket, in.Document.S3Object.Name)
	docs := h.Backend.AnalyzeExpense(ctx, uri)

	resp := &analyzeExpenseResponse{ExpenseDocuments: docs}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// getExpenseAnalysisInput is the input for GetExpenseAnalysis.
type getExpenseAnalysisInput struct {
	JobID      string `json:"JobId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// getExpenseAnalysisResponse is the response for GetExpenseAnalysis.
type getExpenseAnalysisResponse struct {
	AnalyzeExpenseModelVersion string            `json:"AnalyzeExpenseModelVersion"`
	JobStatus                  string            `json:"JobStatus"`
	StatusMessage              string            `json:"StatusMessage,omitempty"`
	Warnings                   []WarningBlock    `json:"Warnings,omitempty"`
	ExpenseDocuments           []ExpenseDocument `json:"ExpenseDocuments"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetExpenseAnalysis(
	ctx context.Context,
	in *getExpenseAnalysisInput,
) (*getExpenseAnalysisResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetExpenseAnalysis(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getExpenseAnalysisResponse{
		AnalyzeExpenseModelVersion: modelVersion10,
		ExpenseDocuments:           job.ExpenseDocuments,
		JobStatus:                  job.JobStatus,
		StatusMessage:              job.StatusMessage,
		Warnings:                   job.Warnings,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// startExpenseAnalysisInput is the input for StartExpenseAnalysis.
type startExpenseAnalysisInput struct {
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

func (h *Handler) handleStartExpenseAnalysis(
	ctx context.Context,
	in *startExpenseAnalysisInput,
) (*startJobResponse, error) {
	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	var job *ExpenseJob
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		job, err = b.StartExpenseAnalysisWithOptions(
			ctx,
			uri,
			in.OutputConfig,
			in.NotificationChannel,
			in.JobTag,
			in.ClientRequestToken,
		)
	} else {
		job, err = h.Backend.StartExpenseAnalysis(ctx, uri)
	}

	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}
