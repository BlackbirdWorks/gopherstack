package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleStartExport(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startExportRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	task, err := h.Backend.StartExport(req.S3Bucket, req.S3Key, req.S3BucketOwner, req.Tags)
	if err != nil {
		return nil, err
	}

	w := toExportTaskWire(task)

	return marshalResponse(exportTaskEnvelope{ExportTask: &w})
}

func (h *Handler) handleListExports(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listExportsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var ids []string
	if req.Filters != nil {
		ids = req.Filters.ExportIDs
	}

	pg, err := h.Backend.ListExports(ids, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]exportTaskWire, len(pg.Data))
	for i, t := range pg.Data {
		items[i] = toExportTaskWire(t)
	}

	return marshalResponse(listExportsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleListExportErrors(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req exportIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if req.ExportID == "" {
		return nil, validationError("exportID is required")
	}

	if err := h.Backend.ListExportErrors(); err != nil {
		return nil, err
	}

	return marshalResponse(listExportErrorsResponse{Items: []struct{}{}})
}

func (h *Handler) handleStartImport(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startImportRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var source *S3BucketSource
	if req.S3BucketSource != nil {
		source = &S3BucketSource{
			S3Bucket:      req.S3BucketSource.S3Bucket,
			S3BucketOwner: req.S3BucketSource.S3BucketOwner,
			S3Key:         req.S3BucketSource.S3Key,
		}
	}

	task, err := h.Backend.StartImport(source, req.Tags)
	if err != nil {
		return nil, err
	}

	w := toImportTaskWire(task)

	return marshalResponse(importTaskEnvelope{ImportTask: &w})
}

func (h *Handler) handleListImports(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listImportsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var ids []string
	if req.Filters != nil {
		ids = req.Filters.ImportIDs
	}

	pg, err := h.Backend.ListImports(ids, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]importTaskWire, len(pg.Data))
	for i, t := range pg.Data {
		items[i] = toImportTaskWire(t)
	}

	return marshalResponse(listImportsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleListImportErrors(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req importIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if req.ImportID == "" {
		return nil, validationError("importID is required")
	}

	pg, err := h.Backend.ListImportErrors(req.ImportID, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]importTaskErrorWire, len(pg.Data))
	for i, e := range pg.Data {
		items[i] = toImportTaskErrorWire(e)
	}

	return marshalResponse(listImportErrorsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleStartImportFileEnrichment(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startImportFileEnrichmentRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if req.TargetS3Configuration == nil {
		return nil, validationError("targetS3Configuration is required")
	}

	job, err := h.Backend.StartImportFileEnrichment(&EnrichmentTargetS3Configuration{
		S3Bucket: req.TargetS3Configuration.S3Bucket, S3BucketOwner: req.TargetS3Configuration.S3BucketOwner,
		S3Key: req.TargetS3Configuration.S3Key,
	})
	if err != nil {
		return nil, err
	}

	return marshalResponse(toImportFileEnrichmentWire(job))
}

func (h *Handler) handleListImportFileEnrichments(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listImportFileEnrichmentsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	var ids []string
	if req.Filters != nil {
		ids = req.Filters.JobIDs
	}

	pg := h.Backend.ListImportFileEnrichments(ids, req.NextToken, int(req.MaxResults))

	items := make([]importFileEnrichmentWire, len(pg.Data))
	for i, j := range pg.Data {
		items[i] = toImportFileEnrichmentWire(j)
	}

	return marshalResponse(listImportFileEnrichmentsResponse{Items: items, NextToken: pg.Next})
}
