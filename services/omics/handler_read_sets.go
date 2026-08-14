package omics

import (
	"io"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleBatchDeleteReadSet(c *echo.Context, storeID string) error {
	var req struct {
		IDs []string `json:"ids"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	errs, err := h.Backend.BatchDeleteReadSet(storeID, req.IDs)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyErrors: errs})
}

func (h *Handler) handleGetReadSet(c *echo.Context, storeID, id string) error {
	data, err := h.Backend.GetReadSetBytes(storeID, id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", data)
}

func (h *Handler) handleGetReadSetMetadata(c *echo.Context, storeID, id string) error {
	rs, err := h.Backend.GetReadSetMetadata(storeID, id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rs)
}

func (h *Handler) handleListReadSets(c *echo.Context, storeID string) error {
	var req struct {
		Filter *ReadSetFilter `json:"filter"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	readSets, next, err := h.Backend.ListReadSets(
		storeID,
		req.Filter,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"readSets":   readSets,
		keyNextToken: next,
	})
}

func (h *Handler) handleStartReadSetActivationJob(c *echo.Context, storeID string) error {
	var req struct {
		Sources []ReadSetActivationJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReadSetActivationJob(storeID, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReadSetActivationJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReadSetActivationJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReadSetActivationJobs(c *echo.Context, storeID string) error {
	maxResults, nextToken := listQueryParams(c)

	jobs, next, err := h.Backend.ListReadSetActivationJobs(storeID, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"activationJobs": jobs, keyNextToken: next})
}

func (h *Handler) handleStartReadSetExportJob(c *echo.Context, storeID string) error {
	var req struct {
		Destination string                   `json:"destination"`
		Sources     []ReadSetExportJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReadSetExportJob(storeID, req.Destination, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReadSetExportJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReadSetExportJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReadSetExportJobs(c *echo.Context, storeID string) error {
	maxResults, nextToken := listQueryParams(c)

	jobs, next, err := h.Backend.ListReadSetExportJobs(storeID, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"exportJobs": jobs, keyNextToken: next})
}

func (h *Handler) handleStartReadSetImportJob(c *echo.Context, storeID string) error {
	var req struct {
		RoleArn string                   `json:"roleArn"`
		Sources []ReadSetImportJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReadSetImportJob(storeID, req.RoleArn, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReadSetImportJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReadSetImportJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReadSetImportJobs(c *echo.Context, storeID string) error {
	maxResults, nextToken := listQueryParams(c)

	jobs, next, err := h.Backend.ListReadSetImportJobs(storeID, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyImportJobs: jobs, keyNextToken: next})
}

func (h *Handler) handleCreateMultipartReadSetUpload(c *echo.Context, storeID string) error {
	var req struct {
		Tags           map[string]string `json:"tags"`
		Name           string            `json:"name"`
		SourceFileType string            `json:"sourceFileType"`
		SampleID       string            `json:"sampleId"`
		SubjectID      string            `json:"subjectId"`
		GeneratedFrom  string            `json:"generatedFrom"`
		ReferenceArn   string            `json:"referenceArn"`
		Description    string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	upload, err := h.Backend.CreateMultipartReadSetUpload(
		storeID,
		req.Name,
		req.SourceFileType,
		req.SampleID,
		req.SubjectID,
		req.GeneratedFrom,
		req.ReferenceArn,
		req.Description,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, upload)
}

func (h *Handler) handleAbortMultipartReadSetUpload(
	c *echo.Context,
	storeID, uploadID string,
) error {
	if err := h.Backend.AbortMultipartReadSetUpload(storeID, uploadID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCompleteMultipartReadSetUpload(
	c *echo.Context,
	storeID, uploadID string,
) error {
	rs, err := h.Backend.CompleteMultipartReadSetUpload(storeID, uploadID)
	if err != nil {
		return h.mapError(c, err)
	}

	// Real CompleteMultipartReadSetUploadOutput's only member is "readSetId"
	// (deserializers.go's awsRestjson1_deserializeOpDocumentCompleteMultipartReadSetUploadOutput)
	// -- a different key from GetReadSetMetadataOutput's "id" for the same
	// underlying resource. Marshaling the shared ReadSetMetadata struct here
	// left a real client's ReadSetId always nil.
	return c.JSON(http.StatusOK, map[string]any{"readSetId": rs.ID})
}

func (h *Handler) handleListMultipartReadSetUploads(c *echo.Context, storeID string) error {
	maxResults, nextToken := listQueryParams(c)

	uploads, next, err := h.Backend.ListMultipartReadSetUploads(
		storeID,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"uploads": uploads, keyNextToken: next})
}

func (h *Handler) handleListReadSetUploadParts(c *echo.Context, storeID, uploadID string) error {
	maxResults, nextToken := listQueryParams(c)

	parts, next, err := h.Backend.ListReadSetUploadParts(
		storeID,
		uploadID,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"parts": parts, keyNextToken: next})
}

func (h *Handler) handleUploadReadSetPart(c *echo.Context, storeID, uploadID string) error {
	partNumberStr := c.QueryParam("partNumber")
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "partNumber must be a positive integer"))
	}

	partSource := c.QueryParam("partSource")
	if partSource == "" {
		partSource = "SOURCE1"
	}

	data, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			errResp("InternalFailureException", "failed to read request body"),
		)
	}

	checksum, err := h.Backend.UploadReadSetPart(storeID, uploadID, partNumber, partSource, data)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"checksum":          checksum,
		"checksumAlgorithm": "SHA256",
	})
}
