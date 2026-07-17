package pinpoint

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// extractSegmentsOp returns the segments collection operation name.
func extractSegmentsOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateSegment"
	case http.MethodGet:
		return "GetSegments"
	}

	return unknownOperation
}

func (h *Handler) extractSegmentSubOp(method, rest string) string {
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch method {
		case http.MethodGet:
			return "GetSegment"
		case http.MethodPut:
			return "UpdateSegment"
		case http.MethodDelete:
			return "DeleteSegment"
		}
	case subPath == subPathJobsExport:
		return "GetSegmentExportJobs"
	case subPath == subPathJobsImport:
		return "GetSegmentImportJobs"
	case subPath == subPathVersions:
		return "GetSegmentVersions"
	case strings.HasPrefix(subPath, subPathVersions+"/"):
		return "GetSegmentVersion"
	}

	return unknownOperation
}

func (h *Handler) dispatchSegments(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateSegment(c, appID)
	case http.MethodGet:
		return h.handleGetSegments(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchSegmentByID(c *echo.Context, appID, rest string) error {
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	segmentID := parts[0]
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch c.Request().Method {
		case http.MethodGet:
			return h.handleGetSegment(c, appID, segmentID)
		case http.MethodPut:
			return h.handleUpdateSegment(c, appID, segmentID)
		case http.MethodDelete:
			return h.handleDeleteSegment(c, appID, segmentID)
		}
	case subPath == subPathJobsExport:
		return h.handleGetSegmentExportJobs(c, appID, segmentID)
	case subPath == subPathJobsImport:
		return h.handleGetSegmentImportJobs(c, appID, segmentID)
	case subPath == subPathVersions:
		return h.handleGetSegmentVersions(c, appID, segmentID)
	case strings.HasPrefix(subPath, subPathVersions+"/"):
		versionStr := strings.TrimPrefix(subPath, "versions/")
		v, _ := parseVersionParam(versionStr)

		return h.handleGetSegmentVersion(c, appID, segmentID, v)
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

// handleCreateSegment handles POST /v1/apps/{appId}/segments.
func (h *Handler) handleCreateSegment(c *echo.Context, appID string) error {
	return h.handleCreateNamedAppResource(c, appID, func(body []byte, region, appID string) (any, error) {
		var req createSegmentRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, errInvalidRequestBody
		}

		if strings.TrimSpace(req.Name) == "" {
			return nil, errNameRequired
		}

		segment, err := h.Backend.CreateSegment(region, h.AccountID, appID, req)
		if err != nil {
			return nil, err
		}

		return toSegmentResponse(segment), nil
	})
}

// handleGetSegment handles GET /v1/apps/{appId}/segments/{segmentId}.
func (h *Handler) handleGetSegment(c *echo.Context, appID, segmentID string) error {
	segment, err := h.Backend.GetSegment(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleGetSegments handles GET /v1/apps/{appId}/segments.
func (h *Handler) handleGetSegments(c *echo.Context, appID string) error {
	segments, err := h.Backend.GetSegments(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	offset, pageSize := parsePageParams(c)
	start, end, nextToken := applyPageParams(offset, pageSize, len(segments))

	items := make([]segmentResponse, 0, end-start)

	for _, s := range segments[start:end] {
		items = append(items, toSegmentResponse(s))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, pagedSegmentsResponse{
		NextToken: nextToken,
		Item:      items,
	})

	return nil
}

// handleUpdateSegment handles PUT /v1/apps/{appId}/segments/{segmentId}.
func (h *Handler) handleUpdateSegment(c *echo.Context, appID, segmentID string) error {
	var req updateSegmentRequest
	if !unmarshalBody(c, &req) {
		return nil
	}

	segment, backendErr := h.Backend.UpdateSegment(appID, segmentID, req)
	if backendErr != nil {
		return writeNotFoundOrInternal(c, backendErr)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleDeleteSegment handles DELETE /v1/apps/{appId}/segments/{segmentId}.
func (h *Handler) handleDeleteSegment(c *echo.Context, appID, segmentID string) error {
	segment, err := h.Backend.DeleteSegment(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleGetSegmentVersion handles GET /v1/apps/{appId}/segments/{segmentId}/versions/{version}.
func (h *Handler) handleGetSegmentVersion(c *echo.Context, appID, segmentID string, version int) error {
	segment, err := h.Backend.GetSegmentVersion(appID, segmentID, version)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toSegmentResponse(segment))

	return nil
}

// handleGetSegmentVersions handles GET /v1/apps/{appId}/segments/{segmentId}/versions.
func (h *Handler) handleGetSegmentVersions(c *echo.Context, appID, segmentID string) error {
	segments, err := h.Backend.GetSegmentVersions(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]segmentResponse, 0, len(segments))

	for _, s := range segments {
		items = append(items, toSegmentResponse(s))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, segmentVersionsResponse{Item: items})

	return nil
}

// handleGetSegmentExportJobs handles GET /v1/apps/{appId}/segments/{segmentId}/jobs/export.
func (h *Handler) handleGetSegmentExportJobs(c *echo.Context, appID, segmentID string) error {
	jobs, err := h.Backend.GetSegmentExportJobs(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]exportJobResponse, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, toExportJobResponse(j))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, exportJobsListResponse{Item: items})

	return nil
}

// handleGetSegmentImportJobs handles GET /v1/apps/{appId}/segments/{segmentId}/jobs/import.
func (h *Handler) handleGetSegmentImportJobs(c *echo.Context, appID, segmentID string) error {
	jobs, err := h.Backend.GetSegmentImportJobs(appID, segmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]importJobResponse, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, toImportJobResponse(j))
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, importJobsListResponse{Item: items})

	return nil
}

// ──────────────────────────────────────────────────
// Journey handlers
// ──────────────────────────────────────────────────

func toSegmentResponse(s *Segment) segmentResponse {
	return segmentResponse{
		ApplicationID:    s.ApplicationID,
		ARN:              s.ARN,
		ID:               s.ID,
		Name:             s.Name,
		SegmentType:      s.SegmentType,
		Tags:             s.Tags,
		Dimensions:       s.Dimensions,
		SegmentGroups:    s.SegmentGroups,
		ImportDefinition: s.ImportDefinition,
		CreationDate:     s.CreationDate,
		LastModifiedDate: s.LastModifiedDate,
		Version:          s.Version,
	}
}
