package iotanalytics

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

// parseQueryDateTime parses a scheduledBefore/scheduledOnOrAfter query parameter into epoch
// seconds. The real SDK serializes these via smithytime.FormatDateTime
// ("2006-01-02T15:04:05.999Z", always UTC with a literal "Z"), but a raw HTTP caller may
// reasonably send any RFC3339 variant, so parsing tries a short list of layouts. Returns
// ok=false if s is empty or matches none of them, in which case the caller ignores the
// filter rather than rejecting the request -- AWS's own DateTime shape parsing is handled
// entirely client-side by the SDK before the request is ever sent.
func parseQueryDateTime(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}

	layouts := [...]string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999"}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return epochSeconds(t), true
		}
	}

	return 0, false
}

// filterDatasetContentsBySchedule applies the scheduledBefore / scheduledOnOrAfter query
// filters (present on ListDatasetContentsInput) against each content version's ScheduleTime.
func filterDatasetContentsBySchedule(contents []*DatasetContent, c *echo.Context) []*DatasetContent {
	before, hasBefore := parseQueryDateTime(c.Request().URL.Query().Get("scheduledBefore"))
	onOrAfter, hasAfter := parseQueryDateTime(c.Request().URL.Query().Get("scheduledOnOrAfter"))

	if !hasBefore && !hasAfter {
		return contents
	}

	filtered := make([]*DatasetContent, 0, len(contents))

	for _, content := range contents {
		if hasBefore && content.ScheduleTime >= before {
			continue
		}

		if hasAfter && content.ScheduleTime < onOrAfter {
			continue
		}

		filtered = append(filtered, content)
	}

	return filtered
}

func (h *Handler) handleCreateDataset(c *echo.Context, body []byte) error {
	var req createDatasetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.DatasetName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "datasetName is required")
	}

	if err := validateTags(req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	tags := tagsToMap(req.Tags)

	ds, err := h.Backend.CreateDataset(
		c.Request().Context(),
		req.DatasetName,
		tags,
		req.Actions,
		req.Triggers,
		req.ContentDeliveryRules,
		req.VersioningConfiguration,
		req.LateDataRules,
		req.RetentionPeriod,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createDatasetResponse{
		DatasetName:     ds.Name,
		DatasetARN:      ds.ARN,
		RetentionPeriod: ds.RetentionPeriod,
	})
}

func (h *Handler) handleListDatasets(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	datasets := h.Backend.ListDatasets()

	summaries := make([]datasetSummary, 0, len(datasets))
	var nextToken *string

	count := 0

	for _, ds := range datasets {
		if cursor != "" && ds.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].DatasetName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, datasetSummary{
			DatasetName:    ds.Name,
			Status:         ds.Status,
			CreationTime:   ds.CreationTime,
			LastUpdateTime: ds.LastUpdate,
		})
		count++
	}

	return c.JSON(http.StatusOK, listDatasetsResponse{
		DatasetSummaries: summaries,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleDescribeDataset(c *echo.Context, name string) error {
	ds, err := h.Backend.DescribeDataset(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeDatasetResponse{
		Dataset: datasetDetail{
			Actions:                 ds.Actions,
			Triggers:                ds.Triggers,
			ContentDeliveryRules:    ds.ContentDeliveryRules,
			LateDataRules:           ds.LateDataRules,
			VersioningConfiguration: ds.VersioningConfiguration,
			RetentionPeriod:         ds.RetentionPeriod,
			Tags:                    mapToTagsSorted(ds.Tags),
			Name:                    ds.Name,
			ARN:                     ds.ARN,
			Status:                  ds.Status,
			CreationTime:            ds.CreationTime,
			LastUpdateTime:          ds.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdateDataset(c *echo.Context, name string, body []byte) error {
	var req updateDatasetRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	err := h.Backend.UpdateDataset(
		name, req.Actions, req.Triggers, req.ContentDeliveryRules, req.VersioningConfiguration, req.LateDataRules,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteDataset(c *echo.Context, name string) error {
	if err := h.Backend.DeleteDataset(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateDatasetContent(c *echo.Context, datasetName string, body []byte) error {
	var req createDatasetContentRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	content, err := h.Backend.CreateDatasetContent(datasetName, req.VersionID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createDatasetContentResponse{
		VersionID: content.VersionID,
	})
}

func (h *Handler) handleGetDatasetContent(c *echo.Context, datasetName string) error {
	versionID := c.Request().URL.Query().Get("versionId")

	content, err := h.Backend.GetDatasetContent(datasetName, versionID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getDatasetContentResponse{
		Status:    &datasetContentStatusDTO{State: content.Status},
		Entries:   []datasetContentEntry{},
		Timestamp: content.CreationTime,
	})
}

// handleListDatasetContents paginates by position, not by a name-like cursor: unlike
// ListChannels/ListDatastores/ListDatasets/ListPipelines, whose summaries are naturally
// sorted ascending by the same field (Name) used as the cursor threshold, dataset content
// summaries are sorted by CreationTime descending while their identity field (VersionID) is
// a random UUID unrelated to that order. Thresholding on VersionID would skip or repeat
// entries arbitrarily across pages, so the opaque token instead encodes a slice offset.
func (h *Handler) handleListDatasetContents(c *echo.Context, datasetName string) error {
	maxResults, cursor := parsePagination(c)

	contents, err := h.Backend.ListDatasetContents(datasetName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	contents = filterDatasetContentsBySchedule(contents, c)

	start := 0

	if cursor != "" {
		if n, convErr := strconv.Atoi(cursor); convErr == nil && n >= 0 && n <= len(contents) {
			start = n
		}
	}

	end := min(start+maxResults, len(contents))
	summaries := make([]datasetContentSummary, 0, end-start)

	for _, content := range contents[start:end] {
		summaries = append(summaries, datasetContentSummary{
			Version:        content.VersionID,
			Status:         &datasetContentStatusDTO{State: content.Status},
			CreationTime:   content.CreationTime,
			CompletionTime: content.CompletionTime,
			ScheduleTime:   content.ScheduleTime,
		})
	}

	var nextToken *string

	if end < len(contents) {
		tok := encodeNextToken(strconv.Itoa(end))
		nextToken = &tok
	}

	return c.JSON(http.StatusOK, listDatasetContentsResponse{
		DatasetContentSummaries: summaries,
		NextToken:               nextToken,
	})
}

func (h *Handler) handleDeleteDatasetContent(c *echo.Context, datasetName string) error {
	versionID := c.Request().URL.Query().Get("versionId")

	if err := h.Backend.DeleteDatasetContent(datasetName, versionID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
