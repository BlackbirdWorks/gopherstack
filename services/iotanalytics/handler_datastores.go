package iotanalytics

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateDatastore(c *echo.Context, body []byte) error {
	var req createDatastoreRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.DatastoreName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "datastoreName is required")
	}

	if err := validateTags(req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	tags := tagsToMap(req.Tags)

	ds, err := h.Backend.CreateDatastore(
		c.Request().Context(),
		req.DatastoreName,
		tags,
		req.DatastoreStorage,
		req.RetentionPeriod,
		req.FileFormatConfiguration,
		req.Partitions,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createDatastoreResponse{
		DatastoreName:   ds.Name,
		DatastoreARN:    ds.ARN,
		RetentionPeriod: ds.RetentionPeriod,
	})
}

// datastoreFileFormatType derives DatastoreSummary.FileFormatType (types.go:952,
// FileFormatType enum in enums.go: JSON|PARQUET) from a datastore's
// FileFormatConfiguration. AWS IoT Analytics defaults an unconfigured
// datastore to JSON storage.
func datastoreFileFormatType(cfg *FileFormatConfiguration) string {
	if cfg != nil && cfg.ParquetConfiguration != nil {
		return "PARQUET"
	}

	return "JSON"
}

func (h *Handler) handleListDatastores(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	datastores := h.Backend.ListDatastores()

	summaries := make([]datastoreSummary, 0, len(datastores))
	var nextToken *string

	count := 0

	for _, ds := range datastores {
		if cursor != "" && ds.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].DatastoreName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, datastoreSummary{
			DatastoreName:          ds.Name,
			DatastoreStorage:       ds.Storage,
			Partitions:             ds.Partitions,
			FileFormatType:         datastoreFileFormatType(ds.FileFormatConfiguration),
			Status:                 ds.Status,
			CreationTime:           ds.CreationTime,
			LastUpdateTime:         ds.LastUpdate,
			LastMessageArrivalTime: ds.LastMessageArrivalTime,
		})
		count++
	}

	return c.JSON(http.StatusOK, listDatastoresResponse{
		DatastoreSummaries: summaries,
		NextToken:          nextToken,
	})
}

func (h *Handler) handleDescribeDatastore(c *echo.Context, name string) error {
	ds, err := h.Backend.DescribeDatastore(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	detail := datastoreDetail{
		Storage:                 ds.Storage,
		RetentionPeriod:         ds.RetentionPeriod,
		FileFormatConfiguration: ds.FileFormatConfiguration,
		Partitions:              ds.Partitions,
		Tags:                    mapToTagsSorted(ds.Tags),
		Name:                    ds.Name,
		ARN:                     ds.ARN,
		Status:                  ds.Status,
		CreationTime:            ds.CreationTime,
		LastUpdateTime:          ds.LastUpdate,
	}

	resp := describeDatastoreResponse{Datastore: detail}

	if c.Request().URL.Query().Get("includeStatistics") == "true" {
		resp.Statistics = &datastoreStatistics{
			Size: &datastoreStatisticsSize{
				EstimatedSizeInBytes: 0,
				EstimatedOn:          ds.LastUpdate,
			},
		}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateDatastore(c *echo.Context, name string, body []byte) error {
	var req updateDatastoreRequest

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

	err := h.Backend.UpdateDatastore(
		name, req.DatastoreStorage, req.RetentionPeriod, req.FileFormatConfiguration,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteDatastore(c *echo.Context, name string) error {
	if err := h.Backend.DeleteDatastore(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
