package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opGetClustersForImage = "GetClustersForImage"

	pathClusterGet = "/cluster/get"
)

func (h *Handler) handleGetClustersForImage(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	// Real GetClustersForImageInput nests the required resourceId under a
	// "filter" object (ClusterForImageFilterCriteria), not a bare
	// "filterCriteria" map -- decoding into the wrong key silently drops the
	// value on every real request.
	var req struct {
		Filter struct {
			ResourceID string `json:"resourceId"`
		} `json:"filter"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	result, getErr := h.Backend.GetClustersForImage(req.Filter.ResourceID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}
