package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleAddRegion(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		RegionName  string `json:"RegionName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	status, err := h.Backend.AddRegion(req.InstanceArn, req.RegionName)
	if err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyStatus: status,
	})
}

func (h *Handler) handleRemoveRegion(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		RegionName  string `json:"RegionName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.RegionName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "RegionName is required")
	}
	status, err := h.Backend.RemoveRegion(req.InstanceArn, req.RegionName)
	if err != nil {
		return handleBackendError(c, err, "region not found: "+req.RegionName)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyStatus: status,
	})
}

func (h *Handler) handleListRegions(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	regions, err := h.Backend.ListRegions(req.InstanceArn)
	if err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	out := make([]map[string]any, 0, len(regions))
	for _, r := range regions {
		out = append(out, regionMetadataView(r))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Regions":    out,
		keyNextToken: nil,
	})
}

// regionMetadataView renders a RegionMetadata using the real
// ssoadmin.RegionMetadata wire field names (AddedDate as epoch seconds).
func regionMetadataView(r RegionMetadata) map[string]any {
	return map[string]any{
		"RegionName":      r.RegionName,
		keyStatus:         r.Status,
		"IsPrimaryRegion": r.IsPrimaryRegion,
		"AddedDate":       float64(r.AddedDate.Unix()),
	}
}

func (h *Handler) handleDescribeRegion(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		RegionName  string `json:"RegionName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.RegionName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "RegionName is required")
	}

	region, err := h.Backend.DescribeRegion(req.InstanceArn, req.RegionName)
	if err != nil {
		return handleBackendError(c, err, "region not found: "+req.RegionName)
	}

	return writeJSON(c, http.StatusOK, regionMetadataView(*region))
}
