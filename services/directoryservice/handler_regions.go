package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleAddRegion(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		VPCSettings *struct {
			VpcID     string   `json:"VpcId"`
			SubnetIDs []string `json:"SubnetIds"`
		} `json:"VPCSettings"`
		DirectoryID string `json:"DirectoryId"`
		RegionName  string `json:"RegionName"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.RegionName == "" || req.VPCSettings == nil {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId, RegionName and VPCSettings are required"),
		)
	}

	vpcSettings := &DirectoryVpcSettings{
		VpcID:     req.VPCSettings.VpcID,
		SubnetIDs: req.VPCSettings.SubnetIDs,
	}

	addErr := h.Backend.AddRegion(h.contextWithRegion(c), req.DirectoryID, req.RegionName, vpcSettings)
	if addErr != nil {
		return h.mapError(c, addErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRemoveRegion(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	if removeErr := h.Backend.RemoveRegion(h.contextWithRegion(c), req.DirectoryID); removeErr != nil {
		return h.mapError(c, removeErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeRegions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		RegionName  string `json:"RegionName"`
		NextToken   string `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	regions, nextToken, descErr := h.Backend.DescribeRegions(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RegionName,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	regionList := make([]map[string]any, 0, len(regions))
	for _, r := range regions {
		entry := map[string]any{
			keyDirectoryID:                     r.DirectoryID,
			"RegionName":                       r.RegionName,
			"RegionType":                       r.RegionType,
			keyStatus:                          r.Status,
			keyLaunchTime:                      awstime.Epoch(r.LaunchTime),
			"StatusLastUpdatedDateTime":        awstime.Epoch(r.StatusLastUpdatedDateTime),
			"DesiredNumberOfDomainControllers": r.DesiredNumberOfDomainCtrls,
		}
		if r.VpcSettings != nil {
			entry["VpcSettings"] = map[string]any{
				"VpcId":     r.VpcSettings.VpcID,
				"SubnetIds": r.VpcSettings.SubnetIDs,
			}
		}
		regionList = append(regionList, entry)
	}

	resp := map[string]any{"RegionsDescription": regionList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
