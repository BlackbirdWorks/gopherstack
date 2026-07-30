package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleDescribeDomainControllers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID         string   `json:"DirectoryId"`
		NextToken           string   `json:"NextToken"`
		DomainControllerIDs []string `json:"DomainControllerIds"`
		Limit               int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	dcs, nextToken, descErr := h.Backend.DescribeDomainControllers(
		h.contextWithRegion(c),
		req.DirectoryID, req.DomainControllerIDs, req.Limit, req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	dcList := make([]map[string]any, 0, len(dcs))
	for _, dc := range dcs {
		dcList = append(dcList, map[string]any{
			"DomainControllerId":        dc.ControllerID,
			keyDirectoryID:              dc.DirectoryID,
			keyStatus:                   dc.Status,
			"AvailabilityZone":          dc.AvailabilityZone,
			keyLaunchTime:               awstime.Epoch(dc.LaunchTime),
			"StatusLastUpdatedDateTime": awstime.Epoch(dc.StatusLastUpdatedDateTime),
			"DnsIpAddr":                 dc.DNSIPAddr,
			"DnsIpv6Addr":               dc.DNSIPv6Addr,
			"SubnetId":                  dc.SubnetID,
			keyVpcID:                    dc.VpcID,
		})
	}

	resp := map[string]any{"DomainControllers": dcList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateNumberOfDomainControllers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID   string `json:"DirectoryId"`
		DesiredNumber int32  `json:"DesiredNumber"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	updateErr := h.Backend.UpdateNumberOfDomainControllers(h.contextWithRegion(c), req.DirectoryID, req.DesiredNumber)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
