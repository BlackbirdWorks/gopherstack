package directoryservice

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateConditionalForwarder(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID      string   `json:"DirectoryId"`
		RemoteDomainName string   `json:"RemoteDomainName"`
		DNSIpAddrs       []string `json:"DnsIpAddrs"`
		DNSIpv6Addrs     []string `json:"DnsIpv6Addrs"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.RemoteDomainName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and RemoteDomainName are required"),
		)
	}

	if createErr := h.Backend.CreateConditionalForwarder(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RemoteDomainName,
		req.DNSIpAddrs,
		req.DNSIpv6Addrs,
	); createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateConditionalForwarder(c *echo.Context) error { //nolint:dupl // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID      string   `json:"DirectoryId"`
		RemoteDomainName string   `json:"RemoteDomainName"`
		DNSIpAddrs       []string `json:"DnsIpAddrs"`
		DNSIpv6Addrs     []string `json:"DnsIpv6Addrs"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.RemoteDomainName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and RemoteDomainName are required"),
		)
	}

	if updateErr := h.Backend.UpdateConditionalForwarder(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RemoteDomainName,
		req.DNSIpAddrs,
		req.DNSIpv6Addrs,
	); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteConditionalForwarder(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: keyRemoteDomainName,
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeleteConditionalForwarder(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDescribeConditionalForwarders(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID       string   `json:"DirectoryId"`
		RemoteDomainNames []string `json:"RemoteDomainNames"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	fwds, descErr := h.Backend.DescribeConditionalForwarders(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.RemoteDomainNames,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	fwdList := make([]map[string]any, 0, len(fwds))
	for _, f := range fwds {
		fwdList = append(fwdList, map[string]any{
			"RemoteDomainName": f.RemoteDomainName,
			"DnsIpAddrs":       f.DNSIPAddrs,
			"DnsIpv6Addrs":     f.DNSIPv6Addrs,
			"ReplicationScope": f.ReplicationScope,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"ConditionalForwarders": fwdList})
}
