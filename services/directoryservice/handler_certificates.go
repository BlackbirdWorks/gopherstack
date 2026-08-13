package directoryservice

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleRegisterCertificate(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID     string `json:"DirectoryId"`
		CertificateData string `json:"CertificateData"`
		Type            string `json:"Type"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.CertificateData == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and CertificateData are required"),
		)
	}
	if !validEnum(req.Type, "ClientLDAPS", "ClientCertAuth") {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid Type"))
	}

	certType := req.Type
	if certType == "" {
		certType = "ClientLDAPS"
	}

	certID, regErr := h.Backend.RegisterCertificate(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.CertificateData,
		certType,
	)
	if regErr != nil {
		return h.mapError(c, regErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"CertificateId": certID}) //nolint:goconst // existing issue.
}

func (h *Handler) handleDeregisterCertificate(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: "CertificateId",
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeregisterCertificate(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleListCertificates(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		PageSize    int32  `json:"PageSize"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	certs, nextToken, listErr := h.Backend.ListCertificates(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.PageSize,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	certList := make([]map[string]any, 0, len(certs))
	for _, cert := range certs {
		certList = append(certList, map[string]any{
			"CertificateId":  cert.CertificateID,
			"CommonName":     cert.CommonName,
			"Type":           cert.CertType, //nolint:goconst // existing issue.
			"State":          cert.State,
			"ExpiryDateTime": awstime.Epoch(cert.ExpiryDateTime),
		})
	}

	resp := map[string]any{"CertificatesInfo": certList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeCertificate(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID   string `json:"DirectoryId"`
		CertificateID string `json:"CertificateId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.CertificateID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and CertificateId are required"),
		)
	}

	cert, descErr := h.Backend.DescribeCertificate(h.contextWithRegion(c), req.DirectoryID, req.CertificateID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Certificate": map[string]any{
			"CertificateId":      cert.CertificateID,
			"CommonName":         cert.CommonName,
			"Type":               cert.CertType,
			"State":              cert.State,
			"RegisteredDateTime": awstime.Epoch(cert.RegisteredDateTime),
			"ExpiryDateTime":     awstime.Epoch(cert.ExpiryDateTime),
		},
	})
}

// --- CA Enrollment Policy ---

func (h *Handler) handleEnableCAEnrollmentPolicy(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID     string `json:"DirectoryId"`
		PcaConnectorArn string `json:"PcaConnectorArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	if req.PcaConnectorArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "PcaConnectorArn is required"))
	}

	if enableErr := h.Backend.EnableCAEnrollmentPolicy(
		h.contextWithRegion(c), req.DirectoryID, req.PcaConnectorArn,
	); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableCAEnrollmentPolicy(c *echo.Context) error {
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

	if disableErr := h.Backend.DisableCAEnrollmentPolicy(h.contextWithRegion(c), req.DirectoryID); disableErr != nil {
		return h.mapError(c, disableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeCAEnrollmentPolicy(c *echo.Context) error {
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

	policy, descErr := h.Backend.DescribeCAEnrollmentPolicy(h.contextWithRegion(c), req.DirectoryID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	resp := map[string]any{
		"CaEnrollmentPolicyStatus": policy.Status,
		"DirectoryId":              policy.DirectoryID,
	}

	if policy.StatusReason != "" {
		resp["CaEnrollmentPolicyStatusReason"] = policy.StatusReason
	}

	if policy.PcaConnectorArn != "" {
		resp["PcaConnectorArn"] = policy.PcaConnectorArn
	}

	if !policy.LastUpdatedDateTime.IsZero() {
		resp["LastUpdatedDateTime"] = awstime.Epoch(policy.LastUpdatedDateTime)
	}

	return c.JSON(http.StatusOK, resp)
}
