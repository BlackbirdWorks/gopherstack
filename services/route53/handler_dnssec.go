package route53

import (
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) routeHostedZoneDNSSEC(c *echo.Context, path, method string) (bool, error) {
	if strings.HasSuffix(path, route53EnableDNSSECSuffix) {
		if method == http.MethodPost {
			zoneID := strings.TrimSuffix(
				strings.TrimPrefix(path, route53HZPrefix),
				route53EnableDNSSECSuffix,
			)

			return true, h.enableHostedZoneDNSSEC(c, zoneID)
		}

		return true, xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on enable-dnssec")
	}

	if strings.HasSuffix(path, route53DisableDNSSECSuffix) {
		if method == http.MethodPost {
			zoneID := strings.TrimSuffix(
				strings.TrimPrefix(path, route53HZPrefix),
				route53DisableDNSSECSuffix,
			)

			return true, h.disableHostedZoneDNSSEC(c, zoneID)
		}

		return true, xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on disable-dnssec")
	}

	if strings.HasSuffix(path, route53DNSSECSuffix) {
		if method == http.MethodGet {
			zoneID := strings.TrimSuffix(
				strings.TrimPrefix(path, route53HZPrefix),
				route53DNSSECSuffix,
			)

			return true, h.getHostedZoneDNSSEC(c, zoneID)
		}

		return true, xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on dnssec")
	}

	return false, nil
}

type xmlGetDNSSECResponse struct {
	XMLName        xml.Name        `xml:"GetDNSSECResponse"`
	Xmlns          string          `xml:"xmlns,attr"`
	Status         xmlDNSSECStatus `xml:"Status"`
	KeySigningKeys []xmlKSKMember  `xml:"KeySigningKeys>member"`
}

type xmlDNSSECStatus struct {
	ServeSignature string `xml:"ServeSignature"`
	StatusMessage  string `xml:"StatusMessage,omitempty"`
}

func (h *Handler) enableHostedZoneDNSSEC(c *echo.Context, zoneID string) error {
	ctx := c.Request().Context()

	if err := h.Backend.EnableHostedZoneDNSSEC(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 EnableHostedZoneDNSSEC", "zoneID", zoneID)

	resp := struct {
		XMLName    xml.Name      `xml:"EnableHostedZoneDNSSECResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/enable-dnssec-" + zoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) disableHostedZoneDNSSEC(c *echo.Context, zoneID string) error {
	ctx := c.Request().Context()

	if err := h.Backend.DisableHostedZoneDNSSEC(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DisableHostedZoneDNSSEC", "zoneID", zoneID)

	resp := struct {
		XMLName    xml.Name      `xml:"DisableHostedZoneDNSSECResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/disable-dnssec-" + zoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) getHostedZoneDNSSEC(c *echo.Context, zoneID string) error {
	ctx := c.Request().Context()

	enabled, ksks, err := h.Backend.GetDNSSEC(zoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetDNSSEC", "zoneID", zoneID, "enabled", enabled)

	serveSignature := "NOT_SIGNING"
	if enabled {
		serveSignature = "SIGNING"
	}

	xmlKSKs := make([]xmlKSKMember, 0, len(ksks))
	for i := range ksks {
		xmlKSKs = append(xmlKSKs, toXMLKSKMember(&ksks[i]))
	}

	resp := xmlGetDNSSECResponse{
		Xmlns: route53Namespace,
		Status: xmlDNSSECStatus{
			ServeSignature: serveSignature,
		},
		KeySigningKeys: xmlKSKs,
	}

	return writeXML(c, http.StatusOK, resp)
}
