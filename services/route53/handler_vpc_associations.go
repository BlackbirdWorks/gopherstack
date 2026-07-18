package route53

import (
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// routeAuthorizeVPC routes VPC association authorization requests.
func (h *Handler) routeAuthorizeVPC(c *echo.Context, path, method string) error {
	switch method {
	case http.MethodPost:
		return h.createVPCAssociationAuthorization(c, path)
	case http.MethodGet:
		return h.listVPCAssociationAuthorizations(c, path)
	case http.MethodDelete:
		return h.deleteVPCAssociationAuthorization(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on authorizevpcassociation")
	}
}

// routeAssociateVPC routes VPC association requests.
func (h *Handler) routeAssociateVPC(c *echo.Context, path, method string) error {
	switch method {
	case http.MethodPost:
		return h.associateVPCWithHostedZone(c, path)
	case http.MethodGet:
		return h.listVPCAssociationAuthorizations(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on associatevpc")
	}
}

type xmlVPC struct {
	VPCRegion string `xml:"VPCRegion,omitempty"`
	VPCID     string `xml:"VPCId"`
}

type xmlAssociateVPCRequest struct {
	XMLName xml.Name `xml:"AssociateVPCWithHostedZoneRequest"`
	VPC     xmlVPC   `xml:"VPC"`
	Comment string   `xml:"Comment,omitempty"`
}

type xmlAssociateVPCResponse struct {
	XMLName    xml.Name      `xml:"AssociateVPCWithHostedZoneResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

func (h *Handler) associateVPCWithHostedZone(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	// path is /2013-04-01/hostedzone/{Id}/associatevpc
	withoutSuffix := strings.TrimSuffix(path, route53AssociateVPCSuffix)
	zoneID := extractZoneID(withoutSuffix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlAssociateVPCRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	if err = h.Backend.AssociateVPCWithHostedZone(zoneID, req.VPC.VPCID, req.VPC.VPCRegion); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 AssociateVPCWithHostedZone", "zoneID", zoneID, "vpcID", req.VPC.VPCID)

	return writeXML(c, http.StatusOK, xmlAssociateVPCResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + zoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	})
}

type vpcAssocAuthorizationsResponse struct {
	XMLName      xml.Name `xml:"ListVPCAssociationAuthorizationsResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	HostedZoneID string   `xml:"HostedZoneId"`
	VPCs         []xmlVPC `xml:"VPCs>VPC"`
}

func (h *Handler) listVPCAssociationAuthorizations(c *echo.Context, path string) error {
	zoneID := strings.TrimSuffix(strings.TrimPrefix(path, route53HZPrefix), route53AuthorizeVPCSuffix)

	auths, err := h.Backend.ListVPCAssociationAuthorizations(zoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	vpcs := make([]xmlVPC, 0, len(auths))
	for _, a := range auths {
		vpcs = append(vpcs, xmlVPC{VPCRegion: a.VPCRegion, VPCID: a.VPCID})
	}

	return writeXML(c, http.StatusOK, vpcAssocAuthorizationsResponse{
		Xmlns:        route53Namespace,
		HostedZoneID: zoneID,
		VPCs:         vpcs,
	})
}

type createVPCAssocAuthorizationResponse struct {
	XMLName      xml.Name `xml:"CreateVPCAssociationAuthorizationResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	HostedZoneID string   `xml:"HostedZoneId"`
	VPC          xmlVPC   `xml:"VPC"`
}

type createVPCAssocAuthRequest struct {
	XMLName xml.Name `xml:"CreateVPCAssociationAuthorizationRequest"`
	VPC     xmlVPC   `xml:"VPC"`
}

func (h *Handler) createVPCAssociationAuthorization(c *echo.Context, path string) error {
	zoneID := strings.TrimSuffix(strings.TrimPrefix(path, route53HZPrefix), route53AuthorizeVPCSuffix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req createVPCAssocAuthRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	auth, err := h.Backend.CreateVPCAssociationAuthorization(zoneID, req.VPC.VPCID, req.VPC.VPCRegion)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusCreated, createVPCAssocAuthorizationResponse{
		Xmlns:        route53Namespace,
		HostedZoneID: zoneID,
		VPC:          xmlVPC{VPCRegion: auth.VPCRegion, VPCID: auth.VPCID},
	})
}

type deleteVPCAssocAuthorizationResponse struct {
	XMLName xml.Name `xml:"DeleteVPCAssociationAuthorizationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type deleteVPCAssocAuthRequest struct {
	XMLName xml.Name `xml:"DeleteVPCAssociationAuthorizationRequest"`
	VPC     xmlVPC   `xml:"VPC"`
}

func (h *Handler) deleteVPCAssociationAuthorization(c *echo.Context, path string) error {
	zoneID := strings.TrimSuffix(strings.TrimPrefix(path, route53HZPrefix), route53AuthorizeVPCSuffix)
	if zoneID == "" {
		zoneID = strings.TrimSuffix(strings.TrimPrefix(path, route53HZPrefix), route53DeauthorizeVPCSuffix)
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req deleteVPCAssocAuthRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	if err = h.Backend.DeleteVPCAssociationAuthorization(zoneID, req.VPC.VPCID); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, deleteVPCAssocAuthorizationResponse{
		Xmlns: route53Namespace,
	})
}

type disassociateVPCResponse struct {
	XMLName    xml.Name      `xml:"DisassociateVPCFromHostedZoneResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlDisassociateVPCRequest struct {
	XMLName xml.Name `xml:"DisassociateVPCFromHostedZoneRequest"`
	VPC     xmlVPC   `xml:"VPC"`
}

func (h *Handler) disassociateVPCFromHostedZone(c *echo.Context, path string) error {
	zoneID := strings.TrimSuffix(strings.TrimPrefix(path, route53HZPrefix), route53DisassociateVPCSuffix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlDisassociateVPCRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	if err = h.Backend.DisassociateVPCFromHostedZone(zoneID, req.VPC.VPCID); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, disassociateVPCResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + zoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	})
}
