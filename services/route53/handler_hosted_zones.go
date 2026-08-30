package route53

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// IAMAction returns the IAM action for a Route53 HTTP request.
// It implements iam.ActionExtractor, providing per-service action extraction
// for Route53 REST API paths that are not covered by the global action mapper.
// iamActionForHostedZone maps hosted zone / rrset paths to IAM action strings.
// Returns "" when the path does not match.
func iamActionForHostedZone(path, method string) string {
	if action := iamActionForHostedZoneRoot(path, method); action != "" {
		return action
	}

	switch {
	case strings.HasPrefix(path, route53TagsPrefix) && method == http.MethodGet:
		return "route53:ListTagsForResource"
	case strings.HasPrefix(path, route53TagsPrefix):
		return "route53:ChangeTagsForResource"
	case method == http.MethodDelete && strings.HasPrefix(path, route53HZPrefix):
		return "route53:DeleteHostedZone"
	case method == http.MethodGet && strings.HasPrefix(path, route53HZPrefix):
		return "route53:GetHostedZone"
	default:
		return ""
	}
}

// iamActionForHostedZoneRoot maps the hosted zone root and rrset paths to IAM action strings.
func iamActionForHostedZoneRoot(path, method string) string {
	switch {
	case path == route53HostedZone && method == http.MethodPost:
		return "route53:CreateHostedZone"
	case path == route53HostedZone && method == http.MethodGet:
		return "route53:ListHostedZones"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodPost:
		return "route53:ChangeResourceRecordSets"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodGet:
		return "route53:ListResourceRecordSets"
	default:
		return ""
	}
}

func (h *Handler) routeHostedZoneRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createHostedZone(c)
	case http.MethodGet:
		return h.listHostedZones(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on /hostedzone")
	}
}

// routeHostedZone routes requests for a specific hosted zone by path suffix.
func (h *Handler) routeHostedZone(c *echo.Context, path, method string) error {
	if ok, err := h.routeHostedZoneSuffix(c, path, method); ok {
		return err
	}

	if ok, err := h.routeHostedZoneDNSSEC(c, path, method); ok {
		return err
	}

	switch method {
	case http.MethodDelete:
		return h.deleteHostedZone(c)
	case http.MethodGet:
		return h.getHostedZone(c)
	case http.MethodPost:
		return h.updateHostedZoneComment(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on hosted zone")
	}
}

// routeHostedZoneSuffix handles suffix-based sub-paths for a hosted zone.
func (h *Handler) routeHostedZoneSuffix(c *echo.Context, path, method string) (bool, error) {
	if strings.HasSuffix(path, route53RRSetSuffix) {
		return true, h.routeRRSet(c, method)
	}

	if strings.HasSuffix(path, route53AssociateVPCSuffix) {
		return true, h.routeAssociateVPC(c, path, method)
	}

	if strings.HasSuffix(path, route53AuthorizeVPCSuffix) {
		return true, h.routeAuthorizeVPC(c, path, method)
	}

	if strings.HasSuffix(path, route53DeauthorizeVPCSuffix) && method == http.MethodPost {
		return true, h.deleteVPCAssociationAuthorization(c, path)
	}

	if strings.HasSuffix(path, route53DisassociateVPCSuffix) && method == http.MethodPost {
		return true, h.disassociateVPCFromHostedZone(c, path)
	}

	if strings.HasSuffix(path, route53FeaturesSuffix) && method == http.MethodPost {
		return true, h.updateHostedZoneFeatures(c, path)
	}

	return false, nil
}

type xmlHostedZoneConfig struct {
	Comment     string `xml:"Comment,omitempty"`
	PrivateZone bool   `xml:"PrivateZone"`
}

type xmlHostedZone struct {
	XMLName                xml.Name            `xml:"HostedZone"`
	ID                     string              `xml:"Id"`
	Name                   string              `xml:"Name"`
	CallerReference        string              `xml:"CallerReference"`
	Config                 xmlHostedZoneConfig `xml:"Config"`
	ResourceRecordSetCount int                 `xml:"ResourceRecordSetCount"`
}

type xmlDelegationSet struct {
	XMLName         xml.Name `xml:"DelegationSet"`
	ID              string   `xml:"Id,omitempty"`
	CallerReference string   `xml:"CallerReference,omitempty"`
	NameServers     []string `xml:"NameServers>NameServer"`
}

type xmlCreateHostedZoneResponse struct {
	ChangeInfo    xmlChangeInfo    `xml:"ChangeInfo"`
	XMLName       xml.Name         `xml:"CreateHostedZoneResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	HostedZone    xmlHostedZone    `xml:"HostedZone"`
}

type xmlGetHostedZoneResponse struct {
	XMLName       xml.Name         `xml:"GetHostedZoneResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	VPCs          []xmlVPC         `xml:"VPCs>VPC,omitempty"`
	HostedZone    xmlHostedZone    `xml:"HostedZone"`
}

type xmlListHostedZonesResponse struct {
	XMLName     xml.Name        `xml:"ListHostedZonesResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	Marker      string          `xml:"Marker"`
	MaxItems    string          `xml:"MaxItems"`
	NextMarker  string          `xml:"NextMarker,omitempty"`
	HostedZones []xmlHostedZone `xml:"HostedZones>HostedZone"`
	IsTruncated bool            `xml:"IsTruncated"`
}

type xmlDeleteHostedZoneResponse struct {
	XMLName    xml.Name      `xml:"DeleteHostedZoneResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlCreateHostedZoneRequest struct {
	VPC              *xmlVPC             `xml:"VPC"`
	XMLName          xml.Name            `xml:"CreateHostedZoneRequest"`
	Name             string              `xml:"Name"`
	CallerReference  string              `xml:"CallerReference"`
	DelegationSetID  string              `xml:"DelegationSetId,omitempty"`
	HostedZoneConfig xmlHostedZoneConfig `xml:"HostedZoneConfig"`
}

func (h *Handler) createHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateHostedZoneRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	var vpcID, vpcRegion string
	if req.VPC != nil {
		vpcID, vpcRegion = req.VPC.VPCID, req.VPC.VPCRegion
	}

	hz, err := h.Backend.CreateHostedZone(
		req.Name, req.CallerReference,
		req.HostedZoneConfig.Comment, req.HostedZoneConfig.PrivateZone,
		normaliseDelegationSetID(req.DelegationSetID),
		vpcID, vpcRegion,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateHostedZone", "id", hz.ID, "name", hz.Name)

	resp := xmlCreateHostedZoneResponse{
		Xmlns:      route53Namespace,
		HostedZone: toXMLHostedZone(hz),
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + hz.ID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
		DelegationSet: toXMLDelegationSet(hz),
	}

	c.Response().Header().Set("Location", "/2013-04-01/hostedzone/"+hz.ID)

	return writeXML(c, http.StatusCreated, resp)
}

// toXMLDelegationSet builds the DelegationSet wire element for a hosted
// zone's CreateHostedZone/GetHostedZone response. When the zone was created
// with a reusable delegation set, Id is populated; otherwise (the common
// case — a system-assigned delegation set) it is omitted, matching real AWS.
func toXMLDelegationSet(hz *HostedZone) xmlDelegationSet {
	nameServers := hz.NameServers
	if len(nameServers) == 0 {
		// Zones created before NameServers was tracked (e.g. restored from
		// an older snapshot) fall back to the fixed defaults.
		nameServers = []string{dnsNS1Default, dnsNS2Default}
	}

	return xmlDelegationSet{
		ID:          hz.DelegationSetID,
		NameServers: nameServers,
	}
}

func (h *Handler) getHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	hz, err := h.Backend.GetHostedZone(zoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetHostedZone", "id", hz.ID)

	var xmlVPCs []xmlVPC
	if hz.PrivateZone {
		if assocs, assocErr := h.Backend.ListVPCAssociations(zoneID); assocErr == nil {
			xmlVPCs = make([]xmlVPC, 0, len(assocs))
			for _, a := range assocs {
				xmlVPCs = append(xmlVPCs, xmlVPC{VPCID: a.VPCID, VPCRegion: a.VPCRegion})
			}
		}
	}

	resp := xmlGetHostedZoneResponse{
		Xmlns:         route53Namespace,
		HostedZone:    toXMLHostedZone(hz),
		DelegationSet: toXMLDelegationSet(hz),
		VPCs:          xmlVPCs,
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) deleteHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	if err := h.Backend.DeleteHostedZone(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	h.deleteTagsForResource(zoneID)

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteHostedZone", "id", zoneID)

	resp := xmlDeleteHostedZoneResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			SubmittedAt: time.Now(),
			ID:          "/change/C" + zoneID,
			Status:      statusInsync,
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) listHostedZones(c *echo.Context) error {
	q := c.Request().URL.Query()
	marker := q.Get("marker")
	maxItems := 0
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}
	delegationSetID := normaliseDelegationSetID(q.Get("delegationsetid"))
	hostedZoneType := q.Get("hostedzonetype")

	p, err := h.Backend.ListHostedZones(marker, maxItems, delegationSetID, hostedZoneType)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlZones := make([]xmlHostedZone, len(p.Data))
	for i := range p.Data {
		xmlZones[i] = toXMLHostedZone(&p.Data[i])
	}

	resp := xmlListHostedZonesResponse{
		Xmlns:       route53Namespace,
		Marker:      marker,
		HostedZones: xmlZones,
		IsTruncated: p.Next != "",
		NextMarker:  p.Next,
		MaxItems:    strconv.Itoa(maxItems),
	}

	return writeXML(c, http.StatusOK, resp)
}

func toXMLHostedZone(hz *HostedZone) xmlHostedZone {
	return xmlHostedZone{
		ID:              "/hostedzone/" + hz.ID,
		Name:            hz.Name,
		CallerReference: hz.CallerReference,
		Config: xmlHostedZoneConfig{
			Comment:     hz.Comment,
			PrivateZone: hz.PrivateZone,
		},
		ResourceRecordSetCount: hz.ResourceRecordSetCount,
	}
}

type hostedZoneCountResponse struct {
	XMLName         xml.Name `xml:"GetHostedZoneCountResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	HostedZoneCount int      `xml:"HostedZoneCount"`
}

func (h *Handler) getHostedZoneCount(c *echo.Context) error {
	count := h.Backend.GetHostedZoneCount()

	return writeXML(c, http.StatusOK, hostedZoneCountResponse{
		Xmlns:           route53Namespace,
		HostedZoneCount: count,
	})
}

type listHZByNameResponse struct {
	XMLName          xml.Name        `xml:"ListHostedZonesByNameResponse"`
	Xmlns            string          `xml:"xmlns,attr"`
	DNSName          string          `xml:"DNSName,omitempty"`
	HostedZoneID     string          `xml:"HostedZoneId,omitempty"`
	MaxItems         string          `xml:"MaxItems"`
	NextDNSName      string          `xml:"NextDNSName,omitempty"`
	NextHostedZoneID string          `xml:"NextHostedZoneId,omitempty"`
	HostedZones      []xmlHostedZone `xml:"HostedZones>HostedZone"`
	IsTruncated      bool            `xml:"IsTruncated"`
}

func (h *Handler) listHostedZonesByName(c *echo.Context) error {
	dnsName := c.Request().URL.Query().Get("dnsname")
	zoneID := c.Request().URL.Query().Get("hostedzoneid")
	maxItemsStr := c.Request().URL.Query().Get("maxitems")
	maxItems := maxHZByName
	if maxItemsStr != "" {
		if v, err := strconv.Atoi(maxItemsStr); err == nil && v > 0 {
			maxItems = v
		}
	}

	zones, nextDNSName, nextZoneID, err := h.Backend.ListHostedZonesByName(dnsName, zoneID, maxItems)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	xmlZones := make([]xmlHostedZone, 0, len(zones))
	for _, z := range zones {
		xmlZones = append(xmlZones, xmlHostedZone{
			ID:              "/hostedzone/" + z.ID,
			Name:            z.Name,
			CallerReference: z.CallerReference,
			Config:          xmlHostedZoneConfig{Comment: z.Comment},
		})
	}

	return writeXML(c, http.StatusOK, listHZByNameResponse{
		Xmlns:            route53Namespace,
		DNSName:          dnsName,
		HostedZoneID:     zoneID,
		HostedZones:      xmlZones,
		IsTruncated:      nextDNSName != "",
		MaxItems:         strconv.Itoa(maxItems),
		NextDNSName:      nextDNSName,
		NextHostedZoneID: nextZoneID,
	})
}

// xmlHostedZoneOwner mirrors types.HostedZoneOwner (route53@v1.65.6
// deserializers.go: awsRestxml_deserializeDocumentHostedZoneOwner).
type xmlHostedZoneOwner struct {
	OwningAccount string `xml:"OwningAccount,omitempty"`
	OwningService string `xml:"OwningService,omitempty"`
}

// xmlHostedZoneSummary mirrors types.HostedZoneSummary (route53@v1.65.6
// deserializers.go: awsRestxml_deserializeDocumentHostedZoneSummary) — its Id
// field's wire element is "HostedZoneId", not "Id", and it carries a nested
// Owner, unlike the full xmlHostedZone shape ListHostedZones/ByName return.
type xmlHostedZoneSummary struct {
	HostedZoneID string             `xml:"HostedZoneId"`
	Name         string             `xml:"Name"`
	Owner        xmlHostedZoneOwner `xml:"Owner"`
}

type listHZByVPCResponse struct {
	XMLName     xml.Name               `xml:"ListHostedZonesByVPCResponse"`
	Xmlns       string                 `xml:"xmlns,attr"`
	MaxItems    string                 `xml:"MaxItems"`
	NextToken   string                 `xml:"NextToken,omitempty"`
	HostedZones []xmlHostedZoneSummary `xml:"HostedZoneSummaries>HostedZoneSummary"`
}

func (h *Handler) listHostedZonesByVPC(c *echo.Context) error {
	vpcID := c.Request().URL.Query().Get("vpcid")
	vpcRegion := c.Request().URL.Query().Get("vpcregion")
	nextToken := c.Request().URL.Query().Get("nexttoken")
	maxItems := maxHZByVPC
	if v := c.Request().URL.Query().Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	if vpcID == "" || vpcRegion == "" {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "vpcid and vpcregion are required")
	}

	p, err := h.Backend.ListHostedZonesByVPC(vpcID, vpcRegion, nextToken, maxItems)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	xmlZones := make([]xmlHostedZoneSummary, 0, len(p.Data))
	for _, z := range p.Data {
		xmlZones = append(xmlZones, xmlHostedZoneSummary{
			HostedZoneID: "/hostedzone/" + z.ID,
			Name:         z.Name,
			Owner:        xmlHostedZoneOwner{OwningAccount: h.Backend.AccountID()},
		})
	}

	return writeXML(c, http.StatusOK, listHZByVPCResponse{
		Xmlns:       route53Namespace,
		HostedZones: xmlZones,
		MaxItems:    strconv.Itoa(maxItems),
		NextToken:   p.Next,
	})
}

type hostedZoneLimitResponse struct {
	XMLName xml.Name `xml:"GetHostedZoneLimitResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Limit   xmlLimit `xml:"Limit"`
	Count   int      `xml:"Count"`
}

func (h *Handler) getHostedZoneLimit(c *echo.Context, path string) error {
	parts := strings.TrimPrefix(path, route53HostedZoneLimitPrefix)
	zoneID := parts
	limitType := ""
	if before, after, ok := strings.Cut(parts, "/"); ok {
		zoneID = before
		limitType = after
	}
	zoneID = strings.TrimPrefix(zoneID, "/hostedzone/")

	var (
		count int
		err   error
	)

	switch limitType {
	case route53LimitMaxVPCsAssociatedByZone:
		count, err = h.Backend.CountAssociatedVPCs(zoneID)
	default:
		// MAX_RRSETS_BY_ZONE and any other zone-scoped limit.
		count, err = h.Backend.CountResourceRecordSets(zoneID)
	}

	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, hostedZoneLimitResponse{
		Xmlns: route53Namespace,
		Limit: xmlLimit{Type: limitType, Value: maxHostedZoneCount},
		Count: count,
	})
}

type updateHZFeaturesResponse struct {
	XMLName xml.Name `xml:"UpdateHostedZoneFeaturesResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) updateHostedZoneFeatures(c *echo.Context, path string) error {
	zoneID := strings.TrimSuffix(strings.TrimPrefix(path, route53HZPrefix), route53FeaturesSuffix)

	if _, err := h.Backend.GetHostedZone(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, updateHZFeaturesResponse{Xmlns: route53Namespace})
}

type updateHZCommentResponse struct {
	XMLName    xml.Name      `xml:"UpdateHostedZoneCommentResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	HostedZone xmlHostedZone `xml:"HostedZone"`
}

type updateHZCommentRequest struct {
	XMLName xml.Name `xml:"UpdateHostedZoneCommentRequest"`
	Comment string   `xml:"Comment"`
}

func (h *Handler) updateHostedZoneComment(c *echo.Context, path string) error {
	zoneID := strings.TrimPrefix(path, route53HZPrefix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req updateHZCommentRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	zone, err := h.Backend.UpdateHostedZoneComment(zoneID, req.Comment)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, updateHZCommentResponse{
		Xmlns: route53Namespace,
		HostedZone: xmlHostedZone{
			ID:              "/hostedzone/" + zone.ID,
			Name:            zone.Name,
			CallerReference: zone.CallerReference,
			Config:          xmlHostedZoneConfig{Comment: zone.Comment},
		},
	})
}
