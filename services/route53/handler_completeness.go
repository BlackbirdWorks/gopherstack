package route53

import (
	"encoding/xml"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// Route53 limit/count constants for completeness operations.
const (
	maxHealthChecks    = 1000
	maxHostedZoneCount = 10000
	maxHZByName        = 300
	defaultLimitValue  = 500
	defaultDSLimit     = 100
)

// Route53 path constants for completeness operations.
const (
	route53TestDNSAnswerPath       = "/2013-04-01/testdnsanswer"
	route53CheckerIPRangesPath     = "/2013-04-01/checkeripranges"
	route53GeoLocationPath         = "/2013-04-01/geolocation"
	route53GeoLocationsPath        = "/2013-04-01/geolocations"
	route53HealthCheckCountPath    = "/2013-04-01/healthcheckcount"
	route53HostedZoneCountPath     = "/2013-04-01/hostedzonecount"
	route53HostedZonesByNamePath   = "/2013-04-01/hostedzonesbyname"
	route53HostedZonesByVPCPath    = "/2013-04-01/hostedzonesbyvpc"
	route53AccountLimitPrefix      = "/2013-04-01/accountlimit/"
	route53HostedZoneLimitPrefix   = "/2013-04-01/hostedzonelimit/"
	route53ReusableDSLimitPrefix   = "/2013-04-01/reusabledelegationsetlimit/"
	route53AuthorizeVPCSuffix      = "/authorizevpcassociation"
	route53DeauthorizeVPCSuffix    = "/deauthorizevpcassociation"
	route53DisassociateVPCSuffix   = "/disassociatevpc"
	route53FeaturesSuffix          = "/features"
	route53TPInstancesByHZPath     = "/2013-04-01/trafficpolicyinstances/hostedzone"
	route53TPInstancesByPolicyPath = "/2013-04-01/trafficpolicyinstances/trafficpolicy"
	route53LastFailureReasonSuffix = "/lastfailurereason"
)

// routeCompleteness handles previously-notImplemented Route53 paths.
// Returns (true, err) if the path was handled, (false, nil) if not.
func (h *Handler) routeCompleteness(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeCompletenessCore(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessVPC(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessTP(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessDelegationSet(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessQueryLogging(c, path, method); ok {
		return true, err
	}

	return false, nil
}

// routeCompletenessCore handles basic info and limit endpoints.
func (h *Handler) routeCompletenessCore(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeCompletenessInfo(c, path, method); ok {
		return true, err
	}

	return h.routeCompletenessLimits(c, path, method)
}

// routeCompletenessInfo handles DNS answer, geo, health-check, and hosted-zone info endpoints.
func (h *Handler) routeCompletenessInfo(c *echo.Context, path, method string) (bool, error) {
	if method != http.MethodGet {
		return false, nil
	}

	switch path {
	case route53TestDNSAnswerPath:
		return true, h.testDNSAnswer(c)
	case route53CheckerIPRangesPath:
		return true, h.getCheckerIPRanges(c)
	case route53GeoLocationPath, route53GeoLocationsPath:
		q := c.Request().URL.Query()
		if q.Get("continentcode") != "" || q.Get("countrycode") != "" || q.Get("subdivisioncode") != "" {
			return true, h.getGeoLocation(c)
		}

		return true, h.listGeoLocations(c)
	case route53HealthCheckCountPath:
		return true, h.getHealthCheckCount(c)
	case route53HostedZoneCountPath:
		return true, h.getHostedZoneCount(c)
	case route53HostedZonesByNamePath:
		return true, h.listHostedZonesByName(c)
	case route53HostedZonesByVPCPath:
		return true, h.listHostedZonesByVPC(c)
	case route53TPInstancesByHZPath:
		return true, h.listTrafficPolicyInstancesByHostedZone(c)
	case route53TPInstancesByPolicyPath:
		return true, h.listTrafficPolicyInstancesByPolicy(c)
	}

	return false, nil
}

// routeCompletenessLimits handles limit and last-failure-reason endpoints.
func (h *Handler) routeCompletenessLimits(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, route53AccountLimitPrefix) && method == http.MethodGet:
		return true, h.getAccountLimit(c, path)
	case strings.HasPrefix(path, route53HostedZoneLimitPrefix) && method == http.MethodGet:
		return true, h.getHostedZoneLimit(c, path)
	case strings.HasPrefix(path, route53ReusableDSLimitPrefix) && method == http.MethodGet:
		return true, h.getReusableDelegationSetLimit(c, path)
	case strings.HasSuffix(path, route53LastFailureReasonSuffix) && method == http.MethodGet:
		return true, h.getHealthCheckLastFailureReason(c, path)
	}

	return false, nil
}

// routeCompletenessVPC handles VPC association and disassociation endpoints.
func (h *Handler) routeCompletenessVPC(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasSuffix(path, route53AuthorizeVPCSuffix) && method == http.MethodGet:
		return true, h.listVPCAssociationAuthorizations(c, path)
	case strings.HasSuffix(path, route53AuthorizeVPCSuffix) && method == http.MethodPost:
		return true, h.createVPCAssociationAuthorization(c, path)
	case strings.HasSuffix(path, route53DeauthorizeVPCSuffix) && method == http.MethodPost:
		return true, h.deleteVPCAssociationAuthorization(c, path)
	case strings.HasSuffix(path, route53DisassociateVPCSuffix) && method == http.MethodPost:
		return true, h.disassociateVPCFromHostedZone(c, path)
	case strings.HasSuffix(path, route53FeaturesSuffix) && method == http.MethodPost:
		return true, h.updateHostedZoneFeatures(c, path)
	}

	return false, nil
}

// routeCompletenessTP handles traffic policy completeness routes.
func (h *Handler) routeCompletenessTP(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, route53TrafficPolicyPrefix) && method == http.MethodPost:
		// UpdateTrafficPolicyComment — POST /2013-04-01/trafficpolicy/{Id}/{Version}
		return true, h.updateTrafficPolicyComment(c, path)
	case strings.HasPrefix(path, route53TPInstancePrefix) && method == http.MethodPost:
		// UpdateTrafficPolicyInstance — POST /2013-04-01/trafficpolicyinstance/{Id}
		return true, h.updateTrafficPolicyInstance(c, path)
	}

	// UpdateHostedZoneComment — POST /2013-04-01/hostedzone/{Id} (no suffix)
	if method == http.MethodPost && strings.HasPrefix(path, route53HZPrefix) {
		tail := strings.TrimPrefix(path, route53HZPrefix)
		if !strings.Contains(tail, "/") {
			return true, h.updateHostedZoneComment(c, path)
		}
	}

	return false, nil
}

func (h *Handler) routeCompletenessDelegationSet(c *echo.Context, path, method string) (bool, error) {
	if !strings.HasPrefix(path, "/2013-04-01/delegationset/") {
		return false, nil
	}

	switch method {
	case http.MethodGet:
		return true, h.getReusableDelegationSet(c, path)
	case http.MethodDelete:
		return true, h.deleteReusableDelegationSet(c, path)
	}

	return false, nil
}

func (h *Handler) routeCompletenessQueryLogging(c *echo.Context, path, method string) (bool, error) {
	if !strings.HasPrefix(path, route53QueryLoggingRoot+"/") {
		return false, nil
	}

	switch method {
	case http.MethodGet:
		return true, h.getQueryLoggingConfig(c, path)
	case http.MethodDelete:
		return true, h.deleteQueryLoggingConfig(c, path)
	}

	return false, nil
}

// ----- Handler implementations -----

type testDNSAnswerResponse struct {
	XMLName      xml.Name `xml:"TestDNSAnswerResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	Nameserver   string   `xml:"Nameserver"`
	RecordName   string   `xml:"RecordName"`
	RecordType   string   `xml:"RecordType"`
	ResponseCode string   `xml:"ResponseCode"`
	Protocol     string   `xml:"Protocol"`
	RecordData   []string `xml:"RecordData>RecordDataEntry"`
}

func (h *Handler) testDNSAnswer(c *echo.Context) error {
	q := c.Request().URL.Query()
	recordName := q.Get("recordname")
	recordType := q.Get("recordtype")
	zoneID := q.Get("hostedzoneid")

	var recordData []string
	responseCode := "NOERROR"

	if zoneID != "" {
		values, err := h.Backend.TestDNSAnswer(zoneID, recordName, recordType)
		if err == nil {
			recordData = values
		}
	}

	if len(recordData) == 0 {
		responseCode = "NXDOMAIN"
	}

	return writeXML(c, http.StatusOK, testDNSAnswerResponse{
		Xmlns:        route53Namespace,
		Nameserver:   dnsNS1Default,
		RecordName:   recordName,
		RecordType:   recordType,
		RecordData:   recordData,
		ResponseCode: responseCode,
		Protocol:     "UDP",
	})
}

type checkerIPRangesResponse struct {
	XMLName    xml.Name `xml:"GetCheckerIpRangesResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	CheckerIPs []string `xml:"CheckerIpRanges>member"`
}

func (h *Handler) getCheckerIPRanges(c *echo.Context) error {
	return writeXML(c, http.StatusOK, checkerIPRangesResponse{
		Xmlns:      route53Namespace,
		CheckerIPs: []string{"15.177.0.0/18"},
	})
}

type geoLocationResponse struct {
	XMLName     xml.Name       `xml:"GetGeoLocationResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	GeoLocation xmlGeoLocation `xml:"GeoLocationDetails"`
}

func (h *Handler) getGeoLocation(c *echo.Context) error {
	q := c.Request().URL.Query()
	continentCode := q.Get("continentcode")
	countryCode := q.Get("countrycode")
	subdivisionCode := q.Get("subdivisioncode")

	for _, loc := range geoLocationTable {
		if continentCode != "" && loc.ContinentCode != continentCode {
			continue
		}

		if countryCode != "" && loc.CountryCode != countryCode {
			continue
		}

		if subdivisionCode != "" && loc.SubdivisionCode != subdivisionCode {
			continue
		}

		return writeXML(c, http.StatusOK, geoLocationResponse{
			Xmlns:       route53Namespace,
			GeoLocation: loc,
		})
	}

	return xmlError(c, http.StatusNotFound, "NoSuchGeoLocation",
		"the specified geographic location was not found")
}

type listGeoLocationsResponse struct {
	XMLName      xml.Name         `xml:"ListGeoLocationsResponse"`
	Xmlns        string           `xml:"xmlns,attr"`
	MaxItems     string           `xml:"MaxItems"`
	GeoLocations []xmlGeoLocation `xml:"GeoLocationDetailsList>GeoLocationDetails"`
	IsTruncated  bool             `xml:"IsTruncated"`
}

func (h *Handler) listGeoLocations(c *echo.Context) error {
	return writeXML(c, http.StatusOK, listGeoLocationsResponse{
		Xmlns:        route53Namespace,
		GeoLocations: geoLocationTable,
		IsTruncated:  false,
		MaxItems:     "100",
	})
}

// geoLocationTable is a static table of AWS Route 53 supported geo locations.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var geoLocationTable = []xmlGeoLocation{
	{ContinentCode: "AF", ContinentName: "Africa"},
	{ContinentCode: "AN", ContinentName: "Antarctica"},
	{ContinentCode: "AS", ContinentName: "Asia"},
	{ContinentCode: "EU", ContinentName: "Europe"},
	{ContinentCode: "NA", ContinentName: "North America"},
	{ContinentCode: "OC", ContinentName: "Oceania"},
	{ContinentCode: "SA", ContinentName: "South America"},
	{ContinentCode: "NA", CountryCode: "US", CountryName: "United States"},
	{ContinentCode: "EU", CountryCode: "GB", CountryName: "United Kingdom"},
	{ContinentCode: "EU", CountryCode: "DE", CountryName: "Germany"},
	{ContinentCode: "EU", CountryCode: "FR", CountryName: "France"},
	{ContinentCode: "AS", CountryCode: "JP", CountryName: "Japan"},
	{ContinentCode: "AS", CountryCode: "CN", CountryName: "China"},
	{ContinentCode: "AS", CountryCode: "IN", CountryName: "India"},
	{ContinentCode: "AS", CountryCode: "SG", CountryName: "Singapore"},
	{ContinentCode: "OC", CountryCode: "AU", CountryName: "Australia"},
	{ContinentCode: "NA", CountryCode: "CA", CountryName: "Canada"},
	{ContinentCode: "SA", CountryCode: "BR", CountryName: "Brazil"},
	{ContinentCode: "NA", CountryCode: "US", SubdivisionCode: "US-CA", SubdivisionName: "California"},
	{ContinentCode: "NA", CountryCode: "US", SubdivisionCode: "US-NY", SubdivisionName: "New York"},
	{ContinentCode: "NA", CountryCode: "US", SubdivisionCode: "US-TX", SubdivisionName: "Texas"},
	{ContinentCode: "NA", CountryCode: "US", SubdivisionCode: "US-WA", SubdivisionName: "Washington"},
	{ContinentCode: "NA", CountryCode: "US", SubdivisionCode: "US-VA", SubdivisionName: "Virginia"},
}

type healthCheckCountResponse struct {
	XMLName          xml.Name `xml:"GetHealthCheckCountResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	HealthCheckCount int      `xml:"HealthCheckCount"`
}

func (h *Handler) getHealthCheckCount(c *echo.Context) error {
	count := h.Backend.GetHealthCheckCount()

	return writeXML(c, http.StatusOK, healthCheckCountResponse{
		Xmlns:            route53Namespace,
		HealthCheckCount: count,
	})
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

type listHZByVPCResponse struct {
	XMLName     xml.Name        `xml:"ListHostedZonesByVPCResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	HostedZones []xmlHostedZone `xml:"HostedZoneSummaries>HostedZoneSummary"`
}

func (h *Handler) listHostedZonesByVPC(c *echo.Context) error {
	vpcID := c.Request().URL.Query().Get("vpcid")
	vpcRegion := c.Request().URL.Query().Get("vpcregion")

	if vpcID == "" || vpcRegion == "" {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "vpcid and vpcregion are required")
	}

	zones, err := h.Backend.ListHostedZonesByVPC(vpcID, vpcRegion)
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

	return writeXML(c, http.StatusOK, listHZByVPCResponse{
		Xmlns:       route53Namespace,
		HostedZones: xmlZones,
	})
}

type accountLimitResponse struct {
	XMLName xml.Name `xml:"GetAccountLimitResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Limit   xmlLimit `xml:"Limit"`
	Count   int      `xml:"Count"`
}

type xmlLimit struct {
	Type  string `xml:"Type"`
	Value int    `xml:"Value"`
}

func (h *Handler) getAccountLimit(c *echo.Context, path string) error {
	limitType := strings.TrimPrefix(path, route53AccountLimitPrefix)

	return writeXML(c, http.StatusOK, accountLimitResponse{
		Xmlns: route53Namespace,
		Limit: xmlLimit{Type: limitType, Value: defaultLimitValue},
		Count: 0,
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
	limitType := ""
	if _, after, ok := strings.Cut(parts, "/"); ok {
		limitType = after
	}

	return writeXML(c, http.StatusOK, hostedZoneLimitResponse{
		Xmlns: route53Namespace,
		Limit: xmlLimit{Type: limitType, Value: maxHostedZoneCount},
		Count: 0,
	})
}

type reusableDSLimitResponse struct {
	XMLName xml.Name `xml:"GetReusableDelegationSetLimitResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Limit   xmlLimit `xml:"Limit"`
	Count   int      `xml:"Count"`
}

func (h *Handler) getReusableDelegationSetLimit(c *echo.Context, path string) error {
	parts := strings.TrimPrefix(path, route53ReusableDSLimitPrefix)
	limitType := ""
	if _, after, ok := strings.Cut(parts, "/"); ok {
		limitType = after
	}

	return writeXML(c, http.StatusOK, reusableDSLimitResponse{
		Xmlns: route53Namespace,
		Limit: xmlLimit{Type: limitType, Value: defaultDSLimit},
		Count: 0,
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

type updateHZFeaturesResponse struct {
	XMLName xml.Name `xml:"UpdateHostedZoneFeaturesResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) updateHostedZoneFeatures(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, updateHZFeaturesResponse{Xmlns: route53Namespace})
}

type lastFailureReasonResponse struct {
	XMLName                 xml.Name          `xml:"GetHealthCheckLastFailureReasonResponse"`
	Xmlns                   string            `xml:"xmlns,attr"`
	HealthCheckObservations []stubObservation `xml:"HealthCheckObservations>HealthCheckObservation"`
}

type stubObservation struct {
	Region       string `xml:"Region"`
	IPAddress    string `xml:"IPAddress"`
	StatusReport struct {
		Status      string `xml:"Status"`
		CheckedTime string `xml:"CheckedTime"`
	} `xml:"StatusReport"`
}

func (h *Handler) getHealthCheckLastFailureReason(c *echo.Context, path string) error {
	id := strings.TrimSuffix(strings.TrimPrefix(path, route53HealthCheckPrefix), "/lastfailurereason")

	hc, err := h.Backend.GetHealthCheck(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	var observations []stubObservation
	for _, obs := range hc.Observations {
		observations = append(observations, stubObservation{
			Region:    obs.Region,
			IPAddress: obs.IPAddress,
			StatusReport: struct {
				Status      string `xml:"Status"`
				CheckedTime string `xml:"CheckedTime"`
			}{
				Status:      obs.Status,
				CheckedTime: obs.CheckedTime.UTC().Format(time.RFC3339),
			},
		})
	}
	if observations == nil {
		observations = []stubObservation{}
	}

	return writeXML(c, http.StatusOK, lastFailureReasonResponse{
		Xmlns:                   route53Namespace,
		HealthCheckObservations: observations,
	})
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

type listTPInstancesByHZResponse struct {
	XMLName                xml.Name                   `xml:"ListTrafficPolicyInstancesByHostedZoneResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	MaxItems               string                     `xml:"MaxItems"`
	TrafficPolicyInstances []xmlTrafficPolicyInstance `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated            bool                       `xml:"IsTruncated"`
}

func (h *Handler) listTrafficPolicyInstancesByHostedZone(c *echo.Context) error {
	hostedZoneID := c.Request().URL.Query().Get("hostedzoneid")

	instances, err := h.Backend.ListTrafficPolicyInstancesByHostedZone(hostedZoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlInstances := make([]xmlTrafficPolicyInstance, 0, len(instances))
	for _, inst := range instances {
		xmlInstances = append(xmlInstances, toXMLTPInstance(inst))
	}

	return writeXML(c, http.StatusOK, listTPInstancesByHZResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: xmlInstances,
		IsTruncated:            false,
		MaxItems:               "100",
	})
}

type listTPInstancesByPolicyResponse struct {
	XMLName                xml.Name                   `xml:"ListTrafficPolicyInstancesByPolicyResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	MaxItems               string                     `xml:"MaxItems"`
	TrafficPolicyInstances []xmlTrafficPolicyInstance `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated            bool                       `xml:"IsTruncated"`
}

func (h *Handler) listTrafficPolicyInstancesByPolicy(c *echo.Context) error {
	tpID := c.Request().URL.Query().Get("trafficpolicyid")
	tpVersionStr := c.Request().URL.Query().Get("trafficpolicyversion")

	var tpVersion int32
	if tpVersionStr != "" {
		v, err := strconv.Atoi(tpVersionStr)
		if err != nil {
			return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid trafficpolicyversion")
		}

		if v < math.MinInt32 || v > math.MaxInt32 {
			return xmlError(c, http.StatusBadRequest, "InvalidInput", "trafficpolicyversion out of range")
		}

		tpVersion = int32(v) //nolint:gosec // bounds checked above
	}

	instances, err := h.Backend.ListTrafficPolicyInstancesByPolicy(tpID, tpVersion)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlInstances := make([]xmlTrafficPolicyInstance, 0, len(instances))
	for _, inst := range instances {
		xmlInstances = append(xmlInstances, toXMLTPInstance(inst))
	}

	return writeXML(c, http.StatusOK, listTPInstancesByPolicyResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: xmlInstances,
		IsTruncated:            false,
		MaxItems:               "100",
	})
}

type updateTPCommentRequest struct {
	XMLName xml.Name `xml:"UpdateTrafficPolicyCommentRequest"`
	Comment string   `xml:"Comment"`
}

type updateTPCommentResponse struct {
	XMLName       xml.Name         `xml:"UpdateTrafficPolicyCommentResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	TrafficPolicy xmlTrafficPolicy `xml:"TrafficPolicy"`
}

func (h *Handler) updateTrafficPolicyComment(c *echo.Context, path string) error {
	// path: /2013-04-01/trafficpolicy/{id}/{version}
	rest := strings.TrimPrefix(path, route53TrafficPolicyPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split id and version

	if len(parts) != 2 { //nolint:mnd // path has two segments: id and version
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid traffic policy path")
	}

	id := parts[0]

	version64, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid version number")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req updateTPCommentRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	tp, err := h.Backend.UpdateTrafficPolicyComment(id, int32(version64), req.Comment)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, updateTPCommentResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: toXMLTrafficPolicy(tp),
	})
}

type updateTPInstanceResponse struct {
	XMLName               xml.Name                 `xml:"UpdateTrafficPolicyInstanceResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	TrafficPolicyInstance xmlTrafficPolicyInstance `xml:"TrafficPolicyInstance"`
}

type updateTPInstanceRequest struct {
	XMLName          xml.Name `xml:"UpdateTrafficPolicyInstanceRequest"`
	TrafficPolicyID  string   `xml:"TrafficPolicyId"`
	TrafficPolicyVer int32    `xml:"TrafficPolicyVersion"`
	TTL              int64    `xml:"TTL"`
}

func (h *Handler) updateTrafficPolicyInstance(c *echo.Context, path string) error {
	instanceID := strings.TrimPrefix(path, "/2013-04-01/trafficpolicyinstance/")

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req updateTPInstanceRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	inst, err := h.Backend.UpdateTrafficPolicyInstance(instanceID, req.TrafficPolicyID, req.TrafficPolicyVer, req.TTL)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, updateTPInstanceResponse{
		Xmlns:                 route53Namespace,
		TrafficPolicyInstance: toXMLTPInstance(inst),
	})
}

type getReusableDSResponse struct {
	XMLName       xml.Name         `xml:"GetReusableDelegationSetResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
}

func (h *Handler) getReusableDelegationSet(c *echo.Context, path string) error {
	rawID := strings.TrimPrefix(path, "/2013-04-01/delegationset/")
	dsID := "/delegationset/" + rawID

	ds, err := h.Backend.GetReusableDelegationSet(dsID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, getReusableDSResponse{
		Xmlns: route53Namespace,
		DelegationSet: xmlDelegationSet{
			ID:          ds.ID,
			NameServers: ds.NameServers,
		},
	})
}

type deleteReusableDSResponse struct {
	XMLName xml.Name `xml:"DeleteReusableDelegationSetResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) deleteReusableDelegationSet(c *echo.Context, path string) error {
	rawID := strings.TrimPrefix(path, "/2013-04-01/delegationset/")
	dsID := "/delegationset/" + rawID

	if err := h.Backend.DeleteReusableDelegationSet(dsID); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, deleteReusableDSResponse{Xmlns: route53Namespace})
}

type getQueryLoggingConfigResponse struct {
	XMLName            xml.Name              `xml:"GetQueryLoggingConfigResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	QueryLoggingConfig xmlQueryLoggingConfig `xml:"QueryLoggingConfig"`
}

func (h *Handler) getQueryLoggingConfig(c *echo.Context, path string) error {
	configID := strings.TrimPrefix(path, route53QueryLoggingRoot+"/")

	cfg, err := h.Backend.GetQueryLoggingConfig(configID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, getQueryLoggingConfigResponse{
		Xmlns: route53Namespace,
		QueryLoggingConfig: xmlQueryLoggingConfig{
			ID:                        cfg.ID,
			HostedZoneID:              cfg.HostedZoneID,
			CloudWatchLogsLogGroupArn: cfg.CloudWatchLogsLogGroupArn,
		},
	})
}

type deleteQueryLoggingConfigResponse struct {
	XMLName xml.Name `xml:"DeleteQueryLoggingConfigResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) deleteQueryLoggingConfig(c *echo.Context, path string) error {
	configID := strings.TrimPrefix(path, route53QueryLoggingRoot+"/")

	if err := h.Backend.DeleteQueryLoggingConfig(configID); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, deleteQueryLoggingConfigResponse{Xmlns: route53Namespace})
}

// listTagsForResources is a batch version of listTagsForResource.
type listTagsForResourcesResponse struct {
	XMLName         xml.Name            `xml:"ListTagsForResourcesResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	ResourceTagSets []xmlResourceTagSet `xml:"ResourceTagSets>ResourceTagSet"`
}

type xmlResourceTagSet struct {
	ResourceType string   `xml:"ResourceType"`
	ResourceID   string   `xml:"ResourceId"`
	Tags         []r53Tag `xml:"Tags>Tag,omitempty"`
}

type listTagsReq struct {
	XMLName      xml.Name `xml:"ListTagsForResourcesRequest"`
	ResourceType string   `xml:"ResourceType"`
	ResourceIDs  []string `xml:"ResourceIds>ResourceId"`
}

func (h *Handler) listTagsForResources(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req listTagsReq
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	tagsMap := h.Backend.ListTagsForResources(req.ResourceIDs)

	var resourceTagSets []xmlResourceTagSet
	for _, id := range req.ResourceIDs {
		tags := tagsMap[id]
		var tagList []r53Tag
		for k, v := range tags {
			tagList = append(tagList, r53Tag{Key: k, Value: v})
		}
		// Route53 XML list expects tags to be present, even if empty array, wait, omitempty might drop it.
		// Usually if tags are absent, the array is empty.
		if len(tagList) == 0 {
			tagList = nil
		}
		resourceTagSets = append(resourceTagSets, xmlResourceTagSet{
			ResourceType: req.ResourceType,
			ResourceID:   id,
			Tags:         tagList,
		})
	}

	return writeXML(c, http.StatusOK, listTagsForResourcesResponse{
		Xmlns:           route53Namespace,
		ResourceTagSets: resourceTagSets,
	})
}

// listQueryLoggingConfigs lists all query logging configs.
type listQueryLoggingConfigsResponse struct {
	XMLName             xml.Name                `xml:"ListQueryLoggingConfigsResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	QueryLoggingConfigs []xmlQueryLoggingConfig `xml:"QueryLoggingConfigs>QueryLoggingConfig"`
}

func (h *Handler) listQueryLoggingConfigs(c *echo.Context) error {
	hostedZoneID := c.Request().URL.Query().Get("hostedzoneid")

	cfgs, err := h.Backend.ListQueryLoggingConfigs(hostedZoneID)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	items := make([]xmlQueryLoggingConfig, 0, len(cfgs))
	for _, cfg := range cfgs {
		items = append(items, xmlQueryLoggingConfig{
			ID:                        cfg.ID,
			HostedZoneID:              cfg.HostedZoneID,
			CloudWatchLogsLogGroupArn: cfg.CloudWatchLogsLogGroupArn,
		})
	}

	return writeXML(c, http.StatusOK, listQueryLoggingConfigsResponse{
		Xmlns:               route53Namespace,
		QueryLoggingConfigs: items,
	})
}

// listReusableDelegationSets lists all reusable delegation sets.
type listReusableDSResponse struct {
	XMLName        xml.Name           `xml:"ListReusableDelegationSetsResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	MaxItems       string             `xml:"MaxItems"`
	DelegationSets []xmlDelegationSet `xml:"DelegationSets>DelegationSet"`
	IsTruncated    bool               `xml:"IsTruncated"`
}

func (h *Handler) listReusableDelegationSets(c *echo.Context) error {
	sets, err := h.Backend.ListReusableDelegationSets()
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	items := make([]xmlDelegationSet, 0, len(sets))
	for _, ds := range sets {
		items = append(items, xmlDelegationSet{
			ID:          ds.ID,
			NameServers: ds.NameServers,
		})
	}

	return writeXML(c, http.StatusOK, listReusableDSResponse{
		Xmlns:          route53Namespace,
		DelegationSets: items,
		IsTruncated:    false,
		MaxItems:       "100",
	})
}

// ----- CIDR stubs -----

type listCidrBlocksResponse struct {
	XMLName     xml.Name `xml:"ListCidrBlocksResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	CidrBlocks  []string `xml:"CidrBlocks>member"`
	IsTruncated bool     `xml:"IsTruncated"`
}

func (h *Handler) listCidrBlocks(c *echo.Context, path string) error {
	// path: /2013-04-01/cidrcollection/{id}/cidrblocks[?location=...]
	trimmed := strings.TrimPrefix(path, route53CidrCollectionPrefix)
	collectionID, _, _ := strings.Cut(trimmed, "/")
	locationName := c.Request().URL.Query().Get("location")

	blocks, err := h.Backend.ListCidrBlocks(collectionID, locationName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, listCidrBlocksResponse{
		Xmlns:       route53Namespace,
		CidrBlocks:  blocks,
		IsTruncated: false,
	})
}

type listCidrLocationsResponse struct {
	XMLName       xml.Name `xml:"ListCidrLocationsResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	CidrLocations []string `xml:"CidrLocations>member"`
	IsTruncated   bool     `xml:"IsTruncated"`
}

func (h *Handler) listCidrLocations(c *echo.Context, path string) error {
	// path: /2013-04-01/cidrcollection/{id}[/cidrlocations]
	trimmed := strings.TrimPrefix(path, route53CidrCollectionPrefix)
	collectionID, _, _ := strings.Cut(trimmed, "/")

	locations, err := h.Backend.ListCidrLocations(collectionID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, listCidrLocationsResponse{
		Xmlns:         route53Namespace,
		CidrLocations: locations,
		IsTruncated:   false,
	})
}
