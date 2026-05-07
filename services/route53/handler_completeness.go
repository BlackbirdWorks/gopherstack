package route53

import (
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
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
//
//nolint:gocognit,gocyclo,cyclop // routes many completeness sub-paths
func (h *Handler) routeCompleteness(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == route53TestDNSAnswerPath && method == http.MethodGet:
		return true, h.testDNSAnswer(c)
	case path == route53CheckerIPRangesPath && method == http.MethodGet:
		return true, h.getCheckerIPRanges(c)
	case path == route53GeoLocationPath && method == http.MethodGet:
		return true, h.getGeoLocation(c)
	case path == route53GeoLocationsPath && method == http.MethodGet:
		return true, h.listGeoLocations(c)
	case path == route53HealthCheckCountPath && method == http.MethodGet:
		return true, h.getHealthCheckCount(c)
	case path == route53HostedZoneCountPath && method == http.MethodGet:
		return true, h.getHostedZoneCount(c)
	case path == route53HostedZonesByNamePath && method == http.MethodGet:
		return true, h.listHostedZonesByName(c)
	case path == route53HostedZonesByVPCPath && method == http.MethodGet:
		return true, h.listHostedZonesByVPC(c)
	case path == route53TPInstancesByHZPath && method == http.MethodGet:
		return true, h.listTrafficPolicyInstancesByHostedZone(c)
	case path == route53TPInstancesByPolicyPath && method == http.MethodGet:
		return true, h.listTrafficPolicyInstancesByPolicy(c)
	case strings.HasPrefix(path, route53AccountLimitPrefix) && method == http.MethodGet:
		return true, h.getAccountLimit(c, path)
	case strings.HasPrefix(path, route53HostedZoneLimitPrefix) && method == http.MethodGet:
		return true, h.getHostedZoneLimit(c, path)
	case strings.HasPrefix(path, route53ReusableDSLimitPrefix) && method == http.MethodGet:
		return true, h.getReusableDelegationSetLimit(c, path)
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
	case strings.HasSuffix(path, route53LastFailureReasonSuffix) && method == http.MethodGet:
		return true, h.getHealthCheckLastFailureReason(c, path)
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

	return writeXML(c, http.StatusOK, testDNSAnswerResponse{
		Xmlns:        route53Namespace,
		Nameserver:   "ns-1234.awsdns-00.org",
		RecordName:   q.Get("recordname"),
		RecordType:   q.Get("recordtype"),
		RecordData:   []string{"1.2.3.4"},
		ResponseCode: "NOERROR",
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

	return writeXML(c, http.StatusOK, geoLocationResponse{
		Xmlns: route53Namespace,
		GeoLocation: xmlGeoLocation{
			ContinentCode: q.Get("continentcode"),
			CountryCode:   q.Get("countrycode"),
		},
	})
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
		GeoLocations: []xmlGeoLocation{{ContinentCode: "*"}},
		IsTruncated:  false,
		MaxItems:     "100",
	})
}

type healthCheckCountResponse struct {
	XMLName          xml.Name `xml:"GetHealthCheckCountResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	HealthCheckCount int      `xml:"HealthCheckCount"`
}

func (h *Handler) getHealthCheckCount(c *echo.Context) error {
	p, err := h.Backend.ListHealthChecks("", maxHealthChecks)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	return writeXML(c, http.StatusOK, healthCheckCountResponse{
		Xmlns:            route53Namespace,
		HealthCheckCount: len(p.Data),
	})
}

type hostedZoneCountResponse struct {
	XMLName         xml.Name `xml:"GetHostedZoneCountResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	HostedZoneCount int      `xml:"HostedZoneCount"`
}

func (h *Handler) getHostedZoneCount(c *echo.Context) error {
	p, err := h.Backend.ListHostedZones("", maxHostedZoneCount)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	return writeXML(c, http.StatusOK, hostedZoneCountResponse{
		Xmlns:           route53Namespace,
		HostedZoneCount: len(p.Data),
	})
}

type listHZByNameResponse struct {
	XMLName     xml.Name        `xml:"ListHostedZonesByNameResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	MaxItems    string          `xml:"MaxItems"`
	HostedZones []xmlHostedZone `xml:"HostedZones>HostedZone"`
	IsTruncated bool            `xml:"IsTruncated"`
}

func (h *Handler) listHostedZonesByName(c *echo.Context) error {
	p, err := h.Backend.ListHostedZones("", maxHZByName)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	zones := make([]xmlHostedZone, 0, len(p.Data))
	for _, z := range p.Data {
		zones = append(zones, xmlHostedZone{
			ID:              "/hostedzone/" + z.ID,
			Name:            z.Name,
			CallerReference: z.CallerReference,
			Config:          xmlHostedZoneConfig{Comment: z.Comment},
		})
	}

	return writeXML(c, http.StatusOK, listHZByNameResponse{
		Xmlns:       route53Namespace,
		HostedZones: zones,
		IsTruncated: false,
		MaxItems:    "300",
	})
}

type listHZByVPCResponse struct {
	XMLName     xml.Name        `xml:"ListHostedZonesByVPCResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	HostedZones []xmlHostedZone `xml:"HostedZoneSummaries>HostedZoneSummary"`
}

func (h *Handler) listHostedZonesByVPC(c *echo.Context) error {
	return writeXML(c, http.StatusOK, listHZByVPCResponse{
		Xmlns:       route53Namespace,
		HostedZones: []xmlHostedZone{},
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

	return writeXML(c, http.StatusOK, vpcAssocAuthorizationsResponse{
		Xmlns:        route53Namespace,
		HostedZoneID: zoneID,
		VPCs:         []xmlVPC{},
	})
}

type createVPCAssocAuthorizationResponse struct {
	XMLName      xml.Name `xml:"CreateVPCAssociationAuthorizationResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	HostedZoneID string   `xml:"HostedZoneId"`
	VPC          xmlVPC   `xml:"VPC"`
}

func (h *Handler) createVPCAssociationAuthorization(c *echo.Context, path string) error {
	zoneID := strings.TrimSuffix(strings.TrimPrefix(path, route53HZPrefix), route53AuthorizeVPCSuffix)

	return writeXML(c, http.StatusOK, createVPCAssocAuthorizationResponse{
		Xmlns:        route53Namespace,
		HostedZoneID: zoneID,
		VPC:          xmlVPC{VPCRegion: "us-east-1", VPCID: "vpc-stub"},
	})
}

type deleteVPCAssocAuthorizationResponse struct {
	XMLName xml.Name `xml:"DeleteVPCAssociationAuthorizationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) deleteVPCAssociationAuthorization(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, deleteVPCAssocAuthorizationResponse{
		Xmlns: route53Namespace,
	})
}

type disassociateVPCResponse struct {
	XMLName    xml.Name      `xml:"DisassociateVPCFromHostedZoneResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

func (h *Handler) disassociateVPCFromHostedZone(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, disassociateVPCResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/stub",
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

func (h *Handler) getHealthCheckLastFailureReason(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, lastFailureReasonResponse{
		Xmlns:                   route53Namespace,
		HealthCheckObservations: []stubObservation{},
	})
}

type updateHZCommentResponse struct {
	XMLName    xml.Name      `xml:"UpdateHostedZoneCommentResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	HostedZone xmlHostedZone `xml:"HostedZone"`
}

func (h *Handler) updateHostedZoneComment(c *echo.Context, path string) error {
	zoneID := strings.TrimPrefix(path, route53HZPrefix)
	zone, err := h.Backend.GetHostedZone(zoneID)
	if err != nil {
		return xmlError(c, http.StatusNotFound, "NoSuchHostedZone", "zone not found: "+zoneID)
	}

	return writeXML(c, http.StatusOK, updateHZCommentResponse{
		Xmlns: route53Namespace,
		HostedZone: xmlHostedZone{
			ID:              "/hostedzone/" + zone.ID,
			Name:            zone.Name,
			CallerReference: zone.CallerReference,
		},
	})
}

type listTPInstancesByHZResponse struct {
	XMLName                xml.Name `xml:"ListTrafficPolicyInstancesByHostedZoneResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	MaxItems               string   `xml:"MaxItems"`
	TrafficPolicyInstances []any    `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated            bool     `xml:"IsTruncated"`
}

func (h *Handler) listTrafficPolicyInstancesByHostedZone(c *echo.Context) error {
	return writeXML(c, http.StatusOK, listTPInstancesByHZResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: []any{},
		IsTruncated:            false,
		MaxItems:               "100",
	})
}

type listTPInstancesByPolicyResponse struct {
	XMLName                xml.Name `xml:"ListTrafficPolicyInstancesByPolicyResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	MaxItems               string   `xml:"MaxItems"`
	TrafficPolicyInstances []any    `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated            bool     `xml:"IsTruncated"`
}

func (h *Handler) listTrafficPolicyInstancesByPolicy(c *echo.Context) error {
	return writeXML(c, http.StatusOK, listTPInstancesByPolicyResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: []any{},
		IsTruncated:            false,
		MaxItems:               "100",
	})
}

type updateTPCommentResponse struct {
	XMLName       xml.Name                `xml:"UpdateTrafficPolicyCommentResponse"`
	Xmlns         string                  `xml:"xmlns,attr"`
	TrafficPolicy xmlTrafficPolicySummary `xml:"TrafficPolicy"`
}

func (h *Handler) updateTrafficPolicyComment(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, updateTPCommentResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: xmlTrafficPolicySummary{},
	})
}

type updateTPInstanceResponse struct {
	XMLName               xml.Name                 `xml:"UpdateTrafficPolicyInstanceResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	TrafficPolicyInstance xmlTrafficPolicyInstance `xml:"TrafficPolicyInstance"`
}

func (h *Handler) updateTrafficPolicyInstance(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, updateTPInstanceResponse{
		Xmlns:                 route53Namespace,
		TrafficPolicyInstance: xmlTrafficPolicyInstance{},
	})
}

type getReusableDSResponse struct {
	XMLName       xml.Name         `xml:"GetReusableDelegationSetResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
}

func (h *Handler) getReusableDelegationSet(c *echo.Context, path string) error {
	dsID := strings.TrimPrefix(path, "/2013-04-01/delegationset/")

	return writeXML(c, http.StatusOK, getReusableDSResponse{
		Xmlns: route53Namespace,
		DelegationSet: xmlDelegationSet{
			ID:          "/delegationset/" + dsID,
			NameServers: []string{"ns-1.awsdns-01.com", "ns-2.awsdns-02.net"},
		},
	})
}

type deleteReusableDSResponse struct {
	XMLName xml.Name `xml:"DeleteReusableDelegationSetResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) deleteReusableDelegationSet(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, deleteReusableDSResponse{Xmlns: route53Namespace})
}

type getQueryLoggingConfigResponse struct {
	XMLName            xml.Name              `xml:"GetQueryLoggingConfigResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	QueryLoggingConfig xmlQueryLoggingConfig `xml:"QueryLoggingConfig"`
}

func (h *Handler) getQueryLoggingConfig(c *echo.Context, path string) error {
	configID := strings.TrimPrefix(path, route53QueryLoggingRoot+"/")

	return writeXML(c, http.StatusOK, getQueryLoggingConfigResponse{
		Xmlns: route53Namespace,
		QueryLoggingConfig: xmlQueryLoggingConfig{
			ID:                        configID,
			HostedZoneID:              "stub",
			CloudWatchLogsLogGroupArn: "arn:aws:logs:us-east-1:123456789012:log-group:stub",
		},
	})
}

type deleteQueryLoggingConfigResponse struct {
	XMLName xml.Name `xml:"DeleteQueryLoggingConfigResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) deleteQueryLoggingConfig(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, deleteQueryLoggingConfigResponse{Xmlns: route53Namespace})
}

// listTagsForResources is a batch version of listTagsForResource.
type listTagsForResourcesResponse struct {
	XMLName         xml.Name            `xml:"ListTagsForResourcesResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	ResourceTagSets []xmlResourceTagSet `xml:"ResourceTagSets>ResourceTagSet"`
}

type xmlResourceTagSet struct {
	ResourceType string `xml:"ResourceType"`
	ResourceID   string `xml:"ResourceId"`
}

func (h *Handler) listTagsForResources(c *echo.Context) error {
	return writeXML(c, http.StatusOK, listTagsForResourcesResponse{
		Xmlns:           route53Namespace,
		ResourceTagSets: []xmlResourceTagSet{},
	})
}

// listQueryLoggingConfigs lists all query logging configs.
type listQueryLoggingConfigsResponse struct {
	XMLName             xml.Name                `xml:"ListQueryLoggingConfigsResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	QueryLoggingConfigs []xmlQueryLoggingConfig `xml:"QueryLoggingConfigs>QueryLoggingConfig"`
}

func (h *Handler) listQueryLoggingConfigs(c *echo.Context) error {
	return writeXML(c, http.StatusOK, listQueryLoggingConfigsResponse{
		Xmlns:               route53Namespace,
		QueryLoggingConfigs: []xmlQueryLoggingConfig{},
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
	return writeXML(c, http.StatusOK, listReusableDSResponse{
		Xmlns:          route53Namespace,
		DelegationSets: []xmlDelegationSet{},
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

func (h *Handler) listCidrBlocks(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, listCidrBlocksResponse{
		Xmlns:       route53Namespace,
		CidrBlocks:  []string{},
		IsTruncated: false,
	})
}

type listCidrLocationsResponse struct {
	XMLName       xml.Name `xml:"ListCidrLocationsResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	CidrLocations []string `xml:"CidrLocations>member"`
	IsTruncated   bool     `xml:"IsTruncated"`
}

func (h *Handler) listCidrLocations(c *echo.Context, _ string) error {
	return writeXML(c, http.StatusOK, listCidrLocationsResponse{
		Xmlns:         route53Namespace,
		CidrLocations: []string{},
		IsTruncated:   false,
	})
}
