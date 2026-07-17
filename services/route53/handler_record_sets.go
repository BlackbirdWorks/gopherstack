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

// routeRRSet routes resource record set requests.
func (h *Handler) routeRRSet(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.changeResourceRecordSets(c)
	case http.MethodGet:
		return h.listResourceRecordSets(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on rrset")
	}
}

func (h *Handler) routeChange(c *echo.Context, path, method string) error {
	if method == http.MethodGet {
		return h.getChange(c, path)
	}

	return xmlError(c, http.StatusNotFound, "NoSuchOperation",
		"unsupported method on change")
}

type xmlChangeResourceRecordSetsResponse struct {
	XMLName    xml.Name      `xml:"ChangeResourceRecordSetsResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlResourceRecord struct {
	Value string `xml:"Value"`
}

type xmlAliasTarget struct {
	HostedZoneID         string `xml:"HostedZoneId"`
	DNSName              string `xml:"DNSName"`
	EvaluateTargetHealth bool   `xml:"EvaluateTargetHealth"`
}

type xmlGeoLocation struct {
	ContinentCode   string `xml:"ContinentCode,omitempty"`
	ContinentName   string `xml:"ContinentName,omitempty"`
	CountryCode     string `xml:"CountryCode,omitempty"`
	CountryName     string `xml:"CountryName,omitempty"`
	SubdivisionCode string `xml:"SubdivisionCode,omitempty"`
	SubdivisionName string `xml:"SubdivisionName,omitempty"`
}

type xmlGeoProximityCoordinates struct {
	Latitude  string `xml:"Latitude,omitempty"`
	Longitude string `xml:"Longitude,omitempty"`
}

type xmlGeoProximityLocation struct {
	Coordinates    *xmlGeoProximityCoordinates `xml:"Coordinates,omitempty"`
	AWSRegion      string                      `xml:"AWSRegion,omitempty"`
	LocalZoneGroup string                      `xml:"LocalZoneGroup,omitempty"`
	Bias           int                         `xml:"Bias,omitempty"`
}

type xmlCidrRoutingConfig struct {
	CollectionID string `xml:"CollectionId,omitempty"`
	LocationName string `xml:"LocationName,omitempty"`
}

type xmlResourceRecordSet struct {
	AliasTarget          *xmlAliasTarget          `xml:"AliasTarget,omitempty"`
	GeoLocation          *xmlGeoLocation          `xml:"GeoLocation,omitempty"`
	GeoProximityLocation *xmlGeoProximityLocation `xml:"GeoProximityLocation,omitempty"`
	CidrRoutingConfig    *xmlCidrRoutingConfig    `xml:"CidrRoutingConfig,omitempty"`
	Weight               *int64                   `xml:"Weight"`
	XMLName              xml.Name                 `xml:"ResourceRecordSet"`
	Type                 string                   `xml:"Type"`
	SetIdentifier        string                   `xml:"SetIdentifier,omitempty"`
	Failover             string                   `xml:"Failover,omitempty"`
	Region               string                   `xml:"Region,omitempty"`
	HealthCheckID        string                   `xml:"HealthCheckId,omitempty"`
	Name                 string                   `xml:"Name"`
	ResourceRecords      []xmlResourceRecord      `xml:"ResourceRecords>ResourceRecord,omitempty"`
	TTL                  int64                    `xml:"TTL,omitempty"`
	MultiValueAnswer     bool                     `xml:"MultiValueAnswer,omitempty"`
}

type xmlListResourceRecordSetsResponse struct {
	XMLName              xml.Name               `xml:"ListResourceRecordSetsResponse"`
	Xmlns                string                 `xml:"xmlns,attr"`
	NextRecordName       string                 `xml:"NextRecordName,omitempty"`
	NextRecordType       string                 `xml:"NextRecordType,omitempty"`
	NextRecordIdentifier string                 `xml:"NextRecordIdentifier,omitempty"`
	MaxItems             string                 `xml:"MaxItems"`
	ResourceRecordSets   []xmlResourceRecordSet `xml:"ResourceRecordSets>ResourceRecordSet"`
	IsTruncated          bool                   `xml:"IsTruncated"`
}

// xmlResourceRecordSetChange is the ResourceRecordSet element within a change batch entry.
type xmlResourceRecordSetChange struct {
	AliasTarget          *xmlAliasTarget          `xml:"AliasTarget"`
	GeoLocation          *xmlGeoLocation          `xml:"GeoLocation"`
	GeoProximityLocation *xmlGeoProximityLocation `xml:"GeoProximityLocation"`
	CidrRoutingConfig    *xmlCidrRoutingConfig    `xml:"CidrRoutingConfig"`
	Weight               *int64                   `xml:"Weight"`
	SetIdentifier        string                   `xml:"SetIdentifier"`
	Type                 string                   `xml:"Type"`
	Failover             string                   `xml:"Failover"`
	Region               string                   `xml:"Region"`
	HealthCheckID        string                   `xml:"HealthCheckId"`
	Name                 string                   `xml:"Name"`
	ResourceRecords      []xmlResourceRecord      `xml:"ResourceRecords>ResourceRecord"`
	TTL                  int64                    `xml:"TTL"`
	MultiValueAnswer     bool                     `xml:"MultiValueAnswer"`
}

// xmlChange is a single change entry within a ChangeBatch.
type xmlChange struct {
	Action            string                     `xml:"Action"`
	ResourceRecordSet xmlResourceRecordSetChange `xml:"ResourceRecordSet"`
}

type xmlChangeBatch struct {
	XMLName xml.Name    `xml:"ChangeBatch"`
	Changes []xmlChange `xml:"Changes>Change"`
}

type xmlChangeResourceRecordSetsRequest struct {
	XMLName     xml.Name       `xml:"ChangeResourceRecordSetsRequest"`
	ChangeBatch xmlChangeBatch `xml:"ChangeBatch"`
}

//nolint:funlen // handles full ChangeResourceRecordSets request including validation and response
func (h *Handler) changeResourceRecordSets(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlChangeResourceRecordSetsRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	changes := make([]Change, 0, len(req.ChangeBatch.Changes))
	for _, ch := range req.ChangeBatch.Changes {
		records := make([]ResourceRecord, len(ch.ResourceRecordSet.ResourceRecords))
		for i, rr := range ch.ResourceRecordSet.ResourceRecords {
			records[i] = ResourceRecord(rr)
		}

		rrs := ResourceRecordSet{
			Name:             ch.ResourceRecordSet.Name,
			Type:             ch.ResourceRecordSet.Type,
			TTL:              ch.ResourceRecordSet.TTL,
			Records:          records,
			SetIdentifier:    ch.ResourceRecordSet.SetIdentifier,
			Failover:         FailoverPolicy(ch.ResourceRecordSet.Failover),
			Region:           ch.ResourceRecordSet.Region,
			HealthCheckID:    ch.ResourceRecordSet.HealthCheckID,
			Weight:           ch.ResourceRecordSet.Weight,
			MultiValueAnswer: ch.ResourceRecordSet.MultiValueAnswer,
		}

		if ch.ResourceRecordSet.AliasTarget != nil {
			at := ch.ResourceRecordSet.AliasTarget
			rrs.AliasTarget = &AliasTarget{
				HostedZoneID:         at.HostedZoneID,
				DNSName:              at.DNSName,
				EvaluateTargetHealth: at.EvaluateTargetHealth,
			}
		}

		if ch.ResourceRecordSet.GeoLocation != nil {
			gl := ch.ResourceRecordSet.GeoLocation
			rrs.GeoLocation = &GeoLocation{
				ContinentCode:   gl.ContinentCode,
				CountryCode:     gl.CountryCode,
				SubdivisionCode: gl.SubdivisionCode,
			}
		}

		if ch.ResourceRecordSet.GeoProximityLocation != nil {
			gpl := ch.ResourceRecordSet.GeoProximityLocation
			rrs.GeoProximityLocation = &GeoProximityLocation{
				AWSRegion:      gpl.AWSRegion,
				LocalZoneGroup: gpl.LocalZoneGroup,
				Bias:           gpl.Bias,
			}
			if gpl.Coordinates != nil {
				rrs.GeoProximityLocation.Coordinates = &GeoProximityCoordinates{
					Latitude:  gpl.Coordinates.Latitude,
					Longitude: gpl.Coordinates.Longitude,
				}
			}
		}

		if ch.ResourceRecordSet.CidrRoutingConfig != nil {
			crc := ch.ResourceRecordSet.CidrRoutingConfig
			rrs.CidrRoutingConfig = &CidrRoutingConfig{
				CollectionID: crc.CollectionID,
				LocationName: crc.LocationName,
			}
		}

		changes = append(changes, Change{
			Action:            ChangeAction(strings.ToUpper(ch.Action)),
			ResourceRecordSet: rrs,
		})
	}

	changeID, err := h.Backend.ChangeResourceRecordSets(zoneID, changes)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ChangeResourceRecordSets", "zoneID", zoneID, "changes", len(changes))

	resp := xmlChangeResourceRecordSetsResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          changeID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) listResourceRecordSets(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)
	q := c.Request().URL.Query()

	startName := q.Get("name")
	startType := q.Get("type")
	startIdentifier := q.Get("identifier")

	mi, parseErr := parseMaxItems(c, q.Get("maxitems"))
	if parseErr != nil {
		return parseErr
	}

	pg, err := h.Backend.ListResourceRecordSets(
		zoneID,
		startName,
		startType,
		startIdentifier,
		mi.requested,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ListResourceRecordSets", "zoneID", zoneID, "count", len(pg.Records))

	xmlRecords := make([]xmlResourceRecordSet, len(pg.Records))
	for i, rrs := range pg.Records {
		xmlRecords[i] = toXMLResourceRecordSet(rrs)
	}

	resp := xmlListResourceRecordSetsResponse{
		Xmlns:              route53Namespace,
		ResourceRecordSets: xmlRecords,
		IsTruncated:        pg.IsTruncated,
		MaxItems:           strconv.Itoa(mi.effective),
	}

	if pg.IsTruncated {
		resp.NextRecordName = pg.NextName
		resp.NextRecordType = pg.NextType
		resp.NextRecordIdentifier = pg.NextIdentifier
	}

	return writeXML(c, http.StatusOK, resp)
}

// toXMLResourceRecordSet converts a ResourceRecordSet to its XML representation.
func toXMLResourceRecordSet(rrs ResourceRecordSet) xmlResourceRecordSet {
	xmlRecs := make([]xmlResourceRecord, len(rrs.Records))
	for j, rr := range rrs.Records {
		xmlRecs[j] = xmlResourceRecord(rr)
	}

	xrrs := xmlResourceRecordSet{
		Name:             rrs.Name,
		Type:             rrs.Type,
		TTL:              rrs.TTL,
		ResourceRecords:  xmlRecs,
		SetIdentifier:    rrs.SetIdentifier,
		Failover:         string(rrs.Failover),
		Region:           rrs.Region,
		HealthCheckID:    rrs.HealthCheckID,
		Weight:           rrs.Weight,
		MultiValueAnswer: rrs.MultiValueAnswer,
	}

	if rrs.AliasTarget != nil {
		xrrs.AliasTarget = &xmlAliasTarget{
			HostedZoneID:         rrs.AliasTarget.HostedZoneID,
			DNSName:              rrs.AliasTarget.DNSName,
			EvaluateTargetHealth: rrs.AliasTarget.EvaluateTargetHealth,
		}
	}

	if rrs.GeoLocation != nil {
		xrrs.GeoLocation = &xmlGeoLocation{
			ContinentCode:   rrs.GeoLocation.ContinentCode,
			CountryCode:     rrs.GeoLocation.CountryCode,
			SubdivisionCode: rrs.GeoLocation.SubdivisionCode,
		}
	}

	if rrs.GeoProximityLocation != nil {
		xrrs.GeoProximityLocation = &xmlGeoProximityLocation{
			AWSRegion:      rrs.GeoProximityLocation.AWSRegion,
			LocalZoneGroup: rrs.GeoProximityLocation.LocalZoneGroup,
			Bias:           rrs.GeoProximityLocation.Bias,
		}
		if rrs.GeoProximityLocation.Coordinates != nil {
			xrrs.GeoProximityLocation.Coordinates = &xmlGeoProximityCoordinates{
				Latitude:  rrs.GeoProximityLocation.Coordinates.Latitude,
				Longitude: rrs.GeoProximityLocation.Coordinates.Longitude,
			}
		}
	}

	if rrs.CidrRoutingConfig != nil {
		xrrs.CidrRoutingConfig = &xmlCidrRoutingConfig{
			CollectionID: rrs.CidrRoutingConfig.CollectionID,
			LocationName: rrs.CidrRoutingConfig.LocationName,
		}
	}

	return xrrs
}

// getChange returns the status of a Route 53 change batch.
func (h *Handler) getChange(c *echo.Context, path string) error {
	type getChangeResp struct {
		XMLName    xml.Name      `xml:"GetChangeResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}

	// Extract change ID from path /2013-04-01/change/{id}.
	changeID := strings.TrimPrefix(path, route53ChangePrefix)

	ci, err := h.Backend.GetChange(changeID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, getChangeResp{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          ci.ID,
			Status:      ci.Status,
			SubmittedAt: ci.SubmittedAt,
		},
	})
}

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

	// Route 53 derives the client's region/geolocation from the resolver IP
	// (or the EDNS0 client-subnet IP when supplied). Build the query context
	// from those signals so routing-policy records resolve correctly.
	clientIP := q.Get("edns0clientsubnetip")
	if clientIP == "" {
		clientIP = q.Get("resolverip")
	}

	region, continent, country, subdivision := geoIPLookup(clientIP)
	qctx := DNSQueryContext{
		ClientRegion:    region,
		ContinentCode:   continent,
		CountryCode:     country,
		SubdivisionCode: subdivision,
		ResolverIP:      clientIP,
	}

	var recordData []string
	responseCode := "NOERROR"

	if zoneID != "" {
		values, err := h.Backend.TestDNSAnswer(zoneID, recordName, recordType, qctx)
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
