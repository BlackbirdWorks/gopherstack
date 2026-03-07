package route53

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	route53PathPrefix        = "/2013-04-01/"
	route53HostedZone        = "/2013-04-01/hostedzone"
	route53Namespace         = "https://route53.amazonaws.com/doc/2013-04-01/"
	route53RRSetSuffix       = "/rrset"
	route53HZPrefix          = "/2013-04-01/hostedzone/"
	route53TagsPrefix        = "/2013-04-01/tags/"
	route53ChangePrefix      = "/2013-04-01/change/"
	route53HealthCheckRoot   = "/2013-04-01/healthcheck"
	route53HealthCheckPrefix = "/2013-04-01/healthcheck/"
	route53StatusSuffix      = "/status"
	// zoneIDAndRest is the number of parts when splitting a zone path at the first "/".
	zoneIDAndRest = 2
)

// Handler is the HTTP service handler for Route 53 operations.
type Handler struct {
	Backend *InMemoryBackend
	tags    map[string]*svcTags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new Route 53 Handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("route53.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("route53." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Route53" }

// MatchPriority returns the routing priority for Route 53.
func (h *Handler) MatchPriority() int { return service.PriorityFormStandard }

// RouteMatcher returns a matcher that selects Route 53 requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, route53PathPrefix)
	}
}

// GetSupportedOperations returns all mocked Route 53 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateHostedZone",
		"DeleteHostedZone",
		"ListHostedZones",
		"GetHostedZone",
		"ChangeResourceRecordSets",
		"ListResourceRecordSets",
		"ListTagsForResource",
		"ChangeTagsForResource",
		"CreateHealthCheck",
		"GetHealthCheck",
		"ListHealthChecks",
		"DeleteHealthCheck",
		"UpdateHealthCheck",
		"GetHealthCheckStatus",
	}
}

// extractHealthCheckOperation maps a health-check path+method to an operation name.
// Returns "" when the path does not match any health check route.
func extractHealthCheckOperation(path, method string) string {
	switch {
	case path == route53HealthCheckRoot && method == http.MethodPost:
		return "CreateHealthCheck"
	case path == route53HealthCheckRoot && method == http.MethodGet:
		return "ListHealthChecks"
	}

	if !strings.HasPrefix(path, route53HealthCheckPrefix) {
		return ""
	}

	if strings.HasSuffix(path, route53StatusSuffix) {
		return "GetHealthCheckStatus"
	}

	switch method {
	case http.MethodGet:
		return "GetHealthCheck"
	case http.MethodDelete:
		return "DeleteHealthCheck"
	case http.MethodPost:
		return "UpdateHealthCheck"
	default:
		return ""
	}
}

// ExtractOperation extracts a human-readable operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	switch {
	case path == route53HostedZone && method == http.MethodPost:
		return "CreateHostedZone"
	case path == route53HostedZone && method == http.MethodGet:
		return "ListHostedZones"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodPost:
		return "ChangeResourceRecordSets"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodGet:
		return "ListResourceRecordSets"
	case method == http.MethodDelete:
		return "DeleteHostedZone"
	case method == http.MethodGet && strings.HasPrefix(path, route53HZPrefix):
		return "GetHostedZone"
	}

	if op := extractHealthCheckOperation(path, method); op != "" {
		return op
	}

	return "Unknown"
}

// ExtractResource extracts the zone ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	// /2013-04-01/hostedzone/{Id}  or  /2013-04-01/hostedzone/{Id}/rrset
	parts := strings.Split(strings.TrimPrefix(path, route53HZPrefix), "/")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}

	return ""
}

// iamActionForHealthCheck maps a health-check path+method to an IAM action string.
// Returns "" when the path does not match any health check route.
func iamActionForHealthCheck(path, method string) string {
	switch {
	case path == route53HealthCheckRoot && method == http.MethodPost:
		return "route53:CreateHealthCheck"
	case path == route53HealthCheckRoot && method == http.MethodGet:
		return "route53:ListHealthChecks"
	}

	if !strings.HasPrefix(path, route53HealthCheckPrefix) {
		return ""
	}

	if strings.HasSuffix(path, route53StatusSuffix) {
		return "route53:GetHealthCheckStatus"
	}

	switch method {
	case http.MethodGet:
		return "route53:GetHealthCheck"
	case http.MethodDelete:
		return "route53:DeleteHealthCheck"
	case http.MethodPost:
		return "route53:UpdateHealthCheck"
	default:
		return ""
	}
}

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

func (h *Handler) IAMAction(r *http.Request) string {
	path := r.URL.Path
	if !strings.HasPrefix(path, route53PathPrefix) {
		return ""
	}

	method := r.Method

	if action := iamActionForHostedZone(path, method); action != "" {
		return action
	}

	if action := iamActionForHealthCheck(path, method); action != "" {
		return action
	}

	return "route53:GetChange"
}

// routeRequest dispatches Route 53 requests to the appropriate handler.
func (h *Handler) routeRequest(c *echo.Context, path, method string) error {
	switch {
	case path == route53HostedZone:
		return h.routeHostedZoneRoot(c, method)
	case strings.HasPrefix(path, route53HZPrefix):
		return h.routeHostedZone(c, path, method)
	case strings.HasPrefix(path, route53TagsPrefix):
		return h.routeTags(c, path, method)
	case strings.HasPrefix(path, route53ChangePrefix):
		return h.routeChange(c, path, method)
	case path == route53HealthCheckRoot:
		return h.routeHealthCheckRoot(c, method)
	case strings.HasPrefix(path, route53HealthCheckPrefix):
		return h.routeHealthCheck(c, path, method)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			fmt.Sprintf("unknown Route53 endpoint: %s %s", method, path))
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

func (h *Handler) routeHostedZone(c *echo.Context, path, method string) error {
	if strings.HasSuffix(path, route53RRSetSuffix) {
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

	switch method {
	case http.MethodDelete:
		return h.deleteHostedZone(c)
	case http.MethodGet:
		return h.getHostedZone(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on hosted zone")
	}
}

func (h *Handler) routeTags(c *echo.Context, path, method string) error {
	switch method {
	case http.MethodGet:
		return h.listTagsForResource(c, path)
	case http.MethodPost:
		return h.changeTagsForResource(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on tags")
	}
}

func (h *Handler) routeChange(c *echo.Context, path, method string) error {
	if method == http.MethodGet {
		return h.getChange(c, path)
	}

	return xmlError(c, http.StatusNotFound, "NoSuchOperation",
		"unsupported method on change")
}

func (h *Handler) routeHealthCheckRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createHealthCheck(c)
	case http.MethodGet:
		return h.listHealthChecks(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on /healthcheck")
	}
}

func (h *Handler) routeHealthCheck(c *echo.Context, path, method string) error {
	if strings.HasSuffix(path, route53StatusSuffix) {
		if method == http.MethodGet {
			return h.getHealthCheckStatus(c, path)
		}

		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on health check status")
	}

	switch method {
	case http.MethodGet:
		return h.getHealthCheck(c, path)
	case http.MethodDelete:
		return h.deleteHealthCheck(c, path)
	case http.MethodPost:
		return h.updateHealthCheck(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on health check")
	}
}

// Handler returns the Echo handler function for Route 53 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		path := c.Request().URL.Path
		method := c.Request().Method

		log.DebugContext(ctx, "Route53 request", "method", method, "path", path)

		return h.routeRequest(c, path, method)
	}
}

// extractZoneID returns the hosted zone ID from a path like /2013-04-01/hostedzone/{Id}...
func extractZoneID(path string) string {
	rest := strings.TrimPrefix(path, route53HZPrefix)
	// rest is either "{Id}" or "{Id}/rrset"
	parts := strings.SplitN(rest, "/", zoneIDAndRest)
	if len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// ---- XML response types ----

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
	XMLName     xml.Name `xml:"DelegationSet"`
	NameServers []string `xml:"NameServers>NameServer"`
}

type xmlChangeInfo struct {
	XMLName     xml.Name  `xml:"ChangeInfo"`
	SubmittedAt time.Time `xml:"SubmittedAt"`
	ID          string    `xml:"Id"`
	Status      string    `xml:"Status"`
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
	HostedZone    xmlHostedZone    `xml:"HostedZone"`
}

type xmlListHostedZonesResponse struct {
	XMLName     xml.Name        `xml:"ListHostedZonesResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	MaxItems    string          `xml:"MaxItems"`
	NextMarker  string          `xml:"NextMarker,omitempty"`
	HostedZones []xmlHostedZone `xml:"HostedZones>HostedZone"`
	IsTruncated bool            `xml:"IsTruncated"`
}

type xmlChangeResourceRecordSetsResponse struct {
	XMLName    xml.Name      `xml:"ChangeResourceRecordSetsResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlDeleteHostedZoneResponse struct {
	XMLName    xml.Name      `xml:"DeleteHostedZoneResponse"`
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
	CountryCode     string `xml:"CountryCode,omitempty"`
	SubdivisionCode string `xml:"SubdivisionCode,omitempty"`
}

type xmlResourceRecordSet struct {
	XMLName         xml.Name            `xml:"ResourceRecordSet"`
	AliasTarget     *xmlAliasTarget     `xml:"AliasTarget,omitempty"`
	GeoLocation     *xmlGeoLocation     `xml:"GeoLocation,omitempty"`
	Name            string              `xml:"Name"`
	Type            string              `xml:"Type"`
	SetIdentifier   string              `xml:"SetIdentifier,omitempty"`
	Failover        string              `xml:"Failover,omitempty"`
	Region          string              `xml:"Region,omitempty"`
	HealthCheckID   string              `xml:"HealthCheckId,omitempty"`
	ResourceRecords []xmlResourceRecord `xml:"ResourceRecords>ResourceRecord,omitempty"`
	TTL             int64               `xml:"TTL,omitempty"`
	Weight          int64               `xml:"Weight,omitempty"`
}

type xmlListResourceRecordSetsResponse struct {
	XMLName            xml.Name               `xml:"ListResourceRecordSetsResponse"`
	Xmlns              string                 `xml:"xmlns,attr"`
	MaxItems           string                 `xml:"MaxItems"`
	ResourceRecordSets []xmlResourceRecordSet `xml:"ResourceRecordSets>ResourceRecordSet"`
	IsTruncated        bool                   `xml:"IsTruncated"`
}

// ---- XML request types ----

type xmlCreateHostedZoneRequest struct {
	XMLName          xml.Name            `xml:"CreateHostedZoneRequest"`
	Name             string              `xml:"Name"`
	CallerReference  string              `xml:"CallerReference"`
	HostedZoneConfig xmlHostedZoneConfig `xml:"HostedZoneConfig"`
}

// xmlResourceRecordSetChange is the ResourceRecordSet element within a change batch entry.
type xmlResourceRecordSetChange struct {
	AliasTarget     *xmlAliasTarget     `xml:"AliasTarget"`
	GeoLocation     *xmlGeoLocation     `xml:"GeoLocation"`
	Name            string              `xml:"Name"`
	Type            string              `xml:"Type"`
	SetIdentifier   string              `xml:"SetIdentifier"`
	Failover        string              `xml:"Failover"`
	Region          string              `xml:"Region"`
	HealthCheckID   string              `xml:"HealthCheckId"`
	ResourceRecords []xmlResourceRecord `xml:"ResourceRecords>ResourceRecord"`
	TTL             int64               `xml:"TTL"`
	Weight          int64               `xml:"Weight"`
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

// ---- Handlers ----

func (h *Handler) createHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateHostedZoneRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	hz, err := h.Backend.CreateHostedZone(
		req.Name, req.CallerReference,
		req.HostedZoneConfig.Comment, req.HostedZoneConfig.PrivateZone,
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
			Status:      "INSYNC",
			SubmittedAt: time.Now(),
		},
		DelegationSet: xmlDelegationSet{
			NameServers: []string{"ns1.gopherstack.invalid", "ns2.gopherstack.invalid"},
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01/hostedzone/"+hz.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) getHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	hz, err := h.Backend.GetHostedZone(zoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetHostedZone", "id", hz.ID)

	resp := xmlGetHostedZoneResponse{
		Xmlns:      route53Namespace,
		HostedZone: toXMLHostedZone(hz),
		DelegationSet: xmlDelegationSet{
			NameServers: []string{"ns1.gopherstack.invalid", "ns2.gopherstack.invalid"},
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) deleteHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	if err := h.Backend.DeleteHostedZone(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteHostedZone", "id", zoneID)

	resp := xmlDeleteHostedZoneResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			SubmittedAt: time.Now(),
			ID:          "/change/C" + zoneID,
			Status:      "INSYNC",
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

	p, err := h.Backend.ListHostedZones(marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlZones := make([]xmlHostedZone, len(p.Data))
	for i := range p.Data {
		xmlZones[i] = toXMLHostedZone(&p.Data[i])
	}

	resp := xmlListHostedZonesResponse{
		Xmlns:       route53Namespace,
		HostedZones: xmlZones,
		IsTruncated: p.Next != "",
		NextMarker:  p.Next,
		MaxItems:    strconv.Itoa(maxItems),
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) changeResourceRecordSets(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlChangeResourceRecordSetsRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	changes := make([]Change, 0, len(req.ChangeBatch.Changes))
	for _, ch := range req.ChangeBatch.Changes {
		records := make([]ResourceRecord, len(ch.ResourceRecordSet.ResourceRecords))
		for i, rr := range ch.ResourceRecordSet.ResourceRecords {
			records[i] = ResourceRecord(rr)
		}

		rrs := ResourceRecordSet{
			Name:          ch.ResourceRecordSet.Name,
			Type:          ch.ResourceRecordSet.Type,
			TTL:           ch.ResourceRecordSet.TTL,
			Records:       records,
			SetIdentifier: ch.ResourceRecordSet.SetIdentifier,
			Failover:      FailoverPolicy(ch.ResourceRecordSet.Failover),
			Region:        ch.ResourceRecordSet.Region,
			HealthCheckID: ch.ResourceRecordSet.HealthCheckID,
			Weight:        ch.ResourceRecordSet.Weight,
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

		changes = append(changes, Change{
			Action:            ChangeAction(strings.ToUpper(ch.Action)),
			ResourceRecordSet: rrs,
		})
	}

	if err = h.Backend.ChangeResourceRecordSets(zoneID, changes); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ChangeResourceRecordSets", "zoneID", zoneID, "changes", len(changes))

	resp := xmlChangeResourceRecordSetsResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + zoneID,
			Status:      "INSYNC",
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) listResourceRecordSets(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	records, err := h.Backend.ListResourceRecordSets(zoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ListResourceRecordSets", "zoneID", zoneID, "count", len(records))

	xmlRecords := make([]xmlResourceRecordSet, len(records))
	for i, rrs := range records {
		xmlRecs := make([]xmlResourceRecord, len(rrs.Records))
		for j, rr := range rrs.Records {
			xmlRecs[j] = xmlResourceRecord(rr)
		}

		xrrs := xmlResourceRecordSet{
			Name:            rrs.Name,
			Type:            rrs.Type,
			TTL:             rrs.TTL,
			ResourceRecords: xmlRecs,
			SetIdentifier:   rrs.SetIdentifier,
			Failover:        string(rrs.Failover),
			Region:          rrs.Region,
			HealthCheckID:   rrs.HealthCheckID,
			Weight:          rrs.Weight,
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

		xmlRecords[i] = xrrs
	}

	resp := xmlListResourceRecordSetsResponse{
		Xmlns:              route53Namespace,
		ResourceRecordSets: xmlRecords,
		IsTruncated:        false,
		MaxItems:           "300",
	}

	return writeXML(c, http.StatusOK, resp)
}

// ---- Helpers ----

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

func (h *Handler) listTagsForResource(c *echo.Context, path string) error {
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split into type + id
	resourceType := ""
	resourceID := ""

	if len(parts) >= 1 {
		resourceType = parts[0]
	}

	if len(parts) >= 2 { //nolint:mnd // path has two segments: type + id
		resourceID = parts[1]
	}

	tags := h.getTags(resourceID)
	type r53Tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	tagList := make([]r53Tag, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, r53Tag{Key: k, Value: v})
	}

	type resourceTagSet struct {
		ResourceType string   `xml:"ResourceType"`
		ResourceID   string   `xml:"ResourceId"`
		Tags         []r53Tag `xml:"Tags>Tag"`
	}
	type tagsResp struct {
		XMLName        xml.Name       `xml:"ListTagsForResourceResponse"`
		Xmlns          string         `xml:"xmlns,attr"`
		ResourceTagSet resourceTagSet `xml:"ResourceTagSet"`
	}

	resp := tagsResp{Xmlns: route53Namespace}
	resp.ResourceTagSet.ResourceType = resourceType
	resp.ResourceTagSet.ResourceID = resourceID
	resp.ResourceTagSet.Tags = tagList

	return writeXML(c, http.StatusOK, resp)
}

// getChange stubs the Route53 GetChange API, always returning INSYNC so callers don't wait.
func (h *Handler) getChange(c *echo.Context, path string) error {
	type getChangeResp struct {
		XMLName    xml.Name      `xml:"GetChangeResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}

	return writeXML(c, http.StatusOK, getChangeResp{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/" + path,
			Status:      "INSYNC",
			SubmittedAt: time.Now(),
		},
	})
}

func (h *Handler) changeTagsForResource(c *echo.Context) error {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // path has two segments: type + id
	resourceID := ""

	if len(parts) >= 2 { //nolint:mnd // path has two segments: type + id
		resourceID = parts[1]
	}

	if err := h.applyTagChanges(resourceID, c.Request()); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", err.Error())
	}

	type changeTagsResp struct {
		XMLName xml.Name `xml:"ChangeTagsForResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return writeXML(c, http.StatusOK, changeTagsResp{Xmlns: route53Namespace})
}

type applyTagChangesInput struct {
	AddTags       []svcTags.KV `xml:"AddTags>Tag"`
	RemoveTagKeys []string     `xml:"RemoveTagKeys>Key"`
}

// applyTagChanges reads a ChangeTagsForResource XML body and applies the add/remove operations.
// It returns an error if the body cannot be read or parsed.
func (h *Handler) applyTagChanges(resourceID string, r *http.Request) error {
	body, err := httputils.ReadBody(r)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}

	if len(body) == 0 {
		return nil
	}

	var req applyTagChangesInput

	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return fmt.Errorf("failed to parse XML: %w", xmlErr)
	}

	if len(req.AddTags) > 0 {
		kv := make(map[string]string, len(req.AddTags))
		for _, t := range req.AddTags {
			kv[t.Key] = t.Value
		}

		h.setTags(resourceID, kv)
	}

	if len(req.RemoveTagKeys) > 0 {
		h.removeTags(resourceID, req.RemoveTagKeys)
	}

	return nil
}

// writeXML marshals v to XML and writes it to the response.
func writeXML(c *echo.Context, statusCode int, v any) error {
	data, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	c.Response().Header().Set("Content-Type", "application/xml")
	c.Response().WriteHeader(statusCode)

	header := xml.Header
	_, _ = io.WriteString(c.Response(), header)
	_, _ = c.Response().Write(data)

	return nil
}

// xmlErrDetail is the nested error detail element in a Route53 error response.
type xmlErrDetail struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// xmlErrResp is the XML error response body for Route53.
type xmlErrResp struct {
	XMLName xml.Name     `xml:"ErrorResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Error   xmlErrDetail `xml:"Error"`
}

// xmlError writes a Route 53-style XML error response.
func xmlError(c *echo.Context, statusCode int, code, message string) error {
	resp := xmlErrResp{Xmlns: route53Namespace}
	resp.Error.Type = "Sender"
	resp.Error.Code = code
	resp.Error.Message = message

	data, _ := xml.MarshalIndent(resp, "", "  ")

	c.Response().Header().Set("Content-Type", "application/xml")
	c.Response().WriteHeader(statusCode)
	_, _ = io.WriteString(c.Response(), xml.Header)
	_, _ = c.Response().Write(data)

	return nil
}

// handleBackendError maps backend errors to HTTP responses.
func handleBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrHostedZoneNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchHostedZone", err.Error())
	case errors.Is(err, ErrHealthCheckNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchHealthCheck", err.Error())
	case errors.Is(err, ErrInvalidInput):
		return xmlError(c, http.StatusBadRequest, "InvalidInput", err.Error())
	case errors.Is(err, ErrInvalidAction):
		return xmlError(c, http.StatusBadRequest, "InvalidChangeBatch", err.Error())
	default:
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// ---- Health check XML types ----

type xmlHealthCheckConfig struct {
	IPAddress                string   `xml:"IPAddress,omitempty"`
	FullyQualifiedDomainName string   `xml:"FullyQualifiedDomainName,omitempty"`
	ResourcePath             string   `xml:"ResourcePath,omitempty"`
	Type                     string   `xml:"Type"`
	ChildHealthChecks        []string `xml:"ChildHealthChecks>ChildHealthCheck,omitempty"`
	Port                     int      `xml:"Port,omitempty"`
	RequestInterval          int      `xml:"RequestInterval,omitempty"`
	FailureThreshold         int      `xml:"FailureThreshold,omitempty"`
	HealthThreshold          int      `xml:"HealthThreshold,omitempty"`
	Inverted                 bool     `xml:"Inverted,omitempty"`
}

type xmlHealthCheck struct {
	XMLName         xml.Name             `xml:"HealthCheck"`
	ID              string               `xml:"Id"`
	CallerReference string               `xml:"CallerReference"`
	Config          xmlHealthCheckConfig `xml:"HealthCheckConfig"`
}

type xmlCreateHealthCheckRequest struct {
	XMLName         xml.Name             `xml:"CreateHealthCheckRequest"`
	CallerReference string               `xml:"CallerReference"`
	Config          xmlHealthCheckConfig `xml:"HealthCheckConfig"`
}

type xmlUpdateHealthCheckRequest struct {
	XMLName                  xml.Name `xml:"UpdateHealthCheckRequest"`
	IPAddress                string   `xml:"IPAddress,omitempty"`
	FullyQualifiedDomainName string   `xml:"FullyQualifiedDomainName,omitempty"`
	ResourcePath             string   `xml:"ResourcePath,omitempty"`
	Port                     int      `xml:"Port,omitempty"`
	RequestInterval          int      `xml:"RequestInterval,omitempty"`
	FailureThreshold         int      `xml:"FailureThreshold,omitempty"`
	HealthThreshold          int      `xml:"HealthThreshold,omitempty"`
	Inverted                 bool     `xml:"Inverted,omitempty"`
}

type xmlCreateHealthCheckResponse struct {
	XMLName     xml.Name       `xml:"CreateHealthCheckResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type xmlGetHealthCheckResponse struct {
	XMLName     xml.Name       `xml:"GetHealthCheckResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type xmlListHealthChecksResponse struct {
	XMLName      xml.Name         `xml:"ListHealthChecksResponse"`
	Xmlns        string           `xml:"xmlns,attr"`
	MaxItems     string           `xml:"MaxItems"`
	NextMarker   string           `xml:"NextMarker,omitempty"`
	HealthChecks []xmlHealthCheck `xml:"HealthChecks>HealthCheck"`
	IsTruncated  bool             `xml:"IsTruncated"`
}

type xmlDeleteHealthCheckResponse struct {
	XMLName xml.Name `xml:"DeleteHealthCheckResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlUpdateHealthCheckResponse struct {
	XMLName     xml.Name       `xml:"UpdateHealthCheckResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type xmlHealthCheckObservation struct {
	StatusReport struct {
		CheckedTime time.Time `xml:"CheckedTime"`
		Status      string    `xml:"Status"`
	} `xml:"StatusReport"`
	Region    string `xml:"Region"`
	IPAddress string `xml:"IPAddress"`
}

type xmlGetHealthCheckStatusResponse struct {
	XMLName                 xml.Name                    `xml:"GetHealthCheckStatusResponse"`
	Xmlns                   string                      `xml:"xmlns,attr"`
	HealthCheckObservations []xmlHealthCheckObservation `xml:"HealthCheckObservations>HealthCheckObservation"`
}

// toXMLHealthCheck converts a HealthCheck to its XML representation.
func toXMLHealthCheck(hc *HealthCheck) xmlHealthCheck {
	return xmlHealthCheck{
		ID:              hc.ID,
		CallerReference: hc.CallerReference,
		Config: xmlHealthCheckConfig{
			IPAddress:                hc.Config.IPAddress,
			FullyQualifiedDomainName: hc.Config.FullyQualifiedDomainName,
			ResourcePath:             hc.Config.ResourcePath,
			Type:                     string(hc.Config.Type),
			Port:                     hc.Config.Port,
			RequestInterval:          hc.Config.RequestInterval,
			FailureThreshold:         hc.Config.FailureThreshold,
			HealthThreshold:          hc.Config.HealthThreshold,
			Inverted:                 hc.Config.Inverted,
			ChildHealthChecks:        hc.Config.ChildHealthChecks,
		},
	}
}

// extractHealthCheckID returns the health check ID from a path like /2013-04-01/healthcheck/{Id}...
func extractHealthCheckID(path string) string {
	rest := strings.TrimPrefix(path, route53HealthCheckPrefix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)
	if len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// ---- Health check handlers ----

func (h *Handler) createHealthCheck(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateHealthCheckRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	cfg := HealthCheckConfig{
		IPAddress:                req.Config.IPAddress,
		FullyQualifiedDomainName: req.Config.FullyQualifiedDomainName,
		ResourcePath:             req.Config.ResourcePath,
		Type:                     HealthCheckType(req.Config.Type),
		Port:                     req.Config.Port,
		RequestInterval:          req.Config.RequestInterval,
		FailureThreshold:         req.Config.FailureThreshold,
		HealthThreshold:          req.Config.HealthThreshold,
		Inverted:                 req.Config.Inverted,
		ChildHealthChecks:        req.Config.ChildHealthChecks,
	}

	hc, err := h.Backend.CreateHealthCheck(req.CallerReference, cfg)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateHealthCheck", "id", hc.ID)

	resp := xmlCreateHealthCheckResponse{
		Xmlns:       route53Namespace,
		HealthCheck: toXMLHealthCheck(hc),
	}

	c.Response().Header().Set("Location", "/2013-04-01/healthcheck/"+hc.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) getHealthCheck(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := extractHealthCheckID(path)

	hc, err := h.Backend.GetHealthCheck(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetHealthCheck", "id", id)

	return writeXML(c, http.StatusOK, xmlGetHealthCheckResponse{
		Xmlns:       route53Namespace,
		HealthCheck: toXMLHealthCheck(hc),
	})
}

func (h *Handler) listHealthChecks(c *echo.Context) error {
	q := c.Request().URL.Query()
	marker := q.Get("marker")
	maxItems := 0

	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListHealthChecks(marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlHCs := make([]xmlHealthCheck, len(p.Data))
	for i := range p.Data {
		xmlHCs[i] = toXMLHealthCheck(&p.Data[i])
	}

	return writeXML(c, http.StatusOK, xmlListHealthChecksResponse{
		Xmlns:        route53Namespace,
		HealthChecks: xmlHCs,
		IsTruncated:  p.Next != "",
		NextMarker:   p.Next,
		MaxItems:     strconv.Itoa(maxItems),
	})
}

func (h *Handler) deleteHealthCheck(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := extractHealthCheckID(path)

	if err := h.Backend.DeleteHealthCheck(id); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteHealthCheck", "id", id)

	return writeXML(c, http.StatusOK, xmlDeleteHealthCheckResponse{Xmlns: route53Namespace})
}

func (h *Handler) updateHealthCheck(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := extractHealthCheckID(path)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlUpdateHealthCheckRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	existing, err := h.Backend.GetHealthCheck(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	// Merge non-zero fields from the request into the existing config.
	cfg := existing.Config
	if req.IPAddress != "" {
		cfg.IPAddress = req.IPAddress
	}

	if req.FullyQualifiedDomainName != "" {
		cfg.FullyQualifiedDomainName = req.FullyQualifiedDomainName
	}

	if req.ResourcePath != "" {
		cfg.ResourcePath = req.ResourcePath
	}

	if req.Port != 0 {
		cfg.Port = req.Port
	}

	if req.RequestInterval != 0 {
		cfg.RequestInterval = req.RequestInterval
	}

	if req.FailureThreshold != 0 {
		cfg.FailureThreshold = req.FailureThreshold
	}

	if req.HealthThreshold != 0 {
		cfg.HealthThreshold = req.HealthThreshold
	}

	cfg.Inverted = req.Inverted

	hc, err := h.Backend.UpdateHealthCheck(id, cfg)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 UpdateHealthCheck", "id", id)

	return writeXML(c, http.StatusOK, xmlUpdateHealthCheckResponse{
		Xmlns:       route53Namespace,
		HealthCheck: toXMLHealthCheck(hc),
	})
}

func (h *Handler) getHealthCheckStatus(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	// path is /2013-04-01/healthcheck/{id}/status — strip the /status suffix first.
	withoutStatus := strings.TrimSuffix(path, route53StatusSuffix)
	id := extractHealthCheckID(withoutStatus)

	status, err := h.Backend.GetHealthCheckStatus(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetHealthCheckStatus", "id", id, "status", status)

	obs := xmlHealthCheckObservation{
		Region:    "us-east-1",
		IPAddress: "0.0.0.0",
	}
	obs.StatusReport.Status = status
	obs.StatusReport.CheckedTime = time.Now()

	return writeXML(c, http.StatusOK, xmlGetHealthCheckStatusResponse{
		Xmlns:                   route53Namespace,
		HealthCheckObservations: []xmlHealthCheckObservation{obs},
	})
}
