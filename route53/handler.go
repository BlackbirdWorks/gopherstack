package route53

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	route53PathPrefix  = "/2013-04-01/"
	route53HostedZone  = "/2013-04-01/hostedzone"
	route53Namespace   = "https://route53.amazonaws.com/doc/2013-04-01/"
	route53RRSetSuffix = "/rrset"
	route53HZPrefix    = "/2013-04-01/hostedzone/"
	route53TagsPrefix  = "/2013-04-01/tags/"
	// matchPriority is higher than path-based dashboard (50) but lower than header-based services (100).
	matchPriority = 80
	// zoneIDAndRest is the number of parts when splitting a zone path at the first "/".
	zoneIDAndRest = 2
)

// Handler is the HTTP service handler for Route 53 operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
	tags    map[string]map[string]string
	tagsMu  sync.RWMutex
}

// NewHandler creates a new Route 53 Handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}

	return &Handler{Backend: backend, Logger: log, tags: make(map[string]map[string]string)}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = make(map[string]string)
	}
	maps.Copy(h.tags[resourceID], kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	for _, k := range keys {
		delete(h.tags[resourceID], k)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock()
	defer h.tagsMu.RUnlock()
	result := make(map[string]string)
	maps.Copy(result, h.tags[resourceID])

	return result
}

// Name returns the service name.
func (h *Handler) Name() string { return "Route53" }

// MatchPriority returns the routing priority for Route 53.
func (h *Handler) MatchPriority() int { return matchPriority }

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
	case method == http.MethodGet:
		return "GetHostedZone"
	default:
		return "Unknown"
	}
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

// routeRequest dispatches Route 53 requests to the appropriate handler.
//
//nolint:cyclop // routing switch inherently requires multiple cases
func (h *Handler) routeRequest(c *echo.Context, path, method string) error {
	isHZPath := strings.HasPrefix(path, route53HZPrefix)
	isTagsPath := strings.HasPrefix(path, route53TagsPrefix)

	switch {
	case path == route53HostedZone && method == http.MethodPost:
		return h.createHostedZone(c)
	case path == route53HostedZone && method == http.MethodGet:
		return h.listHostedZones(c)
	case isHZPath && strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodPost:
		return h.changeResourceRecordSets(c)
	case isHZPath && strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodGet:
		return h.listResourceRecordSets(c)
	case isHZPath && method == http.MethodDelete:
		return h.deleteHostedZone(c)
	case isHZPath && method == http.MethodGet:
		return h.getHostedZone(c)
	case isTagsPath && method == http.MethodGet:
		return h.listTagsForResource(c, path)
	case isTagsPath && method == http.MethodPost:
		return h.changeTagsForResource(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			fmt.Sprintf("unknown Route53 endpoint: %s %s", method, path))
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
	HostedZones []xmlHostedZone `xml:"HostedZones>HostedZone"`
	IsTruncated bool            `xml:"IsTruncated"`
}

type xmlChangeResourceRecordSetsResponse struct {
	XMLName    xml.Name      `xml:"ChangeResourceRecordSetsResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlResourceRecord struct {
	Value string `xml:"Value"`
}

type xmlResourceRecordSet struct {
	XMLName         xml.Name            `xml:"ResourceRecordSet"`
	Name            string              `xml:"Name"`
	Type            string              `xml:"Type"`
	ResourceRecords []xmlResourceRecord `xml:"ResourceRecords>ResourceRecord,omitempty"`
	TTL             int64               `xml:"TTL,omitempty"`
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
	XMLName          xml.Name `xml:"CreateHostedZoneRequest"`
	Name             string   `xml:"Name"`
	CallerReference  string   `xml:"CallerReference"`
	HostedZoneConfig struct {
		Comment     string `xml:"Comment"`
		PrivateZone bool   `xml:"PrivateZone"`
	} `xml:"HostedZoneConfig"`
}

type xmlChangeBatch struct {
	XMLName xml.Name `xml:"ChangeBatch"`
	Changes []struct {
		Action            string `xml:"Action"`
		ResourceRecordSet struct {
			Name            string `xml:"Name"`
			Type            string `xml:"Type"`
			ResourceRecords []struct {
				Value string `xml:"Value"`
			} `xml:"ResourceRecords>ResourceRecord"`
			TTL int64 `xml:"TTL"`
		} `xml:"ResourceRecordSet"`
	} `xml:"Changes>Change"`
}

type xmlChangeResourceRecordSetsRequest struct {
	XMLName     xml.Name       `xml:"ChangeResourceRecordSetsRequest"`
	ChangeBatch xmlChangeBatch `xml:"ChangeBatch"`
}

// ---- Handlers ----

func (h *Handler) createHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputil.ReadBody(c.Request())
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

	h.Logger.DebugContext(ctx, "Route53 CreateHostedZone", "id", hz.ID, "name", hz.Name)

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

	h.Logger.DebugContext(ctx, "Route53 GetHostedZone", "id", hz.ID)

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

	h.Logger.DebugContext(ctx, "Route53 DeleteHostedZone", "id", zoneID)

	resp := struct {
		XMLName    xml.Name      `xml:"DeleteHostedZoneResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
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
	zones, err := h.Backend.ListHostedZones()
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlZones := make([]xmlHostedZone, len(zones))
	for i := range zones {
		xmlZones[i] = toXMLHostedZone(&zones[i])
	}

	resp := xmlListHostedZonesResponse{
		Xmlns:       route53Namespace,
		HostedZones: xmlZones,
		IsTruncated: false,
		MaxItems:    "100",
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) changeResourceRecordSets(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	body, err := httputil.ReadBody(c.Request())
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
			records[i] = ResourceRecord{Value: rr.Value}
		}

		changes = append(changes, Change{
			Action: ChangeAction(strings.ToUpper(ch.Action)),
			ResourceRecordSet: ResourceRecordSet{
				Name:    ch.ResourceRecordSet.Name,
				Type:    ch.ResourceRecordSet.Type,
				TTL:     ch.ResourceRecordSet.TTL,
				Records: records,
			},
		})
	}

	if err = h.Backend.ChangeResourceRecordSets(zoneID, changes); err != nil {
		return handleBackendError(c, err)
	}

	h.Logger.DebugContext(ctx, "Route53 ChangeResourceRecordSets", "zoneID", zoneID, "changes", len(changes))

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

	h.Logger.DebugContext(ctx, "Route53 ListResourceRecordSets", "zoneID", zoneID, "count", len(records))

	xmlRecords := make([]xmlResourceRecordSet, len(records))
	for i, rrs := range records {
		xmlRecs := make([]xmlResourceRecord, len(rrs.Records))
		for j, rr := range rrs.Records {
			xmlRecs[j] = xmlResourceRecord(rr)
		}

		xmlRecords[i] = xmlResourceRecordSet{
			Name:            rrs.Name,
			Type:            rrs.Type,
			TTL:             rrs.TTL,
			ResourceRecords: xmlRecs,
		}
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

func (h *Handler) changeTagsForResource(c *echo.Context) error {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // path has two segments: type + id
	resourceID := ""

	if len(parts) >= 2 { //nolint:mnd // path has two segments: type + id
		resourceID = parts[1]
	}

	h.applyTagChanges(resourceID, c.Request())

	type changeTagsResp struct {
		XMLName xml.Name `xml:"ChangeTagsForResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return writeXML(c, http.StatusOK, changeTagsResp{Xmlns: route53Namespace})
}

// applyTagChanges reads a ChangeTagsForResource XML body and applies the add/remove operations.
func (h *Handler) applyTagChanges(resourceID string, r *http.Request) {
	body, err := httputil.ReadBody(r)
	if err != nil || len(body) == 0 {
		return
	}

	var req struct {
		AddTags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"AddTags>Tag"`
		RemoveTagKeys []string `xml:"RemoveTagKeys>Key"`
	}

	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return
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

// xmlError writes a Route 53-style XML error response.
func xmlError(c *echo.Context, statusCode int, code, message string) error {
	type xmlErrResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
		Error   struct {
			Type    string `xml:"Type"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}

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
	case errors.Is(err, ErrInvalidInput):
		return xmlError(c, http.StatusBadRequest, "InvalidInput", err.Error())
	case errors.Is(err, ErrInvalidAction):
		return xmlError(c, http.StatusBadRequest, "InvalidChangeBatch", err.Error())
	default:
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}
