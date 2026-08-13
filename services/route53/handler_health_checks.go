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

// healthCheckSubPathOp resolves the /status and /lastfailurereason
// sub-paths shared by extractHealthCheckOperation and iamActionForHealthCheck.
// ok is true when path matched one of these suffixes at all (whether or not
// method was valid for it), so the caller knows not to fall through to the
// generic Get/Delete/Update switch.
func healthCheckSubPathOp(path, method, getStatusOp, getReasonOp string) (string, bool) {
	if strings.HasSuffix(path, route53StatusSuffix) {
		if method == http.MethodGet {
			return getStatusOp, true
		}

		return "", true
	}

	if strings.HasSuffix(path, route53LastFailureReasonSuffix) {
		if method == http.MethodGet {
			return getReasonOp, true
		}

		return "", true
	}

	return "", false
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

	if op, ok := healthCheckSubPathOp(path, method, "GetHealthCheckStatus", "GetHealthCheckLastFailureReason"); ok {
		return op
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

	if op, ok := healthCheckSubPathOp(
		path, method, "route53:GetHealthCheckStatus", "route53:GetHealthCheckLastFailureReason",
	); ok {
		return op
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

	// GetHealthCheckLastFailureReason: GET .../healthcheck/{id}/lastfailurereason
	// (api_op_GetHealthCheckLastFailureReason.go, route53@v1.65.6 serializers.go).
	// Must be checked before the generic switch below, or a real request here
	// silently resolves to GetHealthCheck instead (same real ID, wrong response
	// shape -- gopherstack-l5ir).
	if strings.HasSuffix(path, route53LastFailureReasonSuffix) {
		if method == http.MethodGet {
			return h.getHealthCheckLastFailureReason(c, path)
		}

		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on health check last failure reason")
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

type xmlAlarmIdentifier struct {
	Name   string `xml:"Name"`
	Region string `xml:"Region"`
}

type xmlHealthCheckConfig struct {
	AlarmIdentifier              *xmlAlarmIdentifier `xml:"AlarmIdentifier,omitempty"`
	IPAddress                    string              `xml:"IPAddress,omitempty"`
	FullyQualifiedDomainName     string              `xml:"FullyQualifiedDomainName,omitempty"`
	ResourcePath                 string              `xml:"ResourcePath,omitempty"`
	SearchString                 string              `xml:"SearchString,omitempty"`
	InsufficientDataHealthStatus string              `xml:"InsufficientDataHealthStatus,omitempty"`
	RoutingControlArn            string              `xml:"RoutingControlArn,omitempty"`
	Type                         string              `xml:"Type"`
	Regions                      []string            `xml:"Regions>Region,omitempty"`
	ChildHealthChecks            []string            `xml:"ChildHealthChecks>ChildHealthCheck,omitempty"`
	Port                         int                 `xml:"Port,omitempty"`
	RequestInterval              int                 `xml:"RequestInterval,omitempty"`
	FailureThreshold             int                 `xml:"FailureThreshold,omitempty"`
	HealthThreshold              int                 `xml:"HealthThreshold,omitempty"`
	EnableSNI                    bool                `xml:"EnableSNI,omitempty"`
	MeasureLatency               bool                `xml:"MeasureLatency,omitempty"`
	Disabled                     bool                `xml:"Disabled,omitempty"`
	Inverted                     bool                `xml:"Inverted,omitempty"`
}

type xmlHealthCheck struct {
	XMLName            xml.Name             `xml:"HealthCheck"`
	ID                 string               `xml:"Id"`
	CallerReference    string               `xml:"CallerReference"`
	Config             xmlHealthCheckConfig `xml:"HealthCheckConfig"`
	HealthCheckVersion int64                `xml:"HealthCheckVersion"`
}

type xmlCreateHealthCheckRequest struct {
	XMLName         xml.Name             `xml:"CreateHealthCheckRequest"`
	CallerReference string               `xml:"CallerReference"`
	Config          xmlHealthCheckConfig `xml:"HealthCheckConfig"`
}

type xmlUpdateHealthCheckRequest struct {
	AlarmIdentifier              *xmlAlarmIdentifier `xml:"AlarmIdentifier"`
	Inverted                     *bool               `xml:"Inverted"`
	HealthCheckVersion           *int64              `xml:"HealthCheckVersion"`
	XMLName                      xml.Name            `xml:"UpdateHealthCheckRequest"`
	IPAddress                    string              `xml:"IPAddress,omitempty"`
	FullyQualifiedDomainName     string              `xml:"FullyQualifiedDomainName,omitempty"`
	ResourcePath                 string              `xml:"ResourcePath,omitempty"`
	SearchString                 string              `xml:"SearchString,omitempty"`
	InsufficientDataHealthStatus string              `xml:"InsufficientDataHealthStatus,omitempty"`
	RoutingControlArn            string              `xml:"RoutingControlArn,omitempty"`
	Regions                      []string            `xml:"Regions>Region,omitempty"`
	ChildHealthChecks            []string            `xml:"ChildHealthChecks>ChildHealthCheck,omitempty"`
	Port                         int                 `xml:"Port,omitempty"`
	RequestInterval              int                 `xml:"RequestInterval,omitempty"`
	FailureThreshold             int                 `xml:"FailureThreshold,omitempty"`
	HealthThreshold              int                 `xml:"HealthThreshold,omitempty"`
	EnableSNI                    bool                `xml:"EnableSNI,omitempty"`
	MeasureLatency               bool                `xml:"MeasureLatency,omitempty"`
	Disabled                     bool                `xml:"Disabled,omitempty"`
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
	cfg := xmlHealthCheckConfig{
		IPAddress:                    hc.Config.IPAddress,
		FullyQualifiedDomainName:     hc.Config.FullyQualifiedDomainName,
		ResourcePath:                 hc.Config.ResourcePath,
		SearchString:                 hc.Config.SearchString,
		InsufficientDataHealthStatus: hc.Config.InsufficientDataHealthStatus,
		RoutingControlArn:            hc.Config.RoutingControlArn,
		Type:                         string(hc.Config.Type),
		Port:                         hc.Config.Port,
		RequestInterval:              hc.Config.RequestInterval,
		FailureThreshold:             hc.Config.FailureThreshold,
		HealthThreshold:              hc.Config.HealthThreshold,
		EnableSNI:                    hc.Config.EnableSNI,
		MeasureLatency:               hc.Config.MeasureLatency,
		Disabled:                     hc.Config.Disabled,
		Inverted:                     hc.Config.Inverted,
		ChildHealthChecks:            hc.Config.ChildHealthChecks,
		Regions:                      hc.Config.Regions,
	}

	if hc.Config.AlarmIdentifier != nil {
		cfg.AlarmIdentifier = &xmlAlarmIdentifier{
			Name:   hc.Config.AlarmIdentifier.Name,
			Region: hc.Config.AlarmIdentifier.Region,
		}
	}

	return xmlHealthCheck{
		ID:                 hc.ID,
		CallerReference:    hc.CallerReference,
		Config:             cfg,
		HealthCheckVersion: hc.Version,
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

func (h *Handler) createHealthCheck(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateHealthCheckRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	cfg := HealthCheckConfig{
		IPAddress:                    req.Config.IPAddress,
		FullyQualifiedDomainName:     req.Config.FullyQualifiedDomainName,
		ResourcePath:                 req.Config.ResourcePath,
		SearchString:                 req.Config.SearchString,
		InsufficientDataHealthStatus: req.Config.InsufficientDataHealthStatus,
		RoutingControlArn:            req.Config.RoutingControlArn,
		Type:                         HealthCheckType(req.Config.Type),
		Port:                         req.Config.Port,
		RequestInterval:              req.Config.RequestInterval,
		FailureThreshold:             req.Config.FailureThreshold,
		HealthThreshold:              req.Config.HealthThreshold,
		EnableSNI:                    req.Config.EnableSNI,
		MeasureLatency:               req.Config.MeasureLatency,
		Disabled:                     req.Config.Disabled,
		Inverted:                     req.Config.Inverted,
		ChildHealthChecks:            req.Config.ChildHealthChecks,
		Regions:                      req.Config.Regions,
	}

	if req.Config.AlarmIdentifier != nil {
		cfg.AlarmIdentifier = &AlarmIdentifier{
			Name:   req.Config.AlarmIdentifier.Name,
			Region: req.Config.AlarmIdentifier.Region,
		}
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

	// Release any handler-level tags for this health check so the tags map
	// cannot retain entries for resources that no longer exist.
	h.deleteTagsForResource(id)

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteHealthCheck", "id", id)

	return writeXML(c, http.StatusOK, xmlDeleteHealthCheckResponse{Xmlns: route53Namespace})
}

// mergeHealthCheckUpdateStrings merges the request's non-empty string fields
// into cfg. A field left empty on the wire means "leave unchanged" — Route 53
// UpdateHealthCheck has no separate "clear this field" signal for these.
func mergeHealthCheckUpdateStrings(cfg HealthCheckConfig, req xmlUpdateHealthCheckRequest) HealthCheckConfig {
	if req.IPAddress != "" {
		cfg.IPAddress = req.IPAddress
	}

	if req.FullyQualifiedDomainName != "" {
		cfg.FullyQualifiedDomainName = req.FullyQualifiedDomainName
	}

	if req.ResourcePath != "" {
		cfg.ResourcePath = req.ResourcePath
	}

	if req.SearchString != "" {
		cfg.SearchString = req.SearchString
	}

	if req.InsufficientDataHealthStatus != "" {
		cfg.InsufficientDataHealthStatus = req.InsufficientDataHealthStatus
	}

	if req.RoutingControlArn != "" {
		cfg.RoutingControlArn = req.RoutingControlArn
	}

	return cfg
}

// mergeHealthCheckUpdateNumeric merges the request's non-zero numeric fields
// into cfg, matching the same "zero means unchanged" wire convention as
// mergeHealthCheckUpdateStrings.
func mergeHealthCheckUpdateNumeric(cfg HealthCheckConfig, req xmlUpdateHealthCheckRequest) HealthCheckConfig {
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

	return cfg
}

// mergeHealthCheckUpdateFlags merges the request's boolean fields into cfg.
// Inverted is a pointer (false is a meaningful explicit value, distinct from
// "omitted"); the others are plain bools that can only ever turn a flag on
// through this merge, matching real AWS's UpdateHealthCheck wire semantics
// for these fields.
func mergeHealthCheckUpdateFlags(cfg HealthCheckConfig, req xmlUpdateHealthCheckRequest) HealthCheckConfig {
	if req.Inverted != nil {
		cfg.Inverted = *req.Inverted
	}

	if req.EnableSNI {
		cfg.EnableSNI = true
	}

	if req.MeasureLatency {
		cfg.MeasureLatency = true
	}

	if req.Disabled {
		cfg.Disabled = true
	}

	return cfg
}

// mergeHealthCheckUpdateCollections merges the request's list/struct fields
// into cfg.
func mergeHealthCheckUpdateCollections(cfg HealthCheckConfig, req xmlUpdateHealthCheckRequest) HealthCheckConfig {
	if len(req.Regions) > 0 {
		cfg.Regions = req.Regions
	}

	if len(req.ChildHealthChecks) > 0 {
		cfg.ChildHealthChecks = req.ChildHealthChecks
	}

	if req.AlarmIdentifier != nil {
		cfg.AlarmIdentifier = &AlarmIdentifier{
			Name:   req.AlarmIdentifier.Name,
			Region: req.AlarmIdentifier.Region,
		}
	}

	return cfg
}

// mergeHealthCheckUpdate merges every non-zero field UpdateHealthCheck's
// request carries into the health check's existing config, leaving fields
// the request omitted untouched.
func mergeHealthCheckUpdate(cfg HealthCheckConfig, req xmlUpdateHealthCheckRequest) HealthCheckConfig {
	cfg = mergeHealthCheckUpdateStrings(cfg, req)
	cfg = mergeHealthCheckUpdateNumeric(cfg, req)
	cfg = mergeHealthCheckUpdateFlags(cfg, req)
	cfg = mergeHealthCheckUpdateCollections(cfg, req)

	return cfg
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
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	existing, err := h.Backend.GetHealthCheck(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	cfg := mergeHealthCheckUpdate(existing.Config, req)

	hc, err := h.Backend.UpdateHealthCheck(id, cfg, req.HealthCheckVersion)
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

	hc, hcErr := h.Backend.GetHealthCheck(id)
	observerRegions := []string{defaultRegion, regionUSWest2, regionEUWest1}
	if hcErr == nil && len(hc.Config.Regions) > 0 {
		observerRegions = hc.Config.Regions
	}

	checkedAt := time.Now()
	observations := make([]xmlHealthCheckObservation, 0, len(observerRegions))

	for _, region := range observerRegions {
		obsStatus := status
		if hcErr == nil && hc.Config.Inverted {
			if obsStatus == defaultHealthStatus {
				obsStatus = healthUnhealthy
			} else {
				obsStatus = defaultHealthStatus
			}
		}

		obs := xmlHealthCheckObservation{
			Region:    region,
			IPAddress: "0.0.0.0",
		}
		obs.StatusReport.Status = obsStatus
		obs.StatusReport.CheckedTime = checkedAt
		observations = append(observations, obs)
	}

	return writeXML(c, http.StatusOK, xmlGetHealthCheckStatusResponse{
		Xmlns:                   route53Namespace,
		HealthCheckObservations: observations,
	})
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
