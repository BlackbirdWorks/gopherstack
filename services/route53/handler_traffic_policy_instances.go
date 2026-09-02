package route53

import (
	"encoding/xml"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type xmlTrafficPolicyInstance struct {
	XMLName              xml.Name `xml:"TrafficPolicyInstance"`
	ID                   string   `xml:"Id"`
	HostedZoneID         string   `xml:"HostedZoneId"`
	Name                 string   `xml:"Name"`
	TrafficPolicyID      string   `xml:"TrafficPolicyId"`
	TrafficPolicyType    string   `xml:"TrafficPolicyType"`
	State                string   `xml:"State"`
	TTL                  int64    `xml:"TTL"`
	TrafficPolicyVersion int32    `xml:"TrafficPolicyVersion"`
}

type xmlCreateTrafficPolicyInstanceRequest struct {
	XMLName              xml.Name `xml:"CreateTrafficPolicyInstanceRequest"`
	HostedZoneID         string   `xml:"HostedZoneId"`
	Name                 string   `xml:"Name"`
	TrafficPolicyID      string   `xml:"TrafficPolicyId"`
	TrafficPolicyVersion int32    `xml:"TrafficPolicyVersion"`
	TTL                  int64    `xml:"TTL"`
}

type xmlCreateTrafficPolicyInstanceResponse struct {
	XMLName               xml.Name                 `xml:"CreateTrafficPolicyInstanceResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	TrafficPolicyInstance xmlTrafficPolicyInstance `xml:"TrafficPolicyInstance"`
}

type xmlListTrafficPolicyInstancesResponse struct {
	XMLName                         xml.Name                   `xml:"ListTrafficPolicyInstancesResponse"`
	Xmlns                           string                     `xml:"xmlns,attr"`
	MaxItems                        string                     `xml:"MaxItems"`
	HostedZoneIDMarker              string                     `xml:"HostedZoneIdMarker,omitempty"`
	TrafficPolicyInstanceNameMarker string                     `xml:"TrafficPolicyInstanceNameMarker,omitempty"`
	TrafficPolicyInstanceTypeMarker string                     `xml:"TrafficPolicyInstanceTypeMarker,omitempty"`
	TrafficPolicyInstances          []xmlTrafficPolicyInstance `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated                     bool                       `xml:"IsTruncated"`
}

type xmlGetTPInstanceCountResponse struct {
	XMLName                    xml.Name `xml:"GetTrafficPolicyInstanceCountResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	TrafficPolicyInstanceCount int32    `xml:"TrafficPolicyInstanceCount"`
}

func (h *Handler) routeTPInstanceRoot(c *echo.Context, method string) error {
	if method == http.MethodPost {
		return h.createTrafficPolicyInstance(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicyinstance",
	)
}

func (h *Handler) routeTPInstancesRoot(c *echo.Context, method string) error {
	if method == http.MethodGet {
		return h.listTrafficPolicyInstances(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicyinstances",
	)
}

func (h *Handler) routeTPInstanceCount(c *echo.Context, method string) error {
	if method == http.MethodGet {
		return h.getTrafficPolicyInstanceCount(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicyinstancecount",
	)
}

func (h *Handler) routeTPInstance(c *echo.Context, path, method string) error {
	id := strings.TrimPrefix(path, route53TPInstancePrefix)

	switch method {
	case http.MethodGet:
		return h.getTrafficPolicyInstance(c, id)
	case http.MethodDelete:
		return h.deleteTrafficPolicyInstance(c, id)
	case http.MethodPost:
		return h.updateTrafficPolicyInstance(c, path)
	default:
		return xmlError(
			c,
			http.StatusNotFound,
			"NoSuchOperation",
			"unsupported method on traffic policy instance",
		)
	}
}

func toXMLTPInstance(inst *TrafficPolicyInstance) xmlTrafficPolicyInstance {
	return xmlTrafficPolicyInstance{
		ID:                   inst.ID,
		HostedZoneID:         inst.HostedZoneID,
		Name:                 inst.Name,
		TrafficPolicyID:      inst.TrafficPolicyID,
		TrafficPolicyVersion: inst.TrafficPolicyVersion,
		TrafficPolicyType:    inst.TrafficPolicyType,
		TTL:                  inst.TTL,
		State:                inst.State,
	}
}

func (h *Handler) createTrafficPolicyInstance(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateTrafficPolicyInstanceRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	inst, err := h.Backend.CreateTrafficPolicyInstance(
		req.HostedZoneID,
		req.Name,
		req.TrafficPolicyID,
		req.TrafficPolicyVersion,
		req.TTL,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateTrafficPolicyInstance", "id", inst.ID)

	resp := xmlCreateTrafficPolicyInstanceResponse{
		Xmlns:                 route53Namespace,
		TrafficPolicyInstance: toXMLTPInstance(inst),
	}

	c.Response().Header().Set("Location", "/2013-04-01/trafficpolicyinstance/"+inst.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) getTrafficPolicyInstance(c *echo.Context, id string) error {
	ctx := c.Request().Context()

	inst, err := h.Backend.GetTrafficPolicyInstance(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetTrafficPolicyInstance", "id", id)

	return writeXML(c, http.StatusOK, xmlCreateTrafficPolicyInstanceResponse{
		Xmlns:                 route53Namespace,
		TrafficPolicyInstance: toXMLTPInstance(inst),
	})
}

func (h *Handler) deleteTrafficPolicyInstance(c *echo.Context, id string) error {
	ctx := c.Request().Context()

	if err := h.Backend.DeleteTrafficPolicyInstance(id); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteTrafficPolicyInstance", "id", id)

	return writeXML(c, http.StatusOK, struct {
		XMLName xml.Name `xml:"DeleteTrafficPolicyInstanceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}{Xmlns: route53Namespace})
}

func (h *Handler) listTrafficPolicyInstances(c *echo.Context) error {
	ctx := c.Request().Context()
	q := c.Request().URL.Query()
	// route53@v1.65.6 serializers.go's
	// awsRestxml_serializeOpHttpBindingsListTrafficPolicyInstancesInput binds
	// HostedZoneIdMarker to "hostedzoneid"; carried here as the single
	// opaque pagination token (see ListTrafficPolicyInstances's backend doc
	// comment), TrafficPolicyInstanceName/TypeMarker are decorative only.
	marker := q.Get("hostedzoneid")
	maxItems := route53DefaultMaxItems
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListTrafficPolicyInstances(marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ListTrafficPolicyInstances", "count", len(p.Data))

	xmlInstances := make([]xmlTrafficPolicyInstance, 0, len(p.Data))
	for _, inst := range p.Data {
		xmlInstances = append(xmlInstances, toXMLTPInstance(inst))
	}

	return writeXML(c, http.StatusOK, xmlListTrafficPolicyInstancesResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: xmlInstances,
		IsTruncated:            p.Next != "",
		MaxItems:               strconv.Itoa(maxItems),
		HostedZoneIDMarker:     p.Next,
	})
}

func (h *Handler) getTrafficPolicyInstanceCount(c *echo.Context) error {
	ctx := c.Request().Context()

	instances, err := h.Backend.ListTrafficPolicyInstances("", math.MaxInt32)
	if err != nil {
		return handleBackendError(c, err)
	}

	count := int32(len(instances.Data)) //nolint:gosec // instance count fits in int32

	logger.Load(ctx).DebugContext(ctx, "Route53 GetTrafficPolicyInstanceCount", "count", count)

	return writeXML(c, http.StatusOK, xmlGetTPInstanceCountResponse{
		Xmlns:                      route53Namespace,
		TrafficPolicyInstanceCount: count,
	})
}

type listTPInstancesByHZResponse struct {
	XMLName                         xml.Name                   `xml:"ListTrafficPolicyInstancesByHostedZoneResponse"`
	Xmlns                           string                     `xml:"xmlns,attr"`
	MaxItems                        string                     `xml:"MaxItems"`
	TrafficPolicyInstanceNameMarker string                     `xml:"TrafficPolicyInstanceNameMarker,omitempty"`
	TrafficPolicyInstanceTypeMarker string                     `xml:"TrafficPolicyInstanceTypeMarker,omitempty"`
	TrafficPolicyInstances          []xmlTrafficPolicyInstance `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated                     bool                       `xml:"IsTruncated"`
}

func (h *Handler) listTrafficPolicyInstancesByHostedZone(c *echo.Context) error {
	q := c.Request().URL.Query()
	// route53@v1.65.6 serializers.go's
	// awsRestxml_serializeOpHttpBindingsListTrafficPolicyInstancesByHostedZoneInput
	// binds HostedZoneId (the filter) to query key "id", not "hostedzoneid"
	// -- the previous "hostedzoneid" read always came back empty for a real
	// client, so this filter never matched any instance. No
	// HostedZoneIdMarker exists on this op (redundant with the fixed
	// HostedZoneId filter), so TrafficPolicyInstanceNameMarker carries the
	// opaque pagination token instead.
	hostedZoneID := q.Get("id")
	marker := q.Get("trafficpolicyinstancename")
	maxItems := route53DefaultMaxItems
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListTrafficPolicyInstancesByHostedZone(hostedZoneID, marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlInstances := make([]xmlTrafficPolicyInstance, 0, len(p.Data))
	for _, inst := range p.Data {
		xmlInstances = append(xmlInstances, toXMLTPInstance(inst))
	}

	return writeXML(c, http.StatusOK, listTPInstancesByHZResponse{
		Xmlns:                           route53Namespace,
		TrafficPolicyInstances:          xmlInstances,
		IsTruncated:                     p.Next != "",
		MaxItems:                        strconv.Itoa(maxItems),
		TrafficPolicyInstanceNameMarker: p.Next,
	})
}

type listTPInstancesByPolicyResponse struct {
	XMLName                xml.Name                   `xml:"ListTrafficPolicyInstancesByPolicyResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	MaxItems               string                     `xml:"MaxItems"`
	HostedZoneIDMarker     string                     `xml:"HostedZoneIdMarker,omitempty"`
	TrafficPolicyInstances []xmlTrafficPolicyInstance `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated            bool                       `xml:"IsTruncated"`
}

func (h *Handler) listTrafficPolicyInstancesByPolicy(c *echo.Context) error {
	q := c.Request().URL.Query()
	// route53@v1.65.6 serializers.go's
	// awsRestxml_serializeOpHttpBindingsListTrafficPolicyInstancesByPolicyInput
	// binds TrafficPolicyId to query key "id" and TrafficPolicyVersion to
	// "version" -- NOT "trafficpolicyid"/"trafficpolicyversion", which this
	// handler previously read; a real client's filter was always silently
	// ignored (both always empty/zero), so this op always returned nothing.
	// "hostedzoneid" is genuinely HostedZoneIdMarker here (the op's own
	// pagination cursor, distinct from the filter fixed above).
	tpID := q.Get("id")
	tpVersionStr := q.Get("version")
	marker := q.Get("hostedzoneid")
	maxItems := route53DefaultMaxItems
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	var tpVersion int32
	if tpVersionStr != "" {
		v, err := strconv.Atoi(tpVersionStr)
		if err != nil {
			return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid version")
		}

		if v < math.MinInt32 || v > math.MaxInt32 {
			return xmlError(c, http.StatusBadRequest, "InvalidInput", "version out of range")
		}

		tpVersion = int32(v) //nolint:gosec // bounds checked above
	}

	p, err := h.Backend.ListTrafficPolicyInstancesByPolicy(tpID, tpVersion, marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlInstances := make([]xmlTrafficPolicyInstance, 0, len(p.Data))
	for _, inst := range p.Data {
		xmlInstances = append(xmlInstances, toXMLTPInstance(inst))
	}

	return writeXML(c, http.StatusOK, listTPInstancesByPolicyResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: xmlInstances,
		IsTruncated:            p.Next != "",
		MaxItems:               strconv.Itoa(maxItems),
		HostedZoneIDMarker:     p.Next,
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
