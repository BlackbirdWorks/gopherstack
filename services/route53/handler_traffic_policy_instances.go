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
	XMLName                xml.Name                   `xml:"ListTrafficPolicyInstancesResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	MaxItems               string                     `xml:"MaxItems"`
	TrafficPolicyInstances []xmlTrafficPolicyInstance `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated            bool                       `xml:"IsTruncated"`
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

	instances, err := h.Backend.ListTrafficPolicyInstances()
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ListTrafficPolicyInstances", "count", len(instances))

	xmlInstances := make([]xmlTrafficPolicyInstance, 0, len(instances))
	for _, inst := range instances {
		xmlInstances = append(xmlInstances, toXMLTPInstance(inst))
	}

	return writeXML(c, http.StatusOK, xmlListTrafficPolicyInstancesResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: xmlInstances,
		IsTruncated:            false,
		MaxItems:               "100",
	})
}

func (h *Handler) getTrafficPolicyInstanceCount(c *echo.Context) error {
	ctx := c.Request().Context()

	instances, err := h.Backend.ListTrafficPolicyInstances()
	if err != nil {
		return handleBackendError(c, err)
	}

	count := int32(len(instances)) //nolint:gosec // instance count fits in int32

	logger.Load(ctx).DebugContext(ctx, "Route53 GetTrafficPolicyInstanceCount", "count", count)

	return writeXML(c, http.StatusOK, xmlGetTPInstanceCountResponse{
		Xmlns:                      route53Namespace,
		TrafficPolicyInstanceCount: count,
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
