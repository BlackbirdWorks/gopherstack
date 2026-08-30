package route53

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type xmlTrafficPolicy struct {
	XMLName  xml.Name `xml:"TrafficPolicy"`
	ID       string   `xml:"Id"`
	Name     string   `xml:"Name"`
	Document string   `xml:"Document,omitempty"`
	Comment  string   `xml:"Comment,omitempty"`
	Type     string   `xml:"Type"`
	Version  int32    `xml:"Version"`
}

type xmlCreateTrafficPolicyRequest struct {
	XMLName  xml.Name `xml:"CreateTrafficPolicyRequest"`
	Name     string   `xml:"Name"`
	Document string   `xml:"Document"`
	Comment  string   `xml:"Comment,omitempty"`
}

type xmlCreateTrafficPolicyResponse struct {
	XMLName       xml.Name         `xml:"CreateTrafficPolicyResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	TrafficPolicy xmlTrafficPolicy `xml:"TrafficPolicy"`
}

type xmlCreateTrafficPolicyVersionRequest struct {
	XMLName  xml.Name `xml:"CreateTrafficPolicyVersionRequest"`
	Document string   `xml:"Document"`
	Comment  string   `xml:"Comment,omitempty"`
}

type xmlCreateTrafficPolicyVersionResponse struct {
	XMLName       xml.Name         `xml:"CreateTrafficPolicyVersionResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	TrafficPolicy xmlTrafficPolicy `xml:"TrafficPolicy"`
}

type xmlListTrafficPoliciesResponse struct {
	XMLName               xml.Name                  `xml:"ListTrafficPoliciesResponse"`
	Xmlns                 string                    `xml:"xmlns,attr"`
	MaxItems              string                    `xml:"MaxItems"`
	TrafficPolicyIDMarker string                    `xml:"TrafficPolicyIdMarker"`
	TrafficPolicies       []xmlTrafficPolicySummary `xml:"TrafficPolicySummaries>TrafficPolicySummary"`
	IsTruncated           bool                      `xml:"IsTruncated"`
}

type xmlTrafficPolicySummary struct {
	ID                 string `xml:"Id"`
	Name               string `xml:"Name"`
	Type               string `xml:"Type"`
	LatestVersion      int32  `xml:"LatestVersion"`
	TrafficPolicyCount int32  `xml:"TrafficPolicyCount"`
}

type xmlListTrafficPolicyVersionsResponse struct {
	XMLName                    xml.Name           `xml:"ListTrafficPolicyVersionsResponse"`
	Xmlns                      string             `xml:"xmlns,attr"`
	MaxItems                   string             `xml:"MaxItems"`
	TrafficPolicyVersionMarker string             `xml:"TrafficPolicyVersionMarker"`
	TrafficPolicies            []xmlTrafficPolicy `xml:"TrafficPolicies>TrafficPolicy"`
	IsTruncated                bool               `xml:"IsTruncated"`
}

func (h *Handler) routeTrafficPolicyRoot(c *echo.Context, method string) error {
	if method == http.MethodPost {
		return h.createTrafficPolicy(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicy",
	)
}

func (h *Handler) routeTrafficPolicyVersion(c *echo.Context, path, method string) error {
	rest := strings.TrimPrefix(path, route53TrafficPolicyPrefix)
	// If rest contains a "/" it's /{id}/{version}
	if strings.Contains(rest, "/") {
		parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split id and version
		id := parts[0]
		versionStr := parts[1]

		version64, err := strconv.ParseInt(versionStr, 10, 32)
		if err != nil {
			return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid version number")
		}

		version := int32(version64)

		switch method {
		case http.MethodGet:
			return h.getTrafficPolicy(c, id, version)
		case http.MethodDelete:
			return h.deleteTrafficPolicy(c, id, version)
		case http.MethodPost:
			return h.updateTrafficPolicyComment(c, path)
		default:
			return xmlError(c, http.StatusNotFound, "NoSuchOperation",
				"unsupported method on traffic policy version")
		}
	}

	if method == http.MethodPost {
		return h.createTrafficPolicyVersion(c, path)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on traffic policy version",
	)
}

func (h *Handler) routeTrafficPoliciesRoot(c *echo.Context, method string) error {
	if method == http.MethodGet {
		return h.listTrafficPolicies(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicies",
	)
}

func (h *Handler) routeTrafficPoliciesVersions(c *echo.Context, path, method string) error {
	if method == http.MethodGet {
		// path is /2013-04-01/trafficpolicies/{Id}/versions
		id := strings.TrimPrefix(path, route53TrafficPoliciesPrefix)
		id = strings.TrimSuffix(id, "/versions")

		return h.listTrafficPolicyVersions(c, id)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on traffic policy versions",
	)
}

func toXMLTrafficPolicy(tp *TrafficPolicy) xmlTrafficPolicy {
	return xmlTrafficPolicy{
		ID:       tp.ID,
		Name:     tp.Name,
		Document: tp.Document,
		Comment:  tp.Comment,
		Type:     tp.Type,
		Version:  tp.Version,
	}
}

func (h *Handler) createTrafficPolicy(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateTrafficPolicyRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	tp, err := h.Backend.CreateTrafficPolicy(req.Name, req.Document, req.Comment)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateTrafficPolicy", "id", tp.ID)

	resp := xmlCreateTrafficPolicyResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: toXMLTrafficPolicy(tp),
	}

	c.Response().Header().Set("Location", "/2013-04-01/trafficpolicy/"+tp.ID+"/1")

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) createTrafficPolicyVersion(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := strings.TrimPrefix(path, route53TrafficPolicyPrefix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateTrafficPolicyVersionRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	tp, err := h.Backend.CreateTrafficPolicyVersion(id, req.Document, req.Comment)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 CreateTrafficPolicyVersion", "id", id, "version", tp.Version)

	return writeXML(c, http.StatusCreated, xmlCreateTrafficPolicyVersionResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: toXMLTrafficPolicy(tp),
	})
}

func (h *Handler) getTrafficPolicy(c *echo.Context, id string, version int32) error {
	ctx := c.Request().Context()

	tp, err := h.Backend.GetTrafficPolicy(id, version)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetTrafficPolicy", "id", id, "version", version)

	return writeXML(c, http.StatusOK, xmlCreateTrafficPolicyResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: toXMLTrafficPolicy(tp),
	})
}

func (h *Handler) deleteTrafficPolicy(c *echo.Context, id string, version int32) error {
	ctx := c.Request().Context()

	if err := h.Backend.DeleteTrafficPolicy(id, version); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteTrafficPolicy", "id", id, "version", version)

	return writeXML(c, http.StatusOK, struct {
		XMLName xml.Name `xml:"DeleteTrafficPolicyResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}{Xmlns: route53Namespace})
}

func (h *Handler) listTrafficPolicies(c *echo.Context) error {
	ctx := c.Request().Context()
	q := c.Request().URL.Query()
	// Wire query key is "trafficpolicyid", not "trafficpolicyidmarker"
	// (route53@v1.65.6 serializers.go's
	// awsRestxml_serializeOpHttpBindingsListTrafficPoliciesInput).
	marker := q.Get("trafficpolicyid")
	maxItems := route53DefaultMaxItems
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListTrafficPolicies(marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ListTrafficPolicies", "count", len(p.Data))

	summaries := make([]xmlTrafficPolicySummary, 0, len(p.Data))
	for _, tp := range p.Data {
		summaries = append(summaries, xmlTrafficPolicySummary{
			ID:                 tp.ID,
			Name:               tp.Name,
			Type:               tp.Type,
			LatestVersion:      tp.Version,
			TrafficPolicyCount: tp.VersionCount,
		})
	}

	return writeXML(c, http.StatusOK, xmlListTrafficPoliciesResponse{
		Xmlns:                 route53Namespace,
		TrafficPolicies:       summaries,
		IsTruncated:           p.Next != "",
		MaxItems:              strconv.Itoa(maxItems),
		TrafficPolicyIDMarker: p.Next,
	})
}

func (h *Handler) listTrafficPolicyVersions(c *echo.Context, id string) error {
	ctx := c.Request().Context()
	q := c.Request().URL.Query()
	// Wire query key is "trafficpolicyversion", not
	// "trafficpolicyversionmarker" (route53@v1.65.6 serializers.go's
	// awsRestxml_serializeOpHttpBindingsListTrafficPolicyVersionsInput).
	marker := q.Get("trafficpolicyversion")
	maxItems := route53DefaultMaxItems
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListTrafficPolicyVersions(id, marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ListTrafficPolicyVersions", "id", id, "count", len(p.Data))

	xmlPolicies := make([]xmlTrafficPolicy, 0, len(p.Data))
	for _, v := range p.Data {
		xmlPolicies = append(xmlPolicies, toXMLTrafficPolicy(v))
	}

	return writeXML(c, http.StatusOK, xmlListTrafficPolicyVersionsResponse{
		Xmlns:                      route53Namespace,
		TrafficPolicies:            xmlPolicies,
		IsTruncated:                p.Next != "",
		MaxItems:                   strconv.Itoa(maxItems),
		TrafficPolicyVersionMarker: p.Next,
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
