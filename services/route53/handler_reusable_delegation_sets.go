package route53

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type xmlDelegationSetCreate struct {
	XMLName         xml.Name `xml:"CreateReusableDelegationSetRequest"`
	CallerReference string   `xml:"CallerReference"`
	HostedZoneID    string   `xml:"HostedZoneId,omitempty"`
}

type xmlReusableDelegationSetResponse struct {
	XMLName       xml.Name         `xml:"CreateReusableDelegationSetResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
}

func (h *Handler) routeDelegationSetRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createReusableDelegationSet(c)
	case http.MethodGet:
		return h.listReusableDelegationSets(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation", "unsupported method on /delegationset")
	}
}

func (h *Handler) createReusableDelegationSet(c *echo.Context) error {
	ctx := c.Request().Context()

	var req xmlDelegationSetCreate
	if ok, err := readXMLRequest(c, &req); !ok {
		return err
	}

	ds, err := h.Backend.CreateReusableDelegationSet(req.CallerReference, req.HostedZoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateReusableDelegationSet", "id", ds.ID)

	resp := xmlReusableDelegationSetResponse{
		Xmlns: route53Namespace,
		DelegationSet: xmlDelegationSet{
			ID:              ds.ID,
			CallerReference: ds.CallerReference,
			NameServers:     ds.NameServers,
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01"+ds.ID)

	return writeXML(c, http.StatusCreated, resp)
}

type reusableDSLimitResponse struct {
	XMLName xml.Name `xml:"GetReusableDelegationSetLimitResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Limit   xmlLimit `xml:"Limit"`
	Count   int      `xml:"Count"`
}

func (h *Handler) getReusableDelegationSetLimit(c *echo.Context, path string) error {
	parts := strings.TrimPrefix(path, route53ReusableDSLimitPrefix)
	dsID := parts
	limitType := ""
	if before, after, ok := strings.Cut(parts, "/"); ok {
		dsID = before
		limitType = after
	}
	dsID = normaliseDelegationSetID(dsID)

	count, err := h.Backend.CountZonesByReusableDelegationSet(dsID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, reusableDSLimitResponse{
		Xmlns: route53Namespace,
		Limit: xmlLimit{Type: limitType, Value: defaultDSLimit},
		Count: count,
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
			ID:              ds.ID,
			CallerReference: ds.CallerReference,
			NameServers:     ds.NameServers,
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

// listReusableDelegationSets lists all reusable delegation sets.
type listReusableDSResponse struct {
	XMLName        xml.Name           `xml:"ListReusableDelegationSetsResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	Marker         string             `xml:"Marker"`
	MaxItems       string             `xml:"MaxItems"`
	NextMarker     string             `xml:"NextMarker,omitempty"`
	DelegationSets []xmlDelegationSet `xml:"DelegationSets>DelegationSet"`
	IsTruncated    bool               `xml:"IsTruncated"`
}

func (h *Handler) listReusableDelegationSets(c *echo.Context) error {
	q := c.Request().URL.Query()
	marker := q.Get("marker")
	maxItems := route53DefaultMaxItems
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListReusableDelegationSets(marker, maxItems)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	items := make([]xmlDelegationSet, 0, len(p.Data))
	for _, ds := range p.Data {
		items = append(items, xmlDelegationSet{
			ID:              ds.ID,
			CallerReference: ds.CallerReference,
			NameServers:     ds.NameServers,
		})
	}

	return writeXML(c, http.StatusOK, listReusableDSResponse{
		Xmlns:          route53Namespace,
		Marker:         marker,
		DelegationSets: items,
		IsTruncated:    p.Next != "",
		NextMarker:     p.Next,
		MaxItems:       strconv.Itoa(maxItems),
	})
}
