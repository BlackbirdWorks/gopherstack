package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

// describeRCNsResultXML is the XML envelope for DescribeReservedCacheNodes responses.
type describeRCNsResultXML struct {
	XMLName            xml.Name `xml:"DescribeReservedCacheNodesResponse"`
	Xmlns              string   `xml:"xmlns,attr"`
	Marker             string   `xml:"DescribeReservedCacheNodesResult>Marker,omitempty"`
	ReservedCacheNodes struct {
		ReservedCacheNode []reservedCacheNodeXML `xml:"ReservedCacheNode"`
	} `xml:"DescribeReservedCacheNodesResult>ReservedCacheNodes"`
}

type reservedCacheNodeXML struct {
	ReservationID       string  `xml:"ReservationId,omitempty"`
	ReservedCacheNodeID string  `xml:"ReservedCacheNodeId"`
	ARN                 string  `xml:"ReservationARN,omitempty"`
	CacheNodeType       string  `xml:"CacheNodeType"`
	ProductDescription  string  `xml:"ProductDescription"`
	OfferingType        string  `xml:"OfferingType"`
	State               string  `xml:"State"`
	OfferingID          string  `xml:"ReservedCacheNodesOfferingId"`
	StartTime           string  `xml:"StartTime,omitempty"`
	FixedPrice          float64 `xml:"FixedPrice"`
	UsagePrice          float64 `xml:"UsagePrice"`
	Duration            int32   `xml:"Duration"`
	CacheNodeCount      int32   `xml:"CacheNodeCount"`
}

func reservedCacheNodeToXML(rcn *ReservedCacheNode) reservedCacheNodeXML {
	startTime := ""
	if !rcn.StartTime.IsZero() {
		startTime = rcn.StartTime.UTC().Format(time.RFC3339)
	}

	return reservedCacheNodeXML{
		ReservationID:       rcn.ReservationID,
		ReservedCacheNodeID: rcn.ReservedCacheNodeID,
		ARN:                 rcn.ARN,
		CacheNodeType:       rcn.CacheNodeType,
		Duration:            rcn.Duration,
		FixedPrice:          rcn.FixedPrice,
		UsagePrice:          rcn.UsagePrice,
		CacheNodeCount:      rcn.CacheNodeCount,
		ProductDescription:  rcn.ProductDescription,
		OfferingType:        rcn.OfferingType,
		State:               rcn.State,
		OfferingID:          rcn.OfferingID,
		StartTime:           startTime,
	}
}

type reservedCacheNodesOfferingXML struct {
	OfferingID         string  `xml:"ReservedCacheNodesOfferingId"`
	CacheNodeType      string  `xml:"CacheNodeType"`
	ProductDescription string  `xml:"ProductDescription"`
	OfferingType       string  `xml:"OfferingType"`
	FixedPrice         float64 `xml:"FixedPrice"`
	UsagePrice         float64 `xml:"UsagePrice"`
	Duration           int32   `xml:"Duration"`
}

func (h *Handler) describeReservedCacheNodes(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("ReservedCacheNodeId")
	cacheNodeType := form.Get("CacheNodeType")
	offeringType := form.Get("OfferingType")
	duration := form.Get("Duration")
	productDescription := form.Get("ProductDescription")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	p, err := h.Backend.DescribeReservedCacheNodes(
		ctx, id, cacheNodeType, offeringType, duration, productDescription, marker, maxRecords,
	)
	if err != nil {
		if errors.Is(err, ErrReservedCacheNodeNotFound) {
			return xmlError(c, http.StatusNotFound, "ReservedCacheNodeNotFound", "Reserved cache node not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeRCNsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.ReservedCacheNodes.ReservedCacheNode = append(
			res.ReservedCacheNodes.ReservedCacheNode,
			reservedCacheNodeToXML(&p.Data[i]),
		)
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) describeReservedCacheNodesOfferings(ctx context.Context, c *echo.Context, form url.Values) error {
	offeringID := form.Get("ReservedCacheNodesOfferingId")
	cacheNodeType := form.Get("CacheNodeType")
	offeringType := form.Get("OfferingType")
	duration := form.Get("Duration")
	productDescription := form.Get("ProductDescription")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	p, err := h.Backend.DescribeReservedCacheNodesOfferings(
		ctx,
		offeringID,
		cacheNodeType,
		offeringType,
		duration,
		productDescription,
		marker,
		maxRecords,
	)
	if err != nil {
		if errors.Is(err, ErrReservedCacheNodesOfferingNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"ReservedCacheNodesOfferingNotFound",
				"Reserved cache nodes offering not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]reservedCacheNodesOfferingXML, 0, len(p.Data))
	for _, o := range p.Data {
		items = append(items, reservedCacheNodesOfferingXML(o))
	}

	type offeringsListXML struct {
		ReservedCacheNodesOffering []reservedCacheNodesOfferingXML `xml:"ReservedCacheNodesOffering"`
	}

	type result struct {
		XMLName   xml.Name         `xml:"DescribeReservedCacheNodesOfferingsResponse"`
		Xmlns     string           `xml:"xmlns,attr"`
		Marker    string           `xml:"DescribeReservedCacheNodesOfferingsResult>Marker,omitempty"`
		Offerings offeringsListXML `xml:"DescribeReservedCacheNodesOfferingsResult>ReservedCacheNodesOfferings"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:     elasticacheNS,
		Marker:    p.Next,
		Offerings: offeringsListXML{ReservedCacheNodesOffering: items},
	})
}

func (h *Handler) purchaseReservedCacheNodesOffering(ctx context.Context, c *echo.Context, form url.Values) error {
	offeringID := form.Get("ReservedCacheNodesOfferingId")
	reservedCacheNodeID := form.Get("ReservedCacheNodeId")
	count, _ := strconv.ParseInt(form.Get("CacheNodeCount"), 10, 32)

	rcn, err := h.Backend.PurchaseReservedCacheNodesOffering(ctx, offeringID, reservedCacheNodeID, int32(count))
	if err != nil {
		if errors.Is(err, ErrReservedCacheNodesOfferingNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"ReservedCacheNodesOfferingNotFound",
				"Reserved cache nodes offering not found",
			)
		}

		if errors.Is(err, ErrReservedCacheNodeAlreadyExists) {
			return xmlError(c, http.StatusNotFound, "ReservedCacheNodeAlreadyExists", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName           xml.Name             `xml:"PurchaseReservedCacheNodesOfferingResponse"`
		Xmlns             string               `xml:"xmlns,attr"`
		ReservedCacheNode reservedCacheNodeXML `xml:"PurchaseReservedCacheNodesOfferingResult>ReservedCacheNode"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:             elasticacheNS,
		ReservedCacheNode: reservedCacheNodeToXML(rcn),
	})
}
