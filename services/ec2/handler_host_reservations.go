package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// ---- Handler registration ----

func registerHostReservationOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DescribeHostReservationOfferings"] = h.handleDescribeHostReservationOfferings
	ops["GetHostReservationPurchasePreview"] = h.handleGetHostReservationPurchasePreview
	ops["PurchaseHostReservation"] = h.handlePurchaseHostReservation
	ops["DescribeHostReservations"] = h.handleDescribeHostReservations
	ops["ReleaseHosts"] = h.handleReleaseHosts
}

func hostReservationSupportedOperations() []string {
	return []string{
		"DescribeHostReservationOfferings",
		"GetHostReservationPurchasePreview",
		"PurchaseHostReservation",
		"DescribeHostReservations",
		"ReleaseHosts",
	}
}

// ---- XML entity types ----

type hostOfferingItem struct {
	OfferingID     string `xml:"offeringId"`
	InstanceFamily string `xml:"instanceFamily"`
	PaymentOption  string `xml:"paymentOption"`
	CurrencyCode   string `xml:"currencyCode"`
	HourlyPrice    string `xml:"hourlyPrice"`
	UpfrontPrice   string `xml:"upfrontPrice"`
	Duration       int32  `xml:"duration"`
}

func hostOfferingToItem(o HostOffering) hostOfferingItem {
	return hostOfferingItem{
		OfferingID:     o.OfferingID,
		InstanceFamily: o.InstanceFamily,
		PaymentOption:  o.PaymentOption,
		CurrencyCode:   o.CurrencyCode,
		HourlyPrice:    o.HourlyPrice,
		UpfrontPrice:   o.UpfrontPrice,
		Duration:       o.Duration,
	}
}

type hostReservationPurchaseItem struct {
	HostReservationID string   `xml:"hostReservationId,omitempty"`
	InstanceFamily    string   `xml:"instanceFamily"`
	PaymentOption     string   `xml:"paymentOption"`
	CurrencyCode      string   `xml:"currencyCode"`
	HourlyPrice       string   `xml:"hourlyPrice"`
	UpfrontPrice      string   `xml:"upfrontPrice"`
	HostIDSet         []string `xml:"hostIdSet>item"`
	Duration          int32    `xml:"duration"`
}

func hostReservationPurchaseToItem(p HostReservationPurchase) hostReservationPurchaseItem {
	return hostReservationPurchaseItem(p)
}

type hostReservationItem struct {
	State             string          `xml:"state"`
	InstanceFamily    string          `xml:"instanceFamily"`
	PaymentOption     string          `xml:"paymentOption"`
	CurrencyCode      string          `xml:"currencyCode"`
	HourlyPrice       string          `xml:"hourlyPrice"`
	UpfrontPrice      string          `xml:"upfrontPrice"`
	HostReservationID string          `xml:"hostReservationId"`
	Start             string          `xml:"start,omitempty"`
	End               string          `xml:"end,omitempty"`
	HostIDSet         []string        `xml:"hostIdSet>item"`
	TagSet            []simpleTagItem `xml:"tagSet>item"`
	Duration          int32           `xml:"duration"`
	Count             int32           `xml:"count"`
}

func hostReservationToItem(hr *HostReservation, tags map[string]string) hostReservationItem {
	item := hostReservationItem{
		HostReservationID: hr.HostReservationID,
		InstanceFamily:    hr.InstanceFamily,
		PaymentOption:     hr.PaymentOption,
		CurrencyCode:      hr.CurrencyCode,
		HourlyPrice:       hr.HourlyPrice,
		UpfrontPrice:      hr.UpfrontPrice,
		State:             hr.State,
		Duration:          hr.Duration,
		Count:             hr.Count,
		HostIDSet:         hr.HostIDSet,
		TagSet:            tagItemsFromMap(tags),
	}
	if !hr.Start.IsZero() {
		item.Start = hr.Start.Format(time.RFC3339)
	}

	if !hr.End.IsZero() {
		item.End = hr.End.Format(time.RFC3339)
	}

	return item
}

type unsuccessfulItemErrorXML struct {
	Code    string `xml:"code"`
	Message string `xml:"message"`
}

type unsuccessfulItemXML struct {
	ResourceID string                   `xml:"resourceId"`
	Error      unsuccessfulItemErrorXML `xml:"error"`
}

// ---- XML response types ----

type describeHostReservationOfferingsResponse struct {
	XMLName     xml.Name           `xml:"DescribeHostReservationOfferingsResponse"`
	Xmlns       string             `xml:"xmlns,attr"`
	RequestID   string             `xml:"requestId"`
	NextToken   string             `xml:"nextToken,omitempty"`
	OfferingSet []hostOfferingItem `xml:"offeringSet>item"`
}

type getHostReservationPurchasePreviewResponse struct {
	XMLName           xml.Name                      `xml:"GetHostReservationPurchasePreviewResponse"`
	Xmlns             string                        `xml:"xmlns,attr"`
	RequestID         string                        `xml:"requestId"`
	CurrencyCode      string                        `xml:"currencyCode"`
	TotalHourlyPrice  string                        `xml:"totalHourlyPrice"`
	TotalUpfrontPrice string                        `xml:"totalUpfrontPrice"`
	Purchase          []hostReservationPurchaseItem `xml:"purchase>item"`
}

type purchaseHostReservationResponse struct {
	XMLName           xml.Name                      `xml:"PurchaseHostReservationResponse"`
	Xmlns             string                        `xml:"xmlns,attr"`
	RequestID         string                        `xml:"requestId"`
	ClientToken       string                        `xml:"clientToken,omitempty"`
	CurrencyCode      string                        `xml:"currencyCode"`
	TotalHourlyPrice  string                        `xml:"totalHourlyPrice"`
	TotalUpfrontPrice string                        `xml:"totalUpfrontPrice"`
	Purchase          []hostReservationPurchaseItem `xml:"purchase>item"`
}

type describeHostReservationsResponse struct {
	XMLName            xml.Name              `xml:"DescribeHostReservationsResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	RequestID          string                `xml:"requestId"`
	NextToken          string                `xml:"nextToken,omitempty"`
	HostReservationSet []hostReservationItem `xml:"hostReservationSet>item"`
}

type releaseHostsResponse struct {
	XMLName      xml.Name              `xml:"ReleaseHostsResponse"`
	Xmlns        string                `xml:"xmlns,attr"`
	RequestID    string                `xml:"requestId"`
	Successful   []string              `xml:"successful>item"`
	Unsuccessful []unsuccessfulItemXML `xml:"unsuccessful>item"`
}

// ---- handlers ----

func (h *Handler) handleDescribeHostReservationOfferings(vals url.Values, reqID string) (any, error) {
	var minDuration, maxDuration int64

	if v := vals.Get("MinDuration"); v != "" {
		minDuration, _ = strconv.ParseInt(v, 10, 32)
	}

	if v := vals.Get("MaxDuration"); v != "" {
		maxDuration, _ = strconv.ParseInt(v, 10, 32)
	}

	offerings := h.Backend.DescribeHostReservationOfferings(
		vals.Get("OfferingId"),
		int32(minDuration),
		int32(maxDuration),
	)

	resp := &describeHostReservationOfferingsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, o := range offerings {
		resp.OfferingSet = append(resp.OfferingSet, hostOfferingToItem(o))
	}

	return resp, nil
}

func (h *Handler) handleGetHostReservationPurchasePreview(vals url.Values, reqID string) (any, error) {
	hostIDs := parseMemberList(vals, "HostIdSet")

	preview, err := h.Backend.GetHostReservationPurchasePreview(hostIDs, vals.Get("OfferingId"))
	if err != nil {
		return nil, err
	}

	resp := &getHostReservationPurchasePreviewResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		CurrencyCode:      preview.CurrencyCode,
		TotalHourlyPrice:  preview.TotalHourlyPrice,
		TotalUpfrontPrice: preview.TotalUpfrontPrice,
	}
	for _, p := range preview.Purchases {
		resp.Purchase = append(resp.Purchase, hostReservationPurchaseToItem(p))
	}

	return resp, nil
}

func (h *Handler) handlePurchaseHostReservation(vals url.Values, reqID string) (any, error) {
	hostIDs := parseMemberList(vals, "HostIdSet")
	tags := parseTagSpecification(vals, "host-reservation")

	preview, err := h.Backend.PurchaseHostReservation(hostIDs, vals.Get("OfferingId"), tags)
	if err != nil {
		return nil, err
	}

	resp := &purchaseHostReservationResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		ClientToken:       vals.Get("ClientToken"),
		CurrencyCode:      preview.CurrencyCode,
		TotalHourlyPrice:  preview.TotalHourlyPrice,
		TotalUpfrontPrice: preview.TotalUpfrontPrice,
	}
	for _, p := range preview.Purchases {
		resp.Purchase = append(resp.Purchase, hostReservationPurchaseToItem(p))
	}

	return resp, nil
}

func (h *Handler) handleDescribeHostReservations(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "HostReservationIdSet")
	hrs := h.Backend.DescribeHostReservations(ids)

	resp := &describeHostReservationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, hr := range hrs {
		resp.HostReservationSet = append(
			resp.HostReservationSet, hostReservationToItem(hr, h.Backend.TagsForResource(hr.HostReservationID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleReleaseHosts(vals url.Values, reqID string) (any, error) {
	hostIDs := parseMemberList(vals, "HostId")

	successful, unsuccessful := h.Backend.ReleaseHosts(hostIDs)

	resp := &releaseHostsResponse{Xmlns: ec2XMLNS, RequestID: reqID, Successful: successful}
	for _, u := range unsuccessful {
		resp.Unsuccessful = append(resp.Unsuccessful, unsuccessfulItemXML{
			ResourceID: u.HostID,
			Error:      unsuccessfulItemErrorXML{Code: u.Code, Message: u.Message},
		})
	}

	return resp, nil
}
