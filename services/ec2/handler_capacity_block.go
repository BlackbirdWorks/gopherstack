package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// ---- Capacity Block ----

type capacityBlockOfferingItem struct {
	CapacityBlockOfferingID    string `xml:"capacityBlockOfferingId"`
	InstanceType               string `xml:"instanceType,omitempty"`
	AvailabilityZone           string `xml:"availabilityZone,omitempty"`
	Tenancy                    string `xml:"tenancy,omitempty"`
	CurrencyCode               string `xml:"currencyCode,omitempty"`
	UpfrontPrice               string `xml:"upfrontFee,omitempty"`
	StartDate                  string `xml:"startDate,omitempty"`
	EndDate                    string `xml:"endDate,omitempty"`
	CapacityBlockDurationHours int32  `xml:"capacityBlockDurationHours,omitempty"`
	InstanceCount              int32  `xml:"instanceCount,omitempty"`
}

func toCapacityBlockOfferingItem(o *CapacityBlockOffering) capacityBlockOfferingItem {
	return capacityBlockOfferingItem{
		CapacityBlockOfferingID:    o.CapacityBlockOfferingID,
		InstanceType:               o.InstanceType,
		AvailabilityZone:           o.AvailabilityZone,
		Tenancy:                    o.Tenancy,
		CurrencyCode:               o.CurrencyCode,
		UpfrontPrice:               o.UpfrontPrice,
		StartDate:                  o.StartDate.Format(time.RFC3339),
		EndDate:                    o.EndDate.Format(time.RFC3339),
		CapacityBlockDurationHours: o.CapacityBlockDurationHours,
		InstanceCount:              o.InstanceCount,
	}
}

type describeCapacityBlockOfferingsResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityBlockOfferingsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Offerings struct {
		Items []capacityBlockOfferingItem `xml:"item"`
	} `xml:"capacityBlockOfferingSet"`
}

func (h *Handler) handleDescribeCapacityBlockOfferings(vals url.Values, reqID string) (any, error) {
	durationHours, _ := strconv.ParseInt(vals.Get("CapacityDurationHours"), 10, 32)
	instanceCount, _ := strconv.ParseInt(vals.Get("InstanceCount"), 10, 32)

	offerings, err := h.Backend.DescribeCapacityBlockOfferings(
		vals.Get("InstanceType"),
		int32(durationHours),
		int32(instanceCount),
	)
	if err != nil {
		return nil, err
	}

	resp := &describeCapacityBlockOfferingsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, o := range offerings {
		resp.Offerings.Items = append(resp.Offerings.Items, toCapacityBlockOfferingItem(o))
	}

	return resp, nil
}

type capacityBlockItem struct {
	CapacityBlockID        string `xml:"capacityBlockId"`
	AvailabilityZone       string `xml:"availabilityZone,omitempty"`
	State                  string `xml:"state,omitempty"`
	CreateDate             string `xml:"createDate,omitempty"`
	StartDate              string `xml:"startDate,omitempty"`
	EndDate                string `xml:"endDate,omitempty"`
	CapacityReservationIDs struct {
		Items []string `xml:"item"`
	} `xml:"capacityReservationIdSet"`
	TagSet []simpleTagItem `xml:"tagSet>item"`
}

func toCapacityBlockItem(b *CapacityBlock, tags map[string]string) capacityBlockItem {
	item := capacityBlockItem{
		CapacityBlockID:  b.CapacityBlockID,
		AvailabilityZone: b.AvailabilityZone,
		State:            b.State,
		CreateDate:       b.CreateDate.Format(time.RFC3339),
		StartDate:        b.StartDate.Format(time.RFC3339),
		EndDate:          b.EndDate.Format(time.RFC3339),
		TagSet:           tagItemsFromMap(tags),
	}
	item.CapacityReservationIDs.Items = b.CapacityReservationIDs

	return item
}

type purchaseCapacityBlockResponse struct {
	XMLName        xml.Name `xml:"PurchaseCapacityBlockResponse"`
	Xmlns          string   `xml:"xmlns,attr"`
	RequestID      string   `xml:"requestId"`
	CapacityBlocks struct {
		Items []capacityBlockItem `xml:"item"`
	} `xml:"capacityBlockSet"`
	CapacityReservation capacityReservationItem `xml:"capacityReservation"`
}

func (h *Handler) handlePurchaseCapacityBlock(vals url.Values, reqID string) (any, error) {
	tags := parseTagSpecification(vals, "capacity-reservation")

	block, cr, err := h.Backend.PurchaseCapacityBlock(
		vals.Get("CapacityBlockOfferingId"),
		vals.Get("InstancePlatform"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	resp := &purchaseCapacityBlockResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		CapacityReservation: toCapacityReservationItem(cr),
	}
	resp.CapacityBlocks.Items = []capacityBlockItem{
		toCapacityBlockItem(block, h.Backend.TagsForResource(block.CapacityBlockID)),
	}

	return resp, nil
}

type capacityBlockExtensionOfferingItem struct {
	CapacityBlockExtensionOfferingID    string `xml:"capacityBlockExtensionOfferingId"`
	CapacityReservationID               string `xml:"capacityReservationId,omitempty"`
	InstanceType                        string `xml:"instanceType,omitempty"`
	AvailabilityZone                    string `xml:"availabilityZone,omitempty"`
	CurrencyCode                        string `xml:"currencyCode,omitempty"`
	UpfrontPrice                        string `xml:"upfrontFee,omitempty"`
	StartDate                           string `xml:"startDate,omitempty"`
	CapacityBlockExtensionStartDate     string `xml:"capacityBlockExtensionStartDate,omitempty"`
	CapacityBlockExtensionEndDate       string `xml:"capacityBlockExtensionEndDate,omitempty"`
	CapacityBlockExtensionDurationHours int32  `xml:"capacityBlockExtensionDurationHours,omitempty"`
	InstanceCount                       int32  `xml:"instanceCount,omitempty"`
}

func toCapacityBlockExtensionOfferingItem(o *CapacityBlockExtensionOffering) capacityBlockExtensionOfferingItem {
	return capacityBlockExtensionOfferingItem{
		CapacityBlockExtensionOfferingID:    o.CapacityBlockExtensionOfferingID,
		CapacityReservationID:               o.CapacityReservationID,
		InstanceType:                        o.InstanceType,
		AvailabilityZone:                    o.AvailabilityZone,
		CurrencyCode:                        o.CurrencyCode,
		UpfrontPrice:                        o.UpfrontPrice,
		CapacityBlockExtensionStartDate:     o.CapacityBlockExtensionStartDate.Format(time.RFC3339),
		CapacityBlockExtensionEndDate:       o.CapacityBlockExtensionEndDate.Format(time.RFC3339),
		CapacityBlockExtensionDurationHours: o.CapacityBlockExtensionDurationHours,
		InstanceCount:                       o.InstanceCount,
	}
}

type describeCapacityBlockExtensionOfferingsResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityBlockExtensionOfferingsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Offerings struct {
		Items []capacityBlockExtensionOfferingItem `xml:"item"`
	} `xml:"capacityBlockExtensionOfferingSet"`
}

func (h *Handler) handleDescribeCapacityBlockExtensionOfferings(vals url.Values, reqID string) (any, error) {
	durationHours, _ := strconv.ParseInt(vals.Get("CapacityBlockExtensionDurationHours"), 10, 32)

	offerings, err := h.Backend.DescribeCapacityBlockExtensionOfferings(
		vals.Get("CapacityReservationId"),
		int32(durationHours),
	)
	if err != nil {
		return nil, err
	}

	resp := &describeCapacityBlockExtensionOfferingsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, o := range offerings {
		resp.Offerings.Items = append(resp.Offerings.Items, toCapacityBlockExtensionOfferingItem(o))
	}

	return resp, nil
}

type capacityBlockExtensionItem struct {
	CapacityBlockExtensionOfferingID    string `xml:"capacityBlockExtensionOfferingId,omitempty"`
	CapacityReservationID               string `xml:"capacityReservationId,omitempty"`
	AvailabilityZone                    string `xml:"availabilityZone,omitempty"`
	CapacityBlockExtensionStatus        string `xml:"capacityBlockExtensionStatus,omitempty"`
	CapacityBlockExtensionStartDate     string `xml:"capacityBlockExtensionStartDate,omitempty"`
	CapacityBlockExtensionEndDate       string `xml:"capacityBlockExtensionEndDate,omitempty"`
	CapacityBlockExtensionPurchaseDate  string `xml:"capacityBlockExtensionPurchaseDate,omitempty"`
	CapacityBlockExtensionDurationHours int32  `xml:"capacityBlockExtensionDurationHours,omitempty"`
}

func toCapacityBlockExtensionItem(e *CapacityBlockExtension) capacityBlockExtensionItem {
	return capacityBlockExtensionItem{
		CapacityBlockExtensionOfferingID:    e.CapacityBlockExtensionOfferingID,
		CapacityReservationID:               e.CapacityReservationID,
		AvailabilityZone:                    e.AvailabilityZone,
		CapacityBlockExtensionStatus:        e.CapacityBlockExtensionStatus,
		CapacityBlockExtensionStartDate:     e.CapacityBlockExtensionStartDate.Format(time.RFC3339),
		CapacityBlockExtensionEndDate:       e.CapacityBlockExtensionEndDate.Format(time.RFC3339),
		CapacityBlockExtensionPurchaseDate:  e.CapacityBlockExtensionPurchaseDate.Format(time.RFC3339),
		CapacityBlockExtensionDurationHours: e.CapacityBlockExtensionDurationHours,
	}
}

type purchaseCapacityBlockExtensionResponse struct {
	XMLName    xml.Name `xml:"PurchaseCapacityBlockExtensionResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	Extensions struct {
		Items []capacityBlockExtensionItem `xml:"item"`
	} `xml:"capacityBlockExtensionSet"`
}

func (h *Handler) handlePurchaseCapacityBlockExtension(vals url.Values, reqID string) (any, error) {
	ext, err := h.Backend.PurchaseCapacityBlockExtension(
		vals.Get("CapacityBlockExtensionOfferingId"),
		vals.Get("CapacityReservationId"),
	)
	if err != nil {
		return nil, err
	}

	resp := &purchaseCapacityBlockExtensionResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.Extensions.Items = []capacityBlockExtensionItem{toCapacityBlockExtensionItem(ext)}

	return resp, nil
}

type describeCapacityBlocksResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityBlocksResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Blocks    struct {
		Items []capacityBlockItem `xml:"item"`
	} `xml:"capacityBlockSet"`
}

func (h *Handler) handleDescribeCapacityBlocks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityBlockId")
	filters := parseEC2Filters(vals)

	blocks := h.Backend.DescribeCapacityBlocks(ids, filters)

	resp := &describeCapacityBlocksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, blk := range blocks {
		resp.Blocks.Items = append(
			resp.Blocks.Items, toCapacityBlockItem(blk, h.Backend.TagsForResource(blk.CapacityBlockID)),
		)
	}

	return resp, nil
}

type capacityBlockReservationStatusItem struct {
	CapacityReservationID    string `xml:"capacityReservationId"`
	TotalCapacity            int32  `xml:"totalCapacity"`
	TotalAvailableCapacity   int32  `xml:"totalAvailableCapacity"`
	TotalUnavailableCapacity int32  `xml:"totalUnavailableCapacity"`
}

type capacityBlockStatusItem struct {
	CapacityBlockID             string `xml:"capacityBlockId"`
	InterconnectStatus          string `xml:"interconnectStatus,omitempty"`
	CapacityReservationStatuses struct {
		Items []capacityBlockReservationStatusItem `xml:"item"`
	} `xml:"capacityReservationStatusSet"`
	TotalCapacity            int32 `xml:"totalCapacity"`
	TotalAvailableCapacity   int32 `xml:"totalAvailableCapacity"`
	TotalUnavailableCapacity int32 `xml:"totalUnavailableCapacity"`
}

func toCapacityBlockStatusItem(s *CapacityBlockStatus) capacityBlockStatusItem {
	item := capacityBlockStatusItem{
		CapacityBlockID:          s.CapacityBlockID,
		InterconnectStatus:       s.InterconnectStatus,
		TotalCapacity:            s.TotalCapacity,
		TotalAvailableCapacity:   s.TotalAvailableCapacity,
		TotalUnavailableCapacity: s.TotalUnavailableCapacity,
	}
	for _, rs := range s.CapacityReservationStatuses {
		item.CapacityReservationStatuses.Items = append(
			item.CapacityReservationStatuses.Items,
			capacityBlockReservationStatusItem(rs),
		)
	}

	return item
}

type describeCapacityBlockStatusResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityBlockStatusResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Statuses  struct {
		Items []capacityBlockStatusItem `xml:"item"`
	} `xml:"capacityBlockStatusSet"`
}

func (h *Handler) handleDescribeCapacityBlockStatus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityBlockId")
	filters := parseEC2Filters(vals)

	statuses := h.Backend.DescribeCapacityBlockStatus(ids, filters)

	resp := &describeCapacityBlockStatusResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, s := range statuses {
		resp.Statuses.Items = append(resp.Statuses.Items, toCapacityBlockStatusItem(s))
	}

	return resp, nil
}

type describeCapacityBlockExtensionHistoryResponse struct {
	XMLName    xml.Name `xml:"DescribeCapacityBlockExtensionHistoryResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	NextToken  string   `xml:"nextToken,omitempty"`
	Extensions struct {
		Items []capacityBlockExtensionItem `xml:"item"`
	} `xml:"capacityBlockExtensionSet"`
}

func (h *Handler) handleDescribeCapacityBlockExtensionHistory(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityReservationId")
	filters := parseEC2Filters(vals)

	exts := h.Backend.DescribeCapacityBlockExtensionHistory(ids, filters)

	resp := &describeCapacityBlockExtensionHistoryResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, e := range exts {
		resp.Extensions.Items = append(resp.Extensions.Items, toCapacityBlockExtensionItem(e))
	}

	return resp, nil
}
