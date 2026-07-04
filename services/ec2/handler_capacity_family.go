package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// ---- registration ----

// registerCapacityFamilyOps registers the real Capacity Reservation Fleet,
// Capacity Block, and Capacity Manager handlers, overriding any stub entries.
func registerCapacityFamilyOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateCapacityReservationFleet"] = h.handleCreateCapacityReservationFleet
	ops["DescribeCapacityReservationFleets"] = h.handleDescribeCapacityReservationFleets
	ops["ModifyCapacityReservationFleet"] = h.handleModifyCapacityReservationFleet
	ops["CancelCapacityReservationFleets"] = h.handleCancelCapacityReservationFleets

	ops["DescribeCapacityBlockOfferings"] = h.handleDescribeCapacityBlockOfferings
	ops["PurchaseCapacityBlock"] = h.handlePurchaseCapacityBlock
	ops["DescribeCapacityBlockExtensionOfferings"] = h.handleDescribeCapacityBlockExtensionOfferings
	ops["PurchaseCapacityBlockExtension"] = h.handlePurchaseCapacityBlockExtension
	ops["DescribeCapacityBlocks"] = h.handleDescribeCapacityBlocks
	ops["DescribeCapacityBlockStatus"] = h.handleDescribeCapacityBlockStatus
	ops["DescribeCapacityBlockExtensionHistory"] = h.handleDescribeCapacityBlockExtensionHistory

	ops["CreateCapacityReservationBySplitting"] = h.handleCreateCapacityReservationBySplitting
	ops["MoveCapacityReservationInstances"] = h.handleMoveCapacityReservationInstances

	ops["AssociateCapacityReservationBillingOwner"] = h.handleAssociateCapacityReservationBillingOwner
	ops["DisassociateCapacityReservationBillingOwner"] = h.handleDisassociateCapacityReservationBillingOwner
	ops["RejectCapacityReservationBillingOwnership"] = h.handleRejectCapacityReservationBillingOwnership
	ops["DescribeCapacityReservationBillingRequests"] = h.handleDescribeCapacityReservationBillingRequests

	ops["EnableCapacityManager"] = h.handleEnableCapacityManager
	ops["DisableCapacityManager"] = h.handleDisableCapacityManager
	ops["UpdateCapacityManagerOrganizationsAccess"] = h.handleUpdateCapacityManagerOrganizationsAccess
	ops["GetCapacityManagerAttributes"] = h.handleGetCapacityManagerAttributes
	ops["GetCapacityManagerMetricData"] = h.handleGetCapacityManagerMetricData
	ops["GetCapacityManagerMetricDimensions"] = h.handleGetCapacityManagerMetricDimensions
	ops["CreateCapacityManagerDataExport"] = h.handleCreateCapacityManagerDataExport
	ops["DescribeCapacityManagerDataExports"] = h.handleDescribeCapacityManagerDataExports
	ops["DeleteCapacityManagerDataExport"] = h.handleDeleteCapacityManagerDataExport
}

// ---- shared XML fragments ----

type capacityReservationFleetInstanceSpecItem struct {
	AvailabilityZone      string  `xml:"availabilityZone,omitempty"`
	InstancePlatform      string  `xml:"instancePlatform,omitempty"`
	InstanceType          string  `xml:"instanceType,omitempty"`
	CapacityReservationID string  `xml:"capacityReservationId,omitempty"`
	Priority              int32   `xml:"priority,omitempty"`
	Weight                float64 `xml:"weight,omitempty"`
	TotalInstanceCount    int32   `xml:"totalInstanceCount,omitempty"`
	EbsOptimized          bool    `xml:"ebsOptimized,omitempty"`
}

func capacityReservationFleetInstanceSpecItems(
	specs []CapacityReservationFleetInstanceSpec,
) []capacityReservationFleetInstanceSpecItem {
	items := make([]capacityReservationFleetInstanceSpecItem, 0, len(specs))
	for _, s := range specs {
		items = append(items, capacityReservationFleetInstanceSpecItem{
			AvailabilityZone:      s.AvailabilityZone,
			InstancePlatform:      s.InstancePlatform,
			InstanceType:          s.InstanceType,
			CapacityReservationID: s.CapacityReservationID,
			Priority:              s.Priority,
			Weight:                s.Weight,
			TotalInstanceCount:    s.TotalInstanceCount,
			EbsOptimized:          s.EbsOptimized,
		})
	}

	return items
}

type capacityReservationFleetItem struct {
	CapacityReservationFleetID string                                     `xml:"capacityReservationFleetId"`
	AllocationStrategy         string                                     `xml:"allocationStrategy,omitempty"`
	State                      string                                     `xml:"state,omitempty"`
	InstanceMatchCriteria      string                                     `xml:"instanceMatchCriteria,omitempty"`
	Tenancy                    string                                     `xml:"tenancy,omitempty"`
	CreateTime                 string                                     `xml:"createTime,omitempty"`
	EndDate                    string                                     `xml:"endDate,omitempty"`
	InstanceTypeSpecifications []capacityReservationFleetInstanceSpecItem `xml:"instanceTypeSpecificationSet>item"`
	TagSet                     []simpleTagItem                            `xml:"tagSet>item"`
	TotalFulfilledCapacity     float64                                    `xml:"totalFulfilledCapacity,omitempty"`
	TotalTargetCapacity        int32                                      `xml:"totalTargetCapacity,omitempty"`
}

func toCapacityReservationFleetItem(fleet *CapacityReservationFleet) capacityReservationFleetItem {
	item := capacityReservationFleetItem{
		CapacityReservationFleetID: fleet.CapacityReservationFleetID,
		AllocationStrategy:         fleet.AllocationStrategy,
		State:                      fleet.State,
		InstanceMatchCriteria:      fleet.InstanceMatchCriteria,
		Tenancy:                    fleet.Tenancy,
		TotalTargetCapacity:        fleet.TotalTargetCapacity,
		TotalFulfilledCapacity:     fleet.TotalFulfilledCapacity,
		InstanceTypeSpecifications: capacityReservationFleetInstanceSpecItems(fleet.InstanceTypeSpecifications),
		TagSet:                     tagItemsFromMap(fleet.Tags),
	}
	if !fleet.CreateTime.IsZero() {
		item.CreateTime = fleet.CreateTime.Format(time.RFC3339)
	}

	if fleet.EndDate != nil {
		item.EndDate = fleet.EndDate.Format(time.RFC3339)
	}

	return item
}

// ---- Capacity Reservation Fleet ----

func parseCapacityReservationFleetInstanceSpecs(vals url.Values) []CapacityReservationFleetInstanceSpec {
	var specs []CapacityReservationFleetInstanceSpec

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("InstanceTypeSpecification.%d.", i)

		instanceType := vals.Get(prefix + "InstanceType")
		az := vals.Get(prefix + "AvailabilityZone")
		if instanceType == "" && az == "" {
			break
		}

		spec := CapacityReservationFleetInstanceSpec{
			AvailabilityZone: az,
			InstanceType:     instanceType,
			InstancePlatform: vals.Get(prefix + "InstancePlatform"),
			EbsOptimized:     vals.Get(prefix+"EbsOptimized") == ec2BooleanTrue,
		}

		if p, err := strconv.ParseInt(vals.Get(prefix+"Priority"), 10, 32); err == nil {
			spec.Priority = int32(p)
		}

		if w, err := strconv.ParseFloat(vals.Get(prefix+"Weight"), 64); err == nil {
			spec.Weight = w
		}

		specs = append(specs, spec)
	}

	return specs
}

func (h *Handler) handleCreateCapacityReservationFleet(vals url.Values, reqID string) (any, error) {
	specs := parseCapacityReservationFleetInstanceSpecs(vals)

	totalTargetCapacity, err := strconv.ParseInt(vals.Get("TotalTargetCapacity"), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%w: TotalTargetCapacity is required", ErrInvalidParameter)
	}

	var endDate *time.Time
	if v := vals.Get("EndDate"); v != "" {
		if t, parseErr := time.Parse(time.RFC3339, v); parseErr == nil {
			endDate = &t
		}
	}

	tags := parseTagSpecification(vals, "capacity-reservation-fleet")

	fleet, err := h.Backend.CreateCapacityReservationFleet(
		specs,
		int32(totalTargetCapacity),
		vals.Get("AllocationStrategy"),
		vals.Get("InstanceMatchCriteria"),
		vals.Get("Tenancy"),
		endDate,
		tags,
	)
	if err != nil {
		return nil, err
	}

	resp := &createCapacityReservationFleetResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.capacityReservationFleetItem = toCapacityReservationFleetItem(fleet)

	return resp, nil
}

// createCapacityReservationFleetResponse is the CreateCapacityReservationFleet
// response, whose Capacity Reservation Fleet fields sit directly under the
// response root rather than under a nested element, matching the real API shape.
type createCapacityReservationFleetResponse struct {
	XMLName   xml.Name `xml:"CreateCapacityReservationFleetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	capacityReservationFleetItem
}

type capacityReservationFleetSet struct {
	Items []capacityReservationFleetItem `xml:"item"`
}

type describeCapacityReservationFleetsResponse struct {
	XMLName   xml.Name                    `xml:"DescribeCapacityReservationFleetsResponse"`
	Xmlns     string                      `xml:"xmlns,attr"`
	RequestID string                      `xml:"requestId"`
	NextToken string                      `xml:"nextToken,omitempty"`
	Fleets    capacityReservationFleetSet `xml:"capacityReservationFleetSet"`
}

func (h *Handler) handleDescribeCapacityReservationFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityReservationFleetId")
	filters := parseEC2Filters(vals)

	fleets := h.Backend.DescribeCapacityReservationFleets(ids, filters)

	items := make([]capacityReservationFleetItem, 0, len(fleets))
	for _, fleet := range fleets {
		items = append(items, toCapacityReservationFleetItem(fleet))
	}

	return &describeCapacityReservationFleetsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Fleets:    capacityReservationFleetSet{Items: items},
	}, nil
}

func (h *Handler) handleModifyCapacityReservationFleet(vals url.Values, reqID string) (any, error) {
	fleetID := vals.Get("CapacityReservationFleetId")

	var totalTargetCapacity *int32
	if v := vals.Get("TotalTargetCapacity"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			tc := int32(n)
			totalTargetCapacity = &tc
		}
	}

	var endDate *time.Time
	if v := vals.Get("EndDate"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			endDate = &t
		}
	}

	removeEndDate := vals.Get("RemoveEndDate") == ec2BooleanTrue

	err := h.Backend.ModifyCapacityReservationFleet(fleetID, totalTargetCapacity, endDate, removeEndDate)
	if err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyCapacityReservationFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type failedFleetCancellationItem struct {
	CapacityReservationFleetID          string `xml:"capacityReservationFleetId"`
	CancelCapacityReservationFleetError struct {
		Code    string `xml:"code"`
		Message string `xml:"message"`
	} `xml:"cancelCapacityReservationFleetError"`
}

type successfulFleetCancellationItem struct {
	CapacityReservationFleetID string `xml:"capacityReservationFleetId"`
	CurrentFleetState          string `xml:"currentFleetState"`
	PreviousFleetState         string `xml:"previousFleetState"`
}

type cancelCapacityReservationFleetsResponse struct {
	XMLName                      xml.Name `xml:"CancelCapacityReservationFleetsResponse"`
	Xmlns                        string   `xml:"xmlns,attr"`
	RequestID                    string   `xml:"requestId"`
	SuccessfulFleetCancellations struct {
		Items []successfulFleetCancellationItem `xml:"item"`
	} `xml:"successfulFleetCancellationSet"`
	FailedFleetCancellations struct {
		Items []failedFleetCancellationItem `xml:"item"`
	} `xml:"failedFleetCancellationSet"`
}

func (h *Handler) handleCancelCapacityReservationFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityReservationFleetId")

	successful, failed := h.Backend.CancelCapacityReservationFleets(ids)

	resp := &cancelCapacityReservationFleetsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}

	for _, s := range successful {
		resp.SuccessfulFleetCancellations.Items = append(
			resp.SuccessfulFleetCancellations.Items,
			successfulFleetCancellationItem{
				CapacityReservationFleetID: s.CapacityReservationFleetID,
				CurrentFleetState:          s.CurrentState,
				PreviousFleetState:         s.PreviousState,
			},
		)
	}

	for _, f := range failed {
		item := failedFleetCancellationItem{CapacityReservationFleetID: f.CapacityReservationFleetID}
		item.CancelCapacityReservationFleetError.Code = f.ErrorCode
		item.CancelCapacityReservationFleetError.Message = f.ErrorMessage
		resp.FailedFleetCancellations.Items = append(resp.FailedFleetCancellations.Items, item)
	}

	return resp, nil
}

// ---- Capacity Block ----

type capacityBlockOfferingItem struct {
	CapacityBlockOfferingID    string `xml:"capacityBlockOfferingId"`
	InstanceType               string `xml:"instanceType,omitempty"`
	AvailabilityZone           string `xml:"availabilityZone,omitempty"`
	Tenancy                    string `xml:"tenancy,omitempty"`
	CurrencyCode               string `xml:"currencyCode,omitempty"`
	UpfrontPrice               string `xml:"upfrontPrice,omitempty"`
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

func toCapacityBlockItem(b *CapacityBlock) capacityBlockItem {
	item := capacityBlockItem{
		CapacityBlockID:  b.CapacityBlockID,
		AvailabilityZone: b.AvailabilityZone,
		State:            b.State,
		CreateDate:       b.CreateDate.Format(time.RFC3339),
		StartDate:        b.StartDate.Format(time.RFC3339),
		EndDate:          b.EndDate.Format(time.RFC3339),
		TagSet:           tagItemsFromMap(b.Tags),
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
	resp.CapacityBlocks.Items = []capacityBlockItem{toCapacityBlockItem(block)}

	return resp, nil
}

type capacityBlockExtensionOfferingItem struct {
	CapacityBlockExtensionOfferingID    string `xml:"capacityBlockExtensionOfferingId"`
	CapacityReservationID               string `xml:"capacityReservationId,omitempty"`
	InstanceType                        string `xml:"instanceType,omitempty"`
	AvailabilityZone                    string `xml:"availabilityZone,omitempty"`
	CurrencyCode                        string `xml:"currencyCode,omitempty"`
	UpfrontPrice                        string `xml:"upfrontPrice,omitempty"`
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
	for _, b := range blocks {
		resp.Blocks.Items = append(resp.Blocks.Items, toCapacityBlockItem(b))
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

// ---- Capacity Reservation splitting / instance movement ----

type createCapacityReservationBySplittingResponse struct {
	XMLName                        xml.Name                `xml:"CreateCapacityReservationBySplittingResponse"`
	Xmlns                          string                  `xml:"xmlns,attr"`
	RequestID                      string                  `xml:"requestId"`
	SourceCapacityReservation      capacityReservationItem `xml:"sourceCapacityReservation"`
	DestinationCapacityReservation capacityReservationItem `xml:"destinationCapacityReservation"`
	InstanceCount                  int                     `xml:"instanceCount"`
}

func (h *Handler) handleCreateCapacityReservationBySplitting(vals url.Values, reqID string) (any, error) {
	instanceCount, _ := strconv.Atoi(vals.Get("InstanceCount"))
	tags := parseTagSpecification(vals, "capacity-reservation")

	dst, src, err := h.Backend.CreateCapacityReservationBySplitting(
		vals.Get("SourceCapacityReservationId"),
		instanceCount,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createCapacityReservationBySplittingResponse{
		Xmlns:                          ec2XMLNS,
		RequestID:                      reqID,
		SourceCapacityReservation:      toCapacityReservationItem(src),
		DestinationCapacityReservation: toCapacityReservationItem(dst),
		InstanceCount:                  dst.TotalInstanceCount,
	}, nil
}

type moveCapacityReservationInstancesResponse struct {
	XMLName                        xml.Name                `xml:"MoveCapacityReservationInstancesResponse"`
	Xmlns                          string                  `xml:"xmlns,attr"`
	RequestID                      string                  `xml:"requestId"`
	SourceCapacityReservation      capacityReservationItem `xml:"sourceCapacityReservation"`
	DestinationCapacityReservation capacityReservationItem `xml:"destinationCapacityReservation"`
	InstanceCount                  int                     `xml:"instanceCount"`
}

func (h *Handler) handleMoveCapacityReservationInstances(vals url.Values, reqID string) (any, error) {
	instanceCount, _ := strconv.Atoi(vals.Get("InstanceCount"))

	dst, src, err := h.Backend.MoveCapacityReservationInstances(
		vals.Get("SourceCapacityReservationId"),
		vals.Get("DestinationCapacityReservationId"),
		instanceCount,
	)
	if err != nil {
		return nil, err
	}

	return &moveCapacityReservationInstancesResponse{
		Xmlns:                          ec2XMLNS,
		RequestID:                      reqID,
		SourceCapacityReservation:      toCapacityReservationItem(src),
		DestinationCapacityReservation: toCapacityReservationItem(dst),
		InstanceCount:                  instanceCount,
	}, nil
}

// ---- Capacity Reservation billing ownership ----

func (h *Handler) handleAssociateCapacityReservationBillingOwner(vals url.Values, reqID string) (any, error) {
	err := h.Backend.AssociateCapacityReservationBillingOwner(
		vals.Get("CapacityReservationId"),
		vals.Get("UnusedReservationBillingOwnerId"),
	)
	if err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateCapacityReservationBillingOwnerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisassociateCapacityReservationBillingOwner(vals url.Values, reqID string) (any, error) {
	err := h.Backend.DisassociateCapacityReservationBillingOwner(
		vals.Get("CapacityReservationId"),
		vals.Get("UnusedReservationBillingOwnerId"),
	)
	if err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateCapacityReservationBillingOwnerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRejectCapacityReservationBillingOwnership(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.RejectCapacityReservationBillingOwnership(vals.Get("CapacityReservationId")); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RejectCapacityReservationBillingOwnershipResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type capacityReservationBillingRequestItem struct {
	CapacityReservationID           string `xml:"capacityReservationId"`
	RequestedBy                     string `xml:"requestedBy,omitempty"`
	UnusedReservationBillingOwnerID string `xml:"unusedReservationBillingOwnerId,omitempty"`
	Status                          string `xml:"status,omitempty"`
	StatusMessage                   string `xml:"statusMessage,omitempty"`
	LastUpdateTime                  string `xml:"lastUpdateTime,omitempty"`
}

func toCapacityReservationBillingRequestItem(
	r *CapacityReservationBillingRequest,
) capacityReservationBillingRequestItem {
	return capacityReservationBillingRequestItem{
		CapacityReservationID:           r.CapacityReservationID,
		RequestedBy:                     r.RequestedBy,
		UnusedReservationBillingOwnerID: r.UnusedReservationBillingOwnerID,
		Status:                          r.Status,
		StatusMessage:                   r.StatusMessage,
		LastUpdateTime:                  r.LastUpdateTime.Format(time.RFC3339),
	}
}

type describeCapacityReservationBillingRequestsResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityReservationBillingRequestsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Requests  struct {
		Items []capacityReservationBillingRequestItem `xml:"item"`
	} `xml:"capacityReservationBillingRequestSet"`
}

func (h *Handler) handleDescribeCapacityReservationBillingRequests(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityReservationId")
	filters := parseEC2Filters(vals)

	requests := h.Backend.DescribeCapacityReservationBillingRequests(ids, filters)

	resp := &describeCapacityReservationBillingRequestsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range requests {
		resp.Requests.Items = append(resp.Requests.Items, toCapacityReservationBillingRequestItem(r))
	}

	return resp, nil
}

// ---- Capacity Manager ----

type capacityManagerStatusResponse struct {
	XMLName               xml.Name `xml:""`
	Xmlns                 string   `xml:"xmlns,attr"`
	RequestID             string   `xml:"requestId"`
	CapacityManagerStatus string   `xml:"capacityManagerStatus"`
	OrganizationsAccess   bool     `xml:"organizationsAccess"`
}

func (h *Handler) handleEnableCapacityManager(vals url.Values, reqID string) (any, error) {
	orgAccess := vals.Get("OrganizationsAccess") == ec2BooleanTrue
	status, access := h.Backend.EnableCapacityManager(orgAccess)

	return &capacityManagerStatusResponse{
		XMLName:               xml.Name{Local: "EnableCapacityManagerResponse"},
		Xmlns:                 ec2XMLNS,
		RequestID:             reqID,
		CapacityManagerStatus: status,
		OrganizationsAccess:   access,
	}, nil
}

func (h *Handler) handleDisableCapacityManager(_ url.Values, reqID string) (any, error) {
	status, access := h.Backend.DisableCapacityManager()

	return &capacityManagerStatusResponse{
		XMLName:               xml.Name{Local: "DisableCapacityManagerResponse"},
		Xmlns:                 ec2XMLNS,
		RequestID:             reqID,
		CapacityManagerStatus: status,
		OrganizationsAccess:   access,
	}, nil
}

func (h *Handler) handleUpdateCapacityManagerOrganizationsAccess(vals url.Values, reqID string) (any, error) {
	orgAccess := vals.Get("OrganizationsAccess") == ec2BooleanTrue
	status, access := h.Backend.UpdateCapacityManagerOrganizationsAccess(orgAccess)

	return &capacityManagerStatusResponse{
		XMLName:               xml.Name{Local: "UpdateCapacityManagerOrganizationsAccessResponse"},
		Xmlns:                 ec2XMLNS,
		RequestID:             reqID,
		CapacityManagerStatus: status,
		OrganizationsAccess:   access,
	}, nil
}

type getCapacityManagerAttributesResponse struct {
	XMLName                xml.Name `xml:"GetCapacityManagerAttributesResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	CapacityManagerStatus  string   `xml:"capacityManagerStatus"`
	IngestionStatus        string   `xml:"ingestionStatus,omitempty"`
	IngestionStatusMessage string   `xml:"ingestionStatusMessage,omitempty"`
	DataExportCount        int32    `xml:"dataExportCount"`
	OrganizationsAccess    bool     `xml:"organizationsAccess"`
}

func (h *Handler) handleGetCapacityManagerAttributes(_ url.Values, reqID string) (any, error) {
	attrs := h.Backend.GetCapacityManagerAttributes()

	return &getCapacityManagerAttributesResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		CapacityManagerStatus:  attrs.Status,
		OrganizationsAccess:    attrs.OrganizationsAccess,
		DataExportCount:        attrs.DataExportCount,
		IngestionStatus:        attrs.IngestionStatus,
		IngestionStatusMessage: attrs.IngestionStatusMessage,
	}, nil
}

type getCapacityManagerMetricDataResponse struct {
	XMLName           xml.Name `xml:"GetCapacityManagerMetricDataResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	RequestID         string   `xml:"requestId"`
	NextToken         string   `xml:"nextToken,omitempty"`
	MetricDataResults struct {
		Items []struct{} `xml:"item"`
	} `xml:"metricDataResultSet"`
}

func (h *Handler) handleGetCapacityManagerMetricData(vals url.Values, reqID string) (any, error) {
	if vals.Get("StartTime") == "" || vals.Get("EndTime") == "" || vals.Get("Period") == "" ||
		vals.Get("MetricName.1") == "" {
		return nil, fmt.Errorf(
			"%w: StartTime, EndTime, Period, and MetricNames are required",
			ErrInvalidParameter,
		)
	}

	_ = h.Backend.GetCapacityManagerMetricData()

	return &getCapacityManagerMetricDataResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

type getCapacityManagerMetricDimensionsResponse struct {
	XMLName                xml.Name `xml:"GetCapacityManagerMetricDimensionsResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	NextToken              string   `xml:"nextToken,omitempty"`
	MetricDimensionResults struct {
		Items []struct{} `xml:"item"`
	} `xml:"metricDimensionResultSet"`
}

func (h *Handler) handleGetCapacityManagerMetricDimensions(vals url.Values, reqID string) (any, error) {
	if vals.Get("StartTime") == "" || vals.Get("EndTime") == "" ||
		vals.Get("MetricName.1") == "" || vals.Get("GroupBy.1") == "" {
		return nil, fmt.Errorf(
			"%w: StartTime, EndTime, MetricNames, and GroupBy are required",
			ErrInvalidParameter,
		)
	}

	_ = h.Backend.GetCapacityManagerMetricDimensions()

	return &getCapacityManagerMetricDimensionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

type createCapacityManagerDataExportResponse struct {
	XMLName                     xml.Name `xml:"CreateCapacityManagerDataExportResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	RequestID                   string   `xml:"requestId"`
	CapacityManagerDataExportID string   `xml:"capacityManagerDataExportId"`
}

func (h *Handler) handleCreateCapacityManagerDataExport(vals url.Values, reqID string) (any, error) {
	tags := parseTagSpecification(vals, "capacity-manager-data-export")

	export, err := h.Backend.CreateCapacityManagerDataExport(
		vals.Get("OutputFormat"),
		vals.Get("S3BucketName"),
		vals.Get("S3BucketPrefix"),
		vals.Get("Schedule"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createCapacityManagerDataExportResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		CapacityManagerDataExportID: export.CapacityManagerDataExportID,
	}, nil
}

type capacityManagerDataExportItem struct {
	CapacityManagerDataExportID string          `xml:"capacityManagerDataExportId"`
	OutputFormat                string          `xml:"outputFormat,omitempty"`
	S3BucketName                string          `xml:"s3BucketName,omitempty"`
	S3BucketPrefix              string          `xml:"s3BucketPrefix,omitempty"`
	Schedule                    string          `xml:"schedule,omitempty"`
	LatestDeliveryStatus        string          `xml:"latestDeliveryStatus,omitempty"`
	CreateTime                  string          `xml:"createTime,omitempty"`
	TagSet                      []simpleTagItem `xml:"tagSet>item"`
}

func toCapacityManagerDataExportItem(e *CapacityManagerDataExport) capacityManagerDataExportItem {
	return capacityManagerDataExportItem{
		CapacityManagerDataExportID: e.CapacityManagerDataExportID,
		OutputFormat:                e.OutputFormat,
		S3BucketName:                e.S3BucketName,
		S3BucketPrefix:              e.S3BucketPrefix,
		Schedule:                    e.Schedule,
		LatestDeliveryStatus:        e.LatestDeliveryStatus,
		CreateTime:                  e.CreateTime.Format(time.RFC3339),
		TagSet:                      tagItemsFromMap(e.Tags),
	}
}

type describeCapacityManagerDataExportsResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityManagerDataExportsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Exports   struct {
		Items []capacityManagerDataExportItem `xml:"item"`
	} `xml:"capacityManagerDataExportSet"`
}

func (h *Handler) handleDescribeCapacityManagerDataExports(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityManagerDataExportId")

	exports := h.Backend.DescribeCapacityManagerDataExports(ids)

	resp := &describeCapacityManagerDataExportsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, e := range exports {
		resp.Exports.Items = append(resp.Exports.Items, toCapacityManagerDataExportItem(e))
	}

	return resp, nil
}

type deleteCapacityManagerDataExportResponse struct {
	XMLName                     xml.Name `xml:"DeleteCapacityManagerDataExportResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	RequestID                   string   `xml:"requestId"`
	CapacityManagerDataExportID string   `xml:"capacityManagerDataExportId"`
}

func (h *Handler) handleDeleteCapacityManagerDataExport(vals url.Values, reqID string) (any, error) {
	id, err := h.Backend.DeleteCapacityManagerDataExport(vals.Get("CapacityManagerDataExportId"))
	if err != nil {
		return nil, err
	}

	return &deleteCapacityManagerDataExportResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		CapacityManagerDataExportID: id,
	}, nil
}
