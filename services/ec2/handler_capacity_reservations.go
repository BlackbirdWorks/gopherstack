package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

type createCapacityReservationResponse struct {
	XMLName             xml.Name                `xml:"CreateCapacityReservationResponse"`
	RequestID           string                  `xml:"requestId"`
	CapacityReservation capacityReservationItem `xml:"capacityReservation"`
}

type instanceConnectEndpointItem struct {
	InstanceConnectEndpointID string `xml:"instanceConnectEndpointId"`
	SubnetID                  string `xml:"subnetId"`
	VPCID                     string `xml:"vpcId"`
	State                     string `xml:"state"`
	PreserveClientIP          bool   `xml:"preserveClientIp"`
}

type groupsForCapacityReservationResponse struct {
	XMLName                     xml.Name `xml:"GetGroupsForCapacityReservationResponse"`
	RequestID                   string   `xml:"requestId"`
	CapacityReservationGroupSet struct {
		Items []struct {
			GroupARN string `xml:"groupArn"`
		} `xml:"item"`
	} `xml:"capacityReservationGroupSet"`
}

// ---- Handler implementations ----

func toCapacityReservationItem(cr *CapacityReservation, tags map[string]string) capacityReservationItem {
	return capacityReservationItem{
		CapacityReservationID:  cr.CapacityReservationID,
		InstanceType:           cr.InstanceType,
		AvailabilityZone:       cr.AvailabilityZone,
		OwnedBy:                cr.OwnedBy,
		State:                  cr.State,
		TotalInstanceCount:     cr.TotalInstanceCount,
		AvailableInstanceCount: cr.AvailableInstanceCount,
		TagSet:                 tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateCapacityReservation(vals url.Values, reqID string) (any, error) {
	instanceType := vals.Get("InstanceType")
	az := vals.Get("AvailabilityZone")
	count, _ := strconv.Atoi(vals.Get("InstanceCount"))
	if count == 0 {
		count = 1
	}

	tags := parseTagSpecificationPlural(vals, "capacity-reservation")

	cr, err := h.Backend.CreateCapacityReservation(instanceType, az, count, tags)
	if err != nil {
		return nil, err
	}

	return &createCapacityReservationResponse{
		RequestID:           reqID,
		CapacityReservation: toCapacityReservationItem(cr, tags),
	}, nil
}

func (h *Handler) handleCancelCapacityReservation(vals url.Values, reqID string) (any, error) {
	id := vals.Get("CapacityReservationId")
	if err := h.Backend.CancelCapacityReservation(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelCapacityReservationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyCapacityReservation(vals url.Values, reqID string) (any, error) {
	id := vals.Get("CapacityReservationId")
	count, _ := strconv.Atoi(vals.Get("InstanceCount"))
	if err := h.Backend.ModifyCapacityReservation(id, count); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyCapacityReservationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleGetGroupsForCapacityReservation(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("CapacityReservationId")
	groups, err := h.Backend.GetGroupsForCapacityReservation(id)
	if err != nil {
		return nil, err
	}

	resp := &groupsForCapacityReservationResponse{RequestID: reqID}
	for _, g := range groups {
		resp.CapacityReservationGroupSet.Items = append(
			resp.CapacityReservationGroupSet.Items,
			struct {
				GroupARN string `xml:"groupArn"`
			}{GroupARN: g},
		)
	}

	return resp, nil
}

type createInterruptibleCRAllocationResponse struct {
	XMLName                     xml.Name `xml:"CreateInterruptibleCapacityReservationAllocationResponse"`
	RequestID                   string   `xml:"requestId"`
	InterruptionType            string   `xml:"interruptionType,omitempty"`
	SourceCapacityReservationID string   `xml:"sourceCapacityReservationId,omitempty"`
	Status                      string   `xml:"status,omitempty"`
	TargetInstanceCount         int32    `xml:"targetInstanceCount,omitempty"`
}

func (h *Handler) handleCreateInterruptibleCapacityReservationAllocation(
	vals url.Values, reqID string,
) (any, error) {
	crID := vals.Get("CapacityReservationId")
	instanceCount := parseInt32Value(vals.Get("InstanceCount"))

	alloc, err := h.Backend.CreateInterruptibleCapacityReservationAllocation(crID, instanceCount)
	if err != nil {
		return nil, err
	}

	return &createInterruptibleCRAllocationResponse{
		RequestID:                   reqID,
		InterruptionType:            interruptionTypeAdhoc,
		SourceCapacityReservationID: alloc.SourceCapacityReservationID,
		Status:                      alloc.Status,
		TargetInstanceCount:         alloc.TargetInstanceCount,
	}, nil
}

type updateInterruptibleCRAllocationResponse struct {
	XMLName                            xml.Name `xml:"UpdateInterruptibleCapacityReservationAllocationResponse"`
	RequestID                          string   `xml:"requestId"`
	InterruptibleCapacityReservationID string   `xml:"interruptibleCapacityReservationId,omitempty"`
	InterruptionType                   string   `xml:"interruptionType,omitempty"`
	SourceCapacityReservationID        string   `xml:"sourceCapacityReservationId,omitempty"`
	Status                             string   `xml:"status,omitempty"`
	InstanceCount                      int32    `xml:"instanceCount,omitempty"`
	TargetInstanceCount                int32    `xml:"targetInstanceCount,omitempty"`
}

func (h *Handler) handleUpdateInterruptibleCapacityReservationAllocation(
	vals url.Values, reqID string,
) (any, error) {
	crID := vals.Get("CapacityReservationId")
	targetInstanceCount := parseInt32Value(vals.Get("TargetInstanceCount"))

	alloc, err := h.Backend.UpdateInterruptibleCapacityReservationAllocation(crID, targetInstanceCount)
	if err != nil {
		return nil, err
	}

	return &updateInterruptibleCRAllocationResponse{
		RequestID:                          reqID,
		InterruptibleCapacityReservationID: alloc.SourceCapacityReservationID,
		InterruptionType:                   interruptionTypeAdhoc,
		SourceCapacityReservationID:        alloc.SourceCapacityReservationID,
		Status:                             alloc.Status,
		InstanceCount:                      alloc.TargetInstanceCount,
		TargetInstanceCount:                alloc.TargetInstanceCount,
	}, nil
}

const interruptionTypeAdhoc = "adhoc"

type instanceUsageItem struct {
	AccountID         string `xml:"accountId,omitempty"`
	UsedInstanceCount int32  `xml:"usedInstanceCount,omitempty"`
}

type interruptibleCapacityAllocationItem struct {
	InterruptibleCapacityReservationID string `xml:"interruptibleCapacityReservationId,omitempty"`
	InterruptionType                   string `xml:"interruptionType,omitempty"`
	Status                             string `xml:"status,omitempty"`
	InstanceCount                      int32  `xml:"instanceCount,omitempty"`
	TargetInstanceCount                int32  `xml:"targetInstanceCount,omitempty"`
}

type getCapacityReservationUsageResponse struct {
	InterruptibleCapacityAllocation *interruptibleCapacityAllocationItem `xml:"interruptibleCapacityAllocation,omitempty"`
	XMLName                         xml.Name                             `xml:"GetCapacityReservationUsageResponse"`
	RequestID                       string                               `xml:"requestId"`
	CapacityReservationID           string                               `xml:"capacityReservationId,omitempty"`
	InstanceType                    string                               `xml:"instanceType,omitempty"`
	State                           string                               `xml:"state,omitempty"`
	InstanceUsageSet                struct {
		Items []instanceUsageItem `xml:"item"`
	} `xml:"instanceUsageSet"`
	AvailableInstanceCount int32 `xml:"availableInstanceCount"`
	TotalInstanceCount     int32 `xml:"totalInstanceCount"`
	Interruptible          bool  `xml:"interruptible,omitempty"`
}

func (h *Handler) handleGetCapacityReservationUsage(vals url.Values, reqID string) (any, error) {
	id := vals.Get("CapacityReservationId")

	usage, err := h.Backend.GetCapacityReservationUsage(id)
	if err != nil {
		return nil, err
	}

	resp := &getCapacityReservationUsageResponse{
		RequestID:             reqID,
		CapacityReservationID: usage.CapacityReservationID,
		InstanceType:          usage.InstanceType,
		State:                 usage.State,
		// Reservation instance counts are always small (bounded well under
		// int32 range), so the int->int32 narrowing here is safe.
		AvailableInstanceCount: int32(usage.AvailableInstanceCount), //nolint:gosec // bounded instance count
		TotalInstanceCount:     int32(usage.TotalInstanceCount),     //nolint:gosec // bounded instance count
		Interruptible:          usage.Interruptible,
	}

	for _, u := range usage.InstanceUsages {
		resp.InstanceUsageSet.Items = append(resp.InstanceUsageSet.Items, instanceUsageItem(u))
	}

	if usage.InterruptibleAllocation != nil {
		resp.InterruptibleCapacityAllocation = &interruptibleCapacityAllocationItem{
			InterruptibleCapacityReservationID: usage.InterruptibleAllocation.SourceCapacityReservationID,
			InterruptionType:                   interruptionTypeAdhoc,
			Status:                             usage.InterruptibleAllocation.Status,
			InstanceCount:                      usage.InterruptibleAllocation.TargetInstanceCount,
			TargetInstanceCount:                usage.InterruptibleAllocation.TargetInstanceCount,
		}
	}

	return resp, nil
}

type capacityReservationTopologyItem struct {
	CapacityReservationID string `xml:"capacityReservationId,omitempty"`
	InstanceType          string `xml:"instanceType,omitempty"`
	AvailabilityZone      string `xml:"availabilityZone,omitempty"`
}

type describeCapacityReservationTopologyResponse struct {
	XMLName                xml.Name `xml:"DescribeCapacityReservationTopologyResponse"`
	RequestID              string   `xml:"requestId"`
	CapacityReservationSet struct {
		Items []capacityReservationTopologyItem `xml:"item"`
	} `xml:"capacityReservationSet"`
}

func (h *Handler) handleDescribeCapacityReservationTopology(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityReservationId")

	entries := h.Backend.DescribeCapacityReservationTopology(ids)

	resp := &describeCapacityReservationTopologyResponse{RequestID: reqID}
	for _, e := range entries {
		resp.CapacityReservationSet.Items = append(resp.CapacityReservationSet.Items, capacityReservationTopologyItem{
			CapacityReservationID: e.CapacityReservationID,
			InstanceType:          e.InstanceType,
			AvailabilityZone:      e.AvailabilityZone,
		})
	}

	return resp, nil
}
