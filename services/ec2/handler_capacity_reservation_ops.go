package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

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
