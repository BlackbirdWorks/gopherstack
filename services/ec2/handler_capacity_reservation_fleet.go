package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

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

	item := toCapacityReservationFleetItem(fleet, h.Backend.TagsForResource(fleet.CapacityReservationFleetID))

	return &createCapacityReservationFleetResponse{
		Xmlns:                      ec2XMLNS,
		RequestID:                  reqID,
		CapacityReservationFleetID: item.CapacityReservationFleetID,
		AllocationStrategy:         item.AllocationStrategy,
		State:                      item.State,
		InstanceMatchCriteria:      item.InstanceMatchCriteria,
		Tenancy:                    item.Tenancy,
		CreateTime:                 item.CreateTime,
		EndDate:                    item.EndDate,
		InstanceTypeSpecifications: item.InstanceTypeSpecifications,
		TagSet:                     item.TagSet,
		TotalFulfilledCapacity:     item.TotalFulfilledCapacity,
		TotalTargetCapacity:        item.TotalTargetCapacity,
	}, nil
}

// createCapacityReservationFleetResponse is the CreateCapacityReservationFleet
// response. Its fields sit directly under the response root rather than
// under a nested element, matching the real API shape - but its constituent-
// reservation list is wrapped under "fleetCapacityReservationSet", NOT
// "instanceTypeSpecificationSet" like the sibling CapacityReservationFleet
// type used by DescribeCapacityReservationFleets (ec2@v1.319.1
// deserializers.go's awsEc2query_deserializeOpDocumentCreateCapacityReservationFleetOutput
// vs awsEc2query_deserializeDocumentCapacityReservationFleet) - so it cannot
// share capacityReservationFleetItem's tag for that one field.
type createCapacityReservationFleetResponse struct {
	XMLName                    xml.Name                                   `xml:"CreateCapacityReservationFleetResponse"`
	Xmlns                      string                                     `xml:"xmlns,attr"`
	RequestID                  string                                     `xml:"requestId"`
	CapacityReservationFleetID string                                     `xml:"capacityReservationFleetId"`
	AllocationStrategy         string                                     `xml:"allocationStrategy,omitempty"`
	State                      string                                     `xml:"state,omitempty"`
	InstanceMatchCriteria      string                                     `xml:"instanceMatchCriteria,omitempty"`
	Tenancy                    string                                     `xml:"tenancy,omitempty"`
	CreateTime                 string                                     `xml:"createTime,omitempty"`
	EndDate                    string                                     `xml:"endDate,omitempty"`
	InstanceTypeSpecifications []capacityReservationFleetInstanceSpecItem `xml:"fleetCapacityReservationSet>item"`
	TagSet                     []simpleTagItem                            `xml:"tagSet>item"`
	TotalFulfilledCapacity     float64                                    `xml:"totalFulfilledCapacity,omitempty"`
	TotalTargetCapacity        int32                                      `xml:"totalTargetCapacity,omitempty"`
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

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	fleets, nextToken = pageSlice(fleets, offset, maxResults)

	items := make([]capacityReservationFleetItem, 0, len(fleets))
	for _, fleet := range fleets {
		items = append(
			items,
			toCapacityReservationFleetItem(fleet, h.Backend.TagsForResource(fleet.CapacityReservationFleetID)),
		)
	}

	return &describeCapacityReservationFleetsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		NextToken: nextToken,
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
