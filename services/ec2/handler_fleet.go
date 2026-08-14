package ec2

import (
	"encoding/xml"
	"net/url"
)

// createFleetResponse matches the AWS CreateFleet response shape:
// fleetId, errors (per-launch-spec failures), and instances (launched set).
type createFleetResponse struct {
	XMLName   xml.Name             `xml:"CreateFleetResponse"`
	RequestID string               `xml:"requestId"`
	FleetID   string               `xml:"fleetId"`
	Errors    fleetErrorSet        `xml:"errorSet"`
	Instances fleetInstanceItemSet `xml:"fleetInstanceSet"`
}

type deleteFleetsResponse struct {
	XMLName                  xml.Name `xml:"DeleteFleetsResponse"`
	RequestID                string   `xml:"requestId"`
	SuccessfulFleetDeletions struct {
		Items []fleetItem `xml:"item"`
	} `xml:"successfulFleetDeletionSet"`
	UnsuccessfulFleetDeletions struct {
		Items []struct{} `xml:"item"`
	} `xml:"unsuccessfulFleetDeletionSet"`
}

type describeFleetsResponse struct {
	XMLName   xml.Name `xml:"DescribeFleetsResponse"`
	RequestID string   `xml:"requestId"`
	FleetSet  struct {
		Items []fleetItem `xml:"item"`
	} `xml:"fleetSet"`
}

type networkInsightsPathItem struct {
	NetworkInsightsPathID  string `xml:"networkInsightsPathId"`
	NetworkInsightsPathArn string `xml:"networkInsightsPathArn,omitempty"`
	SourceID               string `xml:"source,omitempty"`
	DestinationID          string `xml:"destination,omitempty"`
	Protocol               string `xml:"protocol,omitempty"`
	DestinationPort        int    `xml:"destinationPort,omitempty"`
}

func toFleetItem(f *Fleet) fleetItem {
	return fleetItem{
		FleetID:                         f.FleetID,
		FleetState:                      f.FleetState,
		FleetType:                       f.FleetType,
		TotalTargetCapacity:             f.TotalTargetCapacity,
		ExcessCapacityTerminationPolicy: f.ExcessCapacityTerminationPolicy,
	}
}

func (h *Handler) handleCreateFleet(vals url.Values, reqID string) (any, error) {
	fleetType := vals.Get("Type")
	if fleetType == "" {
		fleetType = fleetTypeDefault
	}

	totalTarget := 0
	parseIntValue(vals.Get("TargetCapacitySpecification.TotalTargetCapacity"), &totalTarget)

	f, err := h.Backend.CreateFleet(fleetType, totalTarget)
	if err != nil {
		return nil, err
	}

	return &createFleetResponse{
		RequestID: reqID,
		FleetID:   f.FleetID,
		Errors:    fleetErrorSet{Items: []fleetErrorItem{}},
		Instances: fleetInstanceItemSet{Items: []fleetInstanceItem{}},
	}, nil
}

func (h *Handler) handleDeleteFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FleetId")
	deleted := h.Backend.DeleteFleets(ids)

	resp := &deleteFleetsResponse{RequestID: reqID}
	for _, id := range deleted {
		resp.SuccessfulFleetDeletions.Items = append(resp.SuccessfulFleetDeletions.Items, fleetItem{
			FleetID:    id,
			FleetState: "deleted",
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FleetId")
	fleets := h.Backend.DescribeFleets(ids)

	resp := &describeFleetsResponse{RequestID: reqID}
	for _, f := range fleets {
		resp.FleetSet.Items = append(resp.FleetSet.Items, toFleetItem(f))
	}

	return resp, nil
}

func (h *Handler) handleModifyFleet(vals url.Values, reqID string) (any, error) {
	id := vals.Get("FleetId")
	excessPolicy := vals.Get("ExcessCapacityTerminationPolicy")

	totalTarget := 0
	parseIntValue(vals.Get("TargetCapacitySpecification.TotalTargetCapacity"), &totalTarget)

	if err := h.Backend.ModifyFleet(id, totalTarget, excessPolicy); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeFleetHistory(_ url.Values, reqID string) (any, error) {
	type describeFleetHistoryResponse struct {
		XMLName        xml.Name `xml:"DescribeFleetHistoryResponse"`
		RequestID      string   `xml:"requestId"`
		HistoryRecords struct {
			Items []struct{} `xml:"item"`
		} `xml:"historyRecords"`
	}

	return &describeFleetHistoryResponse{RequestID: reqID}, nil
}

func (h *Handler) handleDescribeFleetInstances(_ url.Values, reqID string) (any, error) {
	type describeFleetInstancesResponse struct {
		XMLName         xml.Name `xml:"DescribeFleetInstancesResponse"`
		RequestID       string   `xml:"requestId"`
		ActiveInstances struct {
			Items []struct{} `xml:"item"`
		} `xml:"activeInstanceSet"`
	}

	return &describeFleetInstancesResponse{RequestID: reqID}, nil
}

// ---- Network Insights Path handlers ----

// registerFleetOps registers the Fleet operation handlers.
func registerFleetOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateFleet"] = h.handleCreateFleet
	ops["DeleteFleets"] = h.handleDeleteFleets
	ops["DescribeFleets"] = h.handleDescribeFleets
	ops["ModifyFleet"] = h.handleModifyFleet
	ops["DescribeFleetHistory"] = h.handleDescribeFleetHistory
	ops["DescribeFleetInstances"] = h.handleDescribeFleetInstances
}
