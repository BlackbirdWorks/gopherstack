package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
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

// deleteFleetSuccessItem mirrors the real DeleteFleetSuccessItem shape, which
// has no plain fleetState member -- only currentFleetState/previousFleetState.
type deleteFleetSuccessItem struct {
	FleetID            string `xml:"fleetId"`
	CurrentFleetState  string `xml:"currentFleetState"`
	PreviousFleetState string `xml:"previousFleetState,omitempty"`
}

type deleteFleetsResponse struct {
	XMLName                  xml.Name `xml:"DeleteFleetsResponse"`
	RequestID                string   `xml:"requestId"`
	SuccessfulFleetDeletions struct {
		Items []deleteFleetSuccessItem `xml:"item"`
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

// activeFleetInstanceItem mirrors ActiveInstance (ec2@v1.319.1
// types/types.go:202), the item shape DescribeFleetInstances returns.
type activeFleetInstanceItem struct {
	InstanceID     string `xml:"instanceId"`
	InstanceType   string `xml:"instanceType,omitempty"`
	InstanceHealth string `xml:"instanceHealth,omitempty"`
}

type describeFleetInstancesResponse struct {
	XMLName         xml.Name `xml:"DescribeFleetInstancesResponse"`
	RequestID       string   `xml:"requestId"`
	FleetID         string   `xml:"fleetId"`
	NextToken       string   `xml:"nextToken,omitempty"`
	ActiveInstances struct {
		Items []activeFleetInstanceItem `xml:"item"`
	} `xml:"activeInstanceSet"`
}

// fleetHistoryRecordItem mirrors HistoryRecordEntry (ec2@v1.319.1
// types/types.go:7778); EventInformation reuses spotFleetEventInformationItem
// since both ops nest the same eventDescription-wrapping shape.
type fleetHistoryRecordItem struct {
	Timestamp        string                        `xml:"timestamp"`
	EventType        string                        `xml:"eventType,omitempty"`
	EventInformation spotFleetEventInformationItem `xml:"eventInformation"`
}

type describeFleetHistoryResponse struct {
	XMLName           xml.Name `xml:"DescribeFleetHistoryResponse"`
	RequestID         string   `xml:"requestId"`
	FleetID           string   `xml:"fleetId"`
	StartTime         string   `xml:"startTime"`
	LastEvaluatedTime string   `xml:"lastEvaluatedTime,omitempty"`
	NextToken         string   `xml:"nextToken,omitempty"`
	HistoryRecords    struct {
		Items []fleetHistoryRecordItem `xml:"item"`
	} `xml:"historyRecordSet"`
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
		OnDemandTargetCapacity:          f.OnDemandTargetCapacity,
		SpotTargetCapacity:              f.SpotTargetCapacity,
		TargetCapacityUnitType:          f.TargetCapacityUnitType,
		DefaultTargetCapacityType:       f.DefaultTargetCapacityType,
		ExcessCapacityTerminationPolicy: f.ExcessCapacityTerminationPolicy,
		Errors:                          fleetErrorSet{Items: []fleetErrorItem{}},
		Instances:                       fleetInstanceItemSet{Items: []fleetInstanceItem{}},
	}
}

// groupFleetInstancesByType groups instances by InstanceType into the
// fleetInstanceItem shape CreateFleetOutput.Instances / FleetData.Instances
// share (ec2@v1.319.1 types/types.go:3824, :4638). Preserves the input
// order's first-seen instance-type ordering for deterministic output.
func groupFleetInstancesByType(instances []*Instance) []fleetInstanceItem {
	var order []string

	byType := make(map[string][]string)

	for _, inst := range instances {
		if _, seen := byType[inst.InstanceType]; !seen {
			order = append(order, inst.InstanceType)
		}

		byType[inst.InstanceType] = append(byType[inst.InstanceType], inst.ID)
	}

	items := make([]fleetInstanceItem, 0, len(order))
	for _, it := range order {
		items = append(items, fleetInstanceItem{
			InstanceType: it,
			InstanceIDs:  fleetInstanceIDSet{Items: byType[it]},
		})
	}

	return items
}

// parseFleetLaunchTemplateConfigs parses LaunchTemplateConfigs.N.* from an
// EC2-query CreateFleet request. The real serializer FlatKeys both
// LaunchTemplateConfigs and each config's Overrides (ec2@v1.319.1
// serializers.go:57701/:57737), so the wire keys are
// "LaunchTemplateConfigs.N.LaunchTemplateSpecification.*" and
// "LaunchTemplateConfigs.N.Overrides.M.*", not a nested "member"/"Item" level.
func parseFleetLaunchTemplateConfigs(vals url.Values) []FleetLaunchTemplateConfig {
	var configs []FleetLaunchTemplateConfig

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("LaunchTemplateConfigs.%d.", i)
		ltID := vals.Get(prefix + "LaunchTemplateSpecification.LaunchTemplateId")
		ltName := vals.Get(prefix + "LaunchTemplateSpecification.LaunchTemplateName")
		firstOverrideImage := vals.Get(prefix + "Overrides.1.ImageId")
		firstOverrideType := vals.Get(prefix + "Overrides.1.InstanceType")

		if ltID == "" && ltName == "" && firstOverrideImage == "" && firstOverrideType == "" {
			break
		}

		cfg := FleetLaunchTemplateConfig{
			LaunchTemplateID:   ltID,
			LaunchTemplateName: ltName,
			Version:            vals.Get(prefix + "LaunchTemplateSpecification.Version"),
			Overrides:          parseFleetLaunchTemplateOverrides(vals, prefix),
		}

		configs = append(configs, cfg)
	}

	return configs
}

func parseFleetLaunchTemplateOverrides(vals url.Values, prefix string) []FleetLaunchTemplateOverride {
	var overrides []FleetLaunchTemplateOverride

	for j := 1; ; j++ {
		ovPrefix := fmt.Sprintf("%sOverrides.%d.", prefix, j)
		imageID := vals.Get(ovPrefix + "ImageId")
		instanceType := vals.Get(ovPrefix + "InstanceType")
		subnetID := vals.Get(ovPrefix + "SubnetId")
		az := vals.Get(ovPrefix + "AvailabilityZone")
		weightedStr := vals.Get(ovPrefix + "WeightedCapacity")

		if imageID == "" && instanceType == "" && subnetID == "" && az == "" && weightedStr == "" {
			break
		}

		ov := FleetLaunchTemplateOverride{
			ImageID:          imageID,
			InstanceType:     instanceType,
			SubnetID:         subnetID,
			AvailabilityZone: az,
		}

		if weightedStr != "" {
			if w, err := strconv.ParseFloat(weightedStr, 64); err == nil {
				ov.WeightedCapacity = w
			}
		}

		overrides = append(overrides, ov)
	}

	return overrides
}

func (h *Handler) handleCreateFleet(vals url.Values, reqID string) (any, error) {
	input := FleetCreateInput{
		Type:                            vals.Get("Type"),
		ExcessCapacityTerminationPolicy: vals.Get("ExcessCapacityTerminationPolicy"),
		TargetCapacityUnitType:          vals.Get("TargetCapacitySpecification.TargetCapacityUnitType"),
		DefaultTargetCapacityType:       vals.Get("TargetCapacitySpecification.DefaultTargetCapacityType"),
		LaunchTemplateConfigs:           parseFleetLaunchTemplateConfigs(vals),
	}

	parseIntValue(vals.Get("TargetCapacitySpecification.TotalTargetCapacity"), &input.TotalTargetCapacity)
	parseIntValue(vals.Get("TargetCapacitySpecification.OnDemandTargetCapacity"), &input.OnDemandTargetCapacity)
	parseIntValue(vals.Get("TargetCapacitySpecification.SpotTargetCapacity"), &input.SpotTargetCapacity)

	if v := vals.Get("TerminateInstancesWithExpiration"); v != "" {
		input.TerminateInstancesWithExpiration = strings.EqualFold(v, "true")
	}

	f, launched, err := h.Backend.CreateFleet(input)
	if err != nil {
		return nil, err
	}

	resp := &createFleetResponse{
		RequestID: reqID,
		FleetID:   f.FleetID,
		Errors:    fleetErrorSet{Items: []fleetErrorItem{}},
		Instances: fleetInstanceItemSet{Items: []fleetInstanceItem{}},
	}

	if f.FleetType == fleetTypeInstant {
		for _, r := range launched {
			resp.Instances.Items = append(resp.Instances.Items, fleetInstanceItem{
				InstanceType: r.InstanceType,
				InstanceIDs:  fleetInstanceIDSet{Items: r.InstanceIDs},
			})
		}
	}

	return resp, nil
}

func (h *Handler) handleDeleteFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FleetId")

	// The default is to terminate the instances (DeleteFleetsInput.TerminateInstances
	// doc, ec2@v1.319.1 api_op_DeleteFleets.go).
	terminate := true
	if v := vals.Get("TerminateInstances"); v != "" {
		terminate = strings.EqualFold(v, "true")
	}

	deleted := h.Backend.DeleteFleets(ids, terminate)

	resp := &deleteFleetsResponse{RequestID: reqID}
	for _, d := range deleted {
		resp.SuccessfulFleetDeletions.Items = append(resp.SuccessfulFleetDeletions.Items, deleteFleetSuccessItem{
			FleetID:            d.FleetID,
			CurrentFleetState:  tgwRouteStateDeleted,
			PreviousFleetState: d.PreviousFleetState,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeFleets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FleetId")
	fleets := h.Backend.DescribeFleets(ids)

	resp := &describeFleetsResponse{RequestID: reqID}
	for _, f := range fleets {
		item := toFleetItem(f)

		// Instances/Errors are valid only for fleets of type instant
		// (ec2@v1.319.1 types/types.go:6646, FleetData doc comments).
		if f.FleetType == fleetTypeInstant && len(f.InstanceIDs) > 0 {
			item.Instances = fleetInstanceItemSet{
				Items: groupFleetInstancesByType(h.Backend.DescribeInstances(f.InstanceIDs, "")),
			}
		}

		resp.FleetSet.Items = append(resp.FleetSet.Items, item)
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

func (h *Handler) handleDescribeFleetHistory(vals url.Values, reqID string) (any, error) {
	fleetID := vals.Get("FleetId")
	eventType := vals.Get("EventType")

	startTime := time.Time{}
	if s := vals.Get("StartTime"); s != "" {
		if parsed, err := time.Parse(time.RFC3339, s); err == nil {
			startTime = parsed
		}
	}

	records, err := h.Backend.DescribeFleetHistory(fleetID, startTime, eventType)
	if err != nil {
		return nil, err
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	records, nextToken = pageSlice(records, offset, maxResults)

	// LastEvaluatedTime is only present when nextToken is empty -- real AWS
	// documents it as "all records up to this time were retrieved".
	var lastEvaluatedTime string
	if nextToken == "" {
		lastEvaluatedTime = time.Now().UTC().Format(time.RFC3339)
	}

	resp := &describeFleetHistoryResponse{
		RequestID:         reqID,
		FleetID:           fleetID,
		StartTime:         startTime.Format(time.RFC3339),
		LastEvaluatedTime: lastEvaluatedTime,
		NextToken:         nextToken,
	}

	for _, rec := range records {
		resp.HistoryRecords.Items = append(resp.HistoryRecords.Items, fleetHistoryRecordItem{
			Timestamp:        rec.Timestamp.Format(time.RFC3339),
			EventType:        rec.EventType,
			EventInformation: spotFleetEventInformationItem{EventDescription: rec.EventInformation},
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeFleetInstances(vals url.Values, reqID string) (any, error) {
	fleetID := vals.Get("FleetId")
	filters := parseEC2Filters(vals)

	instances, err := h.Backend.DescribeFleetInstances(fleetID, filters)
	if err != nil {
		return nil, err
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	instances, nextToken = pageSlice(instances, offset, maxResults)

	resp := &describeFleetInstancesResponse{RequestID: reqID, FleetID: fleetID, NextToken: nextToken}
	for _, inst := range instances {
		resp.ActiveInstances.Items = append(resp.ActiveInstances.Items, activeFleetInstanceItem(inst))
	}

	return resp, nil
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
