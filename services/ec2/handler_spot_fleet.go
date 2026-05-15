package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// spotFleetSupportedOperations returns the list of real (non-stub) spot fleet operations.
func spotFleetSupportedOperations() []string {
	return []string{
		"RequestSpotFleet",
		"DescribeSpotFleetRequests",
		"CancelSpotFleetRequests",
		"ModifySpotFleetRequest",
		"DescribeSpotFleetInstances",
		"DescribeSpotFleetRequestHistory",
	}
}

// registerSpotFleetOps registers the real spot fleet handlers, overriding stubs.
func registerSpotFleetOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["RequestSpotFleet"] = h.handleRequestSpotFleet
	ops["DescribeSpotFleetRequests"] = h.handleDescribeSpotFleetRequests
	ops["CancelSpotFleetRequests"] = h.handleCancelSpotFleetRequests
	ops["ModifySpotFleetRequest"] = h.handleModifySpotFleetRequest
	ops["DescribeSpotFleetInstances"] = h.handleDescribeSpotFleetInstances
	ops["DescribeSpotFleetRequestHistory"] = h.handleDescribeSpotFleetRequestHistory
}

// handleRequestSpotFleet parses and dispatches a RequestSpotFleet call.
func (h *Handler) handleRequestSpotFleet(vals url.Values, reqID string) (any, error) {
	config := SpotFleetRequestConfig{}
	config.SpotPrice = vals.Get("SpotFleetRequestConfig.SpotPrice")
	config.AllocationStrategy = vals.Get("SpotFleetRequestConfig.AllocationStrategy")
	config.ExcessCapacityTerminationPolicy = vals.Get(
		"SpotFleetRequestConfig.ExcessCapacityTerminationPolicy",
	)
	config.IamFleetRole = vals.Get("SpotFleetRequestConfig.IamFleetRole")
	config.Type = vals.Get("SpotFleetRequestConfig.Type")

	if tcStr := vals.Get("SpotFleetRequestConfig.TargetCapacity"); tcStr != "" {
		tc, err := strconv.Atoi(tcStr)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid TargetCapacity: %s", ErrInvalidParameter, tcStr)
		}

		config.TargetCapacity = tc
	}

	if v := vals.Get("SpotFleetRequestConfig.TerminateInstancesWithExpiration"); v != "" {
		config.TerminateInstancesWithExpiration = strings.EqualFold(v, "true")
	}

	if v := vals.Get("SpotFleetRequestConfig.ReplaceUnhealthyInstances"); v != "" {
		config.ReplaceUnhealthyInstances = strings.EqualFold(v, "true")
	}

	// Parse launch specifications.
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("SpotFleetRequestConfig.LaunchSpecifications.%d.", i)
		imageID := vals.Get(prefix + "ImageId")
		instanceType := vals.Get(prefix + "InstanceType")

		if imageID == "" && instanceType == "" {
			break
		}

		spec := SpotFleetLaunchSpecification{
			ImageID:      imageID,
			InstanceType: instanceType,
			SubnetID:     vals.Get(prefix + "SubnetId"),
			KeyName:      vals.Get(prefix + "KeyName"),
			SpotPrice:    vals.Get(prefix + "SpotPrice"),
		}

		if wcStr := vals.Get(prefix + "WeightedCapacity"); wcStr != "" {
			wc, err := strconv.ParseFloat(wcStr, 64)
			if err == nil {
				spec.WeightedCapacity = wc
			}
		}

		config.LaunchSpecifications = append(config.LaunchSpecifications, spec)
	}

	fleet, err := h.Backend.RequestSpotFleet(config)
	if err != nil {
		return nil, err
	}

	return &requestSpotFleetResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		SpotFleetRequestID: fleet.SpotFleetRequestID,
	}, nil
}

// handleDescribeSpotFleetRequests handles DescribeSpotFleetRequests.
func (h *Handler) handleDescribeSpotFleetRequests(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SpotFleetRequestId")
	fleets, err := h.Backend.DescribeSpotFleetRequests(ids)
	if err != nil {
		return nil, err
	}

	items := make([]spotFleetRequestConfigSetItem, 0, len(fleets))
	for _, fleet := range fleets {
		specs := make(
			[]spotFleetLaunchSpecItem,
			0,
			len(fleet.SpotFleetRequestConfig.LaunchSpecifications),
		)
		for _, spec := range fleet.SpotFleetRequestConfig.LaunchSpecifications {
			specs = append(specs, spotFleetLaunchSpecItem{
				ImageID:          spec.ImageID,
				InstanceType:     spec.InstanceType,
				SubnetID:         spec.SubnetID,
				KeyName:          spec.KeyName,
				SpotPrice:        spec.SpotPrice,
				WeightedCapacity: fmt.Sprintf("%g", spec.WeightedCapacity),
			})
		}

		items = append(items, spotFleetRequestConfigSetItem{
			SpotFleetRequestID:    fleet.SpotFleetRequestID,
			SpotFleetRequestState: fleet.SpotFleetRequestState,
			ActivityStatus:        fleet.ActivityStatus,
			CreateTime:            fleet.CreateTime.Format(time.RFC3339),
			SpotFleetRequestConfig: spotFleetConfigItem{
				SpotPrice:                       fleet.SpotFleetRequestConfig.SpotPrice,
				TargetCapacity:                  fleet.SpotFleetRequestConfig.TargetCapacity,
				AllocationStrategy:              fleet.SpotFleetRequestConfig.AllocationStrategy,
				ExcessCapacityTerminationPolicy: fleet.SpotFleetRequestConfig.ExcessCapacityTerminationPolicy,
				IamFleetRole:                    fleet.SpotFleetRequestConfig.IamFleetRole,
				Type:                            fleet.SpotFleetRequestConfig.Type,
				LaunchSpecifications:            spotFleetLaunchSpecSet{Items: specs},
			},
			FulfilledCapacity: fmt.Sprintf("%g", fleet.FulfilledCapacity),
		})
	}

	return &describeSpotFleetRequestsResponse{
		Xmlns:                     ec2XMLNS,
		RequestID:                 reqID,
		SpotFleetRequestConfigSet: spotFleetRequestConfigSet{Items: items},
	}, nil
}

// handleCancelSpotFleetRequests handles CancelSpotFleetRequests.
func (h *Handler) handleCancelSpotFleetRequests(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SpotFleetRequestId")
	terminateInstances := strings.EqualFold(vals.Get("TerminateInstances"), "true")

	results, err := h.Backend.CancelSpotFleetRequests(ids, terminateInstances)
	if err != nil {
		return nil, err
	}

	successItems := make([]cancelSpotFleetSuccessItem, 0, len(results))
	errorItems := make([]cancelSpotFleetErrorItem, 0, len(results))

	for _, r := range results {
		if r.Error != "" {
			errorItems = append(errorItems, cancelSpotFleetErrorItem{
				SpotFleetRequestID: r.SpotFleetRequestID,
				Error:              r.Error,
			})
		} else {
			successItems = append(successItems, cancelSpotFleetSuccessItem{
				SpotFleetRequestID:            r.SpotFleetRequestID,
				CurrentSpotFleetRequestState:  r.CurrentSpotFleetRequestState,
				PreviousSpotFleetRequestState: r.PreviousSpotFleetRequestState,
			})
		}
	}

	return &cancelSpotFleetRequestsResponse{
		Xmlns:                     ec2XMLNS,
		RequestID:                 reqID,
		SuccessfulFleetRequests:   cancelSpotFleetSuccessSet{Items: successItems},
		UnsuccessfulFleetRequests: cancelSpotFleetErrorSet{Items: errorItems},
	}, nil
}

// handleModifySpotFleetRequest handles ModifySpotFleetRequest.
func (h *Handler) handleModifySpotFleetRequest(vals url.Values, reqID string) (any, error) {
	fleetID := vals.Get("SpotFleetRequestId")
	tcStr := vals.Get("TargetCapacity")
	excessTermination := vals.Get("ExcessCapacityTerminationPolicy")

	tc := 0
	if tcStr != "" {
		parsed, err := strconv.Atoi(tcStr)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid TargetCapacity: %s", ErrInvalidParameter, tcStr)
		}

		tc = parsed
	}

	_, err := h.Backend.ModifySpotFleetRequest(fleetID, tc, excessTermination)
	if err != nil {
		return nil, err
	}

	return &modifySpotFleetRequestResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleDescribeSpotFleetInstances handles DescribeSpotFleetInstances.
func (h *Handler) handleDescribeSpotFleetInstances(vals url.Values, reqID string) (any, error) {
	fleetID := vals.Get("SpotFleetRequestId")

	instances, err := h.Backend.DescribeSpotFleetInstances(fleetID)
	if err != nil {
		return nil, err
	}

	items := make([]spotFleetInstanceItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, spotFleetInstanceItem{
			InstanceID:            inst.InstanceID,
			InstanceType:          inst.InstanceType,
			SpotInstanceRequestID: inst.SpotInstanceRequestID,
			InstanceHealth:        inst.InstanceHealth,
		})
	}

	return &describeSpotFleetInstancesResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		SpotFleetRequestID: fleetID,
		ActiveInstances:    spotFleetInstanceSet{Items: items},
	}, nil
}

// handleDescribeSpotFleetRequestHistory handles DescribeSpotFleetRequestHistory.
func (h *Handler) handleDescribeSpotFleetRequestHistory(
	vals url.Values,
	reqID string,
) (any, error) {
	fleetID := vals.Get("SpotFleetRequestId")
	startTimeStr := vals.Get("StartTime")

	startTime := time.Time{}
	if startTimeStr != "" {
		parsed, err := time.Parse(time.RFC3339, startTimeStr)
		if err == nil {
			startTime = parsed
		}
	}

	records, err := h.Backend.DescribeSpotFleetRequestHistory(fleetID, startTime)
	if err != nil {
		return nil, err
	}

	items := make([]spotFleetHistoryRecordItem, 0, len(records))
	for _, rec := range records {
		items = append(items, spotFleetHistoryRecordItem{
			Timestamp:        rec.Timestamp.Format(time.RFC3339),
			EventType:        rec.EventType,
			EventInformation: rec.EventInformation,
		})
	}

	return &describeSpotFleetRequestHistoryResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		SpotFleetRequestID: fleetID,
		StartTime:          startTime.Format(time.RFC3339),
		HistoryRecords:     spotFleetHistoryRecordSet{Items: items},
	}, nil
}

// ---- XML response types ----

type requestSpotFleetResponse struct {
	XMLName            xml.Name `xml:"RequestSpotFleetResponse"`
	Xmlns              string   `xml:"xmlns,attr"`
	RequestID          string   `xml:"requestId"`
	SpotFleetRequestID string   `xml:"spotFleetRequestId"`
}

type spotFleetLaunchSpecItem struct {
	ImageID          string `xml:"imageId,omitempty"`
	InstanceType     string `xml:"instanceType,omitempty"`
	SubnetID         string `xml:"subnetId,omitempty"`
	KeyName          string `xml:"keyName,omitempty"`
	SpotPrice        string `xml:"spotPrice,omitempty"`
	WeightedCapacity string `xml:"weightedCapacity,omitempty"`
}

type spotFleetLaunchSpecSet struct {
	Items []spotFleetLaunchSpecItem `xml:"item"`
}

type spotFleetConfigItem struct {
	SpotPrice                       string                 `xml:"spotPrice,omitempty"`
	AllocationStrategy              string                 `xml:"allocationStrategy,omitempty"`
	ExcessCapacityTerminationPolicy string                 `xml:"excessCapacityTerminationPolicy,omitempty"`
	IamFleetRole                    string                 `xml:"iamFleetRole,omitempty"`
	Type                            string                 `xml:"type,omitempty"`
	LaunchSpecifications            spotFleetLaunchSpecSet `xml:"launchSpecificationsSet"`
	TargetCapacity                  int                    `xml:"targetCapacity"`
}

type spotFleetRequestConfigSetItem struct {
	SpotFleetRequestID     string              `xml:"spotFleetRequestId"`
	SpotFleetRequestState  string              `xml:"spotFleetRequestState"`
	ActivityStatus         string              `xml:"activityStatus,omitempty"`
	CreateTime             string              `xml:"createTime"`
	FulfilledCapacity      string              `xml:"fulfilledCapacity,omitempty"`
	SpotFleetRequestConfig spotFleetConfigItem `xml:"spotFleetRequestConfig"`
}

type spotFleetRequestConfigSet struct {
	Items []spotFleetRequestConfigSetItem `xml:"item"`
}

type describeSpotFleetRequestsResponse struct {
	XMLName                   xml.Name                  `xml:"DescribeSpotFleetRequestsResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	RequestID                 string                    `xml:"requestId"`
	SpotFleetRequestConfigSet spotFleetRequestConfigSet `xml:"spotFleetRequestConfigSet"`
}

type cancelSpotFleetSuccessItem struct {
	SpotFleetRequestID            string `xml:"spotFleetRequestId"`
	CurrentSpotFleetRequestState  string `xml:"currentSpotFleetRequestState"`
	PreviousSpotFleetRequestState string `xml:"previousSpotFleetRequestState"`
}

type cancelSpotFleetSuccessSet struct {
	Items []cancelSpotFleetSuccessItem `xml:"item"`
}

type cancelSpotFleetErrorItem struct {
	SpotFleetRequestID string `xml:"spotFleetRequestId"`
	Error              string `xml:"error"`
}

type cancelSpotFleetErrorSet struct {
	Items []cancelSpotFleetErrorItem `xml:"item"`
}

type cancelSpotFleetRequestsResponse struct {
	XMLName                   xml.Name                  `xml:"CancelSpotFleetRequestsResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	RequestID                 string                    `xml:"requestId"`
	SuccessfulFleetRequests   cancelSpotFleetSuccessSet `xml:"successfulFleetRequestSet"`
	UnsuccessfulFleetRequests cancelSpotFleetErrorSet   `xml:"unsuccessfulFleetRequestSet"`
}

type modifySpotFleetRequestResponse struct {
	XMLName   xml.Name `xml:"ModifySpotFleetRequestResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type spotFleetInstanceItem struct {
	InstanceID            string `xml:"instanceId"`
	InstanceType          string `xml:"instanceType,omitempty"`
	SpotInstanceRequestID string `xml:"spotInstanceRequestId,omitempty"`
	InstanceHealth        string `xml:"instanceHealth,omitempty"`
}

type spotFleetInstanceSet struct {
	Items []spotFleetInstanceItem `xml:"item"`
}

type describeSpotFleetInstancesResponse struct {
	XMLName            xml.Name             `xml:"DescribeSpotFleetInstancesResponse"`
	Xmlns              string               `xml:"xmlns,attr"`
	RequestID          string               `xml:"requestId"`
	SpotFleetRequestID string               `xml:"spotFleetRequestId"`
	ActiveInstances    spotFleetInstanceSet `xml:"activeInstanceSet"`
}

type spotFleetHistoryRecordItem struct {
	Timestamp        string `xml:"timestamp"`
	EventType        string `xml:"eventType,omitempty"`
	EventInformation string `xml:"eventInformation,omitempty"`
}

type spotFleetHistoryRecordSet struct {
	Items []spotFleetHistoryRecordItem `xml:"item"`
}

type describeSpotFleetRequestHistoryResponse struct {
	XMLName            xml.Name                  `xml:"DescribeSpotFleetRequestHistoryResponse"`
	Xmlns              string                    `xml:"xmlns,attr"`
	RequestID          string                    `xml:"requestId"`
	SpotFleetRequestID string                    `xml:"spotFleetRequestId"`
	StartTime          string                    `xml:"startTime"`
	HistoryRecords     spotFleetHistoryRecordSet `xml:"historyRecordSet"`
}
