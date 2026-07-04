package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// handler_scheduled_instances.go implements the HTTP handlers for the Scheduled Instances
// family, backed by backend_scheduled_instances.go.

// ---- Registration ----

func registerScheduledInstanceOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DescribeScheduledInstanceAvailability"] = h.handleDescribeScheduledInstanceAvailability
	ops["DescribeScheduledInstances"] = h.handleDescribeScheduledInstances
	ops["PurchaseScheduledInstances"] = h.handlePurchaseScheduledInstances
	ops["RunScheduledInstances"] = h.handleRunScheduledInstances
}

// scheduledInstanceSupportedOperations lists the operation names registered by
// registerScheduledInstanceOps, for GetSupportedOperations().
func scheduledInstanceSupportedOperations() []string {
	return []string{
		"DescribeScheduledInstanceAvailability",
		"DescribeScheduledInstances",
		"PurchaseScheduledInstances",
		"RunScheduledInstances",
	}
}

// ---- Response shapes ----

type scheduledInstanceRecurrenceItem struct {
	Frequency        string `xml:"frequency,omitempty"`
	OccurrenceUnit   string `xml:"occurrenceUnit,omitempty"`
	OccurrenceDaySet struct {
		Items []int32 `xml:"item"`
	} `xml:"occurrenceDaySet"`
	Interval                int32 `xml:"interval,omitempty"`
	OccurrenceRelativeToEnd bool  `xml:"occurrenceRelativeToEnd"`
}

func toScheduledInstanceRecurrenceItem(r ScheduledInstanceRecurrence) scheduledInstanceRecurrenceItem {
	item := scheduledInstanceRecurrenceItem{
		Frequency:               r.Frequency,
		Interval:                r.Interval,
		OccurrenceRelativeToEnd: r.OccurrenceRelativeToEnd,
		OccurrenceUnit:          r.OccurrenceUnit,
	}
	item.OccurrenceDaySet.Items = append(item.OccurrenceDaySet.Items, r.OccurrenceDaySet...)

	return item
}

type scheduledInstanceAvailabilityItem struct {
	Platform                    string                          `xml:"platform,omitempty"`
	PurchaseToken               string                          `xml:"purchaseToken,omitempty"`
	FirstSlotStartTime          string                          `xml:"firstSlotStartTime,omitempty"`
	HourlyPrice                 string                          `xml:"hourlyPrice,omitempty"`
	InstanceType                string                          `xml:"instanceType,omitempty"`
	NetworkPlatform             string                          `xml:"networkPlatform,omitempty"`
	AvailabilityZone            string                          `xml:"availabilityZone,omitempty"`
	Recurrence                  scheduledInstanceRecurrenceItem `xml:"recurrence"`
	MaxTermDurationInDays       int32                           `xml:"maxTermDurationInDays,omitempty"`
	MinTermDurationInDays       int32                           `xml:"minTermDurationInDays,omitempty"`
	AvailableInstanceCount      int32                           `xml:"availableInstanceCount,omitempty"`
	SlotDurationInHours         int32                           `xml:"slotDurationInHours,omitempty"`
	TotalScheduledInstanceHours int32                           `xml:"totalScheduledInstanceHours,omitempty"`
}

func toScheduledInstanceAvailabilityItem(a ScheduledInstanceAvailability) scheduledInstanceAvailabilityItem {
	item := scheduledInstanceAvailabilityItem{
		AvailabilityZone:            a.AvailabilityZone,
		AvailableInstanceCount:      a.AvailableInstanceCount,
		HourlyPrice:                 a.HourlyPrice,
		InstanceType:                a.InstanceType,
		MaxTermDurationInDays:       a.MaxTermDurationInDays,
		MinTermDurationInDays:       a.MinTermDurationInDays,
		NetworkPlatform:             a.NetworkPlatform,
		Platform:                    a.Platform,
		PurchaseToken:               a.PurchaseToken,
		Recurrence:                  toScheduledInstanceRecurrenceItem(a.Recurrence),
		SlotDurationInHours:         a.SlotDurationInHours,
		TotalScheduledInstanceHours: a.TotalScheduledInstanceHours,
	}
	if !a.FirstSlotStartTime.IsZero() {
		item.FirstSlotStartTime = a.FirstSlotStartTime.Format(time.RFC3339)
	}

	return item
}

type describeScheduledInstanceAvailabilityResponse struct {
	XMLName                          xml.Name `xml:"DescribeScheduledInstanceAvailabilityResponse"`
	Xmlns                            string   `xml:"xmlns,attr"`
	RequestID                        string   `xml:"requestId"`
	ScheduledInstanceAvailabilitySet struct {
		Items []scheduledInstanceAvailabilityItem `xml:"item"`
	} `xml:"scheduledInstanceAvailabilitySet"`
}

type scheduledInstanceItem struct {
	AvailabilityZone            string                          `xml:"availabilityZone,omitempty"`
	ScheduledInstanceID         string                          `xml:"scheduledInstanceId,omitempty"`
	PreviousSlotEndTime         string                          `xml:"previousSlotEndTime,omitempty"`
	TermStartDate               string                          `xml:"termStartDate,omitempty"`
	InstanceType                string                          `xml:"instanceType,omitempty"`
	NetworkPlatform             string                          `xml:"networkPlatform,omitempty"`
	NextSlotStartTime           string                          `xml:"nextSlotStartTime,omitempty"`
	HourlyPrice                 string                          `xml:"hourlyPrice,omitempty"`
	TermEndDate                 string                          `xml:"termEndDate,omitempty"`
	CreateDate                  string                          `xml:"createDate,omitempty"`
	Platform                    string                          `xml:"platform,omitempty"`
	Recurrence                  scheduledInstanceRecurrenceItem `xml:"recurrence"`
	SlotDurationInHours         int32                           `xml:"slotDurationInHours,omitempty"`
	InstanceCount               int32                           `xml:"instanceCount,omitempty"`
	TotalScheduledInstanceHours int32                           `xml:"totalScheduledInstanceHours,omitempty"`
}

func toScheduledInstanceItem(s *ScheduledInstance) scheduledInstanceItem {
	item := scheduledInstanceItem{
		AvailabilityZone:            s.AvailabilityZone,
		HourlyPrice:                 s.HourlyPrice,
		InstanceCount:               s.InstanceCount,
		InstanceType:                s.InstanceType,
		NetworkPlatform:             s.NetworkPlatform,
		Platform:                    s.Platform,
		Recurrence:                  toScheduledInstanceRecurrenceItem(s.Recurrence),
		ScheduledInstanceID:         s.ScheduledInstanceID,
		SlotDurationInHours:         s.SlotDurationInHours,
		TotalScheduledInstanceHours: s.TotalScheduledInstanceHours,
	}

	if !s.CreateDate.IsZero() {
		item.CreateDate = s.CreateDate.Format(time.RFC3339)
	}

	if !s.NextSlotStartTime.IsZero() {
		item.NextSlotStartTime = s.NextSlotStartTime.Format(time.RFC3339)
	}

	if !s.PreviousSlotEndTime.IsZero() {
		item.PreviousSlotEndTime = s.PreviousSlotEndTime.Format(time.RFC3339)
	}

	if !s.TermStartDate.IsZero() {
		item.TermStartDate = s.TermStartDate.Format(time.RFC3339)
	}

	if !s.TermEndDate.IsZero() {
		item.TermEndDate = s.TermEndDate.Format(time.RFC3339)
	}

	return item
}

type describeScheduledInstancesResponse struct {
	XMLName              xml.Name `xml:"DescribeScheduledInstancesResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	RequestID            string   `xml:"requestId"`
	ScheduledInstanceSet struct {
		Items []scheduledInstanceItem `xml:"item"`
	} `xml:"scheduledInstanceSet"`
}

type purchaseScheduledInstancesResponse struct {
	XMLName              xml.Name `xml:"PurchaseScheduledInstancesResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	RequestID            string   `xml:"requestId"`
	ScheduledInstanceSet struct {
		Items []scheduledInstanceItem `xml:"item"`
	} `xml:"scheduledInstanceSet"`
}

type runScheduledInstancesResponse struct {
	XMLName       xml.Name `xml:"RunScheduledInstancesResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	InstanceIDSet struct {
		Items []struct {
			InstanceID string `xml:"instanceId,omitempty"`
		} `xml:"item"`
	} `xml:"instanceIdSet"`
}

// ---- Handlers ----

func (h *Handler) handleDescribeScheduledInstanceAvailability(vals url.Values, reqID string) (any, error) {
	filters := parseEC2Filters(vals)
	minHours, _ := strconv.ParseInt(vals.Get("MinSlotDurationInHours"), 10, 32)
	maxHours, _ := strconv.ParseInt(vals.Get("MaxSlotDurationInHours"), 10, 32)

	entries := h.Backend.DescribeScheduledInstanceAvailability(filters, int32(minHours), int32(maxHours))

	resp := &describeScheduledInstanceAvailabilityResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, e := range entries {
		resp.ScheduledInstanceAvailabilitySet.Items = append(
			resp.ScheduledInstanceAvailabilitySet.Items, toScheduledInstanceAvailabilityItem(e),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeScheduledInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ScheduledInstanceId")
	instances := h.Backend.DescribeScheduledInstances(ids)

	resp := &describeScheduledInstancesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, s := range instances {
		resp.ScheduledInstanceSet.Items = append(resp.ScheduledInstanceSet.Items, toScheduledInstanceItem(s))
	}

	return resp, nil
}

// parseScheduledInstancePurchaseRequests parses the "PurchaseRequest.N.*" indexed form
// values accepted by PurchaseScheduledInstances.
func parseScheduledInstancePurchaseRequests(vals url.Values) []ScheduledInstancePurchaseRequest {
	var out []ScheduledInstancePurchaseRequest

	for i := 1; ; i++ {
		prefix := "PurchaseRequest." + strconv.Itoa(i)

		token := vals.Get(prefix + ".PurchaseToken")
		if token == "" {
			break
		}

		count, _ := strconv.ParseInt(vals.Get(prefix+".InstanceCount"), 10, 32)
		out = append(out, ScheduledInstancePurchaseRequest{PurchaseToken: token, InstanceCount: int32(count)})
	}

	return out
}

func (h *Handler) handlePurchaseScheduledInstances(vals url.Values, reqID string) (any, error) {
	requests := parseScheduledInstancePurchaseRequests(vals)

	purchased, err := h.Backend.PurchaseScheduledInstances(requests)
	if err != nil {
		return nil, err
	}

	resp := &purchaseScheduledInstancesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, s := range purchased {
		resp.ScheduledInstanceSet.Items = append(resp.ScheduledInstanceSet.Items, toScheduledInstanceItem(s))
	}

	return resp, nil
}

func (h *Handler) handleRunScheduledInstances(vals url.Values, reqID string) (any, error) {
	scheduledInstanceID := vals.Get("ScheduledInstanceId")
	instanceCount, _ := strconv.ParseInt(vals.Get("InstanceCount"), 10, 32)
	imageID := vals.Get("LaunchSpecification.ImageId")
	keyName := vals.Get("LaunchSpecification.KeyName")

	ids, err := h.Backend.RunScheduledInstances(scheduledInstanceID, imageID, keyName, int32(instanceCount))
	if err != nil {
		return nil, err
	}

	resp := &runScheduledInstancesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, id := range ids {
		resp.InstanceIDSet.Items = append(resp.InstanceIDSet.Items, struct {
			InstanceID string `xml:"instanceId,omitempty"`
		}{InstanceID: id})
	}

	return resp, nil
}
