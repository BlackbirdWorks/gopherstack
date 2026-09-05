package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

type consoleOutputResponse struct {
	XMLName    xml.Name `xml:"GetConsoleOutputResponse"`
	RequestID  string   `xml:"requestId"`
	InstanceID string   `xml:"instanceId"`
	Timestamp  string   `xml:"timestamp"`
	Output     string   `xml:"output"`
}

type imdsOptionsItem struct {
	State                   string `xml:"state"`
	HTTPTokens              string `xml:"httpTokens"`
	HTTPEndpoint            string `xml:"httpEndpoint"`
	InstanceMetadataTags    string `xml:"instanceMetadataTags,omitempty"`
	HTTPPutResponseHopLimit int    `xml:"httpPutResponseHopLimit"`
}

type modifyInstanceMetadataOptionsResponse struct {
	XMLName    xml.Name        `xml:"ModifyInstanceMetadataOptionsResponse"`
	RequestID  string          `xml:"requestId"`
	InstanceID string          `xml:"instanceId"`
	Options    imdsOptionsItem `xml:"instanceMetadataOptions"`
}

type instanceMetadataDefaultsResponse struct {
	XMLName      xml.Name `xml:"GetInstanceMetadataDefaultsResponse"`
	RequestID    string   `xml:"requestId"`
	AccountLevel struct {
		HTTPTokens              string `xml:"httpTokens,omitempty"`
		HTTPEndpoint            string `xml:"httpEndpoint,omitempty"`
		InstanceMetadataTags    string `xml:"instanceMetadataTags,omitempty"`
		HTTPPutResponseHopLimit int    `xml:"httpPutResponseHopLimit,omitempty"`
	} `xml:"accountLevel"`
}

type instanceCreditSpecItem struct {
	InstanceID string `xml:"instanceId"`
	CPUCredits string `xml:"cpuCredits"`
}

type describeInstanceCreditSpecsResponse struct {
	XMLName                        xml.Name `xml:"DescribeInstanceCreditSpecificationsResponse"`
	RequestID                      string   `xml:"requestId"`
	NextToken                      string   `xml:"nextToken,omitempty"`
	InstanceCreditSpecificationSet struct {
		Items []instanceCreditSpecItem `xml:"item"`
	} `xml:"instanceCreditSpecificationSet"`
}

type unsuccessfulInstanceCreditSpecItem struct {
	InstanceID string `xml:"instanceId"`
	Error      struct {
		Code    string `xml:"code"`
		Message string `xml:"message"`
	} `xml:"error"`
}

type modifyInstanceCreditSpecResponse struct {
	XMLName                                  xml.Name `xml:"ModifyInstanceCreditSpecificationResponse"`
	RequestID                                string   `xml:"requestId"`
	SuccessfulInstanceCreditSpecificationSet struct {
		Items []instanceCreditSpecItem `xml:"item"`
	} `xml:"successfulInstanceCreditSpecificationSet"`
	UnsuccessfulInstanceCreditSpecificationSet struct {
		Items []unsuccessfulInstanceCreditSpecItem `xml:"item"`
	} `xml:"unsuccessfulInstanceCreditSpecificationSet"`
}

type instanceTopologyItem struct {
	InstanceID       string `xml:"instanceId"`
	InstanceType     string `xml:"instanceType"`
	GroupName        string `xml:"groupName,omitempty"`
	AvailabilityZone string `xml:"availabilityZone"`
	ZoneID           string `xml:"zoneId"`
	NetworkNodeSet   struct {
		Items []string `xml:"item"`
	} `xml:"networkNodeSet"`
}

type describeInstanceTopologyResponse struct {
	XMLName     xml.Name `xml:"DescribeInstanceTopologyResponse"`
	RequestID   string   `xml:"requestId"`
	NextToken   string   `xml:"nextToken,omitempty"`
	InstanceSet struct {
		Items []instanceTopologyItem `xml:"item"`
	} `xml:"instanceSet"`
}

type instanceMonitoringItem struct {
	InstanceID string `xml:"instanceId"`
	Monitoring struct {
		State string `xml:"state"`
	} `xml:"monitoring"`
}

type monitorInstancesResponse struct {
	XMLName      xml.Name `xml:"MonitorInstancesResponse"`
	RequestID    string   `xml:"requestId"`
	InstancesSet struct {
		Items []instanceMonitoringItem `xml:"item"`
	} `xml:"instancesSet"`
}

type unmonitorInstancesResponse struct {
	XMLName      xml.Name `xml:"UnmonitorInstancesResponse"`
	RequestID    string   `xml:"requestId"`
	InstancesSet struct {
		Items []instanceMonitoringItem `xml:"item"`
	} `xml:"instancesSet"`
}

func (h *Handler) handleGetConsoleOutput(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	output, ts, err := h.Backend.GetConsoleOutput(instanceID)
	if err != nil {
		return nil, err
	}

	return &consoleOutputResponse{
		RequestID:  reqID,
		InstanceID: instanceID,
		Timestamp:  ts.UTC().Format("2006-01-02T15:04:05.000Z"),
		Output:     output,
	}, nil
}

func (h *Handler) handleModifyInstanceMetadataOptions(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	httpTokens := vals.Get("HttpTokens")
	httpEndpoint := vals.Get("HttpEndpoint")
	instanceMetadataTags := vals.Get("InstanceMetadataTags")
	hopLimit, _ := strconv.Atoi(vals.Get("HttpPutResponseHopLimit"))

	opts, err := h.Backend.ModifyInstanceMetadataOptions(
		instanceID,
		httpTokens,
		httpEndpoint,
		instanceMetadataTags,
		hopLimit,
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceMetadataOptionsResponse{
		RequestID:  reqID,
		InstanceID: instanceID,
		Options: imdsOptionsItem{
			State:                   opts.State,
			HTTPTokens:              opts.HTTPTokens,
			HTTPPutResponseHopLimit: opts.HTTPPutResponseHopLimit,
			HTTPEndpoint:            opts.HTTPEndpoint,
			InstanceMetadataTags:    opts.InstanceMetadataTags,
		},
	}, nil
}

func (h *Handler) handleGetInstanceMetadataDefaults(_ url.Values, reqID string) (any, error) {
	d := h.Backend.GetInstanceMetadataDefaults()
	resp := &instanceMetadataDefaultsResponse{RequestID: reqID}
	resp.AccountLevel.HTTPTokens = d.HTTPTokens
	resp.AccountLevel.HTTPEndpoint = d.HTTPEndpoint
	resp.AccountLevel.HTTPPutResponseHopLimit = d.HTTPPutResponseHopLimit
	resp.AccountLevel.InstanceMetadataTags = d.InstanceMetadataTags

	return resp, nil
}

func (h *Handler) handleModifyInstanceMetadataDefaults(vals url.Values, reqID string) (any, error) {
	httpTokens := vals.Get("HttpTokens")
	httpEndpoint := vals.Get("HttpEndpoint")
	instanceMetadataTags := vals.Get("InstanceMetadataTags")
	hopLimit, _ := strconv.Atoi(vals.Get("HttpPutResponseHopLimit"))
	if err := h.Backend.ModifyInstanceMetadataDefaults(
		httpTokens, httpEndpoint, instanceMetadataTags, hopLimit,
	); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceMetadataDefaultsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeInstanceCreditSpecifications(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	specs := h.Backend.DescribeInstanceCreditSpecifications(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	specs, nextToken = pageSlice(specs, offset, maxResults)

	resp := &describeInstanceCreditSpecsResponse{RequestID: reqID, NextToken: nextToken}
	for _, s := range specs {
		resp.InstanceCreditSpecificationSet.Items = append(
			resp.InstanceCreditSpecificationSet.Items,
			instanceCreditSpecItem(s),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyInstanceCreditSpecification(
	vals url.Values,
	reqID string,
) (any, error) {
	var specs []InstanceCreditSpec
	for i := 1; ; i++ {
		instanceID := vals.Get(fmt.Sprintf("InstanceCreditSpecification.%d.InstanceId", i))
		if instanceID == "" {
			break
		}
		specs = append(specs, InstanceCreditSpec{
			InstanceID: instanceID,
			CPUCredits: vals.Get(fmt.Sprintf("InstanceCreditSpecification.%d.CpuCredits", i)),
		})
	}

	if len(specs) == 0 {
		return nil, fmt.Errorf(
			"%w: InstanceCreditSpecification is required", ErrInvalidParameter,
		)
	}

	successful, unsuccessful := h.Backend.ModifyInstanceCreditSpecification(specs)

	resp := &modifyInstanceCreditSpecResponse{RequestID: reqID}
	for _, s := range successful {
		resp.SuccessfulInstanceCreditSpecificationSet.Items = append(
			resp.SuccessfulInstanceCreditSpecificationSet.Items,
			instanceCreditSpecItem(s),
		)
	}
	for _, s := range unsuccessful {
		item := unsuccessfulInstanceCreditSpecItem{InstanceID: s.InstanceID}
		item.Error.Code = ErrInstanceNotFound.Error()
		item.Error.Message = fmt.Sprintf("The instance ID '%s' does not exist", s.InstanceID)
		resp.UnsuccessfulInstanceCreditSpecificationSet.Items = append(
			resp.UnsuccessfulInstanceCreditSpecificationSet.Items,
			item,
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeInstanceTopology(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	items := h.Backend.DescribeInstanceTopology(ids)

	maxResults, offset, err := parseEC2Pagination(
		vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageDefaultInstanceTopology,
	)
	if err != nil {
		return nil, err
	}

	var nextToken string
	items, nextToken = pageSlice(items, offset, maxResults)

	resp := &describeInstanceTopologyResponse{RequestID: reqID, NextToken: nextToken}
	for _, item := range items {
		ti := instanceTopologyItem{
			InstanceID:       item.InstanceID,
			InstanceType:     item.InstanceType,
			GroupName:        item.GroupName,
			AvailabilityZone: item.AvailabilityZone,
			ZoneID:           item.ZoneID,
		}
		ti.NetworkNodeSet.Items = item.NetworkNodes
		resp.InstanceSet.Items = append(resp.InstanceSet.Items, ti)
	}

	return resp, nil
}

func (h *Handler) handleMonitorInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	states, err := h.Backend.MonitorInstances(ids)
	if err != nil {
		return nil, err
	}
	resp := &monitorInstancesResponse{RequestID: reqID}
	for _, s := range states {
		item := instanceMonitoringItem{InstanceID: s.InstanceID}
		item.Monitoring.State = s.State
		resp.InstancesSet.Items = append(resp.InstancesSet.Items, item)
	}

	return resp, nil
}

func (h *Handler) handleUnmonitorInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	states, err := h.Backend.UnmonitorInstances(ids)
	if err != nil {
		return nil, err
	}
	resp := &unmonitorInstancesResponse{RequestID: reqID}
	for _, s := range states {
		item := instanceMonitoringItem{InstanceID: s.InstanceID}
		item.Monitoring.State = s.State
		resp.InstancesSet.Items = append(resp.InstancesSet.Items, item)
	}

	return resp, nil
}

// serialConsoleAccessStatusResponse is shared by Get/Enable/DisableSerialConsoleAccess,
// which all share this exact shape. XMLName carries no tag -- three
// different ops render this struct under three different root element
// names, and a tag here would always win over a runtime-set XMLName value
// (encoding/xml: a tagged XMLName field's tag is used unconditionally by
// Marshal, ignoring the field's value), silently forcing every response to
// the tag's single hardcoded name.
type serialConsoleAccessStatusResponse struct {
	XMLName                    xml.Name
	RequestID                  string `xml:"requestId"`
	SerialConsoleAccessEnabled bool   `xml:"serialConsoleAccessEnabled"`
}

type defaultCreditSpecificationResponse struct {
	XMLName                           xml.Name `xml:"GetDefaultCreditSpecificationResponse"`
	RequestID                         string   `xml:"requestId"`
	InstanceFamilyCreditSpecification struct {
		CPUCredits     string `xml:"cpuCredits"`
		InstanceFamily string `xml:"instanceFamily"`
	} `xml:"instanceFamilyCreditSpecification"`
}

type replaceRootVolumeTaskItem struct {
	ReplaceRootVolumeTaskID string `xml:"replaceRootVolumeTaskId"`
	InstanceID              string `xml:"instanceId"`
	TaskState               string `xml:"taskState"`
	StartTime               string `xml:"startTime"`
	CompleteTime            string `xml:"completeTime,omitempty"`
	SnapshotID              string `xml:"snapshotId,omitempty"`
}

// handleEnableSerialConsoleAccess and handleDisableSerialConsoleAccess:
// EnableSerialConsoleAccessOutput/DisableSerialConsoleAccessOutput both have
// no Return member at all -- only SerialConsoleAccessEnabled (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeOpDocumentEnableSerialConsoleAccessOutput
// has no case for "return"), the same shape GetSerialConsoleAccessStatus
// already renders correctly.
func (h *Handler) handleEnableSerialConsoleAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.EnableSerialConsoleAccess()

	return &serialConsoleAccessStatusResponse{
		XMLName:                    xml.Name{Local: "EnableSerialConsoleAccessResponse"},
		RequestID:                  reqID,
		SerialConsoleAccessEnabled: h.Backend.GetSerialConsoleAccessStatus(),
	}, nil
}

func (h *Handler) handleDisableSerialConsoleAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableSerialConsoleAccess()

	return &serialConsoleAccessStatusResponse{
		XMLName:                    xml.Name{Local: "DisableSerialConsoleAccessResponse"},
		RequestID:                  reqID,
		SerialConsoleAccessEnabled: h.Backend.GetSerialConsoleAccessStatus(),
	}, nil
}

func (h *Handler) handleGetSerialConsoleAccessStatus(_ url.Values, reqID string) (any, error) {
	return &serialConsoleAccessStatusResponse{
		XMLName:                    xml.Name{Local: "GetSerialConsoleAccessStatusResponse"},
		RequestID:                  reqID,
		SerialConsoleAccessEnabled: h.Backend.GetSerialConsoleAccessStatus(),
	}, nil
}

func (h *Handler) handleGetDefaultCreditSpecification(vals url.Values, reqID string) (any, error) {
	family := vals.Get("InstanceFamily")
	if family == "" {
		family = "t3"
	}
	resp := &defaultCreditSpecificationResponse{RequestID: reqID}
	resp.InstanceFamilyCreditSpecification.CPUCredits = h.Backend.GetDefaultCreditSpecification()
	resp.InstanceFamilyCreditSpecification.InstanceFamily = family

	return resp, nil
}

func (h *Handler) handleModifyDefaultCreditSpecification(
	vals url.Values,
	reqID string,
) (any, error) {
	cpuCredits := vals.Get("CpuCredits")
	family := vals.Get("InstanceFamily")
	if family == "" {
		family = "t3"
	}
	if err := h.Backend.ModifyDefaultCreditSpecification(cpuCredits); err != nil {
		return nil, err
	}

	resp := &defaultCreditSpecificationResponse{RequestID: reqID}
	resp.InstanceFamilyCreditSpecification.CPUCredits = cpuCredits
	resp.InstanceFamilyCreditSpecification.InstanceFamily = family

	return resp, nil
}

type createInstanceConnectEndpointResponse struct {
	XMLName                 xml.Name                    `xml:"CreateInstanceConnectEndpointResponse"`
	RequestID               string                      `xml:"requestId"`
	InstanceConnectEndpoint instanceConnectEndpointItem `xml:"instanceConnectEndpoint"`
}

type describeInstanceConnectEndpointsResponse struct {
	XMLName                    xml.Name `xml:"DescribeInstanceConnectEndpointsResponse"`
	RequestID                  string   `xml:"requestId"`
	NextToken                  string   `xml:"nextToken,omitempty"`
	InstanceConnectEndpointSet struct {
		Items []instanceConnectEndpointItem `xml:"item"`
	} `xml:"instanceConnectEndpointSet"`
}

type instanceEventWindowAssociationTargetItem struct {
	InstanceIDs      []string `xml:"instanceIdSet>item,omitempty"`
	DedicatedHostIDs []string `xml:"dedicatedHostIdSet>item,omitempty"`
}

type instanceEventWindowItem struct {
	InstanceEventWindowID string                                   `xml:"instanceEventWindowId"`
	Name                  string                                   `xml:"name"`
	CronExpression        string                                   `xml:"cronExpression,omitempty"`
	State                 string                                   `xml:"state"`
	AssociationTarget     instanceEventWindowAssociationTargetItem `xml:"associationTarget"`
}

type createInstanceEventWindowResponse struct {
	XMLName             xml.Name                `xml:"CreateInstanceEventWindowResponse"`
	RequestID           string                  `xml:"requestId"`
	InstanceEventWindow instanceEventWindowItem `xml:"instanceEventWindow"`
}

type describeInstanceEventWindowsResponse struct {
	XMLName                xml.Name `xml:"DescribeInstanceEventWindowsResponse"`
	RequestID              string   `xml:"requestId"`
	NextToken              string   `xml:"nextToken,omitempty"`
	InstanceEventWindowSet struct {
		Items []instanceEventWindowItem `xml:"item"`
	} `xml:"instanceEventWindowSet"`
}

type spotDatafeedItem struct {
	Bucket string `xml:"bucket"`
	Prefix string `xml:"prefix"`
	State  string `xml:"state"`
}

type passwordDataResponse struct {
	XMLName      xml.Name `xml:"GetPasswordDataResponse"`
	RequestID    string   `xml:"requestId"`
	InstanceID   string   `xml:"instanceId"`
	Timestamp    string   `xml:"timestamp"`
	PasswordData string   `xml:"passwordData"`
}

type consoleScreenshotResponse struct {
	XMLName    xml.Name `xml:"GetConsoleScreenshotResponse"`
	RequestID  string   `xml:"requestId"`
	InstanceID string   `xml:"instanceId"`
	ImageData  string   `xml:"imageData"`
}

type instanceTypeOfferingItem2 struct {
	InstanceType string `xml:"instanceType"`
}

type getInstanceTypesFromReqsResponse struct {
	XMLName         xml.Name `xml:"GetInstanceTypesFromInstanceRequirementsResponse"`
	RequestID       string   `xml:"requestId"`
	InstanceTypeSet struct {
		Items []instanceTypeOfferingItem2 `xml:"item"`
	} `xml:"instanceTypeSet"`
}

func toInstanceConnectEndpointItem(ep *InstanceConnectEndpoint) instanceConnectEndpointItem {
	return instanceConnectEndpointItem{
		InstanceConnectEndpointID: ep.InstanceConnectEndpointID,
		SubnetID:                  ep.SubnetID,
		VPCID:                     ep.VPCID,
		State:                     ep.State,
		PreserveClientIP:          ep.PreserveClientIP,
	}
}

func (h *Handler) handleCreateInstanceConnectEndpoint(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	sgIDs := parseMemberList(vals, "SecurityGroupId")
	preserveClientIP := vals.Get("PreserveClientIp") == ec2BooleanTrue

	ep, err := h.Backend.CreateInstanceConnectEndpoint(subnetID, sgIDs, preserveClientIP)
	if err != nil {
		return nil, err
	}

	return &createInstanceConnectEndpointResponse{
		RequestID:               reqID,
		InstanceConnectEndpoint: toInstanceConnectEndpointItem(ep),
	}, nil
}

type deleteInstanceConnectEndpointResponse struct {
	XMLName                 xml.Name                    `xml:"DeleteInstanceConnectEndpointResponse"`
	RequestID               string                      `xml:"requestId"`
	InstanceConnectEndpoint instanceConnectEndpointItem `xml:"instanceConnectEndpoint"`
}

func (h *Handler) handleDeleteInstanceConnectEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InstanceConnectEndpointId")

	ep, err := h.Backend.DeleteInstanceConnectEndpoint(id)
	if err != nil {
		return nil, err
	}

	return &deleteInstanceConnectEndpointResponse{
		RequestID:               reqID,
		InstanceConnectEndpoint: toInstanceConnectEndpointItem(ep),
	}, nil
}

func (h *Handler) handleDescribeInstanceConnectEndpoints(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "InstanceConnectEndpointId")
	eps := h.Backend.DescribeInstanceConnectEndpoints(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	eps, nextToken = pageSlice(eps, offset, maxResults)

	resp := &describeInstanceConnectEndpointsResponse{RequestID: reqID, NextToken: nextToken}
	for _, ep := range eps {
		resp.InstanceConnectEndpointSet.Items = append(
			resp.InstanceConnectEndpointSet.Items,
			toInstanceConnectEndpointItem(ep),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyInstanceConnectEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InstanceConnectEndpointId")
	preserveClientIP := vals.Get("PreserveClientIp") == ec2BooleanTrue
	if err := h.Backend.ModifyInstanceConnectEndpoint(id, preserveClientIP); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceConnectEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func toInstanceEventWindowItem(ew *InstanceEventWindow) instanceEventWindowItem {
	return instanceEventWindowItem{
		InstanceEventWindowID: ew.InstanceEventWindowID,
		Name:                  ew.Name,
		CronExpression:        ew.CronExpression,
		State:                 ew.State,
		AssociationTarget: instanceEventWindowAssociationTargetItem{
			InstanceIDs:      ew.InstanceIDs,
			DedicatedHostIDs: ew.DedicatedHostIDs,
		},
	}
}

func (h *Handler) handleCreateInstanceEventWindow(vals url.Values, reqID string) (any, error) {
	name := vals.Get("Name")
	cron := vals.Get("CronExpression")

	ew, err := h.Backend.CreateInstanceEventWindow(name, cron)
	if err != nil {
		return nil, err
	}

	return &createInstanceEventWindowResponse{
		RequestID:           reqID,
		InstanceEventWindow: toInstanceEventWindowItem(ew),
	}, nil
}

type instanceEventWindowStateItem struct {
	InstanceEventWindowID string `xml:"instanceEventWindowId"`
	State                 string `xml:"state"`
}

type deleteInstanceEventWindowResponse struct {
	XMLName                  xml.Name                     `xml:"DeleteInstanceEventWindowResponse"`
	RequestID                string                       `xml:"requestId"`
	InstanceEventWindowState instanceEventWindowStateItem `xml:"instanceEventWindowState"`
}

func (h *Handler) handleDeleteInstanceEventWindow(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InstanceEventWindowId")
	if err := h.Backend.DeleteInstanceEventWindow(id); err != nil {
		return nil, err
	}

	return &deleteInstanceEventWindowResponse{
		RequestID: reqID,
		InstanceEventWindowState: instanceEventWindowStateItem{
			InstanceEventWindowID: id,
			State:                 stateDeleting,
		},
	}, nil
}

func (h *Handler) handleDescribeInstanceEventWindows(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceEventWindowId")
	ews := h.Backend.DescribeInstanceEventWindows(ids)

	maxResults, offset, err := parseEC2Pagination(
		vals, ec2PageMinEventWindows, ec2PageMaxEventWindows, ec2PageMaxEventWindows,
	)
	if err != nil {
		return nil, err
	}

	var nextToken string
	ews, nextToken = pageSlice(ews, offset, maxResults)

	resp := &describeInstanceEventWindowsResponse{RequestID: reqID, NextToken: nextToken}
	for _, ew := range ews {
		resp.InstanceEventWindowSet.Items = append(
			resp.InstanceEventWindowSet.Items,
			toInstanceEventWindowItem(ew),
		)
	}

	return resp, nil
}

type modifyInstanceEventWindowResponse struct {
	XMLName             xml.Name                `xml:"ModifyInstanceEventWindowResponse"`
	RequestID           string                  `xml:"requestId"`
	InstanceEventWindow instanceEventWindowItem `xml:"instanceEventWindow"`
}

func (h *Handler) handleModifyInstanceEventWindow(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InstanceEventWindowId")
	name := vals.Get("Name")
	cron := vals.Get("CronExpression")

	ew, err := h.Backend.ModifyInstanceEventWindow(id, name, cron)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceEventWindowResponse{
		RequestID:           reqID,
		InstanceEventWindow: toInstanceEventWindowItem(ew),
	}, nil
}

func (h *Handler) handleGetPasswordData(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	data, ts, err := h.Backend.GetPasswordData(instanceID)
	if err != nil {
		return nil, err
	}

	return &passwordDataResponse{
		RequestID:    reqID,
		InstanceID:   instanceID,
		Timestamp:    ts.UTC().Format("2006-01-02T15:04:05.000Z"),
		PasswordData: data,
	}, nil
}

func (h *Handler) handleGetConsoleScreenshot(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	data, err := h.Backend.GetConsoleScreenshot(instanceID)
	if err != nil {
		return nil, err
	}

	return &consoleScreenshotResponse{
		RequestID:  reqID,
		InstanceID: instanceID,
		ImageData:  data,
	}, nil
}

func (h *Handler) handleGetInstanceTypesFromInstanceRequirements(
	_ url.Values,
	reqID string,
) (any, error) {
	types := h.Backend.GetInstanceTypesFromInstanceRequirements()

	resp := &getInstanceTypesFromReqsResponse{RequestID: reqID}
	for _, t := range types {
		resp.InstanceTypeSet.Items = append(
			resp.InstanceTypeSet.Items,
			instanceTypeOfferingItem2{InstanceType: t},
		)
	}

	return resp, nil
}

func (h *Handler) handleReportInstanceStatus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}
	status := vals.Get("Status")
	description := vals.Get("Description")
	if err := h.Backend.ReportInstanceStatus(ids, status, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ReportInstanceStatusResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type describeInstanceTypeOfferingsResponse struct {
	XMLName                 xml.Name `xml:"DescribeInstanceTypeOfferingsResponse"`
	RequestID               string   `xml:"requestId"`
	InstanceTypeOfferingSet struct {
		Items []instanceTypeOfferingItem `xml:"item"`
	} `xml:"instanceTypeOfferingSet"`
}

// applyInstanceTypeOfferingFilters filters offerings by the real "instance-type"
// and "location" filter names (ec2@v1.319.1 api_op_DescribeInstanceTypeOfferings.go
// DescribeInstanceTypeOfferingsInput.Filters doc comment).
func applyInstanceTypeOfferingFilters(
	offerings []InstanceTypeOffering,
	filters map[string][]string,
) []InstanceTypeOffering {
	if len(filters) == 0 {
		return offerings
	}

	out := make([]InstanceTypeOffering, 0, len(offerings))
	for _, o := range offerings {
		if vals, ok := filters[filterKeyInstanceType]; ok && !anyEqual(o.InstanceType, vals) {
			continue
		}
		if vals, ok := filters["location"]; ok && !anyEqual(o.Location, vals) {
			continue
		}
		out = append(out, o)
	}

	return out
}

func (h *Handler) handleDescribeInstanceTypeOfferings(vals url.Values, reqID string) (any, error) {
	resp := &describeInstanceTypeOfferingsResponse{RequestID: reqID}

	// This backend only ever generates availability-zone offerings; an explicit
	// request for another real LocationType (region/availability-zone-id/outpost)
	// honestly has none, rather than fabricating a match.
	if lt := vals.Get("LocationType"); lt != "" && lt != filterKeyAvailabilityZone {
		return resp, nil
	}

	offerings := h.Backend.DescribeInstanceTypeOfferings()
	offerings = applyInstanceTypeOfferingFilters(offerings, parseEC2Filters(vals))

	for _, o := range offerings {
		resp.InstanceTypeOfferingSet.Items = append(
			resp.InstanceTypeOfferingSet.Items,
			instanceTypeOfferingItem{
				InstanceType: o.InstanceType,
				Location:     o.Location,
				LocationType: o.LocationType,
			},
		)
	}

	return resp, nil
}

type sendDiagnosticInterruptResponse struct {
	XMLName   xml.Name `xml:"SendDiagnosticInterruptResponse"`
	RequestID string   `xml:"requestId"`
}

func (h *Handler) handleSendDiagnosticInterrupt(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")

	if err := h.Backend.SendDiagnosticInterrupt(instanceID); err != nil {
		return nil, err
	}

	return &sendDiagnosticInterruptResponse{RequestID: reqID}, nil
}

type describeElasticGpusResponse struct {
	XMLName       xml.Name `xml:"DescribeElasticGpusResponse"`
	RequestID     string   `xml:"requestId"`
	NextToken     string   `xml:"nextToken,omitempty"`
	ElasticGpuSet struct {
		Items []elasticGpuItem `xml:"item"`
	} `xml:"elasticGpuSet"`
}

func (h *Handler) handleDescribeElasticGpus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ElasticGpuId")

	gpus := h.Backend.DescribeElasticGpus(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinElasticGpus, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	gpus, nextToken = pageSlice(gpus, offset, maxResults)

	resp := &describeElasticGpusResponse{RequestID: reqID, NextToken: nextToken}
	for _, g := range gpus {
		resp.ElasticGpuSet.Items = append(resp.ElasticGpuSet.Items, elasticGpuItem{
			ElasticGpuID:   g.ElasticGpuID,
			InstanceID:     g.InstanceID,
			ElasticGpuType: g.ElasticGpuType,
		})
	}

	return resp, nil
}

// registerInstancesOps registers the Instances operation handlers.
func registerInstancesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["GetConsoleOutput"] = h.handleGetConsoleOutput
	ops["ModifyInstanceMetadataOptions"] = h.handleModifyInstanceMetadataOptions
	ops["GetInstanceMetadataDefaults"] = h.handleGetInstanceMetadataDefaults
	ops["ModifyInstanceMetadataDefaults"] = h.handleModifyInstanceMetadataDefaults
	ops["DescribeInstanceCreditSpecifications"] = h.handleDescribeInstanceCreditSpecifications
	ops["ModifyInstanceCreditSpecification"] = h.handleModifyInstanceCreditSpecification
	ops["DescribeInstanceTopology"] = h.handleDescribeInstanceTopology
	ops["MonitorInstances"] = h.handleMonitorInstances
	ops["UnmonitorInstances"] = h.handleUnmonitorInstances
	ops["EnableSerialConsoleAccess"] = h.handleEnableSerialConsoleAccess
	ops["DisableSerialConsoleAccess"] = h.handleDisableSerialConsoleAccess
	ops["GetSerialConsoleAccessStatus"] = h.handleGetSerialConsoleAccessStatus
	ops["GetDefaultCreditSpecification"] = h.handleGetDefaultCreditSpecification
	ops["ModifyDefaultCreditSpecification"] = h.handleModifyDefaultCreditSpecification
	ops["CreateInstanceConnectEndpoint"] = h.handleCreateInstanceConnectEndpoint
	ops["DeleteInstanceConnectEndpoint"] = h.handleDeleteInstanceConnectEndpoint
	ops["DescribeInstanceConnectEndpoints"] = h.handleDescribeInstanceConnectEndpoints
	ops["ModifyInstanceConnectEndpoint"] = h.handleModifyInstanceConnectEndpoint
	ops["CreateInstanceEventWindow"] = h.handleCreateInstanceEventWindow
	ops["DeleteInstanceEventWindow"] = h.handleDeleteInstanceEventWindow
	ops["DescribeInstanceEventWindows"] = h.handleDescribeInstanceEventWindows
	ops["ModifyInstanceEventWindow"] = h.handleModifyInstanceEventWindow
	ops["GetPasswordData"] = h.handleGetPasswordData
	ops["GetConsoleScreenshot"] = h.handleGetConsoleScreenshot
	ops["GetInstanceTypesFromInstanceRequirements"] = h.handleGetInstanceTypesFromInstanceRequirements
	ops["ReportInstanceStatus"] = h.handleReportInstanceStatus
	ops["DescribeInstanceTypeOfferings"] = h.handleDescribeInstanceTypeOfferings
	ops["SendDiagnosticInterrupt"] = h.handleSendDiagnosticInterrupt
	ops["DescribeElasticGpus"] = h.handleDescribeElasticGpus
}

// instancesSupportedOperations lists the operation names registered by
// registerInstancesOps, for GetSupportedOperations().
func instancesSupportedOperations() []string {
	return []string{
		"GetConsoleOutput",
		"ModifyInstanceMetadataOptions",
		"GetInstanceMetadataDefaults",
		"ModifyInstanceMetadataDefaults",
		"DescribeInstanceCreditSpecifications",
		"ModifyInstanceCreditSpecification",
		"DescribeInstanceTopology",
		"MonitorInstances",
		"UnmonitorInstances",
		"EnableSerialConsoleAccess",
		"DisableSerialConsoleAccess",
		"GetSerialConsoleAccessStatus",
		"GetDefaultCreditSpecification",
		"ModifyDefaultCreditSpecification",
		"CreateInstanceConnectEndpoint",
		"DeleteInstanceConnectEndpoint",
		"DescribeInstanceConnectEndpoints",
		"ModifyInstanceConnectEndpoint",
		"CreateInstanceEventWindow",
		"DeleteInstanceEventWindow",
		"DescribeInstanceEventWindows",
		"ModifyInstanceEventWindow",
		"GetPasswordData",
		"GetConsoleScreenshot",
		"GetInstanceTypesFromInstanceRequirements",
		"ReportInstanceStatus",
		"DescribeInstanceTypeOfferings",
		"SendDiagnosticInterrupt",
		"DescribeElasticGpus",
	}
}

// ---- XML response types for extended operations ----

type startInstancesResponse struct {
	XMLName      xml.Name               `xml:"StartInstancesResponse"`
	Xmlns        string                 `xml:"xmlns,attr"`
	RequestID    string                 `xml:"requestId"`
	InstancesSet instanceStateChangeSet `xml:"instancesSet"`
}

type stopInstancesResponse struct {
	XMLName      xml.Name               `xml:"StopInstancesResponse"`
	Xmlns        string                 `xml:"xmlns,attr"`
	RequestID    string                 `xml:"requestId"`
	InstancesSet instanceStateChangeSet `xml:"instancesSet"`
}

type rebootInstancesResponse struct {
	XMLName   xml.Name `xml:"RebootInstancesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// instanceStatusDetail is a single reachability check detail (e.g. name
// "reachability", status "passed").
type instanceStatusDetail struct {
	Name   string `xml:"name"`
	Status string `xml:"status"`
}

// instanceStatusDetails is the health summary AWS reports for both the system
// status and the instance status. Status is "ok", "impaired", "initializing",
// "insufficient-data" or "not-applicable".
type instanceStatusDetails struct {
	Status  string                 `xml:"status"`
	Details []instanceStatusDetail `xml:"details>item"`
}

type instanceStatusItem struct {
	InstanceID     string                `xml:"instanceId"`
	AvailZone      string                `xml:"availabilityZone"`
	InstanceState  stateItem             `xml:"instanceState"`
	SystemStatus   instanceStatusDetails `xml:"systemStatus"`
	InstanceStatus instanceStatusDetails `xml:"instanceStatus"`
}

type instanceStatusSet struct {
	Items []instanceStatusItem `xml:"item"`
}

type describeInstanceStatusResponse struct {
	XMLName           xml.Name          `xml:"DescribeInstanceStatusResponse"`
	Xmlns             string            `xml:"xmlns,attr"`
	RequestID         string            `xml:"requestId"`
	InstanceStatusSet instanceStatusSet `xml:"instanceStatusSet"`
}

// ---- handler functions ----

func (h *Handler) handleStartInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	changes, err := h.Backend.StartInstances(ids)
	if err != nil {
		return nil, err
	}

	if cb, c := h.computeBackend(); c != nil {
		h.computeStartOrStop(h.svcCtx, cb, c, ids, true)
	}

	return &startInstancesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		InstancesSet: instanceStateChangeSet{Items: instanceStateChangeItemsFromChanges(changes)},
	}, nil
}

func (h *Handler) handleStopInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	changes, err := h.Backend.StopInstances(ids)
	if err != nil {
		return nil, err
	}

	if cb, c := h.computeBackend(); c != nil {
		h.computeStartOrStop(h.svcCtx, cb, c, ids, false)
	}

	return &stopInstancesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		InstancesSet: instanceStateChangeSet{Items: instanceStateChangeItemsFromChanges(changes)},
	}, nil
}

// instanceStateChangeItemsFromChanges converts backend InstanceStateChange
// values into the XML payload representation used by Start/Stop/Terminate.
func instanceStateChangeItemsFromChanges(changes []*InstanceStateChange) []instanceStateChangeItem {
	items := make([]instanceStateChangeItem, 0, len(changes))
	for _, ch := range changes {
		if ch == nil {
			continue
		}

		items = append(items, instanceStateChangeItem{
			InstanceID:    ch.InstanceID,
			CurrentState:  stateItem{Code: ch.CurrentState.Code, Name: ch.CurrentState.Name},
			PreviousState: stateItem{Code: ch.PreviousState.Code, Name: ch.PreviousState.Name},
		})
	}

	return items
}

func (h *Handler) handleRebootInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	if err := h.Backend.RebootInstances(ids); err != nil {
		return nil, err
	}

	return &rebootInstancesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleDescribeInstanceStatus mirrors real DescribeInstanceStatus's default
// (api_op_DescribeInstanceStatus.go): when no InstanceId is given and
// IncludeAllInstances isn't "true", only running instances are reported.
// An explicit InstanceId list is always honoured in full. IncludeManagedResources
// is documented but left unread: this backend has no concept of an
// Amazon Web Services-managed instance to hide or reveal.
func (h *Handler) handleDescribeInstanceStatus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	instances := h.Backend.DescribeInstanceStatus(ids)

	if len(ids) == 0 && vals.Get("IncludeAllInstances") != "true" {
		running := instances[:0:0]
		for _, inst := range instances {
			if inst.State.Name == declarativePoliciesReportStateRunning {
				running = append(running, inst)
			}
		}
		instances = running
	}

	instances = applyInstanceStatusFilters(instances, parseEC2Filters(vals))

	items := make([]instanceStatusItem, 0, len(instances))
	for _, inst := range instances {
		// AWS reports system/instance status as "ok" with a passed
		// reachability check for running instances; non-running instances
		// report "initializing" until they reach a steady state. This lets the
		// SDK InstanceStatusOk waiter reach its terminal state.
		health := instanceHealthForState(inst.State.Name)

		az := inst.Placement.AvailabilityZone
		if az == "" {
			az = h.Region + "a"
		}

		items = append(items, instanceStatusItem{
			InstanceID:     inst.ID,
			AvailZone:      az,
			InstanceState:  stateItem{Code: inst.State.Code, Name: inst.State.Name},
			SystemStatus:   health,
			InstanceStatus: health,
		})
	}

	return &describeInstanceStatusResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		InstanceStatusSet: instanceStatusSet{Items: items},
	}, nil
}

// instanceHealthForState returns the AWS-style status summary for an instance in
// the given lifecycle state. Running instances are healthy ("ok"); others are
// still "initializing".
func instanceHealthForState(stateName string) instanceStatusDetails {
	status := "initializing"
	reachability := "initializing"

	if stateName == "running" {
		status = "ok"
		reachability = "passed"
	}

	return instanceStatusDetails{
		Status: status,
		Details: []instanceStatusDetail{
			{Name: "reachability", Status: reachability},
		},
	}
}

const (
	describeImagesMaxResults     = 1000
	describeImagesMinResults     = 1
	describeImagesDefaultResults = 1000
)

// parseImagesPagination parses MaxResults and NextToken from query values,
// returning (maxResults, offset, error).
