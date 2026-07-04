package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- Registration ----

//nolint:funlen // large registration table
func registerBatch3Ops(h *Handler, ops map[string]ec2ActionFn) {
	// Capacity Reservation
	ops["CreateCapacityReservation"] = h.handleCreateCapacityReservation
	ops["CancelCapacityReservation"] = h.handleCancelCapacityReservation
	ops["ModifyCapacityReservation"] = h.handleModifyCapacityReservation
	ops["GetGroupsForCapacityReservation"] = h.handleGetGroupsForCapacityReservation
	// Instance Connect Endpoint
	ops["CreateInstanceConnectEndpoint"] = h.handleCreateInstanceConnectEndpoint
	ops["DeleteInstanceConnectEndpoint"] = h.handleDeleteInstanceConnectEndpoint
	ops["DescribeInstanceConnectEndpoints"] = h.handleDescribeInstanceConnectEndpoints
	ops["ModifyInstanceConnectEndpoint"] = h.handleModifyInstanceConnectEndpoint
	// Instance Event Window
	ops["CreateInstanceEventWindow"] = h.handleCreateInstanceEventWindow
	ops["DeleteInstanceEventWindow"] = h.handleDeleteInstanceEventWindow
	ops["DescribeInstanceEventWindows"] = h.handleDescribeInstanceEventWindows
	ops["ModifyInstanceEventWindow"] = h.handleModifyInstanceEventWindow
	// Spot Datafeed
	ops["CreateSpotDatafeedSubscription"] = h.handleCreateSpotDatafeedSubscription
	ops["DeleteSpotDatafeedSubscription"] = h.handleDeleteSpotDatafeedSubscription
	ops["DescribeSpotDatafeedSubscription"] = h.handleDescribeSpotDatafeedSubscription
	// Image lifecycle
	ops["RegisterImage"] = h.handleRegisterImage
	ops["ImportImage"] = h.handleImportImage
	ops["DescribeImportImageTasks"] = h.handleDescribeImportImageTasks
	ops["ExportImage"] = h.handleExportImage
	ops["DescribeExportImageTasks"] = h.handleDescribeExportImageTasks
	ops["ListImagesInRecycleBin"] = h.handleListImagesInRecycleBin
	ops["RestoreImageFromRecycleBin"] = h.handleRestoreImageFromRecycleBin
	// Snapshot recycle
	ops["ListSnapshotsInRecycleBin"] = h.handleListSnapshotsInRecycleBin
	ops["RestoreSnapshotFromRecycleBin"] = h.handleRestoreSnapshotFromRecycleBin
	ops["RestoreSnapshotTier"] = h.handleRestoreSnapshotTier
	ops["ImportSnapshot"] = h.handleImportSnapshot
	ops["DescribeImportSnapshotTasks"] = h.handleDescribeImportSnapshotTasks
	// Fast launch / fast snapshot
	ops["EnableFastLaunch"] = h.handleEnableFastLaunch
	ops["DisableFastLaunch"] = h.handleDisableFastLaunch
	ops["DescribeFastLaunchImages"] = h.handleDescribeFastLaunchImages
	ops["EnableFastSnapshotRestores"] = h.handleEnableFastSnapshotRestores
	ops["DisableFastSnapshotRestores"] = h.handleDisableFastSnapshotRestores
	ops["DescribeFastSnapshotRestores"] = h.handleDescribeFastSnapshotRestores
	// Instance utilities
	ops["GetPasswordData"] = h.handleGetPasswordData
	ops["GetConsoleScreenshot"] = h.handleGetConsoleScreenshot
	ops["GetInstanceTypesFromInstanceRequirements"] = h.handleGetInstanceTypesFromInstanceRequirements
	ops["GetSubnetCidrReservations"] = h.handleGetSubnetCidrReservations
	ops["GetSecurityGroupsForVpc"] = h.handleGetSecurityGroupsForVpc
	ops["ReplaceRoute"] = h.handleReplaceRoute
	ops["RegisterInstanceEventNotificationAttributes"] = h.handleRegisterInstanceEventNotificationAttributes
	ops["ResetEbsDefaultKmsKeyId"] = h.handleResetEbsDefaultKmsKeyID
	ops["UpdateSecurityGroupRuleDescriptionsIngress"] = h.handleUpdateSGRuleDescriptionsIngress
	ops["UpdateSecurityGroupRuleDescriptionsEgress"] = h.handleUpdateSGRuleDescriptionsEgress
	// Volume / address recycle
	ops["ListVolumesInRecycleBin"] = h.handleListVolumesInRecycleBin
	ops["RestoreVolumeFromRecycleBin"] = h.handleRestoreVolumeFromRecycleBin
	ops["RestoreAddressToClassic"] = h.handleRestoreAddressToClassic
	ops["ReportInstanceStatus"] = h.handleReportInstanceStatus
	// VPN
	ops["ModifyVpnConnection"] = h.handleModifyVpnConnection
	ops["CreateVpnConnectionRoute"] = h.handleCreateVpnConnectionRoute
	ops["DeleteVpnConnectionRoute"] = h.handleDeleteVpnConnectionRoute
	ops["DetachVpnGateway"] = h.handleDetachVpnGatewayB3
	// Transit gateway
	ops["ModifyTransitGateway"] = h.handleModifyTransitGateway
}

func batch3SupportedOperations() []string {
	return []string{
		"CreateCapacityReservation",
		"CancelCapacityReservation",
		"ModifyCapacityReservation",
		"GetGroupsForCapacityReservation",
		"CreateInstanceConnectEndpoint",
		"DeleteInstanceConnectEndpoint",
		"DescribeInstanceConnectEndpoints",
		"ModifyInstanceConnectEndpoint",
		"CreateInstanceEventWindow",
		"DeleteInstanceEventWindow",
		"DescribeInstanceEventWindows",
		"ModifyInstanceEventWindow",
		"CreateSpotDatafeedSubscription",
		"DeleteSpotDatafeedSubscription",
		"DescribeSpotDatafeedSubscription",
		"RegisterImage",
		"ImportImage",
		"DescribeImportImageTasks",
		"ExportImage",
		"DescribeExportImageTasks",
		"ListImagesInRecycleBin",
		"RestoreImageFromRecycleBin",
		"ListSnapshotsInRecycleBin",
		"RestoreSnapshotFromRecycleBin",
		"RestoreSnapshotTier",
		"ImportSnapshot",
		"DescribeImportSnapshotTasks",
		"EnableFastLaunch",
		"DisableFastLaunch",
		"DescribeFastLaunchImages",
		"EnableFastSnapshotRestores",
		"DisableFastSnapshotRestores",
		"DescribeFastSnapshotRestores",
		"GetPasswordData",
		"GetConsoleScreenshot",
		"GetInstanceTypesFromInstanceRequirements",
		"GetSubnetCidrReservations",
		"GetSecurityGroupsForVpc",
		"ReplaceRoute",
		"RegisterInstanceEventNotificationAttributes",
		"ResetEbsDefaultKmsKeyId",
		"UpdateSecurityGroupRuleDescriptionsIngress",
		"UpdateSecurityGroupRuleDescriptionsEgress",
		"ListVolumesInRecycleBin",
		"RestoreVolumeFromRecycleBin",
		"RestoreAddressToClassic",
		"ReportInstanceStatus",
		"ModifyVpnConnection",
		"CreateVpnConnectionRoute",
		"DeleteVpnConnectionRoute",
		"ModifyTransitGateway",
	}
}

// ---- Response types ----

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

type createInstanceConnectEndpointResponse struct {
	XMLName                 xml.Name                    `xml:"CreateInstanceConnectEndpointResponse"`
	RequestID               string                      `xml:"requestId"`
	InstanceConnectEndpoint instanceConnectEndpointItem `xml:"instanceConnectEndpoint"`
}

type describeInstanceConnectEndpointsResponse struct {
	XMLName                    xml.Name `xml:"DescribeInstanceConnectEndpointsResponse"`
	RequestID                  string   `xml:"requestId"`
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
	InstanceEventWindowSet struct {
		Items []instanceEventWindowItem `xml:"item"`
	} `xml:"instanceEventWindowSet"`
}

type spotDatafeedItem struct {
	Bucket string `xml:"bucket"`
	Prefix string `xml:"prefix"`
	State  string `xml:"state"`
}

type createSpotDatafeedResponse struct {
	XMLName                  xml.Name         `xml:"CreateSpotDatafeedSubscriptionResponse"`
	RequestID                string           `xml:"requestId"`
	SpotDatafeedSubscription spotDatafeedItem `xml:"spotDatafeedSubscription"`
}

type describeSpotDatafeedResponse struct {
	XMLName                  xml.Name         `xml:"DescribeSpotDatafeedSubscriptionResponse"`
	RequestID                string           `xml:"requestId"`
	SpotDatafeedSubscription spotDatafeedItem `xml:"spotDatafeedSubscription"`
}

type importImageTaskItem struct {
	ImportTaskID string `xml:"importTaskId"`
	Description  string `xml:"description"`
	Architecture string `xml:"architecture,omitempty"`
	Platform     string `xml:"platform,omitempty"`
	Status       string `xml:"status"`
}

type importImageResponse struct {
	XMLName      xml.Name `xml:"ImportImageResponse"`
	RequestID    string   `xml:"requestId"`
	ImportTaskID string   `xml:"importTaskId"`
	Status       string   `xml:"status"`
}

type describeImportImageTasksResponse struct {
	XMLName            xml.Name `xml:"DescribeImportImageTasksResponse"`
	RequestID          string   `xml:"requestId"`
	ImportImageTaskSet struct {
		Items []importImageTaskItem `xml:"item"`
	} `xml:"importImageTaskSet"`
}

type exportTaskS3LocationItem struct {
	S3Bucket string `xml:"s3Bucket,omitempty"`
	S3Prefix string `xml:"s3Prefix,omitempty"`
}

type exportImageResponse struct {
	XMLName           xml.Name                 `xml:"ExportImageResponse"`
	RequestID         string                   `xml:"requestId"`
	Description       string                   `xml:"description,omitempty"`
	DiskImageFormat   string                   `xml:"diskImageFormat,omitempty"`
	ExportImageTaskID string                   `xml:"exportImageTaskId,omitempty"`
	ImageID           string                   `xml:"imageId,omitempty"`
	Progress          string                   `xml:"progress,omitempty"`
	S3ExportLocation  exportTaskS3LocationItem `xml:"s3ExportLocation"`
	Status            string                   `xml:"status,omitempty"`
	StatusMessage     string                   `xml:"statusMessage,omitempty"`
	RoleName          string                   `xml:"roleName,omitempty"`
}

type exportImageTaskItem struct {
	Description       string                   `xml:"description,omitempty"`
	ExportImageTaskID string                   `xml:"exportImageTaskId,omitempty"`
	ImageID           string                   `xml:"imageId,omitempty"`
	Progress          string                   `xml:"progress,omitempty"`
	S3ExportLocation  exportTaskS3LocationItem `xml:"s3ExportLocation"`
	Status            string                   `xml:"status,omitempty"`
	StatusMessage     string                   `xml:"statusMessage,omitempty"`
}

func toExportImageTaskItem(t *ExportImageTaskRec) exportImageTaskItem {
	return exportImageTaskItem{
		Description:       t.Description,
		ExportImageTaskID: t.ExportImageTaskID,
		ImageID:           t.ImageID,
		Progress:          t.Progress,
		S3ExportLocation:  exportTaskS3LocationItem{S3Bucket: t.S3Bucket, S3Prefix: t.S3Prefix},
		Status:            t.Status,
		StatusMessage:     t.StatusMessage,
	}
}

type describeExportImageTasksResponse struct {
	XMLName            xml.Name `xml:"DescribeExportImageTasksResponse"`
	RequestID          string   `xml:"requestId"`
	ExportImageTaskSet struct {
		Items []exportImageTaskItem `xml:"item"`
	} `xml:"exportImageTaskSet"`
}

type recycleBinImageItem struct {
	ImageID string `xml:"imageId"`
	Name    string `xml:"name"`
}

type listImagesInRecycleBinResponse struct {
	XMLName   xml.Name `xml:"ListImagesInRecycleBinResponse"`
	RequestID string   `xml:"requestId"`
	ImageSet  struct {
		Items []recycleBinImageItem `xml:"item"`
	} `xml:"imageSet"`
}

type recycleBinSnapshotItem struct {
	SnapshotID string `xml:"snapshotId"`
}

type listSnapshotsInRecycleBinResponse struct {
	XMLName     xml.Name `xml:"ListSnapshotsInRecycleBinResponse"`
	RequestID   string   `xml:"requestId"`
	SnapshotSet struct {
		Items []recycleBinSnapshotItem `xml:"item"`
	} `xml:"snapshotSet"`
}

type importSnapshotTaskItem struct {
	ImportTaskID string `xml:"importTaskId"`
	Description  string `xml:"description"`
	Status       string `xml:"status"`
}

type importSnapshotResponse struct {
	XMLName      xml.Name `xml:"ImportSnapshotResponse"`
	RequestID    string   `xml:"requestId"`
	ImportTaskID string   `xml:"importTaskId"`
}

type describeImportSnapshotTasksResponse struct {
	XMLName               xml.Name `xml:"DescribeImportSnapshotTasksResponse"`
	RequestID             string   `xml:"requestId"`
	ImportSnapshotTaskSet struct {
		Items []importSnapshotTaskItem `xml:"item"`
	} `xml:"importSnapshotTaskSet"`
}

type fastLaunchImageItem struct {
	ImageID string `xml:"imageId"`
	State   string `xml:"state"`
}

type describeFastLaunchImagesResponse struct {
	XMLName            xml.Name `xml:"DescribeFastLaunchImagesResponse"`
	RequestID          string   `xml:"requestId"`
	FastLaunchImageSet struct {
		Items []fastLaunchImageItem `xml:"item"`
	} `xml:"fastLaunchImageSet"`
}

type fastSnapshotRestoreItem struct {
	SnapshotID       string `xml:"snapshotId"`
	AvailabilityZone string `xml:"availabilityZone"`
	State            string `xml:"state"`
}

type describeFastSnapshotRestoresResponse struct {
	XMLName                xml.Name `xml:"DescribeFastSnapshotRestoresResponse"`
	RequestID              string   `xml:"requestId"`
	FastSnapshotRestoreSet struct {
		Items []fastSnapshotRestoreItem `xml:"item"`
	} `xml:"fastSnapshotRestoreSet"`
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

type subnetCidrReservationItem2 struct {
	SubnetCidrReservationID string `xml:"subnetCidrReservationId"`
	SubnetID                string `xml:"subnetId"`
	Cidr                    string `xml:"cidr"`
	ReservationType         string `xml:"reservationType"`
	State                   string `xml:"state,omitempty"`
}

type getSubnetCidrReservationsResponse struct {
	XMLName                    xml.Name `xml:"GetSubnetCidrReservationsResponse"`
	RequestID                  string   `xml:"requestId"`
	SubnetIpv4CidrReservations struct {
		Items []subnetCidrReservationItem2 `xml:"item"`
	} `xml:"subnetIpv4CidrReservations"`
}

type sgForVpcItem struct {
	GroupID     string `xml:"groupId"`
	GroupName   string `xml:"groupName"`
	Description string `xml:"description"`
}

type getSecurityGroupsForVpcResponse struct {
	XMLName                xml.Name `xml:"GetSecurityGroupsForVpcResponse"`
	RequestID              string   `xml:"requestId"`
	SecurityGroupForVpcSet struct {
		Items []sgForVpcItem `xml:"item"`
	} `xml:"securityGroupForVpcSet"`
}

type recycleBinVolumeItem struct {
	VolumeID string `xml:"volumeId"`
}

type listVolumesInRecycleBinResponse struct {
	XMLName   xml.Name `xml:"ListVolumesInRecycleBinResponse"`
	RequestID string   `xml:"requestId"`
	VolumeSet struct {
		Items []recycleBinVolumeItem `xml:"item"`
	} `xml:"volumeSet"`
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

func toCapacityReservationItem(cr *CapacityReservation) capacityReservationItem {
	return capacityReservationItem{
		CapacityReservationID:  cr.CapacityReservationID,
		InstanceType:           cr.InstanceType,
		AvailabilityZone:       cr.AvailabilityZone,
		State:                  cr.State,
		TotalInstanceCount:     cr.TotalInstanceCount,
		AvailableInstanceCount: cr.AvailableInstanceCount,
	}
}

func (h *Handler) handleCreateCapacityReservation(vals url.Values, reqID string) (any, error) {
	instanceType := vals.Get("InstanceType")
	az := vals.Get("AvailabilityZone")
	count, _ := strconv.Atoi(vals.Get("InstanceCount"))
	if count == 0 {
		count = 1
	}

	cr, err := h.Backend.CreateCapacityReservation(instanceType, az, count)
	if err != nil {
		return nil, err
	}

	return &createCapacityReservationResponse{
		RequestID:           reqID,
		CapacityReservation: toCapacityReservationItem(cr),
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

func (h *Handler) handleDeleteInstanceConnectEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InstanceConnectEndpointId")
	if err := h.Backend.DeleteInstanceConnectEndpoint(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteInstanceConnectEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeInstanceConnectEndpoints(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "InstanceConnectEndpointId")
	eps := h.Backend.DescribeInstanceConnectEndpoints(ids)

	resp := &describeInstanceConnectEndpointsResponse{RequestID: reqID}
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

func (h *Handler) handleDeleteInstanceEventWindow(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InstanceEventWindowId")
	if err := h.Backend.DeleteInstanceEventWindow(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteInstanceEventWindowResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeInstanceEventWindows(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceEventWindowId")
	ews := h.Backend.DescribeInstanceEventWindows(ids)

	resp := &describeInstanceEventWindowsResponse{RequestID: reqID}
	for _, ew := range ews {
		resp.InstanceEventWindowSet.Items = append(
			resp.InstanceEventWindowSet.Items,
			toInstanceEventWindowItem(ew),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyInstanceEventWindow(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InstanceEventWindowId")
	name := vals.Get("Name")
	cron := vals.Get("CronExpression")
	if err := h.Backend.ModifyInstanceEventWindow(id, name, cron); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceEventWindowResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleCreateSpotDatafeedSubscription(vals url.Values, reqID string) (any, error) {
	bucket := vals.Get("Bucket")
	prefix := vals.Get("Prefix")

	datafeed, err := h.Backend.CreateSpotDatafeedSubscription(bucket, prefix)
	if err != nil {
		return nil, err
	}

	return &createSpotDatafeedResponse{
		RequestID: reqID,
		SpotDatafeedSubscription: spotDatafeedItem{
			Bucket: datafeed.Bucket,
			Prefix: datafeed.Prefix,
			State:  datafeed.State,
		},
	}, nil
}

func (h *Handler) handleDeleteSpotDatafeedSubscription(_ url.Values, reqID string) (any, error) {
	h.Backend.DeleteSpotDatafeedSubscription()

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteSpotDatafeedSubscriptionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeSpotDatafeedSubscription(_ url.Values, reqID string) (any, error) {
	datafeed := h.Backend.DescribeSpotDatafeedSubscription()
	resp := &describeSpotDatafeedResponse{RequestID: reqID}
	if datafeed != nil {
		resp.SpotDatafeedSubscription = spotDatafeedItem{
			Bucket: datafeed.Bucket,
			Prefix: datafeed.Prefix,
			State:  datafeed.State,
		}
	}

	return resp, nil
}

func (h *Handler) handleRegisterImage(vals url.Values, reqID string) (any, error) {
	name := vals.Get("Name")
	description := vals.Get("Description")
	arch := vals.Get("Architecture")

	img, err := h.Backend.RegisterImage(name, description, arch)
	if err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RegisterImageResponse"},
		RequestID: reqID,
		Return:    img != nil,
	}, nil
}

func (h *Handler) handleImportImage(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")
	arch := vals.Get("Architecture")
	platform := vals.Get("Platform")

	task, err := h.Backend.ImportImage(description, arch, platform)
	if err != nil {
		return nil, err
	}

	return &importImageResponse{
		RequestID:    reqID,
		ImportTaskID: task.ImportTaskID,
		Status:       task.Status,
	}, nil
}

func (h *Handler) handleDescribeImportImageTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImportTaskId")
	tasks := h.Backend.DescribeImportImageTasks(ids)

	resp := &describeImportImageTasksResponse{RequestID: reqID}
	for _, t := range tasks {
		resp.ImportImageTaskSet.Items = append(resp.ImportImageTaskSet.Items, importImageTaskItem{
			ImportTaskID: t.ImportTaskID,
			Description:  t.Description,
			Architecture: t.Architecture,
			Platform:     t.Platform,
			Status:       t.Status,
		})
	}

	return resp, nil
}

func (h *Handler) handleExportImage(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	description := vals.Get("Description")
	diskImageFormat := vals.Get("DiskImageFormat")
	s3Bucket := vals.Get("S3ExportLocation.S3Bucket")
	s3Prefix := vals.Get("S3ExportLocation.S3Prefix")
	roleName := vals.Get("RoleName")

	task, err := h.Backend.ExportImage(imageID, description, diskImageFormat, s3Bucket, s3Prefix, roleName)
	if err != nil {
		return nil, err
	}

	return &exportImageResponse{
		RequestID:         reqID,
		Description:       task.Description,
		DiskImageFormat:   task.DiskImageFormat,
		ExportImageTaskID: task.ExportImageTaskID,
		ImageID:           task.ImageID,
		Progress:          task.Progress,
		S3ExportLocation:  exportTaskS3LocationItem{S3Bucket: task.S3Bucket, S3Prefix: task.S3Prefix},
		Status:            task.Status,
		StatusMessage:     task.StatusMessage,
		RoleName:          task.RoleName,
	}, nil
}

func (h *Handler) handleDescribeExportImageTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ExportImageTaskId")
	tasks := h.Backend.DescribeExportImageTasks(ids)

	resp := &describeExportImageTasksResponse{RequestID: reqID}
	for _, t := range tasks {
		resp.ExportImageTaskSet.Items = append(resp.ExportImageTaskSet.Items, toExportImageTaskItem(t))
	}

	return resp, nil
}

func (h *Handler) handleListImagesInRecycleBin(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImageId")
	images := h.Backend.ListImagesInRecycleBin(ids)

	resp := &listImagesInRecycleBinResponse{RequestID: reqID}
	for _, img := range images {
		resp.ImageSet.Items = append(resp.ImageSet.Items, recycleBinImageItem{
			ImageID: img.ImageID,
			Name:    img.Name,
		})
	}

	return resp, nil
}

func (h *Handler) handleRestoreImageFromRecycleBin(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.RestoreImageFromRecycleBin(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreImageFromRecycleBinResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleListSnapshotsInRecycleBin(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SnapshotId")
	snaps := h.Backend.ListSnapshotsInRecycleBin(ids)

	resp := &listSnapshotsInRecycleBinResponse{RequestID: reqID}
	for _, snap := range snaps {
		resp.SnapshotSet.Items = append(
			resp.SnapshotSet.Items,
			recycleBinSnapshotItem{SnapshotID: snap.SnapshotID},
		)
	}

	return resp, nil
}

func (h *Handler) handleRestoreSnapshotFromRecycleBin(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SnapshotId")
	if err := h.Backend.RestoreSnapshotFromRecycleBin(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreSnapshotFromRecycleBinResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRestoreSnapshotTier(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SnapshotId")
	if err := h.Backend.RestoreSnapshotTier(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreSnapshotTierResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleImportSnapshot(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")

	task, err := h.Backend.ImportSnapshot(description)
	if err != nil {
		return nil, err
	}

	return &importSnapshotResponse{
		RequestID:    reqID,
		ImportTaskID: task.ImportTaskID,
	}, nil
}

func (h *Handler) handleDescribeImportSnapshotTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImportTaskId")
	tasks := h.Backend.DescribeImportSnapshotTasks(ids)

	resp := &describeImportSnapshotTasksResponse{RequestID: reqID}
	for _, t := range tasks {
		resp.ImportSnapshotTaskSet.Items = append(
			resp.ImportSnapshotTaskSet.Items,
			importSnapshotTaskItem{
				ImportTaskID: t.ImportTaskID,
				Description:  t.Description,
				Status:       t.Status,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleEnableFastLaunch(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.EnableFastLaunch(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableFastLaunchResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableFastLaunch(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableFastLaunch(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableFastLaunchResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeFastLaunchImages(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImageId")
	items := h.Backend.DescribeFastLaunchImages(ids)

	resp := &describeFastLaunchImagesResponse{RequestID: reqID}
	for _, item := range items {
		resp.FastLaunchImageSet.Items = append(
			resp.FastLaunchImageSet.Items,
			fastLaunchImageItem(item),
		)
	}

	return resp, nil
}

func (h *Handler) handleEnableFastSnapshotRestores(vals url.Values, reqID string) (any, error) {
	snaps := parseMemberList(vals, "SnapshotId")
	azs := parseMemberList(vals, "AvailabilityZone")
	if err := h.Backend.EnableFastSnapshotRestores(snaps, azs); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableFastSnapshotRestoresResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableFastSnapshotRestores(vals url.Values, reqID string) (any, error) {
	snaps := parseMemberList(vals, "SnapshotId")
	azs := parseMemberList(vals, "AvailabilityZone")
	if err := h.Backend.DisableFastSnapshotRestores(snaps, azs); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableFastSnapshotRestoresResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeFastSnapshotRestores(_ url.Values, reqID string) (any, error) {
	items := h.Backend.DescribeFastSnapshotRestores()

	resp := &describeFastSnapshotRestoresResponse{RequestID: reqID}
	for _, item := range items {
		resp.FastSnapshotRestoreSet.Items = append(
			resp.FastSnapshotRestoreSet.Items,
			fastSnapshotRestoreItem(item),
		)
	}

	return resp, nil
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
		Timestamp:    ts.Format("2006-01-02T15:04:05.000Z"),
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

func (h *Handler) handleGetSubnetCidrReservations(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	reservations, err := h.Backend.GetSubnetCidrReservations(subnetID)
	if err != nil {
		return nil, err
	}

	resp := &getSubnetCidrReservationsResponse{RequestID: reqID}
	for _, r := range reservations {
		resp.SubnetIpv4CidrReservations.Items = append(
			resp.SubnetIpv4CidrReservations.Items,
			subnetCidrReservationItem2{
				SubnetCidrReservationID: r.SubnetCIDRReservationID,
				SubnetID:                r.SubnetID,
				Cidr:                    r.CIDR,
				ReservationType:         r.ReservationType,
				State:                   r.State,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleGetSecurityGroupsForVpc(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	sgs, err := h.Backend.GetSecurityGroupsForVpc(vpcID)
	if err != nil {
		return nil, err
	}

	resp := &getSecurityGroupsForVpcResponse{RequestID: reqID}
	for _, sg := range sgs {
		resp.SecurityGroupForVpcSet.Items = append(resp.SecurityGroupForVpcSet.Items, sgForVpcItem{
			GroupID:     sg.GroupID,
			GroupName:   sg.GroupName,
			Description: sg.Description,
		})
	}

	return resp, nil
}

func (h *Handler) handleReplaceRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	destCIDR := vals.Get("DestinationCidrBlock")
	gatewayID := vals.Get("GatewayId")
	natGatewayID := vals.Get("NatGatewayId")

	if err := h.Backend.ReplaceRoute(rtID, destCIDR, gatewayID, natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ReplaceRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRegisterInstanceEventNotificationAttributes(
	vals url.Values,
	reqID string,
) (any, error) {
	includeAllTags := vals.Get("InstanceTagAttribute.IncludeAllTagsOfInstance") == ec2BooleanTrue
	h.Backend.RegisterInstanceEventNotificationAttributes(includeAllTags)

	return &stubResponse{
		XMLName:   xml.Name{Local: "RegisterInstanceEventNotificationAttributesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleResetEbsDefaultKmsKeyID(_ url.Values, reqID string) (any, error) {
	h.Backend.ResetEbsDefaultKmsKeyID()

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetEbsDefaultKmsKeyIdResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleUpdateSGRuleDescriptionsIngress(
	vals url.Values,
	reqID string,
) (any, error) {
	groupID := vals.Get("GroupId")
	if err := h.Backend.UpdateSecurityGroupRuleDescriptionsIngress(groupID, nil); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "UpdateSecurityGroupRuleDescriptionsIngressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleUpdateSGRuleDescriptionsEgress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if err := h.Backend.UpdateSecurityGroupRuleDescriptionsEgress(groupID, nil); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "UpdateSecurityGroupRuleDescriptionsEgressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleListVolumesInRecycleBin(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	vols := h.Backend.ListVolumesInRecycleBin(ids)

	resp := &listVolumesInRecycleBinResponse{RequestID: reqID}
	for _, v := range vols {
		resp.VolumeSet.Items = append(
			resp.VolumeSet.Items,
			recycleBinVolumeItem{VolumeID: v.VolumeID},
		)
	}

	return resp, nil
}

func (h *Handler) handleRestoreVolumeFromRecycleBin(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	if err := h.Backend.RestoreVolumeFromRecycleBin(volumeID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreVolumeFromRecycleBinResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRestoreAddressToClassic(vals url.Values, reqID string) (any, error) {
	publicIP := vals.Get("PublicIp")
	if err := h.Backend.RestoreAddressToClassic(publicIP); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreAddressToClassicResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleReportInstanceStatus(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId.1")
	if instanceID == "" {
		instanceID = vals.Get("InstanceId")
	}
	status := vals.Get("Status")
	description := vals.Get("ReasonCode.1")
	if err := h.Backend.ReportInstanceStatus(instanceID, status, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ReportInstanceStatusResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyVpnConnection(vals url.Values, reqID string) (any, error) {
	vpnID := vals.Get("VpnConnectionId")
	vgwID := vals.Get("VpnGatewayId")
	if err := h.Backend.ModifyVpnConnection(vpnID, vgwID); err != nil {
		return nil, err
	}

	conns := h.Backend.DescribeVpnConnections([]string{vpnID})
	if len(conns) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnID)
	}

	return &modifyVpnConnectionResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		VpnConnection: h.toVpnConnectionItem(conns[0]),
	}, nil
}

func (h *Handler) handleCreateVpnConnectionRoute(vals url.Values, reqID string) (any, error) {
	vpnID := vals.Get("VpnConnectionId")
	destCIDR := vals.Get("DestinationCidrBlock")

	if _, err := h.Backend.CreateVpnConnectionRoute(vpnID, destCIDR); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpnConnectionRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDeleteVpnConnectionRoute(vals url.Values, reqID string) (any, error) {
	vpnID := vals.Get("VpnConnectionId")
	destCIDR := vals.Get("DestinationCidrBlock")
	if err := h.Backend.DeleteVpnConnectionRoute(vpnID, destCIDR); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpnConnectionRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleDetachVpnGateway is implemented in handler_advanced_networking.go.
// This wrapper ensures it stays registered when batch3 overrides stubs.
func (h *Handler) handleDetachVpnGatewayB3(vals url.Values, _ string) (any, error) {
	return h.handleDetachVpnGateway(vals, "")
}

func (h *Handler) handleModifyTransitGateway(vals url.Values, reqID string) (any, error) {
	tgwID := vals.Get("TransitGatewayId")
	description := vals.Get("Description")
	if err := h.Backend.ModifyTransitGateway(tgwID, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTransitGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}
