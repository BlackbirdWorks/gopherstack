package ec2

import (
	"encoding/xml"
	"net/url"
)

// registerParityFinalOps registers the handlers for the final EC2 parity
// sweep (gopherstack-5o9): the last 22 previously-stubbed ops, now backed by
// real state. Its op-name-to-handler assignment table incidentally overlaps
// in shape with other large registration tables in this package (e.g.
// registerTGWPeripheralsOps); the two are unrelated in content.
//
//nolint:dupl // large op-registration table, incidental structural overlap
func registerParityFinalOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ModifyVerifiedAccessGroup"] = h.handleModifyVerifiedAccessGroup
	ops["ModifyVerifiedAccessInstance"] = h.handleModifyVerifiedAccessInstance
	ops["ModifyVerifiedAccessTrustProvider"] = h.handleModifyVerifiedAccessTrustProvider

	ops["EnableTransitGatewayRouteTablePropagation"] = h.handleEnableTransitGatewayRouteTablePropagation
	ops["DisableTransitGatewayRouteTablePropagation"] = h.handleDisableTransitGatewayRouteTablePropagation
	ops["DescribeTransitGatewayAttachments"] = h.handleDescribeTransitGatewayAttachments

	ops["CreateInterruptibleCapacityReservationAllocation"] = h.handleCreateInterruptibleCapacityReservationAllocation
	ops["UpdateInterruptibleCapacityReservationAllocation"] = h.handleUpdateInterruptibleCapacityReservationAllocation
	ops["GetCapacityReservationUsage"] = h.handleGetCapacityReservationUsage
	ops["DescribeCapacityReservationTopology"] = h.handleDescribeCapacityReservationTopology

	ops["DescribeMovingAddresses"] = h.handleDescribeMovingAddresses
	ops["MoveAddressToVpc"] = h.handleMoveAddressToVpc
	ops["RejectVpcEndpointConnections"] = h.handleRejectVpcEndpointConnections
	ops["UnassignPrivateNatGatewayAddress"] = h.handleUnassignPrivateNatGatewayAddress

	ops["CancelImageLaunchPermission"] = h.handleCancelImageLaunchPermission
	ops["DescribeImageReferences"] = h.handleDescribeImageReferences
	ops["GetImageAncestry"] = h.handleGetImageAncestry
	ops["GetFlowLogsIntegrationTemplate"] = h.handleGetFlowLogsIntegrationTemplate

	ops["GetSpotPlacementScores"] = h.handleGetSpotPlacementScores
	ops["SendDiagnosticInterrupt"] = h.handleSendDiagnosticInterrupt
	ops["EnableReachabilityAnalyzerOrganizationSharing"] = h.handleEnableReachabilityAnalyzerOrganizationSharing
	ops["DescribeElasticGpus"] = h.handleDescribeElasticGpus
}

// parityFinalSupportedOperations lists the op names registerParityFinalOps handles.
func parityFinalSupportedOperations() []string {
	return []string{
		"ModifyVerifiedAccessGroup",
		"ModifyVerifiedAccessInstance",
		"ModifyVerifiedAccessTrustProvider",
		"EnableTransitGatewayRouteTablePropagation",
		"DisableTransitGatewayRouteTablePropagation",
		"DescribeTransitGatewayAttachments",
		"CreateInterruptibleCapacityReservationAllocation",
		"UpdateInterruptibleCapacityReservationAllocation",
		"GetCapacityReservationUsage",
		"DescribeCapacityReservationTopology",
		"DescribeMovingAddresses",
		"MoveAddressToVpc",
		"RejectVpcEndpointConnections",
		"UnassignPrivateNatGatewayAddress",
		"CancelImageLaunchPermission",
		"DescribeImageReferences",
		"GetImageAncestry",
		"GetFlowLogsIntegrationTemplate",
		"GetSpotPlacementScores",
		"SendDiagnosticInterrupt",
		"EnableReachabilityAnalyzerOrganizationSharing",
		"DescribeElasticGpus",
	}
}

// ---- VerifiedAccess modify ----

type modifyVerifiedAccessGroupResponse struct {
	XMLName             xml.Name                `xml:"ModifyVerifiedAccessGroupResponse"`
	RequestID           string                  `xml:"requestId"`
	VerifiedAccessGroup verifiedAccessGroupItem `xml:"verifiedAccessGroup"`
}

func (h *Handler) handleModifyVerifiedAccessGroup(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessGroupId")
	instanceID := vals.Get("VerifiedAccessInstanceId")
	description := vals.Get("Description")

	grp, err := h.Backend.ModifyVerifiedAccessGroup(id, instanceID, description)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessGroupResponse{
		RequestID: reqID,
		VerifiedAccessGroup: verifiedAccessGroupItem{
			VerifiedAccessGroupID:    grp.VerifiedAccessGroupID,
			VerifiedAccessInstanceID: grp.VerifiedAccessInstanceID,
			Status:                   grp.Status,
			Description:              grp.Description,
		},
	}, nil
}

type modifyVerifiedAccessInstanceResponse struct {
	XMLName                xml.Name                   `xml:"ModifyVerifiedAccessInstanceResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessInstance verifiedAccessInstanceItem `xml:"verifiedAccessInstance"`
}

func (h *Handler) handleModifyVerifiedAccessInstance(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessInstanceId")
	description := vals.Get("Description")

	inst, err := h.Backend.ModifyVerifiedAccessInstance(id, description)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessInstanceResponse{
		RequestID: reqID,
		VerifiedAccessInstance: verifiedAccessInstanceItem{
			VerifiedAccessInstanceID: inst.VerifiedAccessInstanceID,
			Status:                   inst.Status,
			Description:              inst.Description,
		},
	}, nil
}

type modifyVerifiedAccessTrustProviderResponse struct {
	XMLName                     xml.Name                        `xml:"ModifyVerifiedAccessTrustProviderResponse"`
	RequestID                   string                          `xml:"requestId"`
	VerifiedAccessTrustProvider verifiedAccessTrustProviderItem `xml:"verifiedAccessTrustProvider"`
}

func (h *Handler) handleModifyVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessTrustProviderId")
	description := vals.Get("Description")

	tp, err := h.Backend.ModifyVerifiedAccessTrustProvider(id, description)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessTrustProviderResponse{
		RequestID: reqID,
		VerifiedAccessTrustProvider: verifiedAccessTrustProviderItem{
			VerifiedAccessTrustProviderID: tp.VerifiedAccessTrustProviderID,
			TrustProviderType:             tp.TrustProviderType,
			Status:                        tp.Status,
			Description:                   tp.Description,
		},
	}, nil
}

// ---- Transit Gateway route propagation + unified attachment describe ----

type tgwRouteTablePropagationItem struct {
	ResourceID                             string `xml:"resourceId,omitempty"`
	ResourceType                           string `xml:"resourceType,omitempty"`
	State                                  string `xml:"state"`
	TransitGatewayAttachmentID             string `xml:"transitGatewayAttachmentId,omitempty"`
	TransitGatewayRouteTableAnnouncementID string `xml:"transitGatewayRouteTableAnnouncementId,omitempty"`
}

func toTGWRouteTablePropagationItem(p *TransitGatewayRouteTablePropagation) tgwRouteTablePropagationItem {
	return tgwRouteTablePropagationItem{
		ResourceID:                             p.ResourceID,
		ResourceType:                           p.ResourceType,
		State:                                  p.State,
		TransitGatewayAttachmentID:             p.TransitGatewayAttachmentID,
		TransitGatewayRouteTableAnnouncementID: p.TransitGatewayRouteTableAnnouncementID,
	}
}

type enableTransitGatewayRouteTablePropagationResponse struct {
	XMLName     xml.Name                     `xml:"EnableTransitGatewayRouteTablePropagationResponse"`
	RequestID   string                       `xml:"requestId"`
	Propagation tgwRouteTablePropagationItem `xml:"propagation"`
}

func (h *Handler) handleEnableTransitGatewayRouteTablePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	prop, err := h.Backend.EnableTransitGatewayRouteTablePropagation(routeTableID, attachmentID)
	if err != nil {
		return nil, err
	}

	return &enableTransitGatewayRouteTablePropagationResponse{
		RequestID:   reqID,
		Propagation: toTGWRouteTablePropagationItem(prop),
	}, nil
}

type disableTransitGatewayRouteTablePropagationResponse struct {
	XMLName     xml.Name                     `xml:"DisableTransitGatewayRouteTablePropagationResponse"`
	RequestID   string                       `xml:"requestId"`
	Propagation tgwRouteTablePropagationItem `xml:"propagation"`
}

func (h *Handler) handleDisableTransitGatewayRouteTablePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	prop, err := h.Backend.DisableTransitGatewayRouteTablePropagation(routeTableID, attachmentID)
	if err != nil {
		return nil, err
	}

	return &disableTransitGatewayRouteTablePropagationResponse{
		RequestID:   reqID,
		Propagation: toTGWRouteTablePropagationItem(prop),
	}, nil
}

type tgwAttachmentSummaryItem struct {
	TransitGatewayAttachmentID string `xml:"transitGatewayAttachmentId"`
	TransitGatewayID           string `xml:"transitGatewayId,omitempty"`
	ResourceID                 string `xml:"resourceId,omitempty"`
	ResourceType               string `xml:"resourceType,omitempty"`
	State                      string `xml:"state"`
}

type describeTransitGatewayAttachmentsResponse struct {
	XMLName     xml.Name `xml:"DescribeTransitGatewayAttachmentsResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	Attachments struct {
		Items []tgwAttachmentSummaryItem `xml:"item"`
	} `xml:"transitGatewayAttachments"`
}

func (h *Handler) handleDescribeTransitGatewayAttachments(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayAttachmentId")

	atts := h.Backend.DescribeTransitGatewayAttachments(ids)

	resp := &describeTransitGatewayAttachmentsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, att := range atts {
		resp.Attachments.Items = append(resp.Attachments.Items, tgwAttachmentSummaryItem{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.TransitGatewayID,
			ResourceID:                 att.ResourceID,
			ResourceType:               att.ResourceType,
			State:                      att.State,
		})
	}

	return resp, nil
}

// ---- Capacity Reservation extras ----

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

// ---- Address / VPC endpoint / NAT gateway misc ----

type movingAddressStatusItem struct {
	PublicIP   string `xml:"publicIp"`
	MoveStatus string `xml:"moveStatus,omitempty"`
}

type describeMovingAddressesResponse struct {
	XMLName                xml.Name `xml:"DescribeMovingAddressesResponse"`
	RequestID              string   `xml:"requestId"`
	MovingAddressStatusSet struct {
		Items []movingAddressStatusItem `xml:"item"`
	} `xml:"movingAddressStatusSet"`
}

func (h *Handler) handleDescribeMovingAddresses(vals url.Values, reqID string) (any, error) {
	publicIPs := parseMemberList(vals, "PublicIp")

	statuses := h.Backend.DescribeMovingAddresses(publicIPs)

	resp := &describeMovingAddressesResponse{RequestID: reqID}
	for _, st := range statuses {
		resp.MovingAddressStatusSet.Items = append(resp.MovingAddressStatusSet.Items, movingAddressStatusItem{
			PublicIP:   st.PublicIP,
			MoveStatus: st.MoveStatus,
		})
	}

	return resp, nil
}

type moveAddressToVpcResponse struct {
	XMLName      xml.Name `xml:"MoveAddressToVpcResponse"`
	RequestID    string   `xml:"requestId"`
	AllocationID string   `xml:"allocationId,omitempty"`
	Status       string   `xml:"status,omitempty"`
}

func (h *Handler) handleMoveAddressToVpc(vals url.Values, reqID string) (any, error) {
	publicIP := vals.Get("PublicIp")

	addr, err := h.Backend.MoveAddressToVpc(publicIP)
	if err != nil {
		return nil, err
	}

	return &moveAddressToVpcResponse{
		RequestID:    reqID,
		AllocationID: addr.AllocationID,
		Status:       "MoveInProgress",
	}, nil
}

type rejectVpcEndpointConnectionsResponse struct {
	XMLName xml.Name `xml:"RejectVpcEndpointConnectionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	// RequestID is the AWS request identifier.
	RequestID string `xml:"requestId"`
	// Unsuccessful holds items that FAILED to be rejected (no matching
	// connection). On full success this list is empty.
	Unsuccessful vpcEndpointConnectionSet `xml:"unsuccessful"`
}

func (h *Handler) handleRejectVpcEndpointConnections(vals url.Values, reqID string) (any, error) {
	serviceID := vals.Get("ServiceId")
	endpointIDs := parseMemberList(vals, "VpcEndpointId")

	unsuccessful, err := h.Backend.RejectVpcEndpointConnections(serviceID, endpointIDs)
	if err != nil {
		return nil, err
	}

	resp := &rejectVpcEndpointConnectionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, epID := range unsuccessful {
		resp.Unsuccessful.Items = append(resp.Unsuccessful.Items, vpcEndpointConnectionItem{
			ServiceID:     serviceID,
			VpcEndpointID: epID,
		})
	}

	return resp, nil
}

type unassignPrivateNatGatewayAddressResponse struct {
	XMLName             xml.Name             `xml:"UnassignPrivateNatGatewayAddressResponse"`
	Xmlns               string               `xml:"xmlns,attr"`
	RequestID           string               `xml:"requestId"`
	NatGatewayID        string               `xml:"natGatewayId,omitempty"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
}

func (h *Handler) handleUnassignPrivateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	privateIPs := parseMemberList(vals, "PrivateIpAddress")

	ngw, err := h.Backend.UnassignPrivateNatGatewayAddress(natGatewayID, privateIPs)
	if err != nil {
		return nil, err
	}

	item := toNatGatewayItem(ngw)

	return &unassignPrivateNatGatewayAddressResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NatGatewayID:        ngw.ID,
		NatGatewayAddresses: item.NatGatewayAddresses,
	}, nil
}

// ---- Image extras ----

func (h *Handler) handleCancelImageLaunchPermission(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")

	if err := h.Backend.CancelImageLaunchPermission(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelImageLaunchPermissionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type imageReferenceItem struct {
	ImageID      string `xml:"imageId,omitempty"`
	ResourceType string `xml:"resourceType,omitempty"`
	Arn          string `xml:"arn,omitempty"`
}

type describeImageReferencesResponse struct {
	XMLName           xml.Name `xml:"DescribeImageReferencesResponse"`
	RequestID         string   `xml:"requestId"`
	ImageReferenceSet struct {
		Items []imageReferenceItem `xml:"item"`
	} `xml:"imageReferenceSet"`
}

func (h *Handler) handleDescribeImageReferences(vals url.Values, reqID string) (any, error) {
	imageIDs := parseMemberList(vals, "ImageId")

	refs := h.Backend.DescribeImageReferences(imageIDs)

	resp := &describeImageReferencesResponse{RequestID: reqID}
	for _, r := range refs {
		resp.ImageReferenceSet.Items = append(resp.ImageReferenceSet.Items, imageReferenceItem{
			ImageID:      r.ImageID,
			ResourceType: r.ResourceType,
			Arn:          r.Arn,
		})
	}

	return resp, nil
}

type imageAncestryEntryItem struct {
	ImageID       string `xml:"imageId,omitempty"`
	SourceImageID string `xml:"sourceImageId,omitempty"`
}

type getImageAncestryResponse struct {
	XMLName               xml.Name `xml:"GetImageAncestryResponse"`
	RequestID             string   `xml:"requestId"`
	ImageAncestryEntrySet struct {
		Items []imageAncestryEntryItem `xml:"item"`
	} `xml:"imageAncestryEntrySet"`
}

func (h *Handler) handleGetImageAncestry(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")

	entries, err := h.Backend.GetImageAncestry(imageID)
	if err != nil {
		return nil, err
	}

	resp := &getImageAncestryResponse{RequestID: reqID}
	for _, e := range entries {
		resp.ImageAncestryEntrySet.Items = append(resp.ImageAncestryEntrySet.Items, imageAncestryEntryItem{
			ImageID:       e.ImageID,
			SourceImageID: e.SourceImageID,
		})
	}

	return resp, nil
}

type getFlowLogsIntegrationTemplateResponse struct {
	XMLName   xml.Name `xml:"GetFlowLogsIntegrationTemplateResponse"`
	RequestID string   `xml:"requestId"`
	Result    string   `xml:"result,omitempty"`
}

func (h *Handler) handleGetFlowLogsIntegrationTemplate(vals url.Values, reqID string) (any, error) {
	flowLogID := vals.Get("FlowLogId")
	s3DestinationArn := vals.Get("ConfigDeliveryS3DestinationArn")

	tmpl, err := h.Backend.GetFlowLogsIntegrationTemplate(flowLogID, s3DestinationArn)
	if err != nil {
		return nil, err
	}

	return &getFlowLogsIntegrationTemplateResponse{
		RequestID: reqID,
		Result:    tmpl,
	}, nil
}

// ---- Misc singletons ----

type spotPlacementScoreItem struct {
	Region             string `xml:"region,omitempty"`
	AvailabilityZoneID string `xml:"availabilityZoneId,omitempty"`
	Score              int32  `xml:"score"`
}

type getSpotPlacementScoresResponse struct {
	XMLName               xml.Name `xml:"GetSpotPlacementScoresResponse"`
	RequestID             string   `xml:"requestId"`
	SpotPlacementScoreSet struct {
		Items []spotPlacementScoreItem `xml:"item"`
	} `xml:"spotPlacementScoreSet"`
}

func (h *Handler) handleGetSpotPlacementScores(vals url.Values, reqID string) (any, error) {
	instanceTypes := parseMemberList(vals, "InstanceType")
	regionNames := parseMemberList(vals, "RegionName")
	singleAZ := vals.Get("SingleAvailabilityZone") == ec2BooleanTrue

	scores, err := h.Backend.GetSpotPlacementScores(instanceTypes, regionNames, singleAZ)
	if err != nil {
		return nil, err
	}

	resp := &getSpotPlacementScoresResponse{RequestID: reqID}
	for _, s := range scores {
		resp.SpotPlacementScoreSet.Items = append(resp.SpotPlacementScoreSet.Items, spotPlacementScoreItem{
			Region:             s.Region,
			AvailabilityZoneID: s.AvailabilityZoneID,
			Score:              s.Score,
		})
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

type enableReachabilityAnalyzerOrgSharingResponse struct {
	XMLName     xml.Name `xml:"EnableReachabilityAnalyzerOrganizationSharingResponse"`
	RequestID   string   `xml:"requestId"`
	ReturnValue bool     `xml:"returnValue"`
}

func (h *Handler) handleEnableReachabilityAnalyzerOrganizationSharing(_ url.Values, reqID string) (any, error) {
	ok := h.Backend.EnableReachabilityAnalyzerOrganizationSharing()

	return &enableReachabilityAnalyzerOrgSharingResponse{
		RequestID:   reqID,
		ReturnValue: ok,
	}, nil
}

type elasticGpuItem struct {
	ElasticGpuID   string `xml:"elasticGpuId,omitempty"`
	InstanceID     string `xml:"instanceId,omitempty"`
	ElasticGpuType string `xml:"elasticGpuType,omitempty"`
}

type describeElasticGpusResponse struct {
	XMLName       xml.Name `xml:"DescribeElasticGpusResponse"`
	RequestID     string   `xml:"requestId"`
	ElasticGpuSet struct {
		Items []elasticGpuItem `xml:"item"`
	} `xml:"elasticGpuSet"`
}

func (h *Handler) handleDescribeElasticGpus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ElasticGpuId")

	gpus := h.Backend.DescribeElasticGpus(ids)

	resp := &describeElasticGpusResponse{RequestID: reqID}
	for _, g := range gpus {
		resp.ElasticGpuSet.Items = append(resp.ElasticGpuSet.Items, elasticGpuItem{
			ElasticGpuID:   g.ElasticGpuID,
			InstanceID:     g.InstanceID,
			ElasticGpuType: g.ElasticGpuType,
		})
	}

	return resp, nil
}
