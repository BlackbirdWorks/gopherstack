package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

// ---- Registration ----

//nolint:funlen,dupl // large registration table; dupl is unavoidable across batches
func registerBatch2Ops(h *Handler, ops map[string]ec2ActionFn) {
	// VPC endpoint connection notifications
	ops["CreateVpcEndpointConnectionNotification"] = h.handleCreateVpcEndpointConnectionNotification
	ops["DeleteVpcEndpointConnectionNotifications"] = h.handleDeleteVpcEndpointConnectionNotifications
	ops["DescribeVpcEndpointConnectionNotifications"] = h.handleDescribeVpcEndpointConnectionNotifications
	ops["ModifyVpcEndpointConnectionNotification"] = h.handleModifyVpcEndpointConnectionNotification
	ops["DescribeVpcEndpointConnections"] = h.handleDescribeVpcEndpointConnections
	ops["DescribeVpcEndpointAssociations"] = h.handleDescribeVpcEndpointAssociations
	ops["ModifyVpcEndpointServicePayerResponsibility"] = h.handleModifyVpcEndpointServicePayerResponsibility
	ops["DescribeVpcEndpointServicePermissions"] = h.handleDescribeVpcEndpointServicePermissions
	ops["ModifyVpcEndpointServicePermissions"] = h.handleModifyVpcEndpointServicePermissions
	ops["ModifyVpcEndpoint"] = h.handleModifyVpcEndpoint
	ops["ModifyVpcEndpointServiceConfiguration"] = h.handleModifyVpcEndpointServiceConfigurationB2
	// EBS
	ops["EnableEbsEncryptionByDefault"] = h.handleEnableEbsEncryptionByDefault
	ops["DisableEbsEncryptionByDefault"] = h.handleDisableEbsEncryptionByDefault
	ops["GetEbsEncryptionByDefault"] = h.handleGetEbsEncryptionByDefault
	ops["GetEbsDefaultKmsKeyId"] = h.handleGetEbsDefaultKmsKeyID
	ops["ModifyEbsDefaultKmsKeyId"] = h.handleModifyEbsDefaultKmsKeyID
	ops["EnableVolumeIO"] = h.handleEnableVolumeIO
	// Snapshots
	ops["LockSnapshot"] = h.handleLockSnapshot
	ops["UnlockSnapshot"] = h.handleUnlockSnapshot
	ops["DescribeLockedSnapshots"] = h.handleDescribeLockedSnapshots
	// Volumes/VPC
	ops["CopyVolumes"] = h.handleCopyVolumes
	ops["DisassociateVpcCidrBlock"] = h.handleDisassociateVpcCidrBlock
	// NAT gateway
	ops["DisassociateNatGatewayAddress"] = h.handleDisassociateNatGatewayAddress
	ops["AssociateNatGatewayAddress"] = h.handleAssociateNatGatewayAddress
	ops["AssignPrivateNatGatewayAddress"] = h.handleAssignPrivateNatGatewayAddress
	// Image lifecycle
	ops["DisableImage"] = h.handleDisableImage
	ops["EnableImage"] = h.handleEnableImage
	ops["EnableImageBlockPublicAccess"] = h.handleEnableImageBlockPublicAccess
	ops["DisableImageBlockPublicAccess"] = h.handleDisableImageBlockPublicAccess
	ops["GetImageBlockPublicAccessState"] = h.handleGetImageBlockPublicAccessState
	ops["EnableImageDeprecation"] = h.handleEnableImageDeprecation
	ops["DisableImageDeprecation"] = h.handleDisableImageDeprecation
	ops["EnableImageDeregistrationProtection"] = h.handleEnableImageDeregistrationProtection
	ops["DisableImageDeregistrationProtection"] = h.handleDisableImageDeregistrationProtection
	ops["ModifyImageAttribute"] = h.handleModifyImageAttribute
	ops["ResetImageAttribute"] = h.handleResetImageAttribute
	ops["DescribeInstanceImageMetadata"] = h.handleDescribeInstanceImageMetadata
	// Serial console / VGW / credit
	ops["EnableSerialConsoleAccess"] = h.handleEnableSerialConsoleAccess
	ops["DisableSerialConsoleAccess"] = h.handleDisableSerialConsoleAccess
	ops["GetSerialConsoleAccessStatus"] = h.handleGetSerialConsoleAccessStatus
	ops["EnableVgwRoutePropagation"] = h.handleEnableVgwRoutePropagation
	ops["DisableVgwRoutePropagation"] = h.handleDisableVgwRoutePropagation
	ops["GetDefaultCreditSpecification"] = h.handleGetDefaultCreditSpecification
	ops["ModifyDefaultCreditSpecification"] = h.handleModifyDefaultCreditSpecification
	// Replace root volume
	ops["CreateReplaceRootVolumeTask"] = h.handleCreateReplaceRootVolumeTask
	ops["DescribeReplaceRootVolumeTasks"] = h.handleDescribeReplaceRootVolumeTasks
	// Address transfers
	ops["EnableAddressTransfer"] = h.handleEnableAddressTransfer
	ops["DisableAddressTransfer"] = h.handleDisableAddressTransfer
	ops["DescribeAddressTransfers"] = h.handleDescribeAddressTransfers
	// Subnet CIDR reservations
	ops["CreateSubnetCidrReservation"] = h.handleCreateSubnetCidrReservation
	ops["DeleteSubnetCidrReservation"] = h.handleDeleteSubnetCidrReservation
}

func batch2SupportedOperations() []string {
	return []string{
		"CreateVpcEndpointConnectionNotification",
		"DeleteVpcEndpointConnectionNotifications",
		"DescribeVpcEndpointConnectionNotifications",
		"ModifyVpcEndpointConnectionNotification",
		"DescribeVpcEndpointConnections",
		"DescribeVpcEndpointAssociations",
		"ModifyVpcEndpointServicePayerResponsibility",
		"DescribeVpcEndpointServicePermissions",
		"ModifyVpcEndpointServicePermissions",
		"ModifyVpcEndpoint",
		"EnableEbsEncryptionByDefault",
		"DisableEbsEncryptionByDefault",
		"GetEbsEncryptionByDefault",
		"GetEbsDefaultKmsKeyId",
		"ModifyEbsDefaultKmsKeyId",
		"EnableVolumeIO",
		"LockSnapshot",
		"UnlockSnapshot",
		"DescribeLockedSnapshots",
		"CopyVolumes",
		"DisassociateVpcCidrBlock",
		"DisassociateNatGatewayAddress",
		"AssociateNatGatewayAddress",
		"AssignPrivateNatGatewayAddress",
		"DisableImage",
		"EnableImage",
		"EnableImageBlockPublicAccess",
		"DisableImageBlockPublicAccess",
		"GetImageBlockPublicAccessState",
		"EnableImageDeprecation",
		"DisableImageDeprecation",
		"EnableImageDeregistrationProtection",
		"DisableImageDeregistrationProtection",
		"ModifyImageAttribute",
		"ResetImageAttribute",
		"DescribeInstanceImageMetadata",
		"EnableSerialConsoleAccess",
		"DisableSerialConsoleAccess",
		"GetSerialConsoleAccessStatus",
		"EnableVgwRoutePropagation",
		"DisableVgwRoutePropagation",
		"GetDefaultCreditSpecification",
		"ModifyDefaultCreditSpecification",
		"CreateReplaceRootVolumeTask",
		"DescribeReplaceRootVolumeTasks",
		"EnableAddressTransfer",
		"DisableAddressTransfer",
		"DescribeAddressTransfers",
		"CreateSubnetCidrReservation",
		"DeleteSubnetCidrReservation",
	}
}

// ---- Response types ----

type connectionNotifItem struct {
	ConnectionNotificationID    string `xml:"connectionNotificationId"`
	ServiceID                   string `xml:"serviceId,omitempty"`
	VpcEndpointID               string `xml:"vpcEndpointId,omitempty"`
	ConnectionNotificationARN   string `xml:"connectionNotificationArn"`
	ConnectionNotificationType  string `xml:"connectionNotificationType"`
	ConnectionNotificationState string `xml:"connectionNotificationState"`
	ConnectionEvents            struct {
		Items []struct {
			Event string `xml:"item"`
		} `xml:"item"`
	} `xml:"connectionEvents"`
}

type createVpcEndpointConnectionNotificationResponse struct {
	XMLName                xml.Name            `xml:"CreateVpcEndpointConnectionNotificationResponse"`
	RequestID              string              `xml:"requestId"`
	ConnectionNotification connectionNotifItem `xml:"connectionNotification"`
}

type describeVpcEndpointConnectionNotificationsResponse struct {
	XMLName                   xml.Name `xml:"DescribeVpcEndpointConnectionNotificationsResponse"`
	RequestID                 string   `xml:"requestId"`
	ConnectionNotificationSet struct {
		Items []connectionNotifItem `xml:"item"`
	} `xml:"connectionNotificationSet"`
}

type modifyVpcEndpointConnectionNotificationResponse struct {
	XMLName     xml.Name `xml:"ModifyVpcEndpointConnectionNotificationResponse"`
	RequestID   string   `xml:"requestId"`
	ReturnValue bool     `xml:"return"`
}

type describeVpcEndpointConnectionsResponse struct {
	XMLName                  xml.Name `xml:"DescribeVpcEndpointConnectionsResponse"`
	RequestID                string   `xml:"requestId"`
	VpcEndpointConnectionSet struct {
		Items []vpcEndpointConnectionItem `xml:"item"`
	} `xml:"vpcEndpointConnectionSet"`
}

type vpcEndpointAssocItem struct {
	VpcEndpointID string `xml:"vpcEndpointId"`
	VPCID         string `xml:"vpcId"`
	ServiceName   string `xml:"serviceName"`
	State         string `xml:"state"`
}

type describeVpcEndpointAssociationsResponse struct {
	XMLName                   xml.Name `xml:"DescribeVpcEndpointAssociationsResponse"`
	RequestID                 string   `xml:"requestId"`
	VpcEndpointAssociationSet struct {
		Items []vpcEndpointAssocItem `xml:"item"`
	} `xml:"vpcEndpointAssociationSet"`
}

type describeVpcEndpointServicePermissionsResponse struct {
	XMLName           xml.Name `xml:"DescribeVpcEndpointServicePermissionsResponse"`
	RequestID         string   `xml:"requestId"`
	AllowedPrincipals struct {
		Items []struct {
			Principal string `xml:"principal"`
		} `xml:"item"`
	} `xml:"allowedPrincipals"`
}

type ebsEncryptionByDefaultResponse struct {
	XMLName                xml.Name `xml:"GetEbsEncryptionByDefaultResponse"`
	RequestID              string   `xml:"requestId"`
	EbsEncryptionByDefault bool     `xml:"ebsEncryptionByDefault"`
}

type enableEbsEncryptionByDefaultResponse struct {
	XMLName                xml.Name `xml:"EnableEbsEncryptionByDefaultResponse"`
	RequestID              string   `xml:"requestId"`
	EbsEncryptionByDefault bool     `xml:"ebsEncryptionByDefault"`
}

type disableEbsEncryptionByDefaultResponse struct {
	XMLName                xml.Name `xml:"DisableEbsEncryptionByDefaultResponse"`
	RequestID              string   `xml:"requestId"`
	EbsEncryptionByDefault bool     `xml:"ebsEncryptionByDefault"`
}

type ebsDefaultKmsKeyResponse struct {
	XMLName   xml.Name `xml:"GetEbsDefaultKmsKeyIdResponse"`
	RequestID string   `xml:"requestId"`
	KmsKeyID  string   `xml:"kmsKeyId"`
}

type modifyEbsDefaultKmsKeyResponse struct {
	XMLName   xml.Name `xml:"ModifyEbsDefaultKmsKeyIdResponse"`
	RequestID string   `xml:"requestId"`
	KmsKeyID  string   `xml:"kmsKeyId"`
}

type snapshotLockItem struct {
	SnapshotID       string `xml:"snapshotId"`
	LockState        string `xml:"lockState"`
	LockCreatedOn    string `xml:"lockCreatedOn"`
	LockExpiresOn    string `xml:"lockExpiresOn,omitempty"`
	LockDurationDays int    `xml:"lockDurationDays,omitempty"`
}

type lockSnapshotResponse struct {
	XMLName    xml.Name `xml:"LockSnapshotResponse"`
	RequestID  string   `xml:"requestId"`
	SnapshotID string   `xml:"snapshotId"`
	LockState  string   `xml:"lockState"`
}

type describeLockedSnapshotsResponse struct {
	XMLName     xml.Name `xml:"DescribeLockedSnapshotsResponse"`
	RequestID   string   `xml:"requestId"`
	SnapshotSet struct {
		Items []snapshotLockItem `xml:"item"`
	} `xml:"snapshotSet"`
}

type copyVolumesVolumeItem struct {
	SourceVolumeID string `xml:"sourceVolumeId"`
	DestVolumeID   string `xml:"destVolumeId"`
}

type copyVolumesResponse struct {
	XMLName   xml.Name `xml:"CopyVolumesResponse"`
	RequestID string   `xml:"requestId"`
	VolumeSet struct {
		Items []copyVolumesVolumeItem `xml:"item"`
	} `xml:"volumeSet"`
}

type imageBlockPublicAccessStateResponse struct {
	XMLName                     xml.Name `xml:"GetImageBlockPublicAccessStateResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState struct {
		State string `xml:"state"`
	} `xml:"imageBlockPublicAccessState"`
}

type enableImageBlockPublicAccessResponse struct {
	XMLName                     xml.Name `xml:"EnableImageBlockPublicAccessResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState struct {
		State string `xml:"state"`
	} `xml:"imageBlockPublicAccessState"`
}

type disableImageBlockPublicAccessResponse struct {
	XMLName                     xml.Name `xml:"DisableImageBlockPublicAccessResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState struct {
		State string `xml:"state"`
	} `xml:"imageBlockPublicAccessState"`
}

type serialConsoleAccessStatusResponse struct {
	XMLName                    xml.Name `xml:"GetSerialConsoleAccessStatusResponse"`
	RequestID                  string   `xml:"requestId"`
	SerialConsoleAccessEnabled bool     `xml:"serialConsoleAccessEnabled"`
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

type createReplaceRootVolumeTaskResponse struct {
	XMLName               xml.Name                  `xml:"CreateReplaceRootVolumeTaskResponse"`
	RequestID             string                    `xml:"requestId"`
	ReplaceRootVolumeTask replaceRootVolumeTaskItem `xml:"replaceRootVolumeTask"`
}

type describeReplaceRootVolumeTasksResponse struct {
	XMLName                  xml.Name `xml:"DescribeReplaceRootVolumeTasksResponse"`
	RequestID                string   `xml:"requestId"`
	ReplaceRootVolumeTaskSet struct {
		Items []replaceRootVolumeTaskItem `xml:"item"`
	} `xml:"replaceRootVolumeTaskSet"`
}

type addressTransferDetailItem struct {
	AllocationID        string `xml:"allocationId"`
	PublicIP            string `xml:"publicIp"`
	TransferAccountID   string `xml:"transferAccountId"`
	TransferOfferStatus string `xml:"transferOfferStatus"`
	TransferOfferExpiry string `xml:"transferOfferExpiry"`
}

type enableAddressTransferResponse struct {
	XMLName         xml.Name                  `xml:"EnableAddressTransferResponse"`
	RequestID       string                    `xml:"requestId"`
	AddressTransfer addressTransferDetailItem `xml:"addressTransfer"`
}

type describeAddressTransfersResponse struct {
	XMLName            xml.Name `xml:"DescribeAddressTransfersResponse"`
	RequestID          string   `xml:"requestId"`
	AddressTransferSet struct {
		Items []addressTransferDetailItem `xml:"item"`
	} `xml:"addressTransferSet"`
}

type subnetCidrReservationItem struct {
	SubnetCidrReservationID string `xml:"subnetCidrReservationId"`
	SubnetID                string `xml:"subnetId"`
	Cidr                    string `xml:"cidr"`
	ReservationType         string `xml:"reservationType"`
	Description             string `xml:"description,omitempty"`
	OwnerID                 string `xml:"ownerId"`
}

type createSubnetCidrReservationResponse struct {
	XMLName               xml.Name                  `xml:"CreateSubnetCidrReservationResponse"`
	RequestID             string                    `xml:"requestId"`
	SubnetCidrReservation subnetCidrReservationItem `xml:"subnetCidrReservation"`
}

type instanceImageMetadataItem struct {
	InstanceID string `xml:"instanceId"`
	ImageID    string `xml:"imageId"`
	ImageState string `xml:"imageState"`
}

type describeInstanceImageMetadataResponse struct {
	XMLName                  xml.Name `xml:"DescribeInstanceImageMetadataResponse"`
	RequestID                string   `xml:"requestId"`
	InstanceImageMetadataSet struct {
		Items []instanceImageMetadataItem `xml:"item"`
	} `xml:"instanceImageMetadataSet"`
}

// ---- Handler implementations ----

func toConnectionNotifItem(n *VpcEndpointConnectionNotification) connectionNotifItem {
	item := connectionNotifItem{
		ConnectionNotificationID:    n.ConnectionNotificationID,
		ServiceID:                   n.ServiceID,
		VpcEndpointID:               n.VpcEndpointID,
		ConnectionNotificationARN:   n.ConnectionNotificationARN,
		ConnectionNotificationType:  n.ConnectionNotificationType,
		ConnectionNotificationState: n.ConnectionNotificationState,
	}
	for _, e := range n.ConnectionEvents {
		item.ConnectionEvents.Items = append(item.ConnectionEvents.Items, struct {
			Event string `xml:"item"`
		}{Event: e})
	}

	return item
}

func (h *Handler) handleCreateVpcEndpointConnectionNotification(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	endpointID := vals.Get("VpcEndpointId")
	notifARN := vals.Get("ConnectionNotificationArn")
	events := parseMemberList(vals, "ConnectionEvents.member")
	if len(events) == 0 {
		events = parseMemberList(vals, "ConnectionEvents")
	}

	notif, err := h.Backend.CreateVpcEndpointConnectionNotification(
		serviceID,
		endpointID,
		notifARN,
		events,
	)
	if err != nil {
		return nil, err
	}

	return &createVpcEndpointConnectionNotificationResponse{
		RequestID:              reqID,
		ConnectionNotification: toConnectionNotifItem(notif),
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointConnectionNotifications(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ConnectionNotificationId")
	notifs := h.Backend.DescribeVpcEndpointConnectionNotifications(ids)

	resp := &describeVpcEndpointConnectionNotificationsResponse{RequestID: reqID}
	for _, n := range notifs {
		resp.ConnectionNotificationSet.Items = append(
			resp.ConnectionNotificationSet.Items,
			toConnectionNotifItem(n),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpcEndpointConnectionNotifications(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ConnectionNotificationId")
	if err := h.Backend.DeleteVpcEndpointConnectionNotifications(ids); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpcEndpointConnectionNotificationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyVpcEndpointConnectionNotification(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("ConnectionNotificationId")
	notifARN := vals.Get("ConnectionNotificationArn")
	events := parseMemberList(vals, "ConnectionEvents.member")

	if _, err := h.Backend.ModifyVpcEndpointConnectionNotification(id, notifARN, events); err != nil {
		return nil, err
	}

	return &modifyVpcEndpointConnectionNotificationResponse{
		RequestID:   reqID,
		ReturnValue: true,
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointConnections(vals url.Values, reqID string) (any, error) {
	serviceIDs := parseMemberList(vals, "ServiceId")
	conns := h.Backend.DescribeVpcEndpointConnections(serviceIDs)

	resp := &describeVpcEndpointConnectionsResponse{RequestID: reqID}
	for _, c := range conns {
		resp.VpcEndpointConnectionSet.Items = append(
			resp.VpcEndpointConnectionSet.Items,
			vpcEndpointConnectionItem{
				VpcEndpointID: c.VpcEndpointID,
				ServiceID:     c.ServiceID,
				State:         c.State,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeVpcEndpointAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "VpcEndpointId")
	eps := h.Backend.DescribeVpcEndpointAssociations(ids)

	resp := &describeVpcEndpointAssociationsResponse{RequestID: reqID}
	for _, ep := range eps {
		resp.VpcEndpointAssociationSet.Items = append(
			resp.VpcEndpointAssociationSet.Items,
			vpcEndpointAssocItem{
				VpcEndpointID: ep.ID,
				VPCID:         ep.VPCID,
				ServiceName:   ep.ServiceName,
				State:         ep.State,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyVpcEndpointServicePayerResponsibility(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	payer := vals.Get("PayerResponsibility")
	if err := h.Backend.ModifyVpcEndpointServicePayerResponsibility(serviceID, payer); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEndpointServicePayerResponsibilityResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointServicePermissions(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	principals := h.Backend.DescribeVpcEndpointServicePermissions(serviceID)

	resp := &describeVpcEndpointServicePermissionsResponse{RequestID: reqID}
	for _, p := range principals {
		resp.AllowedPrincipals.Items = append(resp.AllowedPrincipals.Items, struct {
			Principal string `xml:"principal"`
		}{Principal: p})
	}

	return resp, nil
}

func (h *Handler) handleModifyVpcEndpointServicePermissions(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	add := parseMemberList(vals, "AddAllowedPrincipals.member")
	remove := parseMemberList(vals, "RemoveAllowedPrincipals.member")
	if err := h.Backend.ModifyVpcEndpointServicePermissions(serviceID, add, remove); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEndpointServicePermissionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyVpcEndpoint(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("VpcEndpointId")
	addSubnets := parseMemberList(vals, "AddSubnetId")
	removeSubnets := parseMemberList(vals, "RemoveSubnetId")
	if err := h.Backend.ModifyVpcEndpoint(endpointID, addSubnets, removeSubnets); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleModifyVpcEndpointServiceConfigurationB2 wraps the advanced networking impl.
func (h *Handler) handleModifyVpcEndpointServiceConfigurationB2(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	if err := h.Backend.ModifyVpcEndpointServiceConfiguration(serviceID, false); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEndpointServiceConfigurationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableEbsEncryptionByDefault(_ url.Values, reqID string) (any, error) {
	h.Backend.EnableEbsEncryptionByDefault()

	return &enableEbsEncryptionByDefaultResponse{
		RequestID:              reqID,
		EbsEncryptionByDefault: true,
	}, nil
}

func (h *Handler) handleDisableEbsEncryptionByDefault(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableEbsEncryptionByDefault()

	return &disableEbsEncryptionByDefaultResponse{
		RequestID:              reqID,
		EbsEncryptionByDefault: false,
	}, nil
}

func (h *Handler) handleGetEbsEncryptionByDefault(_ url.Values, reqID string) (any, error) {
	return &ebsEncryptionByDefaultResponse{
		XMLName:                xml.Name{Local: "GetEbsEncryptionByDefaultResponse"},
		RequestID:              reqID,
		EbsEncryptionByDefault: h.Backend.GetEbsEncryptionByDefault(),
	}, nil
}

func (h *Handler) handleGetEbsDefaultKmsKeyID(_ url.Values, reqID string) (any, error) {
	return &ebsDefaultKmsKeyResponse{
		RequestID: reqID,
		KmsKeyID:  h.Backend.GetEbsDefaultKmsKeyID(),
	}, nil
}

func (h *Handler) handleModifyEbsDefaultKmsKeyID(vals url.Values, reqID string) (any, error) {
	kmsKeyID := vals.Get("KmsKeyId")
	if err := h.Backend.ModifyEbsDefaultKmsKeyID(kmsKeyID); err != nil {
		return nil, err
	}

	return &modifyEbsDefaultKmsKeyResponse{RequestID: reqID, KmsKeyID: kmsKeyID}, nil
}

func (h *Handler) handleEnableVolumeIO(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	if err := h.Backend.EnableVolumeIO(volumeID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableVolumeIOResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleLockSnapshot(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	lockMode := vals.Get("LockMode")
	if lockMode == "" {
		lockMode = "compliance"
	}
	var durationDays int
	if d := vals.Get("LockDuration"); d != "" {
		_, _ = fmt.Sscan(d, &durationDays)
	}

	lock, err := h.Backend.LockSnapshot(snapshotID, lockMode, durationDays)
	if err != nil {
		return nil, err
	}

	return &lockSnapshotResponse{
		RequestID:  reqID,
		SnapshotID: lock.SnapshotID,
		LockState:  lock.LockState,
	}, nil
}

func (h *Handler) handleUnlockSnapshot(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	if err := h.Backend.UnlockSnapshot(snapshotID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "UnlockSnapshotResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeLockedSnapshots(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SnapshotId")
	locks := h.Backend.DescribeLockedSnapshots(ids)

	resp := &describeLockedSnapshotsResponse{RequestID: reqID}
	for _, l := range locks {
		item := snapshotLockItem{
			SnapshotID:       l.SnapshotID,
			LockState:        l.LockState,
			LockCreatedOn:    l.LockCreatedOn.Format("2006-01-02T15:04:05.000Z"),
			LockDurationDays: l.LockDurationDays,
		}
		if !l.LockExpiresOn.IsZero() {
			item.LockExpiresOn = l.LockExpiresOn.Format("2006-01-02T15:04:05.000Z")
		}
		resp.SnapshotSet.Items = append(resp.SnapshotSet.Items, item)
	}

	return resp, nil
}

func (h *Handler) handleCopyVolumes(vals url.Values, reqID string) (any, error) {
	volumeIDs := parseMemberList(vals, "VolumeId")
	destRegion := vals.Get("DestinationRegion")

	results, err := h.Backend.CopyVolumes(volumeIDs, destRegion)
	if err != nil {
		return nil, err
	}

	resp := &copyVolumesResponse{RequestID: reqID}
	for _, r := range results {
		resp.VolumeSet.Items = append(resp.VolumeSet.Items, copyVolumesVolumeItem(r))
	}

	return resp, nil
}

func (h *Handler) handleDisassociateVpcCidrBlock(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	if err := h.Backend.DisassociateVpcCidrBlock(assocID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateVpcCidrBlockResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisassociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	if err := h.Backend.DisassociateNatGatewayAddress(natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	allocationID := vals.Get("AllocationId")
	if err := h.Backend.AssociateNatGatewayAddress(natGatewayID, allocationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssignPrivateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	if err := h.Backend.AssignPrivateNatGatewayAddress(natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AssignPrivateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableImage(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableImage(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableImage(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.EnableImage(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableImageBlockPublicAccess(vals url.Values, reqID string) (any, error) {
	state := vals.Get("ImageBlockPublicAccessState")
	if state == "" {
		state = stateImageBlockNew
	}
	if err := h.Backend.EnableImageBlockPublicAccess(state); err != nil {
		return nil, err
	}

	resp := &enableImageBlockPublicAccessResponse{RequestID: reqID}
	resp.ImageBlockPublicAccessState.State = state

	return resp, nil
}

func (h *Handler) handleDisableImageBlockPublicAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableImageBlockPublicAccess()

	resp := &disableImageBlockPublicAccessResponse{RequestID: reqID}
	resp.ImageBlockPublicAccessState.State = stateImageUnblocked

	return resp, nil
}

func (h *Handler) handleGetImageBlockPublicAccessState(_ url.Values, reqID string) (any, error) {
	resp := &imageBlockPublicAccessStateResponse{RequestID: reqID}
	resp.ImageBlockPublicAccessState.State = h.Backend.GetImageBlockPublicAccessState()

	return resp, nil
}

func (h *Handler) handleEnableImageDeprecation(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	deprecateAt := vals.Get("DeprecateAt")
	if err := h.Backend.EnableImageDeprecation(imageID, deprecateAt); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableImageDeprecationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableImageDeprecation(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableImageDeprecation(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableImageDeprecationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableImageDeregistrationProtection(
	vals url.Values,
	reqID string,
) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.EnableImageDeregistrationProtection(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableImageDeregistrationProtectionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableImageDeregistrationProtection(
	vals url.Values,
	reqID string,
) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableImageDeregistrationProtection(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableImageDeregistrationProtectionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyImageAttribute(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	attribute := vals.Get("Attribute")
	value := vals.Get("Value")
	if err := h.Backend.ModifyImageAttribute(imageID, attribute, value); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyImageAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleResetImageAttribute(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	attribute := vals.Get("Attribute")
	if err := h.Backend.ResetImageAttribute(imageID, attribute); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetImageAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeInstanceImageMetadata(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	items := h.Backend.DescribeInstanceImageMetadata(ids)

	resp := &describeInstanceImageMetadataResponse{RequestID: reqID}
	for _, item := range items {
		resp.InstanceImageMetadataSet.Items = append(
			resp.InstanceImageMetadataSet.Items,
			instanceImageMetadataItem(item),
		)
	}

	return resp, nil
}

func (h *Handler) handleEnableSerialConsoleAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.EnableSerialConsoleAccess()

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableSerialConsoleAccessResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableSerialConsoleAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableSerialConsoleAccess()

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableSerialConsoleAccessResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleGetSerialConsoleAccessStatus(_ url.Values, reqID string) (any, error) {
	return &serialConsoleAccessStatusResponse{
		RequestID:                  reqID,
		SerialConsoleAccessEnabled: h.Backend.GetSerialConsoleAccessStatus(),
	}, nil
}

func (h *Handler) handleEnableVgwRoutePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("RouteTableId")
	gatewayID := vals.Get("GatewayId")
	if err := h.Backend.EnableVgwRoutePropagation(routeTableID, gatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableVgwRoutePropagationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableVgwRoutePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("RouteTableId")
	gatewayID := vals.Get("GatewayId")
	if err := h.Backend.DisableVgwRoutePropagation(routeTableID, gatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableVgwRoutePropagationResponse"},
		RequestID: reqID,
		Return:    true,
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

func (h *Handler) handleCreateReplaceRootVolumeTask(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	snapshotID := vals.Get("SnapshotId")

	task, err := h.Backend.CreateReplaceRootVolumeTask(instanceID, snapshotID)
	if err != nil {
		return nil, err
	}

	item := replaceRootVolumeTaskItem{
		ReplaceRootVolumeTaskID: task.ReplaceRootVolumeTaskID,
		InstanceID:              task.InstanceID,
		TaskState:               task.TaskState,
		StartTime:               task.StartTime.Format("2006-01-02T15:04:05.000Z"),
		SnapshotID:              task.SnapshotID,
	}
	if !task.CompleteTime.IsZero() {
		item.CompleteTime = task.CompleteTime.Format("2006-01-02T15:04:05.000Z")
	}

	return &createReplaceRootVolumeTaskResponse{RequestID: reqID, ReplaceRootVolumeTask: item}, nil
}

func (h *Handler) handleDescribeReplaceRootVolumeTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReplaceRootVolumeTaskId")
	tasks := h.Backend.DescribeReplaceRootVolumeTasks(ids)

	resp := &describeReplaceRootVolumeTasksResponse{RequestID: reqID}
	for _, task := range tasks {
		item := replaceRootVolumeTaskItem{
			ReplaceRootVolumeTaskID: task.ReplaceRootVolumeTaskID,
			InstanceID:              task.InstanceID,
			TaskState:               task.TaskState,
			StartTime:               task.StartTime.Format("2006-01-02T15:04:05.000Z"),
			SnapshotID:              task.SnapshotID,
		}
		if !task.CompleteTime.IsZero() {
			item.CompleteTime = task.CompleteTime.Format("2006-01-02T15:04:05.000Z")
		}
		resp.ReplaceRootVolumeTaskSet.Items = append(resp.ReplaceRootVolumeTaskSet.Items, item)
	}

	return resp, nil
}

func toAddressTransferDetailItem(t *AddressTransfer) addressTransferDetailItem {
	return addressTransferDetailItem{
		AllocationID:        t.AllocationID,
		PublicIP:            t.PublicIP,
		TransferAccountID:   t.TransferAccountID,
		TransferOfferStatus: t.TransferOfferStatus,
		TransferOfferExpiry: t.TransferOfferExpiry.Format("2006-01-02T15:04:05.000Z"),
	}
}

func (h *Handler) handleEnableAddressTransfer(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	transferAccountID := vals.Get("TransferAccountId")

	transfer, err := h.Backend.EnableAddressTransfer(allocationID, transferAccountID)
	if err != nil {
		return nil, err
	}

	return &enableAddressTransferResponse{
		RequestID:       reqID,
		AddressTransfer: toAddressTransferDetailItem(transfer),
	}, nil
}

func (h *Handler) handleDisableAddressTransfer(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	if err := h.Backend.DisableAddressTransfer(allocationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableAddressTransferResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeAddressTransfers(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "AllocationId")
	transfers := h.Backend.DescribeAddressTransfers(ids)

	resp := &describeAddressTransfersResponse{RequestID: reqID}
	for _, t := range transfers {
		resp.AddressTransferSet.Items = append(
			resp.AddressTransferSet.Items,
			toAddressTransferDetailItem(t),
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateSubnetCidrReservation(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	cidr := vals.Get("Cidr")
	reservationType := vals.Get("ReservationType")
	if reservationType == "" {
		reservationType = "prefix"
	}
	description := vals.Get("Description")

	reservation, err := h.Backend.CreateSubnetCidrReservation(
		subnetID,
		cidr,
		reservationType,
		description,
	)
	if err != nil {
		return nil, err
	}

	return &createSubnetCidrReservationResponse{
		RequestID: reqID,
		SubnetCidrReservation: subnetCidrReservationItem{
			SubnetCidrReservationID: reservation.SubnetCIDRReservationID,
			SubnetID:                reservation.SubnetID,
			Cidr:                    reservation.CIDR,
			ReservationType:         reservation.ReservationType,
			Description:             reservation.Description,
			OwnerID:                 reservation.OwnerID,
		},
	}, nil
}

func (h *Handler) handleDeleteSubnetCidrReservation(vals url.Values, reqID string) (any, error) {
	reservationID := vals.Get("SubnetCidrReservationId")
	if err := h.Backend.DeleteSubnetCidrReservation(reservationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteSubnetCidrReservationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}
