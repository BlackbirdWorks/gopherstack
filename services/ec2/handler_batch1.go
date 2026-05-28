package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- Registration ----

//nolint:funlen,dupl // large registration table; dupl is unavoidable across batches
func registerBatch1Ops(h *Handler, ops map[string]ec2ActionFn) {
	// EBS volume lifecycle
	ops["ModifyVolume"] = h.handleModifyVolume
	ops["DescribeVolumeStatus"] = h.handleDescribeVolumeStatus
	ops["DescribeVolumesModifications"] = h.handleDescribeVolumesModifications
	ops["CopySnapshot"] = h.handleCopySnapshot
	ops["CreateSnapshots"] = h.handleCreateSnapshots
	// Snapshot block public access
	ops["GetSnapshotBlockPublicAccessState"] = h.handleGetSnapshotBlockPublicAccessState
	ops["EnableSnapshotBlockPublicAccess"] = h.handleEnableSnapshotBlockPublicAccess
	ops["DisableSnapshotBlockPublicAccess"] = h.handleDisableSnapshotBlockPublicAccess
	ops["DescribeSnapshotTierStatus"] = h.handleDescribeSnapshotTierStatus
	ops["ModifySnapshotTier"] = h.handleModifySnapshotTier
	ops["ResetSnapshotAttribute"] = h.handleResetSnapshotAttribute
	// VPC/Subnet/SG
	ops["CreateDefaultVpc"] = h.handleCreateDefaultVpc
	ops["CreateDefaultSubnet"] = h.handleCreateDefaultSubnet
	ops["AssociateSubnetCidrBlock"] = h.handleAssociateSubnetCidrBlock
	ops["DisassociateSubnetCidrBlock"] = h.handleDisassociateSubnetCidrBlock
	ops["AssociateSecurityGroupVpc"] = h.handleAssociateSecurityGroupVpc
	ops["DisassociateSecurityGroupVpc"] = h.handleDisassociateSecurityGroupVpc
	ops["DescribeSecurityGroupReferences"] = h.handleDescribeSecurityGroupReferences
	ops["DescribeStaleSecurityGroups"] = h.handleDescribeStaleSecurityGroups
	ops["DescribeSecurityGroupVpcAssociations"] = h.handleDescribeSecurityGroupVpcAssociations
	ops["ModifyVpcTenancy"] = h.handleModifyVpcTenancy
	ops["ModifyVpcPeeringConnectionOptions"] = h.handleModifyVpcPeeringConnectionOptions
	// EIP attributes
	ops["DescribeAddressesAttribute"] = h.handleDescribeAddressesAttribute
	ops["ModifyAddressAttribute"] = h.handleModifyAddressAttribute
	ops["ResetAddressAttribute"] = h.handleResetAddressAttribute
	// Instance
	ops["GetConsoleOutput"] = h.handleGetConsoleOutput
	ops["ModifyInstanceMetadataOptions"] = h.handleModifyInstanceMetadataOptions
	ops["GetInstanceMetadataDefaults"] = h.handleGetInstanceMetadataDefaults
	ops["ModifyInstanceMetadataDefaults"] = h.handleModifyInstanceMetadataDefaults
	ops["DescribeInstanceCreditSpecifications"] = h.handleDescribeInstanceCreditSpecifications
	ops["ModifyInstanceCreditSpecification"] = h.handleModifyInstanceCreditSpecification
	ops["DescribeInstanceTopology"] = h.handleDescribeInstanceTopology
	ops["MonitorInstances"] = h.handleMonitorInstances
	ops["UnmonitorInstances"] = h.handleUnmonitorInstances
	// Network interface
	ops["DescribeNetworkInterfaceAttribute"] = h.handleDescribeNetworkInterfaceAttribute
	ops["ResetNetworkInterfaceAttribute"] = h.handleResetNetworkInterfaceAttribute
	ops["DescribeNetworkInterfacePermissions"] = h.handleDescribeNetworkInterfacePermissions
	ops["CreateNetworkInterfacePermission"] = h.handleCreateNetworkInterfacePermission
	ops["DeleteNetworkInterfacePermission"] = h.handleDeleteNetworkInterfacePermission
	ops["AssignIpv6Addresses"] = h.handleAssignIpv6Addresses
	ops["UnassignIpv6Addresses"] = h.handleUnassignIpv6Addresses
	// Account/misc
	ops["DescribeAccountAttributes"] = h.handleDescribeAccountAttributes
	ops["DescribePrefixLists"] = h.handleDescribePrefixLists
	ops["DescribeIdFormat"] = h.handleDescribeIDFormat
	ops["ModifyIdFormat"] = h.handleModifyIDFormat
	ops["DescribeIdentityIdFormat"] = h.handleDescribeIdentityIDFormat
	ops["ModifyIdentityIdFormat"] = h.handleModifyIdentityIDFormat
	ops["DescribeAggregateIdFormat"] = h.handleDescribeAggregateIDFormat
	ops["DescribePrincipalIdFormat"] = h.handleDescribePrincipalIDFormat
	ops["DescribeInstanceEventNotificationAttributes"] = h.handleDescribeInstanceEventNotificationAttributes
	ops["DeregisterInstanceEventNotificationAttributes"] = h.handleDeregisterInstanceEventNotificationAttributes
}

func batch1SupportedOperations() []string {
	return []string{
		"ModifyVolume",
		"DescribeVolumeStatus",
		"DescribeVolumesModifications",
		"CopySnapshot",
		"CreateSnapshots",
		"GetSnapshotBlockPublicAccessState",
		"EnableSnapshotBlockPublicAccess",
		"DisableSnapshotBlockPublicAccess",
		"DescribeSnapshotTierStatus",
		"ModifySnapshotTier",
		"ResetSnapshotAttribute",
		"CreateDefaultVpc",
		"CreateDefaultSubnet",
		"AssociateSubnetCidrBlock",
		"DisassociateSubnetCidrBlock",
		"AssociateSecurityGroupVpc",
		"DisassociateSecurityGroupVpc",
		"DescribeSecurityGroupReferences",
		"DescribeStaleSecurityGroups",
		"DescribeSecurityGroupVpcAssociations",
		"ModifyVpcTenancy",
		"ModifyVpcPeeringConnectionOptions",
		"DescribeAddressesAttribute",
		"ModifyAddressAttribute",
		"ResetAddressAttribute",
		"GetConsoleOutput",
		"ModifyInstanceMetadataOptions",
		"GetInstanceMetadataDefaults",
		"ModifyInstanceMetadataDefaults",
		"DescribeInstanceCreditSpecifications",
		"ModifyInstanceCreditSpecification",
		"DescribeInstanceTopology",
		"MonitorInstances",
		"UnmonitorInstances",
		"DescribeNetworkInterfaceAttribute",
		"ResetNetworkInterfaceAttribute",
		"DescribeNetworkInterfacePermissions",
		"CreateNetworkInterfacePermission",
		"DeleteNetworkInterfacePermission",
		"AssignIpv6Addresses",
		"UnassignIpv6Addresses",
		"DescribeAccountAttributes",
		"DescribePrefixLists",
		"DescribeIdFormat",
		"ModifyIdFormat",
		"DescribeIdentityIdFormat",
		"ModifyIdentityIdFormat",
		"DescribeAggregateIdFormat",
		"DescribePrincipalIdFormat",
		"DescribeInstanceEventNotificationAttributes",
		"DeregisterInstanceEventNotificationAttributes",
	}
}

// ---- Response types ----

type volumeModificationItem struct {
	VolumeID          string `xml:"volumeId"`
	ModificationState string `xml:"modificationState"`
	TargetVolumeType  string `xml:"targetVolumeType"`
	OrigVolumeType    string `xml:"originalVolumeType"`
	StartTime         string `xml:"startTime"`
	Progress          int64  `xml:"progress"`
	TargetSize        int    `xml:"targetSize"`
	OrigSize          int    `xml:"originalSize"`
}

type modifyVolumeResponse struct {
	XMLName            xml.Name               `xml:"ModifyVolumeResponse"`
	RequestID          string                 `xml:"requestId"`
	VolumeModification volumeModificationItem `xml:"volumeModification"`
}

type volumeStatusInfo struct {
	Status string `xml:"status"`
}

type volumeStatusItem struct {
	VolumeID         string           `xml:"volumeId"`
	AvailabilityZone string           `xml:"availabilityZone"`
	VolumeStatus     volumeStatusInfo `xml:"volumeStatus"`
}

type volumeStatusSet struct {
	Items []volumeStatusItem `xml:"item"`
}

type describeVolumeStatusResponse struct {
	XMLName         xml.Name        `xml:"DescribeVolumeStatusResponse"`
	RequestID       string          `xml:"requestId"`
	VolumeStatusSet volumeStatusSet `xml:"volumeStatusSet"`
}

type describeVolumesModificationsResponse struct {
	XMLName               xml.Name `xml:"DescribeVolumesModificationsResponse"`
	RequestID             string   `xml:"requestId"`
	VolumeModificationSet struct {
		Items []volumeModificationItem `xml:"item"`
	} `xml:"volumeModificationSet"`
}

type copySnapshotResponse struct {
	XMLName    xml.Name `xml:"CopySnapshotResponse"`
	RequestID  string   `xml:"requestId"`
	SnapshotID string   `xml:"snapshotId"`
}

type snapshotSetItem struct {
	SnapshotID  string `xml:"snapshotId"`
	VolumeID    string `xml:"volumeId"`
	Description string `xml:"description"`
	State       string `xml:"state"`
	Progress    string `xml:"progress"`
	StartTime   string `xml:"startTime"`
	VolumeSize  int    `xml:"volumeSize"`
	Encrypted   bool   `xml:"encrypted"`
}

type createSnapshotsResponse struct {
	XMLName     xml.Name `xml:"CreateSnapshotsResponse"`
	RequestID   string   `xml:"requestId"`
	SnapshotSet struct {
		Items []snapshotSetItem `xml:"item"`
	} `xml:"snapshotSet"`
}

type snapshotBlockAccessStateResponse struct {
	XMLName   xml.Name `xml:"GetSnapshotBlockPublicAccessStateResponse"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
}

type enableSnapshotBlockPublicAccessResponse struct {
	XMLName   xml.Name `xml:"EnableSnapshotBlockPublicAccessResponse"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
}

type disableSnapshotBlockPublicAccessResponse struct {
	XMLName   xml.Name `xml:"DisableSnapshotBlockPublicAccessResponse"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
}

type snapshotTierStatusItem struct {
	SnapshotID  string `xml:"snapshotId"`
	VolumeID    string `xml:"volumeId"`
	StorageTier string `xml:"storageTier"`
}

type describeSnapshotTierStatusResponse struct {
	XMLName               xml.Name `xml:"DescribeSnapshotTierStatusResponse"`
	RequestID             string   `xml:"requestId"`
	SnapshotTierStatusSet struct {
		Items []snapshotTierStatusItem `xml:"item"`
	} `xml:"snapshotTierStatusSet"`
}

type modifySnapshotTierResponse struct {
	XMLName          xml.Name `xml:"ModifySnapshotTierResponse"`
	RequestID        string   `xml:"requestId"`
	SnapshotID       string   `xml:"snapshotId"`
	TieringStartTime string   `xml:"tieringStartTime"`
}

type createDefaultVpcResponse struct {
	XMLName   xml.Name `xml:"CreateDefaultVpcResponse"`
	RequestID string   `xml:"requestId"`
	Vpc       struct {
		VpcID     string `xml:"vpcId"`
		CIDRBlock string `xml:"cidrBlock"`
		State     string `xml:"state"`
		IsDefault bool   `xml:"isDefault"`
	} `xml:"vpc"`
}

type createDefaultSubnetResponse struct {
	XMLName   xml.Name `xml:"CreateDefaultSubnetResponse"`
	RequestID string   `xml:"requestId"`
	Subnet    struct {
		SubnetID         string `xml:"subnetId"`
		VpcID            string `xml:"vpcId"`
		CIDRBlock        string `xml:"cidrBlock"`
		AvailabilityZone string `xml:"availabilityZone"`
		State            string `xml:"state"`
		IsDefault        bool   `xml:"defaultForAz"`
	} `xml:"subnet"`
}

type subnetCIDRAssocResponse struct {
	XMLName                  xml.Name `xml:"AssociateSubnetCidrBlockResponse"`
	RequestID                string   `xml:"requestId"`
	SubnetID                 string   `xml:"subnetId"`
	Ipv6CidrBlockAssociation struct {
		AssociationID      string `xml:"associationId"`
		Ipv6CIDRBlock      string `xml:"ipv6CidrBlock"`
		Ipv6CidrBlockState struct {
			State string `xml:"state"`
		} `xml:"ipv6CidrBlockState"`
	} `xml:"ipv6CidrBlockAssociation"`
}

type disassociateSubnetCIDRResponse struct {
	XMLName   xml.Name `xml:"DisassociateSubnetCidrBlockResponse"`
	RequestID string   `xml:"requestId"`
	SubnetID  string   `xml:"subnetId"`
}

type sgVpcAssocStateItem struct {
	State string `xml:"state"`
}

type associateSGVpcResponse struct {
	XMLName   xml.Name            `xml:"AssociateSecurityGroupVpcResponse"`
	RequestID string              `xml:"requestId"`
	State     sgVpcAssocStateItem `xml:"state"`
}

type sgReferenceItem struct {
	GroupID                string `xml:"groupId"`
	ReferencingVpcID       string `xml:"referencingVpcId"`
	VpcPeeringConnectionID string `xml:"vpcPeeringConnectionId,omitempty"`
}

type describeSecurityGroupReferencesResponse struct {
	XMLName                   xml.Name `xml:"DescribeSecurityGroupReferencesResponse"`
	RequestID                 string   `xml:"requestId"`
	SecurityGroupReferenceSet struct {
		Items []sgReferenceItem `xml:"item"`
	} `xml:"securityGroupReferenceSet"`
}

type staleSGItem struct {
	GroupID     string `xml:"groupId"`
	GroupName   string `xml:"groupName"`
	Description string `xml:"description"`
	VpcID       string `xml:"vpcId"`
}

type describeStaleSecurityGroupsResponse struct {
	XMLName               xml.Name `xml:"DescribeStaleSecurityGroupsResponse"`
	RequestID             string   `xml:"requestId"`
	StaleSecurityGroupSet struct {
		Items []staleSGItem `xml:"item"`
	} `xml:"staleSecurityGroupSet"`
}

type sgVpcAssocItem struct {
	GroupID string `xml:"groupId"`
	VpcID   string `xml:"vpcId"`
	State   string `xml:"state"`
}

type describeSecurityGroupVpcAssociationsResponse struct {
	XMLName                        xml.Name `xml:"DescribeSecurityGroupVpcAssociationsResponse"`
	RequestID                      string   `xml:"requestId"`
	SecurityGroupVpcAssociationSet struct {
		Items []sgVpcAssocItem `xml:"item"`
	} `xml:"securityGroupVpcAssociationSet"`
}

type modifyVpcTenancyResponse struct {
	XMLName   xml.Name `xml:"ModifyVpcTenancyResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type peeringOptionsItem struct {
	AllowDNSResolutionFromRemoteVPC            bool `xml:"allowDnsResolutionFromRemoteVpc"`
	AllowEgressFromLocalClassicLinkToRemoteVPC bool `xml:"allowEgressFromLocalClassicLinkToRemoteVpc"`
	AllowEgressFromLocalVPCToRemoteClassicLink bool `xml:"allowEgressFromLocalVpcToRemoteClassicLink"`
}

type modifyVpcPeeringConnectionOptionsResponse struct {
	XMLName                           xml.Name           `xml:"ModifyVpcPeeringConnectionOptionsResponse"`
	RequestID                         string             `xml:"requestId"`
	RequesterPeeringConnectionOptions peeringOptionsItem `xml:"requesterPeeringConnectionOptions"`
	AccepterPeeringConnectionOptions  peeringOptionsItem `xml:"accepterPeeringConnectionOptions"`
}

type addressAttributeItem struct {
	AllocationID string `xml:"allocationId"`
	PublicIP     string `xml:"publicIp"`
	DomainName   string `xml:"domainName,omitempty"`
}

type describeAddressesAttributeResponse struct {
	XMLName    xml.Name `xml:"DescribeAddressesAttributeResponse"`
	RequestID  string   `xml:"requestId"`
	AddressSet struct {
		Items []addressAttributeItem `xml:"item"`
	} `xml:"addressSet"`
}

type modifyAddressAttributeResponse struct {
	XMLName   xml.Name             `xml:"ModifyAddressAttributeResponse"`
	RequestID string               `xml:"requestId"`
	Address   addressAttributeItem `xml:"address"`
}

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
	InstanceCreditSpecificationSet struct {
		Items []instanceCreditSpecItem `xml:"item"`
	} `xml:"instanceCreditSpecificationSet"`
}

type modifyInstanceCreditSpecResponse struct {
	XMLName                                  xml.Name `xml:"ModifyInstanceCreditSpecificationResponse"`
	RequestID                                string   `xml:"requestId"`
	SuccessfulInstanceCreditSpecificationSet struct {
		Items []instanceCreditSpecItem `xml:"item"`
	} `xml:"successfulInstanceCreditSpecificationSet"`
}

type instanceTopologyItem struct {
	InstanceID       string `xml:"instanceId"`
	InstanceType     string `xml:"instanceType"`
	AvailabilityZone string `xml:"availabilityZone"`
	ZoneID           string `xml:"zoneId"`
	NetworkNodeSet   struct {
		Items []struct {
			Value string `xml:"item"`
		} `xml:"item"`
	} `xml:"networkNodeSet"`
}

type describeInstanceTopologyResponse struct {
	XMLName     xml.Name `xml:"DescribeInstanceTopologyResponse"`
	RequestID   string   `xml:"requestId"`
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

type niAttributeResponse struct {
	XMLName            xml.Name `xml:"DescribeNetworkInterfaceAttributeResponse"`
	RequestID          string   `xml:"requestId"`
	NetworkInterfaceID string   `xml:"networkInterfaceId"`
	Description        struct {
		Value string `xml:"value"`
	} `xml:"description"`
	SourceDestCheck struct {
		Value bool `xml:"value"`
	} `xml:"sourceDestCheck"`
}

type niPermissionStateItem struct {
	State string `xml:"state"`
}

type niPermissionItem struct {
	NetworkInterfacePermissionID string                `xml:"networkInterfacePermissionId"`
	NetworkInterfaceID           string                `xml:"networkInterfaceId"`
	AwsAccountID                 string                `xml:"awsAccountId,omitempty"`
	AwsService                   string                `xml:"awsService,omitempty"`
	Permission                   string                `xml:"permission"`
	PermissionState              niPermissionStateItem `xml:"permissionState"`
}

type describeNIPermissionsResponse struct {
	XMLName                     xml.Name `xml:"DescribeNetworkInterfacePermissionsResponse"`
	RequestID                   string   `xml:"requestId"`
	NetworkInterfacePermissions struct {
		Items []niPermissionItem `xml:"item"`
	} `xml:"networkInterfacePermissions"`
}

type createNIPermissionResponse struct {
	XMLName             xml.Name         `xml:"CreateNetworkInterfacePermissionResponse"`
	RequestID           string           `xml:"requestId"`
	InterfacePermission niPermissionItem `xml:"interfacePermission"`
}

type assignIpv6Response struct {
	XMLName               xml.Name `xml:"AssignIpv6AddressesResponse"`
	RequestID             string   `xml:"requestId"`
	NetworkInterfaceID    string   `xml:"networkInterfaceId"`
	AssignedIpv6Addresses struct {
		Items []struct {
			Ipv6Address string `xml:"item"`
		} `xml:"item"`
	} `xml:"assignedIpv6Addresses"`
}

type unassignIpv6Response struct {
	XMLName                 xml.Name `xml:"UnassignIpv6AddressesResponse"`
	RequestID               string   `xml:"requestId"`
	NetworkInterfaceID      string   `xml:"networkInterfaceId"`
	UnassignedIpv6Addresses struct {
		Items []struct {
			Ipv6Address string `xml:"item"`
		} `xml:"item"`
	} `xml:"unassignedIpv6Addresses"`
}

type accountAttributeValueItem struct {
	AttributeValue string `xml:"attributeValue"`
}

type accountAttributeItem struct {
	AttributeName   string                      `xml:"attributeName"`
	AttributeValues []accountAttributeValueItem `xml:"attributeValueSet>item"`
}

type describeAccountAttributesResponse struct {
	XMLName             xml.Name `xml:"DescribeAccountAttributesResponse"`
	RequestID           string   `xml:"requestId"`
	AccountAttributeSet struct {
		Items []accountAttributeItem `xml:"item"`
	} `xml:"accountAttributeSet"`
}

type cidrItem struct {
	CIDR string `xml:"cidrIp"`
}

type prefixListItem struct {
	PrefixListID   string     `xml:"prefixListId"`
	PrefixListName string     `xml:"prefixListName"`
	CidrsSet       []cidrItem `xml:"cidrSet>item"`
}

type describePrefixListsResponse struct {
	XMLName       xml.Name `xml:"DescribePrefixListsResponse"`
	RequestID     string   `xml:"requestId"`
	PrefixListSet struct {
		Items []prefixListItem `xml:"item"`
	} `xml:"prefixListSet"`
}

type idFormatItem struct {
	Resource   string `xml:"resource"`
	UseLongIDs bool   `xml:"useLongIds"`
}

type describeIDFormatResponse struct {
	XMLName   xml.Name `xml:"DescribeIdFormatResponse"`
	RequestID string   `xml:"requestId"`
	StatusSet struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"statusSet"`
}

type describeAggregateIDFormatResponse struct {
	XMLName   xml.Name `xml:"DescribeAggregateIdFormatResponse"`
	RequestID string   `xml:"requestId"`
	Statuses  struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"statuses"`
	UseLongIDsAggregated bool `xml:"useLongIdsAggregated"`
}

type describePrincipalIDFormatResponse struct {
	XMLName    xml.Name `xml:"DescribePrincipalIdFormatResponse"`
	RequestID  string   `xml:"requestId"`
	Principals struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"principals"`
}

type instanceEventNotifAttrsResponse struct {
	XMLName              xml.Name `xml:"DescribeInstanceEventNotificationAttributesResponse"`
	RequestID            string   `xml:"requestId"`
	InstanceTagAttribute struct {
		IncludeAllTagsOfInstance bool `xml:"includeAllTagsOfInstance"`
	} `xml:"instanceTagAttribute"`
}

// ---- Handler implementations ----

func (h *Handler) handleModifyVolume(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	volumeType := vals.Get("VolumeType")
	sizeStr := vals.Get("Size")
	iopsStr := vals.Get("Iops")

	size, _ := strconv.Atoi(sizeStr)
	iops, _ := strconv.Atoi(iopsStr)

	mod, err := h.Backend.ModifyVolume(volumeID, volumeType, size, iops)
	if err != nil {
		return nil, err
	}

	return &modifyVolumeResponse{
		RequestID: reqID,
		VolumeModification: volumeModificationItem{
			VolumeID:          mod.VolumeID,
			ModificationState: mod.ModificationState,
			TargetVolumeType:  mod.TargetVolumeType,
			TargetSize:        mod.TargetSize,
			OrigVolumeType:    mod.OrigVolumeType,
			OrigSize:          mod.OrigSize,
			Progress:          mod.Progress,
			StartTime:         mod.StartTime.Format("2006-01-02T15:04:05.000Z"),
		},
	}, nil
}

func (h *Handler) handleDescribeVolumeStatus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	items := h.Backend.DescribeVolumeStatus(ids)

	out := make([]volumeStatusItem, 0, len(items))
	for _, item := range items {
		out = append(out, volumeStatusItem{
			VolumeID:         item.VolumeID,
			AvailabilityZone: item.AvailabilityZone,
			VolumeStatus:     volumeStatusInfo{Status: item.VolumeStatus},
		})
	}

	return &describeVolumeStatusResponse{
		RequestID:       reqID,
		VolumeStatusSet: volumeStatusSet{Items: out},
	}, nil
}

func (h *Handler) handleDescribeVolumesModifications(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	mods := h.Backend.DescribeVolumesModifications(ids)

	items := make([]volumeModificationItem, 0, len(mods))
	for _, mod := range mods {
		items = append(items, volumeModificationItem{
			VolumeID:          mod.VolumeID,
			ModificationState: mod.ModificationState,
			TargetVolumeType:  mod.TargetVolumeType,
			TargetSize:        mod.TargetSize,
			OrigVolumeType:    mod.OrigVolumeType,
			OrigSize:          mod.OrigSize,
			Progress:          mod.Progress,
			StartTime:         mod.StartTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := &describeVolumesModificationsResponse{RequestID: reqID}
	resp.VolumeModificationSet.Items = items

	return resp, nil
}

func (h *Handler) handleCopySnapshot(vals url.Values, reqID string) (any, error) {
	sourceID := vals.Get("SourceSnapshotId")
	description := vals.Get("Description")

	snap, err := h.Backend.CopySnapshot(sourceID, description)
	if err != nil {
		return nil, err
	}

	return &copySnapshotResponse{RequestID: reqID, SnapshotID: snap.SnapshotID}, nil
}

func (h *Handler) handleCreateSnapshots(vals url.Values, reqID string) (any, error) {
	// InstanceSpecification.InstanceId is the primary instance; volumes derived from it.
	// Also accept direct VolumeId.1, VolumeId.2... form.
	volumeIDs := parseMemberList(vals, "VolumeId")
	if len(volumeIDs) == 0 {
		// Fallback: single volume via InstanceSpecification (simplified)
		if vid := vals.Get("InstanceSpecification.ExcludeBootVolume"); vid != "" {
			volumeIDs = []string{vid}
		}
	}
	if len(volumeIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one VolumeId is required", ErrInvalidParameter)
	}
	description := vals.Get("Description")

	snaps, err := h.Backend.CreateSnapshots(volumeIDs, description)
	if err != nil {
		return nil, err
	}

	resp := &createSnapshotsResponse{RequestID: reqID}
	for _, snap := range snaps {
		resp.SnapshotSet.Items = append(resp.SnapshotSet.Items, snapshotSetItem{
			SnapshotID:  snap.SnapshotID,
			VolumeID:    snap.VolumeID,
			Description: snap.Description,
			State:       snap.State,
			Progress:    snap.Progress,
			StartTime:   snap.StartTime.Format("2006-01-02T15:04:05.000Z"),
			VolumeSize:  snap.VolumeSize,
			Encrypted:   snap.Encrypted,
		})
	}

	return resp, nil
}

func (h *Handler) handleGetSnapshotBlockPublicAccessState(_ url.Values, reqID string) (any, error) {
	state := h.Backend.GetSnapshotBlockPublicAccessState()

	return &snapshotBlockAccessStateResponse{
		XMLName:   xml.Name{Local: "GetSnapshotBlockPublicAccessStateResponse"},
		RequestID: reqID,
		State:     state,
	}, nil
}

func (h *Handler) handleEnableSnapshotBlockPublicAccess(
	vals url.Values,
	reqID string,
) (any, error) {
	state := vals.Get("State")
	if state == "" {
		state = "block-all-sharing"
	}
	if err := h.Backend.EnableSnapshotBlockPublicAccess(state); err != nil {
		return nil, err
	}

	return &enableSnapshotBlockPublicAccessResponse{RequestID: reqID, State: state}, nil
}

func (h *Handler) handleDisableSnapshotBlockPublicAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableSnapshotBlockPublicAccess()

	return &disableSnapshotBlockPublicAccessResponse{RequestID: reqID, State: "unblocked"}, nil
}

func (h *Handler) handleDescribeSnapshotTierStatus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SnapshotId")
	items := h.Backend.DescribeSnapshotTierStatus(ids)

	resp := &describeSnapshotTierStatusResponse{RequestID: reqID}
	for _, item := range items {
		resp.SnapshotTierStatusSet.Items = append(
			resp.SnapshotTierStatusSet.Items,
			snapshotTierStatusItem{ //nolint:staticcheck // xml tags differ from backend type field names
				SnapshotID:  item.SnapshotID,
				VolumeID:    item.VolumeID,
				StorageTier: item.StorageTier,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifySnapshotTier(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	storageTier := vals.Get("StorageTier")
	if storageTier == "" {
		storageTier = "archive"
	}
	if err := h.Backend.ModifySnapshotTier(snapshotID, storageTier); err != nil {
		return nil, err
	}

	return &modifySnapshotTierResponse{
		RequestID:        reqID,
		SnapshotID:       snapshotID,
		TieringStartTime: "2006-01-02T15:04:05.000Z",
	}, nil
}

func (h *Handler) handleResetSnapshotAttribute(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	if err := h.Backend.ResetSnapshotAttribute(snapshotID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetSnapshotAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleCreateDefaultVpc(_ url.Values, reqID string) (any, error) {
	vpc, err := h.Backend.CreateDefaultVpc()
	if err != nil {
		return nil, err
	}
	resp := &createDefaultVpcResponse{RequestID: reqID}
	resp.Vpc.VpcID = vpc.ID
	resp.Vpc.CIDRBlock = vpc.CIDRBlock
	resp.Vpc.IsDefault = vpc.IsDefault
	resp.Vpc.State = stateAvailableImg

	return resp, nil
}

func (h *Handler) handleCreateDefaultSubnet(vals url.Values, reqID string) (any, error) {
	az := vals.Get("AvailabilityZone")
	subnet, err := h.Backend.CreateDefaultSubnet(az)
	if err != nil {
		return nil, err
	}
	resp := &createDefaultSubnetResponse{RequestID: reqID}
	resp.Subnet.SubnetID = subnet.ID
	resp.Subnet.VpcID = subnet.VPCID
	resp.Subnet.CIDRBlock = subnet.CIDRBlock
	resp.Subnet.AvailabilityZone = subnet.AvailabilityZone
	resp.Subnet.IsDefault = subnet.IsDefault
	resp.Subnet.State = "available"

	return resp, nil
}

func (h *Handler) handleAssociateSubnetCidrBlock(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	ipv6CIDR := vals.Get("Ipv6CidrBlock")

	assoc, err := h.Backend.AssociateSubnetCidrBlock(subnetID, ipv6CIDR)
	if err != nil {
		return nil, err
	}
	resp := &subnetCIDRAssocResponse{RequestID: reqID, SubnetID: subnetID}
	resp.Ipv6CidrBlockAssociation.AssociationID = assoc.AssociationID
	resp.Ipv6CidrBlockAssociation.Ipv6CIDRBlock = assoc.IPv6CIDRBlock
	resp.Ipv6CidrBlockAssociation.Ipv6CidrBlockState.State = assoc.State

	return resp, nil
}

func (h *Handler) handleDisassociateSubnetCidrBlock(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	subnetID, err := h.Backend.DisassociateSubnetCidrBlock(assocID)
	if err != nil {
		return nil, err
	}

	return &disassociateSubnetCIDRResponse{RequestID: reqID, SubnetID: subnetID}, nil
}

func (h *Handler) handleAssociateSecurityGroupVpc(vals url.Values, reqID string) (any, error) {
	sgID := vals.Get("GroupId")
	vpcID := vals.Get("VpcId")
	result, err := h.Backend.AssociateSecurityGroupVpc(sgID, vpcID)
	if err != nil {
		return nil, err
	}
	resp := &associateSGVpcResponse{RequestID: reqID}
	resp.State.State = result.State

	return resp, nil
}

func (h *Handler) handleDisassociateSecurityGroupVpc(vals url.Values, reqID string) (any, error) {
	sgID := vals.Get("GroupId")
	vpcID := vals.Get("VpcId")
	if err := h.Backend.DisassociateSecurityGroupVpc(sgID, vpcID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateSecurityGroupVpcResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeSecurityGroupReferences(
	vals url.Values,
	reqID string,
) (any, error) {
	sgIDs := parseMemberList(vals, "GroupId")
	refs := h.Backend.DescribeSecurityGroupReferences(sgIDs)

	resp := &describeSecurityGroupReferencesResponse{RequestID: reqID}
	for _, ref := range refs {
		resp.SecurityGroupReferenceSet.Items = append(
			resp.SecurityGroupReferenceSet.Items,
			sgReferenceItem{
				GroupID:                ref.GroupID,
				ReferencingVpcID:       ref.ReferencingVPCID,
				VpcPeeringConnectionID: ref.VpcPeeringConnectionID,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeStaleSecurityGroups(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}
	stale := h.Backend.DescribeStaleSecurityGroups(vpcID)

	resp := &describeStaleSecurityGroupsResponse{RequestID: reqID}
	for _, sg := range stale {
		resp.StaleSecurityGroupSet.Items = append(resp.StaleSecurityGroupSet.Items, staleSGItem{
			GroupID:     sg.GroupID,
			GroupName:   sg.GroupName,
			Description: sg.Description,
			VpcID:       sg.VPCID,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeSecurityGroupVpcAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	sgIDs := parseMemberList(vals, "GroupId")
	assocs := h.Backend.DescribeSecurityGroupVpcAssociations(sgIDs)

	resp := &describeSecurityGroupVpcAssociationsResponse{RequestID: reqID}
	for _, a := range assocs {
		resp.SecurityGroupVpcAssociationSet.Items = append(
			resp.SecurityGroupVpcAssociationSet.Items,
			sgVpcAssocItem{
				GroupID: a.SGID,
				VpcID:   a.VPCID,
				State:   a.State,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyVpcTenancy(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	tenancy := vals.Get("InstanceTenancy")
	if err := h.Backend.ModifyVpcTenancy(vpcID, tenancy); err != nil {
		return nil, err
	}

	return &modifyVpcTenancyResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleModifyVpcPeeringConnectionOptions(
	vals url.Values,
	reqID string,
) (any, error) {
	peeringID := vals.Get("VpcPeeringConnectionId")
	opts := PeeringConnectionOptions{
		AllowDNSResolutionFromRemoteVPC: vals.Get(
			"RequesterPeeringConnectionOptions.AllowDnsResolutionFromRemoteVpc",
		) == ec2BooleanTrue,
		AllowEgressFromLocalClassicLinkToRemoteVPC: vals.Get(
			"RequesterPeeringConnectionOptions.AllowEgressFromLocalClassicLinkToRemoteVpc",
		) == ec2BooleanTrue,
		AllowEgressFromLocalVPCToRemoteClassicLink: vals.Get(
			"RequesterPeeringConnectionOptions.AllowEgressFromLocalVpcToRemoteClassicLink",
		) == ec2BooleanTrue,
	}
	if err := h.Backend.ModifyVpcPeeringConnectionOptions(peeringID, opts); err != nil {
		return nil, err
	}
	resp := &modifyVpcPeeringConnectionOptionsResponse{RequestID: reqID}
	resp.RequesterPeeringConnectionOptions.AllowDNSResolutionFromRemoteVPC =
		opts.AllowDNSResolutionFromRemoteVPC
	resp.RequesterPeeringConnectionOptions.AllowEgressFromLocalClassicLinkToRemoteVPC =
		opts.AllowEgressFromLocalClassicLinkToRemoteVPC
	resp.RequesterPeeringConnectionOptions.AllowEgressFromLocalVPCToRemoteClassicLink =
		opts.AllowEgressFromLocalVPCToRemoteClassicLink

	return resp, nil
}

func (h *Handler) handleDescribeAddressesAttribute(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "AllocationId")
	attrs := h.Backend.DescribeAddressesAttribute(ids)

	resp := &describeAddressesAttributeResponse{RequestID: reqID}
	for _, attr := range attrs {
		resp.AddressSet.Items = append(
			resp.AddressSet.Items,
			addressAttributeItem{ //nolint:staticcheck // xml tags differ from backend type
				AllocationID: attr.AllocationID,
				PublicIP:     attr.PublicIP,
				DomainName:   attr.DomainName,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyAddressAttribute(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	domainName := vals.Get("DomainName")
	if err := h.Backend.ModifyAddressAttribute(allocationID, domainName); err != nil {
		return nil, err
	}

	return &modifyAddressAttributeResponse{
		RequestID: reqID,
		Address: addressAttributeItem{
			AllocationID: allocationID,
			DomainName:   domainName,
		},
	}, nil
}

func (h *Handler) handleResetAddressAttribute(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	if err := h.Backend.ResetAddressAttribute(allocationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetAddressAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
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
		Timestamp:  ts.Format("2006-01-02T15:04:05.000Z"),
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

	resp := &describeInstanceCreditSpecsResponse{RequestID: reqID}
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
	// Support InstanceCreditSpecification.1.InstanceId / CpuCredits form
	instanceID := vals.Get("InstanceCreditSpecification.1.InstanceId")
	cpuCredits := vals.Get("InstanceCreditSpecification.1.CpuCredits")
	if instanceID == "" {
		instanceID = vals.Get("InstanceId")
		cpuCredits = vals.Get("CpuCredits")
	}

	if err := h.Backend.ModifyInstanceCreditSpecification(instanceID, cpuCredits); err != nil {
		return nil, err
	}
	resp := &modifyInstanceCreditSpecResponse{RequestID: reqID}
	resp.SuccessfulInstanceCreditSpecificationSet.Items = []instanceCreditSpecItem{
		{InstanceID: instanceID, CPUCredits: cpuCredits},
	}

	return resp, nil
}

func (h *Handler) handleDescribeInstanceTopology(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	items := h.Backend.DescribeInstanceTopology(ids)

	resp := &describeInstanceTopologyResponse{RequestID: reqID}
	for _, item := range items {
		ti := instanceTopologyItem{
			InstanceID:       item.InstanceID,
			InstanceType:     item.InstanceType,
			AvailabilityZone: item.AvailabilityZone,
			ZoneID:           item.ZoneID,
		}
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

func (h *Handler) handleDescribeNetworkInterfaceAttribute(
	vals url.Values,
	reqID string,
) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	attribute := vals.Get("Attribute")
	result, err := h.Backend.DescribeNetworkInterfaceAttribute(niID, attribute)
	if err != nil {
		return nil, err
	}
	resp := &niAttributeResponse{
		RequestID:          reqID,
		NetworkInterfaceID: result.NetworkInterfaceID,
	}
	resp.Description.Value = result.Description
	resp.SourceDestCheck.Value = result.SourceDestCheck

	return resp, nil
}

func (h *Handler) handleResetNetworkInterfaceAttribute(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	if err := h.Backend.ResetNetworkInterfaceAttribute(niID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetNetworkInterfaceAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeNetworkInterfacePermissions(
	vals url.Values,
	reqID string,
) (any, error) {
	niIDs := parseMemberList(vals, "NetworkInterfaceId")
	perms := h.Backend.DescribeNetworkInterfacePermissions(niIDs)

	resp := &describeNIPermissionsResponse{RequestID: reqID}
	for _, p := range perms {
		item := niPermissionItem{
			NetworkInterfacePermissionID: p.PermissionID,
			NetworkInterfaceID:           p.NetworkInterfaceID,
			AwsAccountID:                 p.AwsAccountID,
			AwsService:                   p.AwsService,
			Permission:                   p.Permission,
		}
		item.PermissionState.State = p.State
		resp.NetworkInterfacePermissions.Items = append(
			resp.NetworkInterfacePermissions.Items,
			item,
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateNetworkInterfacePermission(
	vals url.Values,
	reqID string,
) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	awsAccountID := vals.Get("AwsAccountId")
	awsService := vals.Get("AwsService")
	permission := vals.Get("Permission")

	perm, err := h.Backend.CreateNetworkInterfacePermission(
		niID,
		awsAccountID,
		awsService,
		permission,
	)
	if err != nil {
		return nil, err
	}
	resp := &createNIPermissionResponse{RequestID: reqID}
	resp.InterfacePermission.NetworkInterfacePermissionID = perm.PermissionID
	resp.InterfacePermission.NetworkInterfaceID = perm.NetworkInterfaceID
	resp.InterfacePermission.AwsAccountID = perm.AwsAccountID
	resp.InterfacePermission.AwsService = perm.AwsService
	resp.InterfacePermission.Permission = perm.Permission
	resp.InterfacePermission.PermissionState.State = perm.State

	return resp, nil
}

func (h *Handler) handleDeleteNetworkInterfacePermission(
	vals url.Values,
	reqID string,
) (any, error) {
	permID := vals.Get("NetworkInterfacePermissionId")
	if err := h.Backend.DeleteNetworkInterfacePermission(permID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInterfacePermissionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssignIpv6Addresses(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	count, _ := strconv.Atoi(vals.Get("Ipv6AddressCount"))
	if count == 0 {
		count = 1
	}

	assigned, err := h.Backend.AssignIpv6Addresses(niID, count)
	if err != nil {
		return nil, err
	}

	resp := &assignIpv6Response{RequestID: reqID, NetworkInterfaceID: niID}
	for _, addr := range assigned {
		resp.AssignedIpv6Addresses.Items = append(resp.AssignedIpv6Addresses.Items, struct {
			Ipv6Address string `xml:"item"`
		}{Ipv6Address: addr})
	}

	return resp, nil
}

func (h *Handler) handleUnassignIpv6Addresses(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	addrs := parseMemberList(vals, "Ipv6Addresses")

	if err := h.Backend.UnassignIpv6Addresses(niID, addrs); err != nil {
		return nil, err
	}

	resp := &unassignIpv6Response{RequestID: reqID, NetworkInterfaceID: niID}
	for _, addr := range addrs {
		resp.UnassignedIpv6Addresses.Items = append(resp.UnassignedIpv6Addresses.Items, struct {
			Ipv6Address string `xml:"item"`
		}{Ipv6Address: addr})
	}

	return resp, nil
}

func (h *Handler) handleDescribeAccountAttributes(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "AttributeName")
	attrs := h.Backend.DescribeAccountAttributes(names)

	resp := &describeAccountAttributesResponse{RequestID: reqID}
	for _, attr := range attrs {
		item := accountAttributeItem{AttributeName: attr.Name}
		for _, v := range attr.Values {
			item.AttributeValues = append(
				item.AttributeValues,
				accountAttributeValueItem{AttributeValue: v},
			)
		}
		resp.AccountAttributeSet.Items = append(resp.AccountAttributeSet.Items, item)
	}

	return resp, nil
}

func (h *Handler) handleDescribePrefixLists(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "PrefixListId")
	lists := h.Backend.DescribePrefixLists(ids)

	resp := &describePrefixListsResponse{RequestID: reqID}
	for _, pl := range lists {
		item := prefixListItem{
			PrefixListID:   pl.PrefixListID,
			PrefixListName: pl.PrefixListName,
		}
		for _, cidr := range pl.CIDRs {
			item.CidrsSet = append(item.CidrsSet, cidrItem{CIDR: cidr})
		}
		resp.PrefixListSet.Items = append(resp.PrefixListSet.Items, item)
	}

	return resp, nil
}

func (h *Handler) handleDescribeIDFormat(vals url.Values, reqID string) (any, error) {
	resources := parseMemberList(vals, "Resource")
	items := h.Backend.DescribeIDFormat(resources)

	resp := &describeIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.StatusSet.Items = append(resp.StatusSet.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleModifyIDFormat(vals url.Values, reqID string) (any, error) {
	resource := vals.Get("Resource")
	useLong := vals.Get("UseLongIds") == ec2BooleanTrue
	if err := h.Backend.ModifyIDFormat(resource, useLong); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIdFormatResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeIdentityIDFormat(vals url.Values, reqID string) (any, error) {
	principalARN := vals.Get("PrincipalArn")
	resources := parseMemberList(vals, "Resource")
	items := h.Backend.DescribeIdentityIDFormat(principalARN, resources)

	resp := &describeIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.StatusSet.Items = append(resp.StatusSet.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleModifyIdentityIDFormat(vals url.Values, reqID string) (any, error) {
	principalARN := vals.Get("PrincipalArn")
	resource := vals.Get("Resource")
	useLong := vals.Get("UseLongIds") == ec2BooleanTrue
	if err := h.Backend.ModifyIdentityIDFormat(principalARN, resource, useLong); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIdentityIdFormatResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeAggregateIDFormat(_ url.Values, reqID string) (any, error) {
	items := h.Backend.DescribeAggregateIDFormat()
	resp := &describeAggregateIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.Statuses.Items = append(resp.Statuses.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribePrincipalIDFormat(vals url.Values, reqID string) (any, error) {
	principalARN := vals.Get("PrincipalArn")
	items := h.Backend.DescribePrincipalIDFormat(principalARN)
	resp := &describePrincipalIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.Principals.Items = append(resp.Principals.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeInstanceEventNotificationAttributes(
	_ url.Values,
	reqID string,
) (any, error) {
	attrs := h.Backend.DescribeInstanceEventNotificationAttributes()
	resp := &instanceEventNotifAttrsResponse{RequestID: reqID}
	resp.InstanceTagAttribute.IncludeAllTagsOfInstance = attrs.IncludeAllTagsOfInstance

	return resp, nil
}

func (h *Handler) handleDeregisterInstanceEventNotificationAttributes(
	_ url.Values,
	reqID string,
) (any, error) {
	h.Backend.DeregisterInstanceEventNotificationAttributes()

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeregisterInstanceEventNotificationAttributesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}
