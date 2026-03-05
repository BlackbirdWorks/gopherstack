package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"

	"github.com/google/uuid"
)

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

type instanceStatusItem struct {
	InstanceID    string    `xml:"instanceId"`
	AvailZone     string    `xml:"availabilityZone"`
	InstanceState stateItem `xml:"instanceState"`
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

type amiItem struct {
	ImageID      string `xml:"imageId"`
	Name         string `xml:"name"`
	Description  string `xml:"description,omitempty"`
	Architecture string `xml:"architecture"`
	Platform     string `xml:"platform,omitempty"`
	State        string `xml:"imageState"`
}

type amiItemSet struct {
	Items []amiItem `xml:"item"`
}

type describeImagesResponse struct {
	XMLName   xml.Name   `xml:"DescribeImagesResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	ImagesSet amiItemSet `xml:"imagesSet"`
}

type regionItem struct {
	RegionName string `xml:"regionName"`
	Endpoint   string `xml:"regionEndpoint"`
}

type regionItemSet struct {
	Items []regionItem `xml:"item"`
}

type describeRegionsResponse struct {
	XMLName    xml.Name      `xml:"DescribeRegionsResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	RequestID  string        `xml:"requestId"`
	RegionInfo regionItemSet `xml:"regionInfo"`
}

type azItem struct {
	ZoneName   string `xml:"zoneName"`
	RegionName string `xml:"regionName"`
	State      string `xml:"zoneState"`
}

type azItemSet struct {
	Items []azItem `xml:"item"`
}

type describeAvailabilityZonesResponse struct {
	XMLName              xml.Name  `xml:"DescribeAvailabilityZonesResponse"`
	Xmlns                string    `xml:"xmlns,attr"`
	RequestID            string    `xml:"requestId"`
	AvailabilityZoneInfo azItemSet `xml:"availabilityZoneInfo"`
}

type keyPairItem struct {
	KeyName        string `xml:"keyName"`
	KeyFingerprint string `xml:"keyFingerprint"`
}

type keyPairItemSet struct {
	Items []keyPairItem `xml:"item"`
}

type describeKeyPairsResponse struct {
	XMLName   xml.Name       `xml:"DescribeKeyPairsResponse"`
	Xmlns     string         `xml:"xmlns,attr"`
	RequestID string         `xml:"requestId"`
	KeySet    keyPairItemSet `xml:"keySet"`
}

type createKeyPairResponse struct {
	XMLName        xml.Name `xml:"CreateKeyPairResponse"`
	Xmlns          string   `xml:"xmlns,attr"`
	RequestID      string   `xml:"requestId"`
	KeyName        string   `xml:"keyName"`
	KeyFingerprint string   `xml:"keyFingerprint"`
	KeyMaterial    string   `xml:"keyMaterial"`
}

type deleteKeyPairResponse struct {
	XMLName   xml.Name `xml:"DeleteKeyPairResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type volumeItem struct {
	Attachment *attachmentItem `xml:"attachmentSet>item,omitempty"`
	VolumeID   string          `xml:"volumeId"`
	AZ         string          `xml:"availabilityZone"`
	VolumeType string          `xml:"volumeType"`
	State      string          `xml:"status"`
	CreateTime string          `xml:"createTime"`
	Size       int             `xml:"size"`
}

type attachmentItem struct {
	VolumeID   string `xml:"volumeId"`
	InstanceID string `xml:"instanceId"`
	Device     string `xml:"device"`
	State      string `xml:"status"`
	AttachTime string `xml:"attachTime"`
}

type volumeItemSet struct {
	Items []volumeItem `xml:"item"`
}

type describeVolumesResponse struct {
	XMLName   xml.Name      `xml:"DescribeVolumesResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	VolumeSet volumeItemSet `xml:"volumeSet"`
}

type createVolumeResponse struct {
	XMLName    xml.Name `xml:"CreateVolumeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	VolumeID   string   `xml:"volumeId"`
	AZ         string   `xml:"availabilityZone"`
	VolumeType string   `xml:"volumeType"`
	State      string   `xml:"status"`
	CreateTime string   `xml:"createTime"`
	Size       int      `xml:"size"`
}

type deleteVolumeResponse struct {
	XMLName   xml.Name `xml:"DeleteVolumeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type attachVolumeResponse struct {
	XMLName    xml.Name `xml:"AttachVolumeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	VolumeID   string   `xml:"volumeId"`
	InstanceID string   `xml:"instanceId"`
	Device     string   `xml:"device"`
	State      string   `xml:"status"`
	AttachTime string   `xml:"attachTime"`
}

type detachVolumeResponse struct {
	XMLName    xml.Name `xml:"DetachVolumeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	VolumeID   string   `xml:"volumeId"`
	InstanceID string   `xml:"instanceId"`
	Device     string   `xml:"device"`
	State      string   `xml:"status"`
}

type addressItem struct {
	AllocationID  string `xml:"allocationId"`
	AssociationID string `xml:"associationId,omitempty"`
	PublicIP      string `xml:"publicIp"`
	InstanceID    string `xml:"instanceId,omitempty"`
	Domain        string `xml:"domain"`
}

type addressItemSet struct {
	Items []addressItem `xml:"item"`
}

type describeAddressesResponse struct {
	XMLName      xml.Name       `xml:"DescribeAddressesResponse"`
	Xmlns        string         `xml:"xmlns,attr"`
	RequestID    string         `xml:"requestId"`
	AddressesSet addressItemSet `xml:"addressesSet"`
}

type allocateAddressResponse struct {
	XMLName      xml.Name `xml:"AllocateAddressResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	PublicIP     string   `xml:"publicIp"`
	AllocationID string   `xml:"allocationId"`
	Domain       string   `xml:"domain"`
}

type associateAddressResponse struct {
	XMLName       xml.Name `xml:"AssociateAddressResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	AssociationID string   `xml:"associationId"`
	Return        bool     `xml:"return"`
}

type disassociateAddressResponse struct {
	XMLName   xml.Name `xml:"DisassociateAddressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type releaseAddressResponse struct {
	XMLName   xml.Name `xml:"ReleaseAddressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type igwAttachmentItem struct {
	VPCID string `xml:"vpcId"`
	State string `xml:"state"`
}

type igwItem struct {
	InternetGatewayID string              `xml:"internetGatewayId"`
	AttachmentSet     []igwAttachmentItem `xml:"attachmentSet>item"`
}

type igwItemSet struct {
	Items []igwItem `xml:"item"`
}

type describeInternetGatewaysResponse struct {
	XMLName            xml.Name   `xml:"DescribeInternetGatewaysResponse"`
	Xmlns              string     `xml:"xmlns,attr"`
	RequestID          string     `xml:"requestId"`
	InternetGatewaySet igwItemSet `xml:"internetGatewaySet"`
}

type createInternetGatewayResponse struct {
	XMLName         xml.Name `xml:"CreateInternetGatewayResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	RequestID       string   `xml:"requestId"`
	InternetGateway igwItem  `xml:"internetGateway"`
}

type deleteInternetGatewayResponse struct {
	XMLName   xml.Name `xml:"DeleteInternetGatewayResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type attachInternetGatewayResponse struct {
	XMLName   xml.Name `xml:"AttachInternetGatewayResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type detachInternetGatewayResponse struct {
	XMLName   xml.Name `xml:"DetachInternetGatewayResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type routeItem struct {
	DestinationCIDR string `xml:"destinationCidrBlock"`
	GatewayID       string `xml:"gatewayId,omitempty"`
	NatGatewayID    string `xml:"natGatewayId,omitempty"`
	State           string `xml:"state"`
}

type routeSet struct {
	Items []routeItem `xml:"item"`
}

type assocItem struct {
	RouteTableAssociationID string `xml:"routeTableAssociationId"`
	RouteTableID            string `xml:"routeTableId"`
	SubnetID                string `xml:"subnetId,omitempty"`
}

type assocSet struct {
	Items []assocItem `xml:"item"`
}

type routeTableItem struct {
	RouteTableID   string   `xml:"routeTableId"`
	VPCID          string   `xml:"vpcId"`
	RouteSet       routeSet `xml:"routeSet"`
	AssociationSet assocSet `xml:"associationSet"`
}

type routeTableItemSet struct {
	Items []routeTableItem `xml:"item"`
}

type describeRouteTablesResponse struct {
	XMLName       xml.Name          `xml:"DescribeRouteTablesResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	RouteTableSet routeTableItemSet `xml:"routeTableSet"`
}

type createRouteTableResponse struct {
	XMLName    xml.Name       `xml:"CreateRouteTableResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	RouteTable routeTableItem `xml:"routeTable"`
}

type deleteRouteTableResponse struct {
	XMLName   xml.Name `xml:"DeleteRouteTableResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type createRouteResponse struct {
	XMLName   xml.Name `xml:"CreateRouteResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type deleteRouteResponse struct {
	XMLName   xml.Name `xml:"DeleteRouteResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type associateRouteTableResponse struct {
	XMLName       xml.Name `xml:"AssociateRouteTableResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	AssociationID string   `xml:"associationId"`
}

type disassociateRouteTableResponse struct {
	XMLName   xml.Name `xml:"DisassociateRouteTableResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type natGatewayAddressItem struct {
	AllocationID string `xml:"allocationId"`
	PublicIP     string `xml:"publicIp"`
	PrivateIP    string `xml:"privateIp"`
}

type natGatewayAddressSet struct {
	Items []natGatewayAddressItem `xml:"item"`
}

type natGatewayItem struct {
	NatGatewayID        string               `xml:"natGatewayId"`
	SubnetID            string               `xml:"subnetId"`
	State               string               `xml:"state"`
	CreateTime          string               `xml:"createTime"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
}

type natGatewayItemSet struct {
	Items []natGatewayItem `xml:"item"`
}

type describeNatGatewaysResponse struct {
	XMLName       xml.Name          `xml:"DescribeNatGatewaysResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	NatGatewaySet natGatewayItemSet `xml:"natGatewaySet"`
}

type createNatGatewayResponse struct {
	XMLName    xml.Name       `xml:"CreateNatGatewayResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	NatGateway natGatewayItem `xml:"natGateway"`
}

type deleteNatGatewayResponse struct {
	XMLName      xml.Name `xml:"DeleteNatGatewayResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	NatGatewayID string   `xml:"natGatewayId"`
}

type networkInterfaceItem struct {
	NetworkInterfaceID string `xml:"networkInterfaceId"`
	SubnetID           string `xml:"subnetId"`
	VPCID              string `xml:"vpcId"`
	PrivateIPAddress   string `xml:"privateIpAddress"`
	Status             string `xml:"status"`
}

type networkInterfaceItemSet struct {
	Items []networkInterfaceItem `xml:"item"`
}

type describeNetworkInterfacesResponse struct {
	XMLName             xml.Name                `xml:"DescribeNetworkInterfacesResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	NetworkInterfaceSet networkInterfaceItemSet `xml:"networkInterfaceSet"`
}

type authorizeSecurityGroupIngressResponse struct {
	XMLName   xml.Name `xml:"AuthorizeSecurityGroupIngressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type authorizeSecurityGroupEgressResponse struct {
	XMLName   xml.Name `xml:"AuthorizeSecurityGroupEgressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type revokeSecurityGroupIngressResponse struct {
	XMLName   xml.Name `xml:"RevokeSecurityGroupIngressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// ---- handler functions ----

func (h *Handler) handleStartInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	instances, err := h.Backend.StartInstances(ids)
	if err != nil {
		return nil, err
	}

	items := make([]instanceStateChangeItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, instanceStateChangeItem{
			InstanceID:    inst.ID,
			CurrentState:  stateItem{Code: inst.State.Code, Name: inst.State.Name},
			PreviousState: stateItem(StateStopped),
		})
	}

	return &startInstancesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		InstancesSet: instanceStateChangeSet{Items: items},
	}, nil
}

func (h *Handler) handleStopInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	instances, err := h.Backend.StopInstances(ids)
	if err != nil {
		return nil, err
	}

	items := make([]instanceStateChangeItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, instanceStateChangeItem{
			InstanceID:    inst.ID,
			CurrentState:  stateItem{Code: inst.State.Code, Name: inst.State.Name},
			PreviousState: stateItem(StateRunning),
		})
	}

	return &stopInstancesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		InstancesSet: instanceStateChangeSet{Items: items},
	}, nil
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

func (h *Handler) handleDescribeInstanceStatus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	instances := h.Backend.DescribeInstanceStatus(ids)

	items := make([]instanceStatusItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, instanceStatusItem{
			InstanceID:    inst.ID,
			AvailZone:     h.Region + "a",
			InstanceState: stateItem{Code: inst.State.Code, Name: inst.State.Name},
		})
	}

	return &describeInstanceStatusResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		InstanceStatusSet: instanceStatusSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeImages(_ url.Values, reqID string) (any, error) {
	amis := h.Backend.DescribeImages()

	items := make([]amiItem, 0, len(amis))
	for _, a := range amis {
		items = append(items, amiItem{
			ImageID:      a.ImageID,
			Name:         a.Name,
			Description:  a.Description,
			Architecture: a.Architecture,
			Platform:     a.Platform,
			State:        "available",
		})
	}

	return &describeImagesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ImagesSet: amiItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeRegions(_ url.Values, reqID string) (any, error) {
	regions := h.Backend.DescribeRegions()

	items := make([]regionItem, 0, len(regions))
	for _, r := range regions {
		items = append(items, regionItem{
			RegionName: r,
			Endpoint:   fmt.Sprintf("ec2.%s.amazonaws.com", r),
		})
	}

	return &describeRegionsResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		RegionInfo: regionItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeAvailabilityZones(vals url.Values, reqID string) (any, error) {
	region := vals.Get("RegionName")
	azs := h.Backend.DescribeAvailabilityZones(region)

	effectiveRegion := region
	if effectiveRegion == "" {
		effectiveRegion = h.Region
	}

	items := make([]azItem, 0, len(azs))
	for _, az := range azs {
		items = append(items, azItem{
			ZoneName:   az,
			RegionName: effectiveRegion,
			State:      "available",
		})
	}

	return &describeAvailabilityZonesResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		AvailabilityZoneInfo: azItemSet{Items: items},
	}, nil
}

func (h *Handler) handleCreateKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")

	kp, err := h.Backend.CreateKeyPair(name)
	if err != nil {
		return nil, err
	}

	return &createKeyPairResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		KeyName:        kp.Name,
		KeyFingerprint: kp.Fingerprint,
		KeyMaterial:    kp.Material,
	}, nil
}

func (h *Handler) handleDescribeKeyPairs(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "KeyName")
	kps := h.Backend.DescribeKeyPairs(names)

	items := make([]keyPairItem, 0, len(kps))
	for _, kp := range kps {
		items = append(items, keyPairItem{
			KeyName:        kp.Name,
			KeyFingerprint: kp.Fingerprint,
		})
	}

	return &describeKeyPairsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		KeySet:    keyPairItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDeleteKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteKeyPair(name); err != nil {
		return nil, err
	}

	return &deleteKeyPairResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func toVolumeItem(vol *Volume) volumeItem {
	item := volumeItem{
		VolumeID:   vol.ID,
		Size:       vol.Size,
		AZ:         vol.AZ,
		VolumeType: vol.VolumeType,
		State:      vol.State,
		CreateTime: vol.CreateTime.Format("2006-01-02T15:04:05.000Z"),
	}

	if vol.Attachment != nil {
		item.Attachment = &attachmentItem{
			VolumeID:   vol.Attachment.VolumeID,
			InstanceID: vol.Attachment.InstanceID,
			Device:     vol.Attachment.Device,
			State:      vol.Attachment.State,
			AttachTime: vol.Attachment.AttachTime.Format("2006-01-02T15:04:05.000Z"),
		}
	}

	return item
}

func (h *Handler) handleCreateVolume(vals url.Values, reqID string) (any, error) {
	az := vals.Get("AvailabilityZone")
	volType := vals.Get("VolumeType")
	sizeStr := vals.Get("Size")

	size := 0
	if sizeStr != "" {
		// If parsing fails, size defaults to 0 and CreateVolume will use the default size.
		_, _ = fmt.Sscan(sizeStr, &size)
	}

	vol, err := h.Backend.CreateVolume(az, volType, size)
	if err != nil {
		return nil, err
	}

	return &createVolumeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VolumeID:   vol.ID,
		Size:       vol.Size,
		AZ:         vol.AZ,
		VolumeType: vol.VolumeType,
		State:      vol.State,
		CreateTime: vol.CreateTime.Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

func (h *Handler) handleDescribeVolumes(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	vols := h.Backend.DescribeVolumes(ids)

	items := make([]volumeItem, 0, len(vols))
	for _, vol := range vols {
		items = append(items, toVolumeItem(vol))
	}

	return &describeVolumesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VolumeSet: volumeItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDeleteVolume(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VolumeId")
	if id == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteVolume(id); err != nil {
		return nil, err
	}

	return &deleteVolumeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAttachVolume(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	instanceID := vals.Get("InstanceId")
	device := vals.Get("Device")

	if volumeID == "" || instanceID == "" {
		return nil, fmt.Errorf("%w: VolumeId and InstanceId are required", ErrInvalidParameter)
	}

	att, err := h.Backend.AttachVolume(volumeID, instanceID, device)
	if err != nil {
		return nil, err
	}

	return &attachVolumeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VolumeID:   att.VolumeID,
		InstanceID: att.InstanceID,
		Device:     att.Device,
		State:      att.State,
		AttachTime: att.AttachTime.Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

func (h *Handler) handleDetachVolume(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	forceStr := vals.Get("Force")
	force, _ := strconv.ParseBool(forceStr)

	att, err := h.Backend.DetachVolume(volumeID, force)
	if err != nil {
		return nil, err
	}

	return &detachVolumeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VolumeID:   att.VolumeID,
		InstanceID: att.InstanceID,
		Device:     att.Device,
		State:      att.State,
	}, nil
}

func (h *Handler) handleAllocateAddress(_ url.Values, reqID string) (any, error) {
	addr, err := h.Backend.AllocateAddress()
	if err != nil {
		return nil, err
	}

	return &allocateAddressResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		PublicIP:     addr.PublicIP,
		AllocationID: addr.AllocationID,
		Domain:       "vpc",
	}, nil
}

func (h *Handler) handleAssociateAddress(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	instanceID := vals.Get("InstanceId")

	if allocationID == "" || instanceID == "" {
		return nil, fmt.Errorf("%w: AllocationId and InstanceId are required", ErrInvalidParameter)
	}

	assocID, err := h.Backend.AssociateAddress(allocationID, instanceID)
	if err != nil {
		return nil, err
	}

	return &associateAddressResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		Return:        true,
		AssociationID: assocID,
	}, nil
}

func (h *Handler) handleDisassociateAddress(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	if assocID == "" {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateAddress(assocID); err != nil {
		return nil, err
	}

	return &disassociateAddressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleReleaseAddress(vals url.Values, reqID string) (any, error) {
	allocationID := vals.Get("AllocationId")
	if allocationID == "" {
		return nil, fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.ReleaseAddress(allocationID); err != nil {
		return nil, err
	}

	return &releaseAddressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeAddresses(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "AllocationId")
	addrs := h.Backend.DescribeAddresses(ids)

	items := make([]addressItem, 0, len(addrs))
	for _, addr := range addrs {
		items = append(items, addressItem{
			AllocationID:  addr.AllocationID,
			AssociationID: addr.AssociationID,
			PublicIP:      addr.PublicIP,
			InstanceID:    addr.InstanceID,
			Domain:        "vpc",
		})
	}

	return &describeAddressesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		AddressesSet: addressItemSet{Items: items},
	}, nil
}

func toIGWItem(igw *InternetGateway) igwItem {
	atts := make([]igwAttachmentItem, 0, len(igw.Attachments))
	for _, att := range igw.Attachments {
		atts = append(atts, igwAttachmentItem(att))
	}

	return igwItem{
		InternetGatewayID: igw.ID,
		AttachmentSet:     atts,
	}
}

func (h *Handler) handleCreateInternetGateway(_ url.Values, reqID string) (any, error) {
	igw, err := h.Backend.CreateInternetGateway()
	if err != nil {
		return nil, err
	}

	return &createInternetGatewayResponse{
		Xmlns:           ec2XMLNS,
		RequestID:       reqID,
		InternetGateway: toIGWItem(igw),
	}, nil
}

func (h *Handler) handleDeleteInternetGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("InternetGatewayId")
	if id == "" {
		return nil, fmt.Errorf("%w: InternetGatewayId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteInternetGateway(id); err != nil {
		return nil, err
	}

	return &deleteInternetGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeInternetGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InternetGatewayId")
	igws := h.Backend.DescribeInternetGateways(ids)

	items := make([]igwItem, 0, len(igws))
	for _, igw := range igws {
		items = append(items, toIGWItem(igw))
	}

	return &describeInternetGatewaysResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		InternetGatewaySet: igwItemSet{Items: items},
	}, nil
}

func (h *Handler) handleAttachInternetGateway(vals url.Values, reqID string) (any, error) {
	igwID := vals.Get("InternetGatewayId")
	vpcID := vals.Get("VpcId")

	if igwID == "" || vpcID == "" {
		return nil, fmt.Errorf("%w: InternetGatewayId and VpcId are required", ErrInvalidParameter)
	}

	if err := h.Backend.AttachInternetGateway(igwID, vpcID); err != nil {
		return nil, err
	}

	return &attachInternetGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDetachInternetGateway(vals url.Values, reqID string) (any, error) {
	igwID := vals.Get("InternetGatewayId")
	vpcID := vals.Get("VpcId")

	if igwID == "" || vpcID == "" {
		return nil, fmt.Errorf("%w: InternetGatewayId and VpcId are required", ErrInvalidParameter)
	}

	if err := h.Backend.DetachInternetGateway(igwID, vpcID); err != nil {
		return nil, err
	}

	return &detachInternetGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func toRouteTableItem(rt *RouteTable) routeTableItem {
	routes := make([]routeItem, 0, len(rt.Routes))
	for _, r := range rt.Routes {
		routes = append(routes, routeItem(r))
	}

	assocs := make([]assocItem, 0, len(rt.Associations))
	for _, a := range rt.Associations {
		assocs = append(assocs, assocItem{
			RouteTableAssociationID: a.ID,
			RouteTableID:            a.RouteTableID,
			SubnetID:                a.SubnetID,
		})
	}

	return routeTableItem{
		RouteTableID:   rt.ID,
		VPCID:          rt.VPCID,
		RouteSet:       routeSet{Items: routes},
		AssociationSet: assocSet{Items: assocs},
	}
}

func (h *Handler) handleCreateRouteTable(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	rt, err := h.Backend.CreateRouteTable(vpcID)
	if err != nil {
		return nil, err
	}

	return &createRouteTableResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		RouteTable: toRouteTableItem(rt),
	}, nil
}

func (h *Handler) handleDeleteRouteTable(vals url.Values, reqID string) (any, error) {
	id := vals.Get("RouteTableId")
	if id == "" {
		return nil, fmt.Errorf("%w: RouteTableId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteRouteTable(id); err != nil {
		return nil, err
	}

	return &deleteRouteTableResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeRouteTables(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteTableId")
	rts := h.Backend.DescribeRouteTables(ids)

	items := make([]routeTableItem, 0, len(rts))
	for _, rt := range rts {
		items = append(items, toRouteTableItem(rt))
	}

	return &describeRouteTablesResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		RouteTableSet: routeTableItemSet{Items: items},
	}, nil
}

func (h *Handler) handleCreateRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	destCIDR := vals.Get("DestinationCidrBlock")
	gatewayID := vals.Get("GatewayId")
	natGatewayID := vals.Get("NatGatewayId")

	if rtID == "" || destCIDR == "" {
		return nil, fmt.Errorf("%w: RouteTableId and DestinationCidrBlock are required", ErrInvalidParameter)
	}

	if err := h.Backend.CreateRoute(rtID, destCIDR, gatewayID, natGatewayID); err != nil {
		return nil, err
	}

	return &createRouteResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDeleteRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	destCIDR := vals.Get("DestinationCidrBlock")

	if rtID == "" || destCIDR == "" {
		return nil, fmt.Errorf("%w: RouteTableId and DestinationCidrBlock are required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteRoute(rtID, destCIDR); err != nil {
		return nil, err
	}

	return &deleteRouteResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssociateRouteTable(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	subnetID := vals.Get("SubnetId")

	if rtID == "" || subnetID == "" {
		return nil, fmt.Errorf("%w: RouteTableId and SubnetId are required", ErrInvalidParameter)
	}

	assocID, err := h.Backend.AssociateRouteTable(rtID, subnetID)
	if err != nil {
		return nil, err
	}

	return &associateRouteTableResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		AssociationID: assocID,
	}, nil
}

func (h *Handler) handleDisassociateRouteTable(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	if assocID == "" {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateRouteTable(assocID); err != nil {
		return nil, err
	}

	return &disassociateRouteTableResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func toNatGatewayItem(ngw *NatGateway) natGatewayItem {
	return natGatewayItem{
		NatGatewayID: ngw.ID,
		SubnetID:     ngw.SubnetID,
		State:        ngw.State,
		CreateTime:   ngw.CreateTime.Format("2006-01-02T15:04:05.000Z"),
		NatGatewayAddresses: natGatewayAddressSet{Items: []natGatewayAddressItem{
			{
				AllocationID: ngw.AllocationID,
				PublicIP:     ngw.PublicIP,
				PrivateIP:    ngw.PrivateIP,
			},
		}},
	}
}

func (h *Handler) handleCreateNatGateway(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	allocationID := vals.Get("AllocationId")

	if subnetID == "" || allocationID == "" {
		return nil, fmt.Errorf("%w: SubnetId and AllocationId are required", ErrInvalidParameter)
	}

	ngw, err := h.Backend.CreateNatGateway(subnetID, allocationID)
	if err != nil {
		return nil, err
	}

	return &createNatGatewayResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		NatGateway: toNatGatewayItem(ngw),
	}, nil
}

func (h *Handler) handleDeleteNatGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("NatGatewayId")
	if id == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteNatGateway(id); err != nil {
		return nil, err
	}

	return &deleteNatGatewayResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		NatGatewayID: id,
	}, nil
}

func (h *Handler) handleDescribeNatGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "NatGatewayId")
	ngws := h.Backend.DescribeNatGateways(ids)

	items := make([]natGatewayItem, 0, len(ngws))
	for _, ngw := range ngws {
		items = append(items, toNatGatewayItem(ngw))
	}

	return &describeNatGatewaysResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		NatGatewaySet: natGatewayItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeNetworkInterfaces(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "NetworkInterfaceId")
	enis := h.Backend.DescribeNetworkInterfaces(ids)

	items := make([]networkInterfaceItem, 0, len(enis))
	for _, eni := range enis {
		items = append(items, networkInterfaceItem{
			NetworkInterfaceID: eni.ID,
			SubnetID:           eni.SubnetID,
			VPCID:              eni.VPCID,
			PrivateIPAddress:   eni.PrivateIP,
			Status:             eni.Status,
		})
	}

	return &describeNetworkInterfacesResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NetworkInterfaceSet: networkInterfaceItemSet{Items: items},
	}, nil
}

// parseIPPermissions parses EC2 IpPermissions from [url.Values].
// Handles: IpPermissions.N.IpProtocol, .FromPort, .ToPort, .IpRanges.M.CidrIp.
func parseIPPermissions(vals url.Values) []SecurityGroupRule {
	var rules []SecurityGroupRule

	for i := 1; ; i++ {
		proto := vals.Get(fmt.Sprintf("IpPermissions.%d.IpProtocol", i))
		if proto == "" {
			break
		}

		fromPort := 0
		toPort := 0

		fromKey := fmt.Sprintf("IpPermissions.%d.FromPort", i)
		toKey := fmt.Sprintf("IpPermissions.%d.ToPort", i)
		// Ports default to 0 if not provided or unparseable, which is correct for protocols
		// like -1 (all traffic) where port ranges are not meaningful.
		_, _ = fmt.Sscan(vals.Get(fromKey), &fromPort)
		_, _ = fmt.Sscan(vals.Get(toKey), &toPort)

		for j := 1; ; j++ {
			cidr := vals.Get(fmt.Sprintf("IpPermissions.%d.IpRanges.%d.CidrIp", i, j))
			if cidr == "" {
				break
			}

			rules = append(rules, SecurityGroupRule{
				Protocol: proto,
				FromPort: fromPort,
				ToPort:   toPort,
				IPRange:  cidr,
			})
		}
	}

	return rules
}

func (h *Handler) handleAuthorizeSecurityGroupIngress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	if err := h.Backend.AuthorizeSecurityGroupIngress(groupID, rules); err != nil {
		return nil, err
	}

	return &authorizeSecurityGroupIngressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAuthorizeSecurityGroupEgress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	if err := h.Backend.AuthorizeSecurityGroupEgress(groupID, rules); err != nil {
		return nil, err
	}

	return &authorizeSecurityGroupEgressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRevokeSecurityGroupIngress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	if err := h.Backend.RevokeSecurityGroupIngress(groupID, rules); err != nil {
		return nil, err
	}

	return &revokeSecurityGroupIngressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleImportKeyPair is a stub for ImportKeyPair (accepts public key material, stores fingerprint).
func (h *Handler) handleImportKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	kp := &KeyPair{
		Name:        name,
		Fingerprint: fmt.Sprintf("aa:bb:cc:dd:%s", uuid.New().String()[:11]),
	}

	h.Backend.mu.Lock("ImportKeyPair")
	defer h.Backend.mu.Unlock()

	for _, existing := range h.Backend.keyPairs {
		if existing.Name == name {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateKeyPairName, name)
		}
	}

	h.Backend.keyPairs[name] = kp

	return &createKeyPairResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		KeyName:        kp.Name,
		KeyFingerprint: kp.Fingerprint,
	}, nil
}
