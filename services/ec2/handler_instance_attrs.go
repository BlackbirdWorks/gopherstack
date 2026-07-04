package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// handler_instance_attrs.go implements the HTTP handlers for the
// instance-attribute misc operation cluster, backed by
// backend_instance_attrs.go: ModifyAvailabilityZoneGroup, ModifyHosts,
// ModifyInstanceCapacityReservationAttributes, ModifyInstanceCpuOptions,
// ModifyInstanceEventStartTime, ModifyInstanceMaintenanceOptions,
// ModifyInstanceNetworkPerformanceOptions, ModifyInstancePlacement,
// ModifyPrivateDnsNameOptions, ModifyPublicIpDnsNameOptions,
// AssociateInstanceEventWindow, DisassociateInstanceEventWindow,
// GetInstanceTpmEkPub, and GetInstanceUefiData.

func registerInstanceAttrOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ModifyAvailabilityZoneGroup"] = h.handleModifyAvailabilityZoneGroup
	ops["ModifyHosts"] = h.handleModifyHosts
	ops["ModifyInstanceCapacityReservationAttributes"] = h.handleModifyInstanceCapacityReservationAttributes
	ops["ModifyInstanceCpuOptions"] = h.handleModifyInstanceCPUOptions
	ops["ModifyInstanceEventStartTime"] = h.handleModifyInstanceEventStartTime
	ops["ModifyInstanceMaintenanceOptions"] = h.handleModifyInstanceMaintenanceOptions
	ops["ModifyInstanceNetworkPerformanceOptions"] = h.handleModifyInstanceNetworkPerformanceOptions
	ops["ModifyInstancePlacement"] = h.handleModifyInstancePlacement
	ops["ModifyPrivateDnsNameOptions"] = h.handleModifyPrivateDNSNameOptions
	ops["ModifyPublicIpDnsNameOptions"] = h.handleModifyPublicIPDNSNameOptions
	ops["AssociateInstanceEventWindow"] = h.handleAssociateInstanceEventWindow
	ops["DisassociateInstanceEventWindow"] = h.handleDisassociateInstanceEventWindow
	ops["GetInstanceTpmEkPub"] = h.handleGetInstanceTpmEkPub
	ops["GetInstanceUefiData"] = h.handleGetInstanceUefiData
}

// instanceAttrSupportedOperations lists the operation names registered by
// registerInstanceAttrOps, for GetSupportedOperations().
func instanceAttrSupportedOperations() []string {
	return []string{
		"ModifyAvailabilityZoneGroup",
		"ModifyHosts",
		"ModifyInstanceCapacityReservationAttributes",
		"ModifyInstanceCpuOptions",
		"ModifyInstanceEventStartTime",
		"ModifyInstanceMaintenanceOptions",
		"ModifyInstanceNetworkPerformanceOptions",
		"ModifyInstancePlacement",
		"ModifyPrivateDnsNameOptions",
		"ModifyPublicIpDnsNameOptions",
		"AssociateInstanceEventWindow",
		"DisassociateInstanceEventWindow",
		"GetInstanceTpmEkPub",
		"GetInstanceUefiData",
	}
}

// ---- ModifyAvailabilityZoneGroup ----

type modifyAvailabilityZoneGroupResponse struct {
	XMLName   xml.Name `xml:"ModifyAvailabilityZoneGroupResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleModifyAvailabilityZoneGroup(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.ModifyAvailabilityZoneGroup(vals.Get("GroupName"), vals.Get("OptInStatus"))
	if err != nil {
		return nil, err
	}

	return &modifyAvailabilityZoneGroupResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: ok}, nil
}

// ---- ModifyHosts ----

type unsuccessfulHostItemError struct {
	Code    string `xml:"code,omitempty"`
	Message string `xml:"message,omitempty"`
}

type unsuccessfulHostItem struct {
	ResourceID string                    `xml:"resourceId,omitempty"`
	Error      unsuccessfulHostItemError `xml:"error"`
}

type modifyHostsResponse struct {
	XMLName    xml.Name `xml:"ModifyHostsResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	Successful struct {
		Items []string `xml:"item"`
	} `xml:"successful"`
	Unsuccessful struct {
		Items []unsuccessfulHostItem `xml:"item"`
	} `xml:"unsuccessful"`
}

func (h *Handler) handleModifyHosts(vals url.Values, reqID string) (any, error) {
	hostIDs := parseMemberList(vals, "HostId")

	successful, unsuccessful, err := h.Backend.ModifyHosts(
		hostIDs,
		vals.Get("AutoPlacement"),
		vals.Get("HostMaintenance"),
		vals.Get("HostRecovery"),
		vals.Get("InstanceFamily"),
		vals.Get("InstanceType"),
	)
	if err != nil {
		return nil, err
	}

	resp := &modifyHostsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.Successful.Items = successful

	for _, u := range unsuccessful {
		resp.Unsuccessful.Items = append(resp.Unsuccessful.Items, unsuccessfulHostItem{
			ResourceID: u.HostID,
			Error:      unsuccessfulHostItemError{Code: u.Code, Message: u.Message},
		})
	}

	return resp, nil
}

// ---- ModifyInstanceCapacityReservationAttributes ----

type modifyInstanceCapacityReservationAttributesResponse struct {
	XMLName   xml.Name `xml:"ModifyInstanceCapacityReservationAttributesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleModifyInstanceCapacityReservationAttributes(vals url.Values, reqID string) (any, error) {
	_, err := h.Backend.ModifyInstanceCapacityReservationAttributes(
		vals.Get("InstanceId"),
		vals.Get("CapacityReservationSpecification.CapacityReservationPreference"),
		vals.Get("CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationId"),
		vals.Get("CapacityReservationSpecification.CapacityReservationTarget.CapacityReservationResourceGroupArn"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceCapacityReservationAttributesResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

// ---- ModifyInstanceCpuOptions ----

type modifyInstanceCPUOptionsResponse struct {
	XMLName              xml.Name `xml:"ModifyInstanceCpuOptionsResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	RequestID            string   `xml:"requestId"`
	InstanceID           string   `xml:"instanceId,omitempty"`
	NestedVirtualization string   `xml:"nestedVirtualization,omitempty"`
	CoreCount            int32    `xml:"coreCount,omitempty"`
	ThreadsPerCore       int32    `xml:"threadsPerCore,omitempty"`
}

func parseOptionalInt32(vals url.Values, key string) *int32 {
	v := vals.Get(key)
	if v == "" {
		return nil
	}

	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return nil
	}

	n32 := int32(n)

	return &n32
}

func (h *Handler) handleModifyInstanceCPUOptions(vals url.Values, reqID string) (any, error) {
	inst, err := h.Backend.ModifyInstanceCPUOptions(
		vals.Get("InstanceId"),
		parseOptionalInt32(vals, "CoreCount"),
		parseOptionalInt32(vals, "ThreadsPerCore"),
		vals.Get("NestedVirtualization"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceCPUOptionsResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		InstanceID:           inst.ID,
		CoreCount:            inst.CPUOptions.CoreCount,
		ThreadsPerCore:       inst.CPUOptions.ThreadsPerCore,
		NestedVirtualization: inst.CPUOptions.NestedVirtualization,
	}, nil
}

// ---- ModifyInstanceEventStartTime ----

type modifyInstanceEventStartTimeResponse struct {
	XMLName   xml.Name `xml:"ModifyInstanceEventStartTimeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Event     struct {
		Code            string `xml:"code,omitempty"`
		Description     string `xml:"description,omitempty"`
		InstanceEventID string `xml:"instanceEventId,omitempty"`
		NotBefore       string `xml:"notBefore,omitempty"`
	} `xml:"event"`
}

func (h *Handler) handleModifyInstanceEventStartTime(vals url.Values, reqID string) (any, error) {
	notBefore, _ := time.Parse(time.RFC3339, vals.Get("NotBefore"))

	event, err := h.Backend.ModifyInstanceEventStartTime(vals.Get("InstanceId"), vals.Get("InstanceEventId"), notBefore)
	if err != nil {
		return nil, err
	}

	resp := &modifyInstanceEventStartTimeResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.Event.Code = event.Code
	resp.Event.Description = event.Description
	resp.Event.InstanceEventID = event.InstanceEventID
	resp.Event.NotBefore = event.NotBefore.UTC().Format(time.RFC3339)

	return resp, nil
}

// ---- ModifyInstanceMaintenanceOptions ----

type modifyInstanceMaintenanceOptionsResponse struct {
	XMLName         xml.Name `xml:"ModifyInstanceMaintenanceOptionsResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	RequestID       string   `xml:"requestId"`
	InstanceID      string   `xml:"instanceId,omitempty"`
	AutoRecovery    string   `xml:"autoRecovery,omitempty"`
	RebootMigration string   `xml:"rebootMigration,omitempty"`
}

func (h *Handler) handleModifyInstanceMaintenanceOptions(vals url.Values, reqID string) (any, error) {
	inst, err := h.Backend.ModifyInstanceMaintenanceOptions(
		vals.Get("InstanceId"), vals.Get("AutoRecovery"), vals.Get("RebootMigration"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceMaintenanceOptionsResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		InstanceID:      inst.ID,
		AutoRecovery:    inst.MaintenanceOptions.AutoRecovery,
		RebootMigration: inst.MaintenanceOptions.RebootMigration,
	}, nil
}

// ---- ModifyInstanceNetworkPerformanceOptions ----

type modifyInstanceNetworkPerformanceOptionsResponse struct {
	XMLName            xml.Name `xml:"ModifyInstanceNetworkPerformanceOptionsResponse"`
	Xmlns              string   `xml:"xmlns,attr"`
	RequestID          string   `xml:"requestId"`
	InstanceID         string   `xml:"instanceId,omitempty"`
	BandwidthWeighting string   `xml:"bandwidthWeighting,omitempty"`
}

func (h *Handler) handleModifyInstanceNetworkPerformanceOptions(vals url.Values, reqID string) (any, error) {
	inst, err := h.Backend.ModifyInstanceNetworkPerformanceOptions(
		vals.Get("InstanceId"), vals.Get("BandwidthWeighting"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceNetworkPerformanceOptionsResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		InstanceID:         inst.ID,
		BandwidthWeighting: inst.NetworkPerformanceOptions.BandwidthWeighting,
	}, nil
}

// ---- ModifyInstancePlacement ----

type modifyInstancePlacementResponse struct {
	XMLName   xml.Name `xml:"ModifyInstancePlacementResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleModifyInstancePlacement(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.ModifyInstancePlacement(ModifyInstancePlacementInput{
		InstanceID:           vals.Get("InstanceId"),
		Affinity:             vals.Get("Affinity"),
		GroupID:              vals.Get("GroupId"),
		GroupName:            vals.Get("GroupName"),
		HostID:               vals.Get("HostId"),
		HostResourceGroupArn: vals.Get("HostResourceGroupArn"),
		Tenancy:              vals.Get("Tenancy"),
		PartitionNumber:      parseOptionalInt32(vals, "PartitionNumber"),
	})
	if err != nil {
		return nil, err
	}

	return &modifyInstancePlacementResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: ok}, nil
}

// ---- ModifyPrivateDnsNameOptions ----

type modifyPrivateDNSNameOptionsResponse struct {
	XMLName   xml.Name `xml:"ModifyPrivateDnsNameOptionsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleModifyPrivateDNSNameOptions(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.ModifyPrivateDNSNameOptions(
		vals.Get("InstanceId"),
		vals.Get("PrivateDnsHostnameType"),
		parseOptionalBool(vals, "EnableResourceNameDnsARecord"),
		parseOptionalBool(vals, "EnableResourceNameDnsAAAARecord"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyPrivateDNSNameOptionsResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: ok}, nil
}

// ---- ModifyPublicIpDnsNameOptions ----

type modifyPublicIPDNSNameOptionsResponse struct {
	XMLName    xml.Name `xml:"ModifyPublicIpDnsNameOptionsResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	Successful bool     `xml:"successful"`
}

func (h *Handler) handleModifyPublicIPDNSNameOptions(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.ModifyPublicIPDNSNameOptions(vals.Get("NetworkInterfaceId"), vals.Get("HostnameType"))
	if err != nil {
		return nil, err
	}

	return &modifyPublicIPDNSNameOptionsResponse{Xmlns: ec2XMLNS, RequestID: reqID, Successful: ok}, nil
}

// ---- AssociateInstanceEventWindow / DisassociateInstanceEventWindow ----

type associateInstanceEventWindowResponse struct {
	XMLName             xml.Name                `xml:"AssociateInstanceEventWindowResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	InstanceEventWindow instanceEventWindowItem `xml:"instanceEventWindow"`
}

type disassociateInstanceEventWindowResponse struct {
	XMLName             xml.Name                `xml:"DisassociateInstanceEventWindowResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	InstanceEventWindow instanceEventWindowItem `xml:"instanceEventWindow"`
}

func (h *Handler) handleAssociateInstanceEventWindow(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "AssociationTarget.InstanceId")
	hostIDs := parseMemberList(vals, "AssociationTarget.DedicatedHostId")

	ew, err := h.Backend.AssociateInstanceEventWindow(vals.Get("InstanceEventWindowId"), instanceIDs, hostIDs)
	if err != nil {
		return nil, err
	}

	return &associateInstanceEventWindowResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, InstanceEventWindow: toInstanceEventWindowItem(ew),
	}, nil
}

func (h *Handler) handleDisassociateInstanceEventWindow(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "AssociationTarget.InstanceId")
	hostIDs := parseMemberList(vals, "AssociationTarget.DedicatedHostId")

	ew, err := h.Backend.DisassociateInstanceEventWindow(vals.Get("InstanceEventWindowId"), instanceIDs, hostIDs)
	if err != nil {
		return nil, err
	}

	return &disassociateInstanceEventWindowResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, InstanceEventWindow: toInstanceEventWindowItem(ew),
	}, nil
}

// ---- GetInstanceTpmEkPub / GetInstanceUefiData ----

type getInstanceTPMEkPubResponse struct {
	XMLName    xml.Name `xml:"GetInstanceTpmEkPubResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	InstanceID string   `xml:"instanceId,omitempty"`
	KeyFormat  string   `xml:"keyFormat,omitempty"`
	KeyType    string   `xml:"keyType,omitempty"`
	KeyValue   string   `xml:"keyValue,omitempty"`
}

func (h *Handler) handleGetInstanceTpmEkPub(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	keyFormat := vals.Get("KeyFormat")
	keyType := vals.Get("KeyType")

	keyValue, err := h.Backend.GetInstanceTpmEkPub(instanceID, keyFormat, keyType)
	if err != nil {
		return nil, err
	}

	return &getInstanceTPMEkPubResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		InstanceID: instanceID, KeyFormat: keyFormat, KeyType: keyType, KeyValue: keyValue,
	}, nil
}

type getInstanceUefiDataResponse struct {
	XMLName    xml.Name `xml:"GetInstanceUefiDataResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	InstanceID string   `xml:"instanceId,omitempty"`
	UefiData   string   `xml:"uefiData,omitempty"`
}

func (h *Handler) handleGetInstanceUefiData(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")

	uefiData, err := h.Backend.GetInstanceUefiData(instanceID)
	if err != nil {
		return nil, err
	}

	return &getInstanceUefiDataResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, InstanceID: instanceID, UefiData: uefiData,
	}, nil
}
