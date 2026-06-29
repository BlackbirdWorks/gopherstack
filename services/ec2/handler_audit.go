package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// registerAuditOps registers the real handlers for operations that were
// previously hardcoded stubs in handler_stubs.go. It must run after
// registerStubOps so these entries override any leftover stub registration.
func registerAuditOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ModifyInstancePlacement"] = h.handleModifyInstancePlacement
	ops["ModifyInstanceCpuOptions"] = h.handleModifyInstanceCPUOptions
	ops["ModifyInstanceMaintenanceOptions"] = h.handleModifyInstanceMaintenanceOptions
	ops["ModifyInstanceNetworkPerformanceOptions"] = h.handleModifyInstanceNetworkPerformanceOptions
	ops["AssociateInstanceEventWindow"] = h.handleAssociateInstanceEventWindow
}

// ---- ModifyInstancePlacement ----

type modifyInstancePlacementResponse struct {
	XMLName   xml.Name `xml:"ModifyInstancePlacementResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleModifyInstancePlacement(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")

	placement := InstancePlacement{
		Tenancy:          vals.Get("Tenancy"),
		AvailabilityZone: vals.Get("AvailabilityZone"),
		GroupName:        vals.Get("GroupName"),
		Affinity:         vals.Get("Affinity"),
	}

	if _, err := h.Backend.ModifyInstancePlacement(instanceID, placement); err != nil {
		return nil, err
	}

	return &modifyInstancePlacementResponse{RequestID: reqID, Return: true}, nil
}

// ---- ModifyInstanceCpuOptions ----

type modifyInstanceCPUOptionsResponse struct {
	XMLName        xml.Name `xml:"ModifyInstanceCpuOptionsResponse"`
	RequestID      string   `xml:"requestId"`
	InstanceID     string   `xml:"instanceId"`
	CoreCount      int      `xml:"coreCount"`
	ThreadsPerCore int      `xml:"threadsPerCore"`
}

func (h *Handler) handleModifyInstanceCPUOptions(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")

	coreCount, _ := strconv.Atoi(vals.Get("CoreCount"))
	threadsPerCore, _ := strconv.Atoi(vals.Get("ThreadsPerCore"))

	inst, err := h.Backend.ModifyInstanceCPUOptions(instanceID, InstanceCPUOptions{
		CoreCount:      coreCount,
		ThreadsPerCore: threadsPerCore,
	})
	if err != nil {
		return nil, err
	}

	return &modifyInstanceCPUOptionsResponse{
		RequestID:      reqID,
		InstanceID:     inst.ID,
		CoreCount:      inst.CPUOptions.CoreCount,
		ThreadsPerCore: inst.CPUOptions.ThreadsPerCore,
	}, nil
}

// ---- ModifyInstanceMaintenanceOptions ----

type modifyInstanceMaintenanceOptionsResponse struct {
	XMLName      xml.Name `xml:"ModifyInstanceMaintenanceOptionsResponse"`
	RequestID    string   `xml:"requestId"`
	InstanceID   string   `xml:"instanceId"`
	AutoRecovery string   `xml:"autoRecovery,omitempty"`
}

func (h *Handler) handleModifyInstanceMaintenanceOptions(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceID := vals.Get("InstanceId")

	inst, err := h.Backend.ModifyInstanceMaintenanceOptions(instanceID, InstanceMaintenanceOptions{
		AutoRecovery: vals.Get("AutoRecovery"),
	})
	if err != nil {
		return nil, err
	}

	return &modifyInstanceMaintenanceOptionsResponse{
		RequestID:    reqID,
		InstanceID:   inst.ID,
		AutoRecovery: inst.MaintenanceOptions.AutoRecovery,
	}, nil
}

// ---- ModifyInstanceNetworkPerformanceOptions ----

type modifyInstanceNetworkPerformanceOptionsResponse struct {
	XMLName            xml.Name `xml:"ModifyInstanceNetworkPerformanceOptionsResponse"`
	RequestID          string   `xml:"requestId"`
	InstanceID         string   `xml:"instanceId"`
	BandwidthWeighting string   `xml:"bandwidthWeighting,omitempty"`
}

func (h *Handler) handleModifyInstanceNetworkPerformanceOptions(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceID := vals.Get("InstanceId")

	inst, err := h.Backend.ModifyInstanceNetworkPerformanceOptions(
		instanceID,
		InstanceNetworkPerformanceOptions{
			BandwidthWeighting: vals.Get("BandwidthWeighting"),
		},
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceNetworkPerformanceOptionsResponse{
		RequestID:          reqID,
		InstanceID:         inst.ID,
		BandwidthWeighting: inst.NetworkPerformanceOptions.BandwidthWeighting,
	}, nil
}

// ---- AssociateInstanceEventWindow ----

type associateInstanceEventWindowResponse struct {
	InstanceEventWindow instanceEventWindowItem `xml:"instanceEventWindow"`
	XMLName             xml.Name                `xml:"AssociateInstanceEventWindowResponse"`
	RequestID           string                  `xml:"requestId"`
}

func (h *Handler) handleAssociateInstanceEventWindow(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("InstanceEventWindowId")

	instanceIDs := parseMemberList(vals, "AssociationTarget.InstanceId")
	dedicatedHostIDs := parseMemberList(vals, "AssociationTarget.DedicatedHostId")

	// Instance tags are encoded as AssociationTarget.InstanceTag.N.Key /
	// .Value; record them as "key=value" strings for reflection.
	var instanceTags []string
	for i := 1; ; i++ {
		key := vals.Get("AssociationTarget.InstanceTag." + strconv.Itoa(i) + ".Key")
		if key == "" {
			break
		}
		val := vals.Get("AssociationTarget.InstanceTag." + strconv.Itoa(i) + ".Value")
		instanceTags = append(instanceTags, key+"="+val)
	}

	ew, err := h.Backend.AssociateInstanceEventWindow(
		id, instanceIDs, instanceTags, dedicatedHostIDs,
	)
	if err != nil {
		return nil, err
	}

	return &associateInstanceEventWindowResponse{
		RequestID:           reqID,
		InstanceEventWindow: toInstanceEventWindowItem(ew),
	}, nil
}
