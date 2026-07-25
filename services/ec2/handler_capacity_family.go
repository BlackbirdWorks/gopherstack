package ec2

import (
	"time"
)

// ---- registration ----

// registerCapacityFamilyOps registers the real Capacity Reservation Fleet,
// Capacity Block, and Capacity Manager handlers, overriding any stub entries.
func registerCapacityFamilyOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateCapacityReservationFleet"] = h.handleCreateCapacityReservationFleet
	ops["DescribeCapacityReservationFleets"] = h.handleDescribeCapacityReservationFleets
	ops["ModifyCapacityReservationFleet"] = h.handleModifyCapacityReservationFleet
	ops["CancelCapacityReservationFleets"] = h.handleCancelCapacityReservationFleets

	ops["DescribeCapacityBlockOfferings"] = h.handleDescribeCapacityBlockOfferings
	ops["PurchaseCapacityBlock"] = h.handlePurchaseCapacityBlock
	ops["DescribeCapacityBlockExtensionOfferings"] = h.handleDescribeCapacityBlockExtensionOfferings
	ops["PurchaseCapacityBlockExtension"] = h.handlePurchaseCapacityBlockExtension
	ops["DescribeCapacityBlocks"] = h.handleDescribeCapacityBlocks
	ops["DescribeCapacityBlockStatus"] = h.handleDescribeCapacityBlockStatus
	ops["DescribeCapacityBlockExtensionHistory"] = h.handleDescribeCapacityBlockExtensionHistory

	ops["CreateCapacityReservationBySplitting"] = h.handleCreateCapacityReservationBySplitting
	ops["MoveCapacityReservationInstances"] = h.handleMoveCapacityReservationInstances

	ops["AssociateCapacityReservationBillingOwner"] = h.handleAssociateCapacityReservationBillingOwner
	ops["DisassociateCapacityReservationBillingOwner"] = h.handleDisassociateCapacityReservationBillingOwner
	ops["RejectCapacityReservationBillingOwnership"] = h.handleRejectCapacityReservationBillingOwnership
	ops["DescribeCapacityReservationBillingRequests"] = h.handleDescribeCapacityReservationBillingRequests

	ops["EnableCapacityManager"] = h.handleEnableCapacityManager
	ops["DisableCapacityManager"] = h.handleDisableCapacityManager
	ops["UpdateCapacityManagerOrganizationsAccess"] = h.handleUpdateCapacityManagerOrganizationsAccess
	ops["GetCapacityManagerAttributes"] = h.handleGetCapacityManagerAttributes
	ops["GetCapacityManagerMetricData"] = h.handleGetCapacityManagerMetricData
	ops["GetCapacityManagerMetricDimensions"] = h.handleGetCapacityManagerMetricDimensions
	ops["CreateCapacityManagerDataExport"] = h.handleCreateCapacityManagerDataExport
	ops["DescribeCapacityManagerDataExports"] = h.handleDescribeCapacityManagerDataExports
	ops["DeleteCapacityManagerDataExport"] = h.handleDeleteCapacityManagerDataExport

	ops["CreateCapacityReservation"] = h.handleCreateCapacityReservation
	ops["CancelCapacityReservation"] = h.handleCancelCapacityReservation
	ops["ModifyCapacityReservation"] = h.handleModifyCapacityReservation
	ops["GetGroupsForCapacityReservation"] = h.handleGetGroupsForCapacityReservation
	ops["CreateInterruptibleCapacityReservationAllocation"] = h.handleCreateInterruptibleCapacityReservationAllocation
	ops["UpdateInterruptibleCapacityReservationAllocation"] = h.handleUpdateInterruptibleCapacityReservationAllocation
	ops["GetCapacityReservationUsage"] = h.handleGetCapacityReservationUsage
	ops["DescribeCapacityReservationTopology"] = h.handleDescribeCapacityReservationTopology

	ops["CreateCapacityReservationCancellationQuote"] = h.handleCreateCapacityReservationCancellationQuote
	ops["DescribeCapacityReservationCancellationQuotes"] = h.handleDescribeCapacityReservationCancellationQuotes

	ops["GetCapacityManagerMonitoredTagKeys"] = h.handleGetCapacityManagerMonitoredTagKeys
	ops["UpdateCapacityManagerMonitoredTagKeys"] = h.handleUpdateCapacityManagerMonitoredTagKeys
}

// capacityFamilySupportedOperations lists the operation names registered by
// registerCapacityFamilyOps that were previously exposed via
// batch3SupportedOperations/parityFinalSupportedOperations, for
// GetSupportedOperations().
func capacityFamilySupportedOperations() []string {
	return []string{
		"CreateCapacityReservation",
		"CancelCapacityReservation",
		"ModifyCapacityReservation",
		"GetGroupsForCapacityReservation",
		"CreateInterruptibleCapacityReservationAllocation",
		"UpdateInterruptibleCapacityReservationAllocation",
		"GetCapacityReservationUsage",
		"DescribeCapacityReservationTopology",
		"CreateCapacityReservationCancellationQuote",
		"DescribeCapacityReservationCancellationQuotes",
		"GetCapacityManagerMonitoredTagKeys",
		"UpdateCapacityManagerMonitoredTagKeys",
	}
}

// ---- shared XML fragments ----

type capacityReservationFleetInstanceSpecItem struct {
	AvailabilityZone      string  `xml:"availabilityZone,omitempty"`
	InstancePlatform      string  `xml:"instancePlatform,omitempty"`
	InstanceType          string  `xml:"instanceType,omitempty"`
	CapacityReservationID string  `xml:"capacityReservationId,omitempty"`
	Priority              int32   `xml:"priority,omitempty"`
	Weight                float64 `xml:"weight,omitempty"`
	TotalInstanceCount    int32   `xml:"totalInstanceCount,omitempty"`
	EbsOptimized          bool    `xml:"ebsOptimized,omitempty"`
}

func capacityReservationFleetInstanceSpecItems(
	specs []CapacityReservationFleetInstanceSpec,
) []capacityReservationFleetInstanceSpecItem {
	items := make([]capacityReservationFleetInstanceSpecItem, 0, len(specs))
	for _, s := range specs {
		items = append(items, capacityReservationFleetInstanceSpecItem{
			AvailabilityZone:      s.AvailabilityZone,
			InstancePlatform:      s.InstancePlatform,
			InstanceType:          s.InstanceType,
			CapacityReservationID: s.CapacityReservationID,
			Priority:              s.Priority,
			Weight:                s.Weight,
			TotalInstanceCount:    s.TotalInstanceCount,
			EbsOptimized:          s.EbsOptimized,
		})
	}

	return items
}

type capacityReservationFleetItem struct {
	CapacityReservationFleetID string                                     `xml:"capacityReservationFleetId"`
	AllocationStrategy         string                                     `xml:"allocationStrategy,omitempty"`
	State                      string                                     `xml:"state,omitempty"`
	InstanceMatchCriteria      string                                     `xml:"instanceMatchCriteria,omitempty"`
	Tenancy                    string                                     `xml:"tenancy,omitempty"`
	CreateTime                 string                                     `xml:"createTime,omitempty"`
	EndDate                    string                                     `xml:"endDate,omitempty"`
	InstanceTypeSpecifications []capacityReservationFleetInstanceSpecItem `xml:"instanceTypeSpecificationSet>item"`
	TagSet                     []simpleTagItem                            `xml:"tagSet>item"`
	TotalFulfilledCapacity     float64                                    `xml:"totalFulfilledCapacity,omitempty"`
	TotalTargetCapacity        int32                                      `xml:"totalTargetCapacity,omitempty"`
}

func toCapacityReservationFleetItem(fleet *CapacityReservationFleet) capacityReservationFleetItem {
	item := capacityReservationFleetItem{
		CapacityReservationFleetID: fleet.CapacityReservationFleetID,
		AllocationStrategy:         fleet.AllocationStrategy,
		State:                      fleet.State,
		InstanceMatchCriteria:      fleet.InstanceMatchCriteria,
		Tenancy:                    fleet.Tenancy,
		TotalTargetCapacity:        fleet.TotalTargetCapacity,
		TotalFulfilledCapacity:     fleet.TotalFulfilledCapacity,
		InstanceTypeSpecifications: capacityReservationFleetInstanceSpecItems(fleet.InstanceTypeSpecifications),
		TagSet:                     tagItemsFromMap(fleet.Tags),
	}
	if !fleet.CreateTime.IsZero() {
		item.CreateTime = fleet.CreateTime.Format(time.RFC3339)
	}

	if fleet.EndDate != nil {
		item.EndDate = fleet.EndDate.Format(time.RFC3339)
	}

	return item
}
