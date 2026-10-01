package ec2

import "strconv"

// applyInstanceConnectEndpointFilters supports the filters documented in api_op_DescribeInstanceConnectEndpoints.go.
func applyInstanceConnectEndpointFilters(
	eps []*InstanceConnectEndpoint, filters map[string][]string, b Backend,
) []*InstanceConnectEndpoint {
	return applyFilterList(eps, filters, func(ep *InstanceConnectEndpoint, name string, values []string) bool {
		switch name {
		case "instance-connect-endpoint-id":
			return anyEqual(ep.InstanceConnectEndpointID, values)
		case filterKeyState:
			return anyEqual(ep.State, values)
		case filterKeySubnetID:
			return anyEqual(ep.SubnetID, values)
		case filterKeyVPCID:
			return anyEqual(ep.VPCID, values)
		case "tag-value":
			for _, v := range b.TagsForResource(ep.InstanceConnectEndpointID) {
				if anyEqual(v, values) {
					return true
				}
			}

			return false
		}

		if ok, handled := matchesTagFilter(ep.InstanceConnectEndpointID, name, values, b); handled {
			return ok
		}

		return true
	})
}

// applyStoreImageTaskFilters supports task-state and bucket (api_op_DescribeStoreImageTasks.go).
func applyStoreImageTaskFilters(tasks []*StoreImageTask, filters map[string][]string) []*StoreImageTask {
	return applyFilterList(tasks, filters, func(t *StoreImageTask, name string, values []string) bool {
		switch name {
		case filterKeyTaskState:
			return anyEqual(t.StoreTaskState, values)
		case "bucket":
			return anyEqual(t.Bucket, values)
		}

		return true
	})
}

// applyScheduledInstanceFilters supports availability-zone, instance-type and platform
// (api_op_DescribeScheduledInstances.go).
func applyScheduledInstanceFilters(
	items []*ScheduledInstance, filters map[string][]string,
) []*ScheduledInstance {
	return applyFilterList(items, filters, func(s *ScheduledInstance, name string, values []string) bool {
		switch name {
		case filterKeyAvailabilityZone:
			return anyEqual(s.AvailabilityZone, values)
		case filterKeyInstanceType:
			return anyEqual(s.InstanceType, values)
		case filterKeyPlatform:
			return anyEqual(s.Platform, values)
		}

		return true
	})
}

// applyTrunkInterfaceAssociationFilters supports gre-key and interface-protocol
// (api_op_DescribeTrunkInterfaceAssociations.go).
func applyTrunkInterfaceAssociationFilters(
	items []*TrunkInterfaceAssociation, filters map[string][]string,
) []*TrunkInterfaceAssociation {
	return applyFilterList(items, filters, func(a *TrunkInterfaceAssociation, name string, values []string) bool {
		switch name {
		case "gre-key":
			return anyEqual(strconv.Itoa(int(a.GreKey)), values)
		case "interface-protocol":
			return anyEqual(a.InterfaceProtocol, values)
		}

		return true
	})
}

// applyReservedInstancesListingFilters supports the four filters in
// api_op_DescribeReservedInstancesListings.go.
func applyReservedInstancesListingFilters(
	items []*ReservedInstancesListing, filters map[string][]string,
) []*ReservedInstancesListing {
	return applyFilterList(items, filters, func(l *ReservedInstancesListing, name string, values []string) bool {
		switch name {
		case "reserved-instances-id":
			return anyEqual(l.ReservedInstancesID, values)
		case "reserved-instances-listing-id":
			return anyEqual(l.ReservedInstancesListingID, values)
		case filterKeyStatus:
			return anyEqual(l.Status, values)
		case "status-message":
			return anyEqual(l.StatusMessage, values)
		}

		return true
	})
}

// applyFastSnapshotRestoreFilters supports availability-zone, owner-id, snapshot-id and state
// (api_op_DescribeFastSnapshotRestores.go).
func applyFastSnapshotRestoreFilters(
	items []FastSnapshotRestoreItem, filters map[string][]string, ownerID string,
) []FastSnapshotRestoreItem {
	return applyFilterList(items, filters, func(i FastSnapshotRestoreItem, name string, values []string) bool {
		switch name {
		case filterKeyAvailabilityZone:
			return anyEqual(i.AvailabilityZone, values)
		case filterKeyOwnerID:
			return anyEqual(ownerID, values)
		case filterKeySnapshotID:
			return anyEqual(i.SnapshotID, values)
		case filterKeyState:
			return anyEqual(i.State, values)
		}

		return true
	})
}

// applyMacModificationTaskFilters supports instance-id, task-state and task-type
// (api_op_DescribeMacModificationTasks.go).
func applyMacModificationTaskFilters(
	items []*MacModificationTask, filters map[string][]string,
) []*MacModificationTask {
	return applyFilterList(items, filters, func(t *MacModificationTask, name string, values []string) bool {
		switch name {
		case filterKeyInstanceID:
			return anyEqual(t.InstanceID, values)
		case filterKeyTaskState:
			return anyEqual(t.TaskState, values)
		case "task-type":
			return anyEqual(t.TaskType, values)
		}

		return true
	})
}

// applySGVpcAssociationFilters supports group-id, group-owner-id, state, vpc-id and vpc-owner-id
// (api_op_DescribeSecurityGroupVpcAssociations.go).
func applySGVpcAssociationFilters(items []SGVpcAssocItem, filters map[string][]string) []SGVpcAssocItem {
	return applyFilterList(items, filters, func(a SGVpcAssocItem, name string, values []string) bool {
		switch name {
		case filterKeyGroupID:
			return anyEqual(a.SGID, values)
		case "group-owner-id":
			return anyEqual(a.GroupOwnerID, values)
		case filterKeyState:
			return anyEqual(a.State, values)
		case filterKeyVPCID:
			return anyEqual(a.VPCID, values)
		case "vpc-owner-id":
			return anyEqual(a.VPCOwnerID, values)
		}

		return true
	})
}

// applyExportImageTaskFilters supports task-state (api_op_DescribeExportImageTasks.go).
func applyExportImageTaskFilters(tasks []*ExportImageTaskRec, filters map[string][]string) []*ExportImageTaskRec {
	return applyFilterList(tasks, filters, func(t *ExportImageTaskRec, name string, values []string) bool {
		if name == filterKeyTaskState {
			return anyEqual(t.Status, values)
		}

		return true
	})
}

// applyFastLaunchImageFilters supports resource-type, owner-id and state
// (api_op_DescribeFastLaunchImages.go).
func applyFastLaunchImageFilters(
	items []FastLaunchImageItem, filters map[string][]string, ownerID string,
) []FastLaunchImageItem {
	return applyFilterList(items, filters, func(i FastLaunchImageItem, name string, values []string) bool {
		switch name {
		case filterKeyResourceType:
			return anyEqual(i.ResourceType, values)
		case filterKeyOwnerID:
			return anyEqual(ownerID, values)
		case filterKeyState:
			return anyEqual(i.State, values)
		}

		return true
	})
}
