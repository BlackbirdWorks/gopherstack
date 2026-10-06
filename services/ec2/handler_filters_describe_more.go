package ec2

import (
	"strconv"
	"time"
)

const (
	filterKeyInstanceTagKey = "instance-tag-key"
	filterKeyInstanceTagVal = "instance-tag-value"
	filterKeyInstStateName  = "instance-state-name"
)

// applyVpcPeeringConnectionFilters supports the stored filters in api_op_DescribeVpcPeeringConnections.go.
func applyVpcPeeringConnectionFilters(
	items []*VpcPeeringConnection, filters map[string][]string, b Backend,
) []*VpcPeeringConnection {
	return applyFilterList(items, filters, func(p *VpcPeeringConnection, name string, values []string) bool {
		if ok, handled := matchesTagFilter(p.VpcPeeringConnectionID, name, values, b); handled {
			return ok
		}

		switch name {
		case "vpc-peering-connection-id":
			return anyEqual(p.VpcPeeringConnectionID, values)
		case "status-code":
			return anyEqual(p.State, values)
		case "requester-vpc-info.vpc-id":
			return anyEqual(p.RequesterVpcID, values)
		case "accepter-vpc-info.vpc-id":
			return anyEqual(p.AccepterVpcID, values)
		case "accepter-vpc-info.owner-id":
			return anyEqual(p.AccepterOwnerID, values)
		}

		return true
	})
}

// applySecurityGroupForVpcFilters supports the five filters in api_op_GetSecurityGroupsForVpc.go.
func applySecurityGroupForVpcFilters(
	items []SecurityGroupForVpcItem, filters map[string][]string, ownerID string,
) []SecurityGroupForVpcItem {
	return applyFilterList(items, filters, func(s SecurityGroupForVpcItem, name string, values []string) bool {
		switch name {
		case filterKeyGroupID:
			return anyEqual(s.GroupID, values)
		case filterKeyDescription:
			return anyEqual(s.Description, values)
		case filterKeyGroupName:
			return anyEqual(s.GroupName, values)
		case filterKeyOwnerID:
			return anyEqual(ownerID, values)
		case "primary-vpc-id":
			return anyEqual(s.VPCID, values)
		}

		return true
	})
}

// applyTGWPolicyTableEntryFilters supports the nine filters in api_op_GetTransitGatewayPolicyTableEntries.go.
func applyTGWPolicyTableEntryFilters(
	items []*TransitGatewayPolicyTableEntry, filters map[string][]string,
) []*TransitGatewayPolicyTableEntry {
	return applyFilterList(items, filters, func(e *TransitGatewayPolicyTableEntry, name string, values []string) bool {
		switch name {
		case "policy-rule-number":
			return anyEqual(strconv.Itoa(e.PolicyRuleNumber), values)
		case "target-route-table-id":
			return anyEqual(e.TargetRouteTableID, values)
		case "policy-rule.source-ip":
			return anyEqual(e.SourceCidrBlock, values)
		case "policy-rule.destination-ip":
			return anyEqual(e.DestinationCidrBlock, values)
		case "policy-rule.source-port":
			return anyEqual(e.SourcePortRange, values)
		case "policy-rule.destination-port":
			return anyEqual(e.DestinationPortRange, values)
		case "policy-rule.protocol":
			return anyEqual(e.Protocol, values)
		case "policy-rule.meta-data.key":
			return anyEqual(e.MetaDataKey, values)
		case "policy-rule.meta-data.value":
			return anyEqual(e.MetaDataValue, values)
		}

		return true
	})
}

func multicastTypeFor(active bool) string {
	if active {
		return tgwRouteTypeStatic
	}

	return ""
}

// applyTGWMulticastGroupFilters supports the filters in api_op_SearchTransitGatewayMulticastGroups.go
// that the entry models; subnet-id and transit-gateway-attachment-id are unmodeled.
func applyTGWMulticastGroupFilters(
	items []*TransitGatewayMulticastGroupEntry, filters map[string][]string,
) []*TransitGatewayMulticastGroupEntry {
	return applyFilterList(items, filters, func(
		e *TransitGatewayMulticastGroupEntry, name string, values []string,
	) bool {
		switch name {
		case "group-ip-address":
			return anyEqual(e.GroupIPAddress, values)
		case "is-group-member":
			return anyEqual(strconv.FormatBool(e.IsMember), values)
		case "is-group-source":
			return anyEqual(strconv.FormatBool(e.IsSource), values)
		case "member-type":
			return anyEqual(multicastTypeFor(e.IsMember), values)
		case "source-type":
			return anyEqual(multicastTypeFor(e.IsSource), values)
		case filterKeyResourceID:
			return anyEqual(e.ResourceID, values)
		case filterKeyResourceType:
			return anyEqual(e.ResourceType, values)
		}

		return true
	})
}

// applyInstanceTopologyFilters supports the filters in api_op_DescribeInstanceTopology.go;
// instance-type allows * and ? wildcards.
func applyInstanceTopologyFilters(
	items []InstanceTopologyItem, filters map[string][]string,
) []InstanceTopologyItem {
	return applyFilterList(items, filters, func(i InstanceTopologyItem, name string, values []string) bool {
		switch name {
		case filterKeyAvailabilityZone:
			return anyEqual(i.AvailabilityZone, values)
		case filterKeyInstanceType:
			return anyWildcardMatch(i.InstanceType, values)
		case zoneIDFilterKey:
			return anyEqual(i.ZoneID, values)
		}

		return true
	})
}

// applyInstanceImageMetadataFilters supports the filters in api_op_DescribeInstanceImageMetadata.go
// except image-allowed and owner-alias, which have no backing data.
func applyInstanceImageMetadataFilters(
	items []InstanceImageMetadataItem, filters map[string][]string, b Backend,
) []InstanceImageMetadataItem {
	return applyFilterList(items, filters, func(i InstanceImageMetadataItem, name string, values []string) bool {
		if ok, handled := matchesTagFilter(i.InstanceID, name, values, b); handled {
			return ok
		}

		switch name {
		case filterKeyAvailabilityZone:
			return anyEqual(i.AvailabilityZone, values)
		case filterKeyInstanceID:
			return anyEqual(i.InstanceID, values)
		case filterKeyInstStateName:
			return anyEqual(i.StateName, values)
		case filterKeyInstanceType:
			return anyEqual(i.InstanceType, values)
		case filterKeyOwnerID:
			return anyEqual(i.OwnerID, values)
		case zoneIDFilterKey:
			return anyEqual(i.ZoneID, values)
		case "launch-time":
			return matchesWildcardTimeFilter(i.LaunchTime.UTC().Format(timeLayoutISO), values)
		}

		return true
	})
}

// applyCapacityReservationTopologyFilters supports availability-zone and instance-type
// (wildcards allowed) from api_op_DescribeCapacityReservationTopology.go.
func applyCapacityReservationTopologyFilters(
	items []*CapacityReservationTopologyEntry, filters map[string][]string,
) []*CapacityReservationTopologyEntry {
	return applyFilterList(
		items, filters, func(e *CapacityReservationTopologyEntry, name string, values []string) bool {
			switch name {
			case filterKeyAvailabilityZone:
				return anyEqual(e.AvailabilityZone, values)
			case filterKeyInstanceType:
				return anyWildcardMatch(e.InstanceType, values)
			}

			return true
		})
}

// applyLocalGatewayRouteFilters supports type and prefix-list-id from
// api_op_SearchLocalGatewayRoutes.go; state is applied by the backend search.
func applyLocalGatewayRouteFilters(items []*LocalGatewayRoute, filters map[string][]string) []*LocalGatewayRoute {
	return applyFilterList(items, filters, func(r *LocalGatewayRoute, name string, values []string) bool {
		switch name {
		case filterKeyType:
			return anyEqual(r.Type, values)
		case filterKeyPrefixListID:
			return anyEqual(r.DestinationPrefixListID, values)
		}

		return true
	})
}

// applyCapacityBlockDateFilters supports create-date, start-date and end-date
// (wildcard suffix allowed) from api_op_DescribeCapacityBlocks.go.
func applyCapacityBlockDateFilters(items []*CapacityBlock, filters map[string][]string) []*CapacityBlock {
	return applyFilterList(items, filters, func(c *CapacityBlock, name string, values []string) bool {
		switch name {
		case "create-date":
			return matchesWildcardTimeFilter(c.CreateDate.Format(time.RFC3339), values)
		case "start-date":
			return matchesWildcardTimeFilter(c.StartDate.Format(time.RFC3339), values)
		case "end-date":
			return matchesWildcardTimeFilter(c.EndDate.Format(time.RFC3339), values)
		}

		return true
	})
}

// applyCapacityBlockExtensionOfferingFilter supports capacity-block-extension-offering-id from
// api_op_DescribeCapacityBlockExtensionHistory.go.
func applyCapacityBlockExtensionOfferingFilter(
	items []*CapacityBlockExtension, filters map[string][]string,
) []*CapacityBlockExtension {
	return applyFilterList(items, filters, func(e *CapacityBlockExtension, name string, values []string) bool {
		if name == "capacity-block-extension-offering-id" {
			return anyEqual(e.CapacityBlockExtensionOfferingID, values)
		}

		return true
	})
}

// applyEventWindowInstanceTagFilters supports instance-tag-key and instance-tag-value from
// api_op_DescribeInstanceEventWindows.go; instance-tag's value syntax is undocumented.
func applyEventWindowInstanceTagFilters(
	items []*InstanceEventWindow, filters map[string][]string, b Backend,
) []*InstanceEventWindow {
	return applyFilterList(items, filters, func(w *InstanceEventWindow, name string, values []string) bool {
		if name != filterKeyInstanceTagKey && name != filterKeyInstanceTagVal {
			return true
		}

		for _, id := range w.InstanceIDs {
			for k, v := range b.TagsForResource(id) {
				got := v
				if name == filterKeyInstanceTagKey {
					got = k
				}

				if anyEqual(got, values) {
					return true
				}
			}
		}

		return false
	})
}
