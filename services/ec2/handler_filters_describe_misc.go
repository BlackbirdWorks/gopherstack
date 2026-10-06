package ec2

import "strconv"

func tagValueMatches(resourceID string, values []string, b Backend) bool {
	for _, v := range b.TagsForResource(resourceID) {
		if anyEqual(v, values) {
			return true
		}
	}

	return false
}

// applyRegionFilters supports endpoint and region-name (api_op_DescribeRegions.go).
func applyRegionFilters(items []regionItem, filters map[string][]string) []regionItem {
	return applyFilterList(items, filters, func(r regionItem, name string, values []string) bool {
		switch name {
		case "endpoint":
			return anyEqual(r.Endpoint, values)
		case "region-name":
			return anyEqual(r.RegionName, values)
		case "opt-in-status":
			return anyEqual(r.OptInStatus, values)
		}

		return true
	})
}

// applyNIPermissionFilters supports the network-interface-permission.* filters
// (api_op_DescribeNetworkInterfacePermissions.go).
func applyNIPermissionFilters(
	items []*NetworkInterfacePermission, filters map[string][]string,
) []*NetworkInterfacePermission {
	return applyFilterList(items, filters, func(p *NetworkInterfacePermission, name string, values []string) bool {
		switch name {
		case "network-interface-permission.network-interface-permission-id":
			return anyEqual(p.PermissionID, values)
		case "network-interface-permission.network-interface-id":
			return anyEqual(p.NetworkInterfaceID, values)
		case "network-interface-permission.aws-account-id":
			return anyEqual(p.AwsAccountID, values)
		case "network-interface-permission.aws-service":
			return anyEqual(p.AwsService, values)
		case "network-interface-permission.permission":
			return anyEqual(p.Permission, values)
		}

		return true
	})
}

// applyIpv6PoolFilters supports tag:<key> and tag-key (api_op_DescribeIpv6Pools.go).
func applyIpv6PoolFilters(items []*Ipv6Pool, filters map[string][]string, b Backend) []*Ipv6Pool {
	return applyFilterList(items, filters, func(p *Ipv6Pool, name string, values []string) bool {
		if ok, handled := matchesTagFilter(p.PoolID, name, values, b); handled {
			return ok
		}

		return true
	})
}

// applyTrafficMirrorFilterRuleFilters supports the nine filters in
// api_op_DescribeTrafficMirrorFilterRules.go.
func applyTrafficMirrorFilterRuleFilters(
	items []*TrafficMirrorFilterRule, filters map[string][]string,
) []*TrafficMirrorFilterRule {
	return applyFilterList(items, filters, func(r *TrafficMirrorFilterRule, name string, values []string) bool {
		switch name {
		case "traffic-mirror-filter-rule-id":
			return anyEqual(r.TrafficMirrorFilterRuleID, values)
		case filterKeyTMFilterID:
			return anyEqual(r.TrafficMirrorFilterID, values)
		case "rule-number":
			return anyEqual(strconv.Itoa(r.RuleNumber), values)
		case "rule-action":
			return anyEqual(r.RuleAction, values)
		case "traffic-direction":
			return anyEqual(r.TrafficDirection, values)
		case filterKeyProtocol:
			return anyEqual(strconv.Itoa(r.Protocol), values)
		case "source-cidr-block":
			return anyEqual(r.SourceCidrBlock, values)
		case "destination-cidr-block":
			return anyEqual(r.DestinationCidrBlock, values)
		case filterKeyDescription:
			return anyEqual(r.Description, values)
		}

		return true
	})
}

// applyReplaceRootVolumeTaskFilters supports instance-id (api_op_DescribeReplaceRootVolumeTasks.go).
func applyReplaceRootVolumeTaskFilters(
	items []*ReplaceRootVolumeTask, filters map[string][]string,
) []*ReplaceRootVolumeTask {
	return applyFilterList(items, filters, func(t *ReplaceRootVolumeTask, name string, values []string) bool {
		if name == filterKeyInstanceID {
			return anyEqual(t.InstanceID, values)
		}

		return true
	})
}

// applyVpcClassicLinkFilters supports is-classic-link-enabled, tag:<key> and tag-key
// (api_op_DescribeVpcClassicLink.go).
func applyVpcClassicLinkFilters(items []*VPC, filters map[string][]string, b Backend) []*VPC {
	return applyFilterList(items, filters, func(v *VPC, name string, values []string) bool {
		if name == "is-classic-link-enabled" {
			return anyEqual(strconv.FormatBool(v.ClassicLinkEnabled), values)
		}

		if ok, handled := matchesTagFilter(v.ID, name, values, b); handled {
			return ok
		}

		return true
	})
}

// applyReservedInstancesModificationFilters supports the filters in
// api_op_DescribeReservedInstancesModifications.go that this backend stores.
func applyReservedInstancesModificationFilters(
	items []*ReservedInstancesModification, filters map[string][]string,
) []*ReservedInstancesModification {
	return applyFilterList(items, filters, func(m *ReservedInstancesModification, name string, values []string) bool {
		switch name {
		case "reserved-instances-modification-id":
			return anyEqual(m.ReservedInstancesModificationID, values)
		case filterKeyRIID:
			return anyOverlap(m.ReservedInstancesIDs, values)
		case filterKeyStatus:
			return anyEqual(m.Status, values)
		case "status-message":
			return anyEqual(m.StatusMessage, values)
		}

		return modificationResultMatches(m.ModificationResults, name, values)
	})
}

func modificationResultMatches(results []ReservedInstancesModificationResult, name string, values []string) bool {
	var field func(t ReservedInstancesConfigurationTarget) string

	switch name {
	case "modification-result.target-configuration.availability-zone":
		field = func(t ReservedInstancesConfigurationTarget) string { return t.AvailabilityZone }
	case "modification-result.target-configuration.availability-zone-id":
		field = func(t ReservedInstancesConfigurationTarget) string { return t.AvailabilityZoneID }
	case "modification-result.target-configuration.instance-count":
		field = func(t ReservedInstancesConfigurationTarget) string { return strconv.Itoa(t.InstanceCount) }
	case "modification-result.target-configuration.instance-type":
		field = func(t ReservedInstancesConfigurationTarget) string { return t.InstanceType }
	default:
		return true
	}

	for _, r := range results {
		if anyEqual(field(r.TargetConfiguration), values) {
			return true
		}
	}

	return false
}

func anyOverlap(have, values []string) bool {
	for _, h := range have {
		if anyEqual(h, values) {
			return true
		}
	}

	return false
}

// applyOutpostLagFilters supports outpost-lag-id, outpost-arn and owner-id
// (api_op_DescribeOutpostLags.go).
func applyOutpostLagFilters(items []*OutpostLag, filters map[string][]string) []*OutpostLag {
	return applyFilterList(items, filters, func(l *OutpostLag, name string, values []string) bool {
		switch name {
		case "outpost-lag-id":
			return anyEqual(l.OutpostLagID, values)
		case filterKeyOutpostArn:
			return anyEqual(l.OutpostArn, values)
		case filterKeyOwnerID:
			return anyEqual(l.OwnerID, values)
		}

		return true
	})
}

// applyVpcBPAExclusionFilters supports the filters in
// api_op_DescribeVpcBlockPublicAccessExclusions.go.
func applyVpcBPAExclusionFilters(
	items []*VpcBlockPublicAccessExclusion, filters map[string][]string, b Backend,
) []*VpcBlockPublicAccessExclusion {
	return applyFilterList(items, filters, func(e *VpcBlockPublicAccessExclusion, name string, values []string) bool {
		switch name {
		case "resource-arn":
			return anyEqual(e.ResourceArn, values)
		case "internet-gateway-exclusion-mode":
			return anyEqual(e.InternetGatewayExclusionMode, values)
		case filterKeyState:
			return anyEqual(e.State, values)
		case filterKeyTagValue:
			return tagValueMatches(e.ExclusionID, values, b)
		}

		if ok, handled := matchesTagFilter(e.ExclusionID, name, values, b); handled {
			return ok
		}

		return true
	})
}

// applySubnetCidrReservationFilters supports reservationType, subnet-id, tag:<key> and tag-key
// (api_op_GetSubnetCidrReservations.go).
func applySubnetCidrReservationFilters(
	items []*SubnetCIDRReservation, filters map[string][]string, b Backend,
) []*SubnetCIDRReservation {
	return applyFilterList(items, filters, func(r *SubnetCIDRReservation, name string, values []string) bool {
		switch name {
		case "reservationType":
			return anyEqual(r.ReservationType, values)
		case filterKeySubnetID:
			return anyEqual(r.SubnetID, values)
		}

		if ok, handled := matchesTagFilter(r.SubnetCIDRReservationID, name, values, b); handled {
			return ok
		}

		return true
	})
}

// applyImageUsageReportFilters supports state, tag:<key> and tag-key
// (api_op_DescribeImageUsageReports.go); creation-time wildcards are not modeled.
func applyImageUsageReportFilters(items []*UsageReport, filters map[string][]string, b Backend) []*UsageReport {
	return applyFilterList(items, filters, func(r *UsageReport, name string, values []string) bool {
		if name == filterKeyState {
			return anyEqual(r.State, values)
		}

		if ok, handled := matchesTagFilter(r.ReportID, name, values, b); handled {
			return ok
		}

		return true
	})
}
