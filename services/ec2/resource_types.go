package ec2

import "strings"

// This file maps an EC2 resource ID to (a) whether the resource is known to the
// backend (resourceExistsLocked, gating CreateTags/DeleteTags) and (b) its AWS
// ResourceType string (resourceTypeByID, used by DescribeTags and the
// resource-type Filter). Covers every resource type this backend models that AWS
// exposes as independently taggable (per aws-sdk-go-v2 ec2/types.ResourceType);
// non-taggable association/index objects are intentionally omitted.

// resourceTypePrefix pairs an ID prefix with its AWS ResourceType string.
type resourceTypePrefix struct {
	prefix string
	rtype  string
}

// resourceTypePrefixes lists ID-prefix → ResourceType mappings, ordered most-
// specific-prefix-first within each ambiguous family (e.g. "tgw-rtb-ann-"
// before "tgw-rtb-", "ipam-pool-" before "ipam-") so the first HasPrefix match
// in resourceTypeByID is always the correct one.
//
//nolint:gochecknoglobals // package-level lookup table, analogous to errCodeLookup
var resourceTypePrefixes = []resourceTypePrefix{
	// ---- core (pre-existing) ----
	{"i-", conversionKindInstance},
	{"sg-", "security-group"},
	{"subnet-", "subnet"},
	{"vol-", conversionKindVolume},
	{"igw-", "internet-gateway"},
	{"rtb-", "route-table"},
	{"nat-", "natgateway"},
	{"eipalloc-", "elastic-ip"},
	{"eni-", resourceTypeENI},
	{"sir-", "spot-instances-request"},
	{"sfr-", "spot-fleet-request"},

	// ---- images / snapshots / templates ----
	{"ami-", "image"},
	{"imgusgrpt-", "image-usage-report"},
	{"snap-", resourceTypeSnapshot},
	{"lt-", "launch-template"},
	{"import-ami-", "import-image-task"},
	{"import-snap-", "import-snapshot-task"},
	{"export-ami-", "export-image-task"},
	{"export-i-", "export-instance-task"},

	// ---- VPC networking ----
	{"vpc-ec-", "vpc-encryption-control"}, // must precede generic "vpc-"
	{"vpc-", resourceTypeVPC},
	{"acl-", "network-acl"},
	{"dopt-", "dhcp-options"},
	{"eigw-", "egress-only-internet-gateway"},
	{"pcx-", "vpc-peering-connection"},
	{"vpce-svc-", "vpc-endpoint-service"}, // must precede generic "vpce-"
	{"vpce-", "vpc-endpoint"},
	{"pl-", "prefix-list"},
	{"fl-", "vpc-flow-log"},
	{"vpcbpa-exclusion-", "vpc-block-public-access-exclusion"},
	{"eice-", "instance-connect-endpoint"},
	{"cagw-", "carrier-gateway"},

	// ---- VPN / customer gateways ----
	{"vgw-", "vpn-gateway"},
	{"cgw-", "customer-gateway"},
	{"vpn-", "vpn-connection"},
	{"vpnc-", "vpn-concentrator"},

	// ---- transit gateway family (generic "tgw-" must be last) ----
	{"tgw-rtb-ann-", "transit-gateway-route-table-announcement"},
	{"tgw-rtb-", "transit-gateway-route-table"},
	{"tgw-ptb-", "transit-gateway-policy-table"},
	{"tgw-metering-policy-", "transit-gateway-metering-policy"},
	{"tgw-mcast-domain-", "transit-gateway-multicast-domain"},
	{"tgw-connect-peer-", "transit-gateway-connect-peer"},
	{"tgw-attach-", "transit-gateway-attachment"},
	{"tgw-", "transit-gateway"},

	// ---- local gateway family (generic "lgw-" must be last) ----
	{"lgw-vif-grp-", "local-gateway-virtual-interface-group"},
	{"lgw-vif-", "local-gateway-virtual-interface"},
	{"lgw-rtb-", "local-gateway-route-table"},
	{"lgw-route-table-vpc-assoc-", "local-gateway-route-table-vpc-association"},
	{
		"lgw-route-table-virtual-interface-group-assoc-",
		"local-gateway-route-table-virtual-interface-group-association",
	},
	{"lgw-", "local-gateway"},

	// ---- IPAM family (generic "ipam-" must be last) ----
	{"ipam-prefix-list-resolver-target-", "ipam-prefix-list-resolver-target"},
	{"ipam-prefix-list-resolver-", "ipam-prefix-list-resolver"},
	{"ipam-res-disco-assoc-", "ipam-resource-discovery-association"},
	{"ipam-res-disco-", "ipam-resource-discovery"},
	{"ipam-ext-res-verification-token-", "ipam-external-resource-verification-token"},
	{"ipam-pool-", "ipam-pool"},
	{"ipam-scope-", "ipam-scope"},
	{"ipam-policy-", "ipam-policy"},
	{"ipam-", "ipam"},

	// ---- capacity reservations / hosts ----
	{"crf-", "capacity-reservation-fleet"},
	{"crcq-", "capacity-reservation-cancellation-quote"},
	{"cr-", "capacity-reservation"},
	{"cb-", "capacity-block"},
	{"cmde-", "capacity-manager-data-export"},
	{"hr-", "host-reservation"},
	{"h-", "dedicated-host"},

	// ---- fleets / reserved instances ----
	{"fleet-", "fleet"},
	{"ri-", "reserved-instances"},

	// ---- verified access ----
	{"vae-", "verified-access-endpoint"},
	{"vagr-", "verified-access-group"},
	{"vai-", "verified-access-instance"},
	{"vatp-", "verified-access-trust-provider"},

	// ---- traffic mirroring ----
	{"tmf-", "traffic-mirror-filter"},
	{"tmfr-", "traffic-mirror-filter-rule"},
	{"tms-", "traffic-mirror-session"},
	{"tmt-", "traffic-mirror-target"},

	// ---- network insights ----
	{"niasa-", "network-insights-access-scope-analysis"},
	{"nias-", "network-insights-access-scope"},
	{"nia-", "network-insights-analysis"},
	{"nip-", "network-insights-path"},

	// ---- route server ----
	{"rse-", "route-server-endpoint"},
	{"rsp-", "route-server-peer"},
	{"rs-", "route-server"},

	// ---- client VPN / FPGA / mac / misc ----
	{"cvpn-endpoint-", "client-vpn-endpoint"},
	{"afi-", "fpga-image"},
	{"agfi-", "fpga-image"},
	{"macmodtask-", "mac-modification-task"},
	{"lag-", "outpost-lag"},
	{"iew-", "instance-event-window"},
	{"report-", "declarative-policies-report"},

	// ---- secondary networking ----
	{"secnet-", "secondary-network"},
	{"secsubnet-", "secondary-subnet"},
	{"secni-", "secondary-interface"},
	{"svif-", "service-link-virtual-interface"},

	// ---- IP address pools ----
	{"ipv4pool-coip-", "coip-pool"},
	{"ipv4pool-ec2-", "ipv4pool-ec2"},
	{"ipv6pool-ec2-", "ipv6pool-ec2"},
	{"asc-", "application-status-check"},
}

// resourceTypeByID infers the EC2 resource type from the ID prefix.
func resourceTypeByID(id string) string {
	for _, e := range resourceTypePrefixes {
		if strings.HasPrefix(id, e.prefix) {
			return e.rtype
		}
	}

	return "resource"
}

// resourceExistsLocked reports whether id refers to any known EC2 resource.
// Must be called with b.mu held. Split across several helpers (one per
// resource family, each kept small enough to stay under the cyclomatic/
// cognitive complexity limits) rather than one flat function.
func (b *InMemoryBackend) resourceExistsLocked(id string) bool {
	return b.resourceExistsCoreLocked(id) ||
		b.resourceExistsImagesLocked(id) ||
		b.resourceExistsVpcAuxLocked(id) ||
		b.resourceExistsGatewayLocked(id) ||
		b.resourceExistsTGWLocked(id) ||
		b.resourceExistsLGWLocked(id) ||
		b.resourceExistsIpamLocked(id) ||
		b.resourceExistsVerifiedAccessAndMirrorLocked(id) ||
		b.resourceExistsInsightsAndRouteServerLocked(id) ||
		b.resourceExistsSecondaryAndMiscLocked(id) ||
		b.applicationStatusChecks.Has(id)
}

// resourceExistsCoreLocked checks the original core resource maps (instances,
// security groups, VPC/subnet, storage, networking primitives).
func (b *InMemoryBackend) resourceExistsCoreLocked(id string) bool {
	_, ok := b.instances.Get(id)
	ok = ok || b.securityGroups.Has(id)
	ok = ok || b.vpcs.Has(id)
	ok = ok || b.subnets.Has(id)
	ok = ok || b.keyPairs.Has(id)
	ok = ok || b.volumes.Has(id)
	ok = ok || b.addresses.Has(id)
	ok = ok || b.internetGateways.Has(id)
	ok = ok || b.routeTables.Has(id)
	ok = ok || b.natGateways.Has(id)
	ok = ok || b.networkInterfaces.Has(id)
	ok = ok || b.spotRequests.Has(id)
	ok = ok || b.placementGroups.Has(id)
	ok = ok || b.spotFleets.Has(id)

	return ok
}

// resourceExistsImagesLocked checks AMIs, snapshots, launch templates, and
// their import/export tasks.
func (b *InMemoryBackend) resourceExistsImagesLocked(id string) bool {
	ok := b.images.Has(id)
	ok = ok || b.usageReports.Has(id)
	ok = ok || b.snapshots.Has(id)
	ok = ok || b.recycleBinSnapshots.Has(id)
	ok = ok || b.launchTemplates.Has(id)
	ok = ok || b.imageImportTasks.Has(id)
	ok = ok || b.snapshotImportTasks.Has(id)
	ok = ok || b.exportImageTasks.Has(id)
	ok = ok || b.exportTasks.Has(id)

	return ok
}

// resourceExistsVpcAuxLocked checks VPC-adjacent networking resources
// (endpoints, peering, ACLs, DHCP options, flow logs, prefix lists).
func (b *InMemoryBackend) resourceExistsVpcAuxLocked(id string) bool {
	ok := b.networkACLs.Has(id)
	ok = ok || b.dhcpOptionSets.Has(id)
	ok = ok || b.egressOnlyIGWs.Has(id)
	ok = ok || b.vpcPeeringConnections.Has(id)
	ok = ok || b.vpcEndpoints.Has(id)
	ok = ok || b.vpcEndpointServiceConfigs.Has(id)
	ok = ok || b.managedPrefixLists.Has(id)
	ok = ok || b.flowLogs.Has(id)
	ok = ok || b.vpcBlockPublicAccessExclusions.Has(id)
	ok = ok || b.instanceConnectEndpoints.Has(id)
	ok = ok || b.carrierGateways.Has(id)
	ok = ok || b.vpcEncryptionControls.Has(id)

	return ok
}

// resourceExistsGatewayLocked checks VPN/customer gateways, capacity
// reservations/hosts, fleets, and reserved instances.
func (b *InMemoryBackend) resourceExistsGatewayLocked(id string) bool {
	ok := b.vpnGateways.Has(id)
	ok = ok || b.customerGateways.Has(id)
	ok = ok || b.vpnConnections.Has(id)
	ok = ok || b.vpnConcentrators.Has(id)
	ok = ok || b.capacityReservations.Has(id)
	ok = ok || b.capacityReservationFleets.Has(id)
	ok = ok || b.capacityReservationCancellationQuotes.Has(id)
	ok = ok || b.capacityBlocks.Has(id)
	ok = ok || b.capacityManagerDataExports.Has(id)
	ok = ok || b.hostReservations.Has(id)
	ok = ok || b.dedicatedHosts.Has(id)
	ok = ok || b.fleets.Has(id)
	ok = ok || b.reservedInstances.Has(id)

	return ok
}

// resourceExistsTGWLocked checks the transit-gateway resource family.
func (b *InMemoryBackend) resourceExistsTGWLocked(id string) bool {
	ok := b.transitGateways.Has(id)
	ok = ok || b.tgwRouteTables.Has(id)
	ok = ok || b.tgwPolicyTables.Has(id)
	ok = ok || b.tgwRouteTableAnnouncements.Has(id)
	ok = ok || b.tgwMeteringPolicies.Has(id)
	ok = ok || b.tgwMulticastDomains.Has(id)
	ok = ok || b.tgwConnectPeers.Has(id)
	ok = ok || b.tgwConnects.Has(id)
	ok = ok || b.tgwVpcAttachments.Has(id)
	ok = ok || b.tgwPeeringAttachments.Has(id)

	return ok
}

// resourceExistsLGWLocked checks the local-gateway resource family.
func (b *InMemoryBackend) resourceExistsLGWLocked(id string) bool {
	ok := b.localGateways.Has(id)
	ok = ok || b.localGatewayVirtualInterfaces.Has(id)
	ok = ok || b.localGatewayVirtualInterfaceGroups.Has(id)
	ok = ok || b.localGatewayRouteTables.Has(id)
	ok = ok || b.localGatewayRouteTableVpcAssociations.Has(id)
	ok = ok || b.localGatewayRouteTableVifGroupAssociations.Has(id)

	return ok
}

// resourceExistsIpamLocked checks the IPAM resource family.
func (b *InMemoryBackend) resourceExistsIpamLocked(id string) bool {
	ok := b.ipams.Has(id)
	ok = ok || b.ipamPools.Has(id)
	ok = ok || b.ipamScopes.Has(id)
	ok = ok || b.ipamResourceDiscoveries.Has(id)
	ok = ok || b.ipamResourceDiscoveryAssocs.Has(id)
	ok = ok || b.ipamVerificationTokens.Has(id)
	ok = ok || b.ipamPolicies.Has(id)
	ok = ok || b.ipamPrefixListResolvers.Has(id)
	ok = ok || b.ipamPrefixListResolverTargets.Has(id)
	ok = ok || b.ipv4Pools.Has(id)
	ok = ok || b.ipv6Pools.Has(id)
	ok = ok || b.coipPools.Has(id)

	return ok
}

// resourceExistsVerifiedAccessAndMirrorLocked checks Verified Access and
// traffic-mirroring resources.
func (b *InMemoryBackend) resourceExistsVerifiedAccessAndMirrorLocked(id string) bool {
	ok := b.verifiedAccessEndpoints.Has(id)
	ok = ok || b.verifiedAccessGroups.Has(id)
	ok = ok || b.verifiedAccessInstances.Has(id)
	ok = ok || b.verifiedAccessTrustProviders.Has(id)
	ok = ok || b.trafficMirrorFilters.Has(id)
	ok = ok || b.trafficMirrorFilterRules.Has(id)
	ok = ok || b.trafficMirrorSessions.Has(id)
	ok = ok || b.trafficMirrorTargets.Has(id)

	return ok
}

// resourceExistsInsightsAndRouteServerLocked checks Network Insights, Route
// Server, and Client VPN resources.
func (b *InMemoryBackend) resourceExistsInsightsAndRouteServerLocked(id string) bool {
	ok := b.networkInsightsPaths.Has(id)
	ok = ok || b.networkInsightsAnalyses.Has(id)
	ok = ok || b.networkInsightsAccessScopes.Has(id)
	ok = ok || b.networkInsightsAccessScopeAnalyses.Has(id)
	ok = ok || b.routeServers.Has(id)
	ok = ok || b.routeServerEndpoints.Has(id)
	ok = ok || b.routeServerPeers.Has(id)
	ok = ok || b.clientVpnEndpoints.Has(id)

	return ok
}

// resourceExistsSecondaryAndMiscLocked checks FPGA images, Mac modification
// tasks, Outpost LAGs, instance event windows, declarative policy reports,
// and secondary-networking resources.
func (b *InMemoryBackend) resourceExistsSecondaryAndMiscLocked(id string) bool {
	ok := b.fpgaImages.Has(id)
	ok = ok || b.macModificationTasks.Has(id)
	ok = ok || b.outpostLags.Has(id)
	ok = ok || b.instanceEventWindows.Has(id)
	ok = ok || b.declarativePoliciesReports.Has(id)
	ok = ok || b.secondaryNetworks.Has(id)
	ok = ok || b.secondarySubnets.Has(id)
	ok = ok || b.secondaryInterfaces.Has(id)
	ok = ok || b.serviceLinkVirtualInterfaces.Has(id)

	return ok
}
