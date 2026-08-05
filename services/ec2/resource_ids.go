package ec2

import (
	"strings"

	"github.com/google/uuid"
)

// This file centralizes ID-generator helpers for the many EC2 resource
// families that mint an ID as "<prefix>-" followed by hex characters taken
// from a fresh UUID. uuid.New().String() is hyphenated (8-4-4-4-12), so a
// naive [:N] slice embeds literal "-" characters into the ID once N crosses
// a hyphen boundary, producing shapes real AWS never returns (e.g.
// "subnet-44eea3bc-ae2c-4c2" instead of "subnet-44eea3bcae2c4c2..."). Every
// helper below strips the hyphens first via newHexUUID, matching the fix
// already applied to newSubnetID in subnets.go.

// ec2IDHexLen is the hex-character length AWS uses for the current
// (post-2016) EC2 resource ID format, e.g. "i-0123456789abcdef0". A handful
// of families use a different, deliberately-chosen length instead (see the
// named *PrefixLength constants and stubFingerprintUUIDLen/
// vpnPreSharedKeyLength/newIPAMVerificationTokenName's literal 12).
const ec2IDHexLen = 17

// newHexUUID returns n lowercase hex characters derived from a fresh UUID
// with the separating hyphens stripped, safe to embed directly after a
// resource ID prefix.
func newHexUUID(n int) string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")[:n]
}

// ---- Compute / instances ----

func newInstanceID() string                 { return "i-" + newHexUUID(ec2IDHexLen) }
func newReservationID() string              { return "r-" + newHexUUID(ec2IDHexLen) }
func newDedicatedHostID() string            { return "h-" + newHexUUID(ec2IDHexLen) }
func newScheduledInstanceID() string        { return "sci-" + newHexUUID(ec2IDHexLen) }
func newMacModificationTaskID() string      { return "macmodtask-" + newHexUUID(ec2IDHexLen) }
func newReservedInstanceExchangeID() string { return "riex-" + newHexUUID(ec2IDHexLen) }
func newReservedInstanceTargetID() string   { return "ri-" + newHexUUID(ec2IDHexLen) }
func newLaunchTemplateID() string           { return "lt-" + newHexUUID(ec2IDHexLen) }

// ---- Images / snapshots / volumes ----

func newAMIID() string             { return "ami-" + newHexUUID(ec2IDHexLen) }
func newFPGAImageID() string       { return "afi-" + newHexUUID(ec2IDHexLen) }
func newFPGAGlobalImageID() string { return "agfi-" + newHexUUID(ec2IDHexLen) }
func newSnapshotID() string        { return "snap-" + newHexUUID(ec2IDHexLen) }
func newVolumeID() string          { return "vol-" + newHexUUID(ec2IDHexLen) }

// ---- Networking core: VPC / subnet / security / ACL / route tables ----

func newVPCID() string                           { return "vpc-" + newHexUUID(ec2IDHexLen) }
func newSecurityGroupID() string                 { return "sg-" + newHexUUID(ec2IDHexLen) }
func newNetworkACLID() string                    { return "acl-" + newHexUUID(ec2IDHexLen) }
func newRouteTableID() string                    { return "rtb-" + newHexUUID(ec2IDHexLen) }
func newRouteTableAssociationID() string         { return "rtbassoc-" + newHexUUID(ec2IDHexLen) }
func newVPCCIDRAssociationID() string            { return "vpc-cidr-assoc-" + newHexUUID(ec2IDHexLen) }
func newInternetGatewayID() string               { return "igw-" + newHexUUID(ec2IDHexLen) }
func newEgressOnlyInternetGatewayID() string     { return "eigw-" + newHexUUID(ec2IDHexLen) }
func newDHCPOptionsID() string                   { return "dopt-" + newHexUUID(ec2IDHexLen) }
func newFlowLogID() string                       { return "fl-" + newHexUUID(ec2IDHexLen) }
func newIAMInstanceProfileAssociationID() string { return "iip-assoc-" + newHexUUID(ec2IDHexLen) }

// ---- ENIs / NAT / elastic IPs ----

func newENIID() string            { return "eni-" + newHexUUID(ec2IDHexLen) }
func newENIAttachmentID() string  { return "eni-attach-" + newHexUUID(ec2IDHexLen) }
func newNATGatewayID() string     { return "nat-" + newHexUUID(ec2IDHexLen) }
func newEIPAllocationID() string  { return "eipalloc-" + newHexUUID(ec2IDHexLen) }
func newEIPAssociationID() string { return "eipassoc-" + newHexUUID(ec2IDHexLen) }

// ---- VPC endpoints / peering / encryption / BPA ----

func newVPCEndpointID() string { return "vpce-" + newHexUUID(ec2IDHexLen) }

func newVPCBPAExclusionID() string {
	return "vpcbpa-exclusion-" + newHexUUID(vpcBPAExclusionIDPrefixLength)
}

func newVPCEncryptionControlID() string {
	return "vpc-ec-" + newHexUUID(vpcEncryptionControlIDPrefixLength)
}

// ---- Transit gateways / local gateways / VPN ----

func newTransitGatewayID() string            { return "tgw-" + newHexUUID(ec2IDHexLen) }
func newTransitGatewayAttachmentID() string  { return "tgw-attach-" + newHexUUID(ec2IDHexLen) }
func newTransitGatewayRouteTableID() string  { return "tgw-rtb-" + newHexUUID(ec2IDHexLen) }
func newTransitGatewayPolicyTableID() string { return "tgw-ptb-" + newHexUUID(ec2IDHexLen) }

func newTransitGatewayRouteTableAnnouncementID() string {
	return "tgw-rtb-ann-" + newHexUUID(ec2IDHexLen)
}

func newTransitGatewayMulticastDomainID() string {
	return "tgw-mcast-domain-" + newHexUUID(ec2IDHexLen)
}

func newTransitGatewayMeteringPolicyID() string {
	return "tgw-metering-policy-" + newHexUUID(ec2IDHexLen)
}

func newLocalGatewayID() string                      { return "lgw-" + newHexUUID(ec2IDHexLen) }
func newLocalGatewayVirtualInterfaceID() string      { return "lgw-vif-" + newHexUUID(ec2IDHexLen) }
func newLocalGatewayVirtualInterfaceGroupID() string { return "lgw-vif-grp-" + newHexUUID(ec2IDHexLen) }
func newLocalGatewayRouteTableID() string            { return "lgw-rtb-" + newHexUUID(ec2IDHexLen) }

func newLocalGatewayRouteTableVPCAssociationID() string {
	return "lgw-route-table-vpc-assoc-" + newHexUUID(ec2IDHexLen)
}

func newLocalGatewayRouteTableVirtualInterfaceGroupAssociationID() string {
	return "lgw-route-table-virtual-interface-group-assoc-" + newHexUUID(ec2IDHexLen)
}

func newVPNConcentratorID() string {
	return "vpnc-" + newHexUUID(vpnConcentratorIDPrefixLength)
}

func newClientVPNAssociationID() string { return "cvpn-assoc-" + newHexUUID(ec2IDHexLen) }

// newVPNPreSharedKey returns a generated IKE tunnel pre-shared key. It is not
// a resource ID, but the same hyphen-embedding bug applied here too: AWS
// pre-shared keys may only contain alphanumerics, '.', and '_', so a raw
// UUID slice could smuggle in an invalid '-' character.
func newVPNPreSharedKey() string { return newHexUUID(vpnPreSharedKeyLength) }

// ---- Secondary networking (Outposts) ----

func newSecondaryNetworkID() string            { return "secnet-" + newHexUUID(ec2IDHexLen) }
func newSecondarySubnetID() string             { return "secsubnet-" + newHexUUID(ec2IDHexLen) }
func newSecondaryInterfaceID() string          { return "secni-" + newHexUUID(ec2IDHexLen) }
func newServiceLinkVirtualInterfaceID() string { return "svif-" + newHexUUID(ec2IDHexLen) }
func newOutpostLagID() string                  { return "lag-" + newHexUUID(ec2IDHexLen) }
func newTrunkAssociationID() string            { return "trunk-assoc-" + newHexUUID(ec2IDHexLen) }

// ---- IPAM / host reservations / declarative policies ----

// ipamVerificationTokenHexLen is the deliberately shorter hex length AWS
// uses for IPAM external resource verification token names.
const ipamVerificationTokenHexLen = 12

func newIPAMVerificationTokenName() string {
	return "ipam-verify-" + newHexUUID(ipamVerificationTokenHexLen)
}

func newHostReservationID() string {
	return "hr-" + newHexUUID(hostReservationIDPrefixLength)
}

func newDeclarativePoliciesReportID() string {
	return "report-" + newHexUUID(declarativePoliciesReportIDPrefixLength)
}

// ---- CoIP / IPv4 / IPv6 pools ----

func newCoIPPoolID() string { return "ipv4pool-coip-" + newHexUUID(ec2IDHexLen) }
func newIPv4PoolID() string { return "ipv4pool-ec2-" + newHexUUID(ec2IDHexLen) }
func newIPv6PoolID() string { return "ipv6pool-ec2-" + newHexUUID(ec2IDHexLen) }

// newKeyPairFingerprint returns a stub ImportKeyPair fingerprint. No actual
// public key is parsed, so this is not a real MD5 fingerprint, but it must
// still avoid embedding a UUID hyphen where AWS would never emit one.
func newKeyPairFingerprint() string {
	return "aa:bb:cc:dd:" + newHexUUID(stubFingerprintUUIDLen)
}

func newApplicationStatusCheckID() string { return "asc-" + newHexUUID(ec2IDHexLen) }
