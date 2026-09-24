package ec2

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// This file adds EC2 filter matching for resource types that previously
// supported only ID-based lookup. Each applyXxxFilters function follows the
// standard EC2 convention: AND across filter names, OR within each filter's
// values. Unknown filter names pass through (lenient mock behaviour).
//
// tag:<key> filters are supported on all types that store tags. They delegate
// to Backend.TagsForResource which is already used by applyInstanceFilters.

// Common EC2 filter key name constants — shared across filter match functions.
const (
	filterKeyVPCID            = "vpc-id"
	filterKeySubnetID         = "subnet-id"
	filterKeyState            = "state"
	filterKeyStatus           = "status"
	filterKeyDescription      = "description"
	filterKeyInstanceID       = "instance-id"
	filterKeyAvailabilityZone = "availability-zone"
	filterKeyVolumeID         = "volume-id"
	filterKeyDhcpConfigKey    = "key"
	filterKeyDhcpConfigValue  = "value"
	filterKeyResourceID       = "resource-id"
	filterKeyInstanceType     = "instance-type"
	filterKeyType             = "type"
	filterKeyOwnerID          = "owner-id"
	filterKeySecondaryNetID   = "secondary-network-id"
	filterKeyResourceType     = "resource-type"
	filterKeyAttachInstanceID = "attachment.instance-id"
	filterKeyImageID          = "image-id"
	filterKeyIsDefault        = "is-default"
	filterKeyTransitGatewayID = "transit-gateway-id"
	filterKeyTagKey           = "tag-key"
	filterKeyTGWAttachmentID  = "transit-gateway-attachment-id"
	filterKeyServiceID        = "service-id"
	filterKeyDestinationCidr  = "destination-cidr"
	filterKeyPrefixListID     = "prefix-list-id"
	filterKeyGroupName        = "group-name"
	filterKeyNetworkIfaceID   = "network-interface-id"
	filterKeyLocalGatewayID   = "local-gateway-id"
	filterKeyLGWRouteTableArn = "local-gateway-route-table-arn"
	filterKeyLGWRouteTableID  = "local-gateway-route-table-id"
	filterKeyVpcEndpointID    = "vpc-endpoint-id"
	filterKeyProductDesc      = "product-description"
	filterKeyOutpostArn       = "outpost-arn"
)

// applyFilterList runs the standard AND-across-names/OR-within-values filter
// loop shared by every applyXxxFilters function below, so each one only has
// to supply its own per-item matcher instead of repeating the loop body.
func applyFilterList[T any](
	items []T, filters map[string][]string, matches func(item T, name string, values []string) bool,
) []T {
	if len(filters) == 0 {
		return items
	}

	out := items[:0:0]

itemLoop:
	for _, item := range items {
		for name, values := range filters {
			if !matches(item, name, values) {
				continue itemLoop
			}
		}

		out = append(out, item)
	}

	return out
}

// matchesTGWResourceFilter matches the resource-id/resource-type/
// transit-gateway-attachment-id filters shared by GetTransitGatewayRouteTableAssociations,
// GetTransitGatewayRouteTablePropagations, and
// GetTransitGatewayMulticastDomainAssociations. handled is false when
// filterName isn't one of these three, so callers can fall through to their
// own additional filters.
func matchesTGWResourceFilter(
	filterName string, values []string, resourceID, resourceType, attachmentID string,
) (bool, bool) {
	switch filterName {
	case filterKeyResourceID:
		return anyEqual(resourceID, values), true
	case filterKeyResourceType:
		return anyEqual(resourceType, values), true
	case filterKeyTGWAttachmentID:
		return anyEqual(attachmentID, values), true
	}

	return false, false
}

// matchesTagFilter handles the tag-key/tag:<key> filter pair shared by
// several apply*Filters functions. handled is false when filterName is
// neither, so callers fall through to their own filters.
func matchesTagFilter(resourceID, filterName string, values []string, b Backend) (bool, bool) {
	if filterName == filterKeyTagKey {
		for k := range b.TagsForResource(resourceID) {
			if anyEqual(k, values) {
				return true, true
			}
		}

		return false, true
	}

	if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
		return tagMatch(resourceID, tagKey, values, b), true
	}

	return false, false
}

// tagMatch returns true when the resource's tag at tagKey equals any of values.
func tagMatch(resourceID string, tagKey string, values []string, b Backend) bool {
	tags := b.TagsForResource(resourceID)
	tagVal, exists := tags[tagKey]
	if !exists {
		return false
	}

	return anyEqual(tagVal, values)
}

// ---- VPC filters ----

func applyVPCFilters(vpcs []*VPC, filters map[string][]string, b Backend) []*VPC {
	if len(filters) == 0 {
		return vpcs
	}

	out := vpcs[:0:0]
vpcLoop:
	for _, v := range vpcs {
		for name, values := range filters {
			if !vpcMatchesFilter(v, name, values, b) {
				continue vpcLoop
			}
		}

		out = append(out, v)
	}

	return out
}

// DescribeVpcs' "cidr-block-association.*" / "ipv6-cidr-block-association.*"
// filter names.
const (
	filterCidrBlockAssocCidrBlock     = "cidr-block-association.cidr-block"
	filterCidrBlockAssocAssociationID = "cidr-block-association.association-id"
	filterCidrBlockAssocState         = "cidr-block-association.state"
	filterIpv6CidrBlockAssocCidrBlock = "ipv6-cidr-block-association.ipv6-cidr-block"
	filterIpv6CidrBlockAssocAssocID   = "ipv6-cidr-block-association.association-id"
	filterIpv6CidrBlockAssocPool      = "ipv6-cidr-block-association.ipv6-pool"
	filterIpv6CidrBlockAssocState     = "ipv6-cidr-block-association.state"
)

func vpcMatchesFilter(v *VPC, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyVPCID:
		return anyEqual(v.ID, values)
	case "cidr", "cidr-block", "cidrBlock":
		return anyEqual(v.CIDRBlock, values)
	case "isDefault", filterKeyIsDefault:
		want := anyEqual("true", values)

		return v.IsDefault == want
	case filterKeyState:
		return anyEqual("available", values)
	case filterCidrBlockAssocCidrBlock, filterCidrBlockAssocAssociationID, filterCidrBlockAssocState:
		return vpcMatchesCidrBlockAssocFilter(v, filterName, values, b)
	case filterIpv6CidrBlockAssocCidrBlock, filterIpv6CidrBlockAssocAssocID,
		filterIpv6CidrBlockAssocPool, filterIpv6CidrBlockAssocState:
		return vpcMatchesIpv6CidrBlockAssocFilter(v, filterName, values, b)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(v.ID, tagKey, values, b)
		}
	}

	return true
}

// vpcMatchesCidrBlockAssocFilter matches the "cidr-block-association.*"
// DescribeVpcs filters against v's primary CIDR (always "associated") plus
// its secondary IPv4 CIDR associations. Without this, a filtered
// DescribeVpcs returns every VPC instead of just the matching one, which
// broke terraform-provider-aws's wait-for-associated waiter for
// aws_vpc_ipv4_cidr_block_association (it polls by association-id filter
// and treats "not exactly one VPC" as not-found).
func vpcMatchesCidrBlockAssocFilter(v *VPC, filterName string, values []string, b Backend) bool {
	if anyEqual(v.CIDRBlock, values) && filterName == filterCidrBlockAssocCidrBlock {
		return true
	}

	if filterName == filterCidrBlockAssocState && anyEqual(stateAssociated, values) {
		return true
	}

	for _, assoc := range b.SecondaryCidrBlockAssociationsForVPC(v.ID) {
		switch filterName {
		case filterCidrBlockAssocCidrBlock:
			if anyEqual(assoc.CidrBlock, values) {
				return true
			}
		case filterCidrBlockAssocAssociationID:
			if anyEqual(assoc.AssociationID, values) {
				return true
			}
		case filterCidrBlockAssocState:
			if anyEqual(assoc.State, values) {
				return true
			}
		}
	}

	return false
}

// vpcMatchesIpv6CidrBlockAssocFilter matches the
// "ipv6-cidr-block-association.*" DescribeVpcs filters against v's IPv6
// CIDR associations -- see vpcMatchesCidrBlockAssocFilter for why this
// matters for the corresponding waiter.
func vpcMatchesIpv6CidrBlockAssocFilter(v *VPC, filterName string, values []string, b Backend) bool {
	for _, assoc := range b.SecondaryIpv6CidrBlockAssociationsForVPC(v.ID) {
		switch filterName {
		case filterIpv6CidrBlockAssocCidrBlock:
			if anyEqual(assoc.Ipv6CidrBlock, values) {
				return true
			}
		case filterIpv6CidrBlockAssocAssocID:
			if anyEqual(assoc.AssociationID, values) {
				return true
			}
		case filterIpv6CidrBlockAssocPool:
			if anyEqual(assoc.Ipv6Pool, values) {
				return true
			}
		case filterIpv6CidrBlockAssocState:
			if anyEqual(assoc.State, values) {
				return true
			}
		}
	}

	return false
}

// ---- Subnet filters ----

func applySubnetFilters(subnets []*Subnet, filters map[string][]string, b Backend) []*Subnet {
	if len(filters) == 0 {
		return subnets
	}

	out := subnets[:0:0]
subnetLoop:
	for _, s := range subnets {
		for name, values := range filters {
			if !subnetMatchesFilter(s, name, values, b) {
				continue subnetLoop
			}
		}

		out = append(out, s)
	}

	return out
}

func subnetMatchesFilter(s *Subnet, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeySubnetID:
		return anyEqual(s.ID, values)
	case filterKeyVPCID:
		return anyEqual(s.VPCID, values)
	case "cidr", "cidr-block", "cidrBlock":
		return anyEqual(s.CIDRBlock, values)
	case "availabilityZone", filterKeyAvailabilityZone:
		return anyEqual(s.AvailabilityZone, values)
	case filterKeyState:
		return anyEqual("available", values)
	case "defaultForAz", "default-for-az":
		want := anyEqual("true", values)

		return s.IsDefault == want
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(s.ID, tagKey, values, b)
		}
	}

	return true
}

// ---- Volume filters ----

func applyVolumeFilters(vols []*Volume, filters map[string][]string, b Backend) []*Volume {
	if len(filters) == 0 {
		return vols
	}

	out := vols[:0:0]
volLoop:
	for _, vol := range vols {
		for name, values := range filters {
			if !volumeMatchesFilter(vol, name, values, b) {
				continue volLoop
			}
		}

		out = append(out, vol)
	}

	return out
}

func volumeMatchesFilter(vol *Volume, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyVolumeID:
		return anyEqual(vol.ID, values)
	case filterKeyStatus:
		return anyEqual(vol.State, values)
	case filterKeyAvailabilityZone:
		return anyEqual(vol.AZ, values)
	case "volume-type":
		return anyEqual(vol.VolumeType, values)
	case "encrypted":
		want := anyEqual("true", values)

		return vol.Encrypted == want
	case filterKeyAttachInstanceID:
		if vol.Attachment == nil {
			return false
		}

		return anyEqual(vol.Attachment.InstanceID, values)
	case "attachment.status":
		if vol.Attachment == nil {
			return false
		}

		return anyEqual(vol.Attachment.State, values)
	case "attachment.device":
		if vol.Attachment == nil {
			return false
		}

		return anyEqual(vol.Attachment.Device, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(vol.ID, tagKey, values, b)
		}
	}

	return true
}

// ---- KeyPair filters ----

func applyKeyPairFilters(kps []*KeyPair, filters map[string][]string, b Backend) []*KeyPair {
	if len(filters) == 0 {
		return kps
	}

	out := kps[:0:0]
kpLoop:
	for _, kp := range kps {
		for name, values := range filters {
			if !keyPairMatchesFilter(kp, name, values, b) {
				continue kpLoop
			}
		}

		out = append(out, kp)
	}

	return out
}

func keyPairMatchesFilter(kp *KeyPair, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "key-name":
		return anyEqual(kp.Name, values)
	case "key-pair-id":
		return anyEqual(kp.KeyPairID, values)
	case "fingerprint":
		return anyEqual(kp.Fingerprint, values)
	default:
		// Tags are stored under the key pair's Name (its only real,
		// stable identifier in this backend — see resourceExistsCoreLocked);
		// this previously looked up "keypair-"+Name, a key nothing ever
		// wrote to, so the filter silently never matched.
		if handled, ok := matchesTagFilter(kp.Name, filterName, values, b); ok {
			return handled
		}
	}

	return true
}

// ---- Snapshot filters ----

func applySnapshotFilters(snaps []*Snapshot, filters map[string][]string, b Backend) []*Snapshot {
	if len(filters) == 0 {
		return snaps
	}

	out := snaps[:0:0]
snapLoop:
	for _, s := range snaps {
		for name, values := range filters {
			if !snapshotMatchesFilter(s, name, values, b) {
				continue snapLoop
			}
		}

		out = append(out, s)
	}

	return out
}

func snapshotMatchesFilter(s *Snapshot, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "snapshot-id":
		return anyEqual(s.SnapshotID, values)
	case filterKeyVolumeID:
		return anyEqual(s.VolumeID, values)
	case filterKeyStatus:
		return anyEqual(s.State, values)
	case "encrypted":
		want := anyEqual("true", values)

		return s.Encrypted == want
	case filterKeyDescription:
		return anyEqual(s.Description, values)
	case filterKeyOwnerID:
		return anyEqual(s.OwnerID, values)
	case "volume-size":
		return anyEqual(strconv.Itoa(s.VolumeSize), values)
	default:
		if handled, ok := matchesTagFilter(s.SnapshotID, filterName, values, b); ok {
			return handled
		}
	}

	return true
}

// ---- InternetGateway filters ----

func applyIGWFilters(igws []*InternetGateway, filters map[string][]string, b Backend) []*InternetGateway {
	if len(filters) == 0 {
		return igws
	}

	out := igws[:0:0]
igwLoop:
	for _, igw := range igws {
		for name, values := range filters {
			if !igwMatchesFilter(igw, name, values, b) {
				continue igwLoop
			}
		}

		out = append(out, igw)
	}

	return out
}

func igwMatchesFilter(igw *InternetGateway, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "internet-gateway-id":
		return anyEqual(igw.ID, values)
	case "attachment.vpc-id":
		for _, att := range igw.Attachments {
			if anyEqual(att.VPCID, values) {
				return true
			}
		}

		return false
	case "attachment.state":
		for _, att := range igw.Attachments {
			if anyEqual(att.State, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(igw.ID, tagKey, values, b)
		}
	}

	return true
}

// ---- NatGateway filters ----

func applyNatGWFilters(ngws []*NatGateway, filters map[string][]string, b Backend) []*NatGateway {
	if len(filters) == 0 {
		return ngws
	}

	out := ngws[:0:0]
natLoop:
	for _, ngw := range ngws {
		for name, values := range filters {
			if !natGWMatchesFilter(ngw, name, values, b) {
				continue natLoop
			}
		}

		out = append(out, ngw)
	}

	return out
}

func natGWMatchesFilter(ngw *NatGateway, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "nat-gateway-id":
		return anyEqual(ngw.ID, values)
	case filterKeySubnetID:
		return anyEqual(ngw.SubnetID, values)
	case filterKeyState:
		return anyEqual(ngw.State, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(ngw.ID, tagKey, values, b)
		}
	}

	return true
}

// ---- NetworkInterface filters ----

func applyENIFilters(enis []*NetworkInterface, filters map[string][]string, b Backend) []*NetworkInterface {
	if len(filters) == 0 {
		return enis
	}

	out := enis[:0:0]
eniLoop:
	for _, eni := range enis {
		for name, values := range filters {
			if !eniMatchesFilter(eni, name, values, b) {
				continue eniLoop
			}
		}

		out = append(out, eni)
	}

	return out
}

func eniMatchesFilter(eni *NetworkInterface, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "network-interface-id":
		return anyEqual(eni.ID, values)
	case filterKeyVPCID:
		return anyEqual(eni.VPCID, values)
	case filterKeySubnetID:
		return anyEqual(eni.SubnetID, values)
	case filterKeyStatus:
		return anyEqual(eni.Status, values)
	case filterKeyDescription:
		return anyEqual(eni.Description, values)
	case "private-ip-address":
		return anyEqual(eni.PrivateIP, values)
	case filterKeyAttachInstanceID:
		return anyEqual(eni.InstanceID, values)
	case "attachment.attachment-id":
		return eni.AttachmentID != "" && anyEqual(eni.AttachmentID, values)
	case "attachment.status":
		return anyEqual(eni.Status, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(eni.ID, tagKey, values, b)
		}
	}

	return true
}

// ---- Address (EIP) filters ----

func applyAddressFilters(addrs []*Address, filters map[string][]string, b Backend) []*Address {
	if len(filters) == 0 {
		return addrs
	}

	out := addrs[:0:0]
addrLoop:
	for _, addr := range addrs {
		for name, values := range filters {
			if !addressMatchesFilter(addr, name, values, b) {
				continue addrLoop
			}
		}

		out = append(out, addr)
	}

	return out
}

func addressMatchesFilter(addr *Address, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "allocation-id":
		return anyEqual(addr.AllocationID, values)
	case "public-ip":
		return anyEqual(addr.PublicIP, values)
	case "association-id":
		return anyEqual(addr.AssociationID, values)
	case filterKeyInstanceID:
		return anyEqual(addr.InstanceID, values)
	case "domain":
		return anyEqual(resourceTypeVPC, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(addr.AllocationID, tagKey, values, b)
		}
	}

	return true
}

// ---- RouteTable filters ----

// applyRouteTableFilters supports route-table-id, vpc-id, association.subnet-id,
// association.route-table-association-id, association.main, route.destination-cidr-block,
// and tag: (api_op_DescribeRouteTables.go).
func applyRouteTableFilters(rts []*RouteTable, filters map[string][]string, b Backend) []*RouteTable {
	if len(filters) == 0 {
		return rts
	}

	out := rts[:0:0]
rtLoop:
	for _, rt := range rts {
		for name, values := range filters {
			if !routeTableMatchesFilter(rt, name, values, b) {
				continue rtLoop
			}
		}

		out = append(out, rt)
	}

	return out
}

func routeTableMatchesFilter(rt *RouteTable, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "route-table-id":
		return anyEqual(rt.ID, values)
	case filterKeyVPCID:
		return anyEqual(rt.VPCID, values)
	case "association.subnet-id":
		return routeTableHasAssocSubnet(rt, values)
	case "association.route-table-association-id":
		return routeTableHasAssocID(rt, values)
	case "association.main":
		return routeTableHasMainAssoc(rt) == anyEqual("true", values)
	case "route.destination-cidr-block":
		return routeTableHasRoute(rt, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(rt.ID, tagKey, values, b)
		}
	}

	return true
}

func routeTableHasAssocSubnet(rt *RouteTable, values []string) bool {
	for _, assoc := range rt.Associations {
		if anyEqual(assoc.SubnetID, values) {
			return true
		}
	}

	return false
}

func routeTableHasMainAssoc(rt *RouteTable) bool {
	for _, assoc := range rt.Associations {
		if assoc.Main {
			return true
		}
	}

	return false
}

func routeTableHasAssocID(rt *RouteTable, values []string) bool {
	for _, assoc := range rt.Associations {
		if anyEqual(assoc.ID, values) {
			return true
		}
	}

	return false
}

func routeTableHasRoute(rt *RouteTable, values []string) bool {
	for _, r := range rt.Routes {
		if anyEqual(r.DestinationCIDR, values) {
			return true
		}
	}

	return false
}

// ---- AMI / Image filters ----

func applyImageFilters(amis []*AMIStub, filters map[string][]string, b Backend) []*AMIStub {
	if len(filters) == 0 {
		return amis
	}

	out := amis[:0:0]
amiLoop:
	for _, a := range amis {
		for name, values := range filters {
			if !imageMatchesFilter(a, name, values, b) {
				continue amiLoop
			}
		}

		out = append(out, a)
	}

	return out
}

func imageMatchesFilter(a *AMIStub, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyImageID:
		return anyEqual(a.ImageID, values)
	case "name":
		return anyEqual(a.Name, values)
	case "architecture":
		return anyEqual(a.Architecture, values)
	case "platform":
		return anyEqual(a.Platform, values)
	case filterKeyState:
		st := a.State
		if st == "" {
			st = stateAvailable
		}

		return anyEqual(st, values)
	case "root-device-name":
		return anyEqual(a.RootDeviceName, values)
	case filterKeyDescription:
		return anyEqual(a.Description, values)
	case filterKeyOwnerID:
		return anyEqual(a.OwnerID, values)
	case "virtualization-type":
		return anyEqual(a.VirtualizationType, values)
	default:
		if handled, ok := imageMatchesBlockDeviceMappingFilter(a, filterName, values); ok {
			return handled
		}

		if handled, ok := matchesTagFilter(a.ImageID, filterName, values, b); ok {
			return handled
		}
	}

	return true
}

// imageMatchesBlockDeviceMappingFilter handles the documented
// block-device-mapping.* filter family (device-name, snapshot-id,
// volume-type, volume-size, delete-on-termination, encrypted). handled is
// false when filterName isn't one of these six.
func imageMatchesBlockDeviceMappingFilter(a *AMIStub, filterName string, values []string) (bool, bool) {
	var field func(ImageBlockDeviceMapping) string

	switch filterName {
	case "block-device-mapping.device-name":
		field = func(m ImageBlockDeviceMapping) string { return m.DeviceName }
	case "block-device-mapping.snapshot-id":
		field = func(m ImageBlockDeviceMapping) string { return m.SnapshotID }
	case "block-device-mapping.volume-type":
		field = func(m ImageBlockDeviceMapping) string { return m.VolumeType }
	case "block-device-mapping.volume-size":
		field = func(m ImageBlockDeviceMapping) string { return strconv.Itoa(int(m.VolumeSize)) }
	case "block-device-mapping.delete-on-termination":
		field = func(m ImageBlockDeviceMapping) string { return strconv.FormatBool(m.DeleteOnTermination) }
	case "block-device-mapping.encrypted":
		field = func(m ImageBlockDeviceMapping) string { return strconv.FormatBool(m.Encrypted) }
	default:
		return false, false
	}

	return imageHasBlockDeviceMapping(a, values, field), true
}

// imageHasBlockDeviceMapping reports whether any of a's block device
// mappings has field(mapping) equal to one of values, matching AWS's
// documented block-device-mapping.* filter behaviour (matches if any mapping
// in the list matches).
func imageHasBlockDeviceMapping(a *AMIStub, values []string, field func(ImageBlockDeviceMapping) string) bool {
	for _, m := range a.BlockDeviceMappings {
		if anyEqual(field(m), values) {
			return true
		}
	}

	return false
}

// ---- SpotInstanceRequest filters ----

func applySpotRequestFilters(
	reqs []*SpotInstanceRequest,
	filters map[string][]string,
	b Backend,
) []*SpotInstanceRequest {
	if len(filters) == 0 {
		return reqs
	}

	out := reqs[:0:0]
spotLoop:
	for _, req := range reqs {
		for name, values := range filters {
			if !spotRequestMatchesFilter(req, name, values, b) {
				continue spotLoop
			}
		}

		out = append(out, req)
	}

	return out
}

func spotRequestMatchesFilter(req *SpotInstanceRequest, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "spot-instance-request-id":
		return anyEqual(req.ID, values)
	case filterKeyState:
		return anyEqual(req.State, values)
	case filterKeyInstanceID:
		return anyEqual(req.InstanceID, values)
	case "launch-specification.image-id":
		return anyEqual(req.LaunchSpec.ImageID, values)
	case "launch-specification.instance-type":
		return anyEqual(req.LaunchSpec.InstanceType, values)
	case "launch-specification.subnet-id":
		return anyEqual(req.LaunchSpec.SubnetID, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(req.ID, tagKey, values, b)
		}
	}

	return true
}

// itoa converts an int to decimal string.
func itoa(i int) string {
	return strconv.Itoa(i)
}

// parseIntValue parses s into *v. Ignores parse errors (best-effort).

// parseIntValue parses s into *v. Ignores parse errors (best-effort).
func parseIntValue(s string, v *int) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return
	}
	if v != nil {
		*v = n
	}
}

// parseInt32Value parses s directly into an int32 (via ParseInt with a 32-bit
// size, so there is no separate overflow-prone truncation step), returning 0
// for empty/unparseable/out-of-range input (best-effort, mirrors parseIntValue).

// parseInt32Value parses s directly into an int32 (via ParseInt with a 32-bit
// size, so there is no separate overflow-prone truncation step), returning 0
// for empty/unparseable/out-of-range input (best-effort, mirrors parseIntValue).
func parseInt32Value(s string) int32 {
	if s == "" {
		return 0
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}

// maxInstancesPerRunInstancesRequest bounds MinCount/MaxCount so a
// client-supplied value can never drive an unbounded slice allocation in
// RunInstances (CodeQL go/uncontrolled-allocation-size, alert #253). This is
// gopherstack's own allocation-safety cap, not a modeled AWS quota -- real
// EC2 has no flat per-request instance-count limit, only per-account/
// instance-type quotas (see gopherstack-x6r7).
const maxInstancesPerRunInstancesRequest = 1000

// parseRunInstancesCounts validates and returns MinCount and MaxCount from RunInstances params.
// MinCount defaults to 1 when absent. MaxCount defaults to MinCount when absent.
func parseRunInstancesCounts(vals url.Values) (int, int, error) {
	minCnt := 1
	if v := vals.Get("MinCount"); v != "" {
		if _, scanErr := fmt.Sscan(v, &minCnt); scanErr != nil || minCnt < 1 {
			return 0, 0, fmt.Errorf("%w: MinCount must be a positive integer", ErrInvalidParameter)
		}
	}

	if minCnt > maxInstancesPerRunInstancesRequest {
		return 0, 0, fmt.Errorf(
			"%w: MinCount must not exceed %d",
			ErrResourceCountExceeded, maxInstancesPerRunInstancesRequest,
		)
	}

	maxCnt := minCnt
	if v := vals.Get("MaxCount"); v != "" {
		if _, scanErr := fmt.Sscan(v, &maxCnt); scanErr != nil || maxCnt < 1 {
			return 0, 0, fmt.Errorf("%w: MaxCount must be a positive integer", ErrInvalidParameter)
		}
	}

	if maxCnt < minCnt {
		return 0, 0, fmt.Errorf("%w: MaxCount must be greater than or equal to MinCount", ErrInvalidParameter)
	}

	if maxCnt > maxInstancesPerRunInstancesRequest {
		return 0, 0, fmt.Errorf(
			"%w: MaxCount must not exceed %d",
			ErrResourceCountExceeded, maxInstancesPerRunInstancesRequest,
		)
	}

	return minCnt, maxCnt, nil
}

// validateSecurityGroupIDs parses RunInstances' SecurityGroupId.N (IDs, any
// VPC) and SecurityGroup.N (names) from vals and resolves both to security
// group IDs. Real RunInstancesInput.SecurityGroups is documented "[Default
// VPC] The names of the security groups" (ec2@v1.319.1 api_op_RunInstances.go)
// -- a name only resolves for the default VPC; supplying one for a subnet in
// a non-default VPC is rejected, matching AWS's InvalidParameterCombination
// ("The parameter groupName cannot be used with the parameter subnet").
func (h *Handler) validateSecurityGroupIDs(vals url.Values) ([]string, error) {
	sgIDs := parseMemberList(vals, "SecurityGroupId")
	if len(sgIDs) > 0 {
		existing := h.Backend.DescribeSecurityGroups(sgIDs)
		if len(existing) != len(sgIDs) {
			return nil, fmt.Errorf("%w: one or more SecurityGroupId values not found", ErrSecurityGroupNotFound)
		}
	}

	names := parseMemberList(vals, "SecurityGroup")
	if len(names) == 0 {
		return sgIDs, nil
	}

	resolvedIDs, err := h.resolveSecurityGroupNames(names, vals.Get("SubnetId"))
	if err != nil {
		return nil, err
	}

	return append(sgIDs, resolvedIDs...), nil
}

// resolveSecurityGroupNames resolves RunInstances SecurityGroup.N names to
// group IDs within the launch target's VPC (the subnet's VPC, or the
// account's default VPC when subnetID is empty).
func (h *Handler) resolveSecurityGroupNames(names []string, subnetID string) ([]string, error) {
	vpcID := vpcDefaultName
	if subnetID != "" {
		if subs := h.Backend.DescribeSubnets([]string{subnetID}); len(subs) == 1 {
			vpcID = subs[0].VPCID
		}
	}

	if vpcs := h.Backend.DescribeVpcs([]string{vpcID}); len(vpcs) == 1 && !vpcs[0].IsDefault {
		return nil, fmt.Errorf(
			"%w: The parameter groupName cannot be used with the parameter subnet",
			ErrInvalidParameterCombination,
		)
	}

	all := h.Backend.DescribeSecurityGroups(nil)
	resolvedIDs := make([]string, 0, len(names))

	for _, name := range names {
		id := ""

		for _, sg := range all {
			if sg.Name == name && sg.VPCID == vpcID {
				id = sg.ID

				break
			}
		}

		if id == "" {
			return nil, fmt.Errorf("%w: security group %q not found in VPC %s", ErrSecurityGroupNotFound, name, vpcID)
		}

		resolvedIDs = append(resolvedIDs, id)
	}

	return resolvedIDs, nil
}

// parseEC2Filters parses Filter.N.Name / Filter.N.Value.M from EC2 form values.
// Returns a map of filter name → list of accepted values (OR semantics per AWS).

// parseEC2Filters parses Filter.N.Name / Filter.N.Value.M from EC2 form values.
// Returns a map of filter name → list of accepted values (OR semantics per AWS).
func parseEC2Filters(vals url.Values) map[string][]string {
	return parseEC2FilterListKeyed(vals, "Filter")
}

// parseEC2FilterListKeyed parses "<prefix>.N.Name"/"<prefix>.N.Value.M" from
// EC2 form values. Almost every Describe*/Get* op flattens its filter list
// member under "Filter" regardless of the Go SDK field's "Filters" name, but
// a handful (e.g. DescribeImportImageTasks) flatten it under "Filters"
// instead (confirmed against the pinned SDK's awsEc2query_serializeOpDocument*
// FlatKey call) -- parseEC2Filters covers the common case; callers for the
// rare "Filters" ops call this directly.
func parseEC2FilterListKeyed(vals url.Values, prefix string) map[string][]string {
	filters := make(map[string][]string)

	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("%s.%d.Name", prefix, i))
		if name == "" {
			break
		}

		var values []string
		for j := 1; ; j++ {
			v := vals.Get(fmt.Sprintf("%s.%d.Value.%d", prefix, i, j))
			if v == "" {
				break
			}

			values = append(values, v)
		}

		if len(values) > 0 {
			filters[name] = values
		}
	}

	return filters
}

// applyInstanceFilters ANDs across filter names, ORs within each filter's values.
// Supports instance-state-name, image-id, vpc-id, subnet-id, instance-type, key-name,
// private-ip-address, ip-address, and tag:<key>. tagsByID is a pre-fetched
// resourceID→tags snapshot (see Backend.TagsForResources) so filtering N
// instances costs one backend lock instead of one TagsForResource call per
// instance with a tag: filter.
func applyInstanceFilters(
	instances []*Instance, filters map[string][]string, tagsByID map[string]map[string]string,
) []*Instance {
	if len(filters) == 0 {
		return instances
	}

	out := instances[:0:0]

instanceLoop:
	for _, inst := range instances {
		for name, values := range filters {
			if !instanceMatchesFilter(inst, name, values, tagsByID[inst.ID]) {
				continue instanceLoop
			}
		}

		out = append(out, inst)
	}

	return out
}

// instanceMatchesFilter returns true if the instance matches any value in the filter.
func instanceMatchesFilter(inst *Instance, filterName string, values []string, tags map[string]string) bool {
	switch filterName {
	case "instance-state-name":
		return anyEqual(inst.State.Name, values)
	case filterKeyImageID:
		return anyEqual(inst.ImageID, values)
	case filterKeyVPCID:
		return anyEqual(inst.VPCID, values)
	case filterKeySubnetID:
		return anyEqual(inst.SubnetID, values)
	case filterKeyInstanceType:
		return anyEqual(inst.InstanceType, values)
	case "key-name":
		return anyEqual(inst.KeyName, values)
	case "private-ip-address":
		return anyEqual(inst.PrivateIP, values)
	case "ip-address":
		return anyEqual(inst.PublicIPAddress, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			tagVal, exists := tags[tagKey]

			if !exists {
				return false
			}

			return slices.Contains(values, tagVal)
		}
	}

	// Unknown filters: pass through (lenient, per common mock behaviour).
	return true
}

// anyEqual returns true if target equals any element in vals.

// anyEqual returns true if target equals any element in vals.
func anyEqual(target string, vals []string) bool {
	return slices.Contains(vals, target)
}

// applySecurityGroupFilters filters security groups by named EC2 filter values.
// Supported filter names: vpc-id, group-name, group-id, tag:<key>.
func applySecurityGroupFilters(
	groups []*SecurityGroup,
	filters map[string][]string,
	b Backend,
) []*SecurityGroup {
	if len(filters) == 0 {
		return groups
	}

	out := groups[:0:0]

groupLoop:
	for _, sg := range groups {
		for name, values := range filters {
			if !sgMatchesFilter(sg, name, values, b) {
				continue groupLoop
			}
		}

		out = append(out, sg)
	}

	return out
}

// sgMatchesFilter returns true if the security group matches any value in the filter.
func sgMatchesFilter(sg *SecurityGroup, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyVPCID:
		return anyEqual(sg.VPCID, values)
	case "group-name":
		return anyEqual(sg.Name, values)
	case "group-id":
		return anyEqual(sg.ID, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(sg.ID, tagKey, values, b)
		}
	}

	// Unknown filters: pass through (lenient).
	return true
}

// gopherstack-j2v5: the apply*Filters functions below wire up Filters for
// Describe operations that previously declared the parameter but never read
// it, so a real client's filter was silently ignored and every item came
// back. Each implements only the filter names its own SDK doc comment
// (api_op_Describe*.go) lists AND that this backend's struct actually
// stores; a documented name naming untracked data is left unimplemented and
// noted in PARITY.md rather than fabricated.

// ---- DhcpOptions filters ----

// applyDhcpOptionsFilters supports dhcp-options-id, key, value, tag,
// tag-key (api_op_DescribeDhcpOptions.go). owner-id is documented but left:
// this backend does not store a per-resource owner distinct from the single
// account, matching how the rest of this file omits owner-id elsewhere
// (e.g. imageMatchesFilter).
func applyDhcpOptionsFilters(opts []*DhcpOptions, filters map[string][]string, b Backend) []*DhcpOptions {
	if len(filters) == 0 {
		return opts
	}

	out := opts[:0:0]
dhcpLoop:
	for _, o := range opts {
		for name, values := range filters {
			if !dhcpOptionsMatchesFilter(o, name, values, b) {
				continue dhcpLoop
			}
		}

		out = append(out, o)
	}

	return out
}

func dhcpOptionsMatchesFilter(o *DhcpOptions, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "dhcp-options-id":
		return anyEqual(o.DhcpOptionsID, values)
	case filterKeyDhcpConfigKey:
		for _, cfg := range o.Configurations {
			if anyEqual(cfg.Key, values) {
				return true
			}
		}

		return false
	case filterKeyDhcpConfigValue:
		for _, cfg := range o.Configurations {
			for _, v := range cfg.Values {
				if anyEqual(v, values) {
					return true
				}
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(o.DhcpOptionsID, tagKey, values, b)
		}
	}

	return true
}

// ---- EgressOnlyInternetGateway filters ----

// applyEOIGWFilters supports only tag/tag-key
// (api_op_DescribeEgressOnlyInternetGateways.go documents no other filter names).
func applyEOIGWFilters(
	igws []*EgressOnlyInternetGateway,
	filters map[string][]string,
	b Backend,
) []*EgressOnlyInternetGateway {
	if len(filters) == 0 {
		return igws
	}

	out := igws[:0:0]
eoigwLoop:
	for _, igw := range igws {
		for name, values := range filters {
			if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
				if !tagMatch(igw.ID, tagKey, values, b) {
					continue eoigwLoop
				}

				continue
			}
			// Unknown/unsupported filter names pass through (lenient).
		}

		out = append(out, igw)
	}

	return out
}

// ---- Static PrefixList filters ----

// applyPrefixListFilters supports prefix-list-id, prefix-list-name
// (api_op_DescribePrefixLists.go).
func applyPrefixListFilters(lists []PrefixList, filters map[string][]string) []PrefixList {
	if len(filters) == 0 {
		return lists
	}

	out := lists[:0:0]
plLoop:
	for _, pl := range lists {
		for name, values := range filters {
			switch name {
			case filterKeyPrefixListID:
				if !anyEqual(pl.PrefixListID, values) {
					continue plLoop
				}
			case "prefix-list-name":
				if !anyEqual(pl.PrefixListName, values) {
					continue plLoop
				}
			}
		}

		out = append(out, pl)
	}

	return out
}

// ---- ManagedPrefixList filters ----

// applyManagedPrefixListFilters supports owner-id, prefix-list-id,
// prefix-list-name (api_op_DescribeManagedPrefixLists.go).
func applyManagedPrefixListFilters(
	lists []*ManagedPrefixList,
	filters map[string][]string,
) []*ManagedPrefixList {
	if len(filters) == 0 {
		return lists
	}

	out := lists[:0:0]
mplLoop:
	for _, pl := range lists {
		for name, values := range filters {
			if !managedPrefixListMatchesFilter(pl, name, values) {
				continue mplLoop
			}
		}

		out = append(out, pl)
	}

	return out
}

func managedPrefixListMatchesFilter(pl *ManagedPrefixList, filterName string, values []string) bool {
	switch filterName {
	case filterKeyOwnerID:
		return anyEqual(pl.OwnerID, values)
	case filterKeyPrefixListID:
		return anyEqual(pl.PrefixListID, values)
	case "prefix-list-name":
		return anyEqual(pl.PrefixListName, values)
	}

	return true
}

// ---- Ipv4Pool (DescribePublicIpv4Pools) filters ----

// applyIpv4PoolFilters supports only tag/tag-key
// (api_op_DescribePublicIpv4Pools.go documents no other filter names).
func applyIpv4PoolFilters(pools []*Ipv4Pool, filters map[string][]string, b Backend) []*Ipv4Pool {
	if len(filters) == 0 {
		return pools
	}

	out := pools[:0:0]
poolLoop:
	for _, p := range pools {
		for name, values := range filters {
			if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
				if !tagMatch(p.PoolID, tagKey, values, b) {
					continue poolLoop
				}

				continue
			}
			// Unknown/unsupported filter names pass through (lenient).
		}

		out = append(out, p)
	}

	return out
}

// ---- BundleTask filters ----

// applyBundleTaskFilters supports bundle-id, error-code, error-message,
// instance-id, progress, s3-bucket, s3-prefix, state
// (api_op_DescribeBundleTasks.go). start-time/update-time are documented but
// left: matching a Filter value against a timestamp requires the SDK's
// exact wire format, which BundleTask's Go time.Time doesn't preserve
// losslessly for string equality, and no other filter in this file matches
// on a timestamp field either.
func applyBundleTaskFilters(tasks []*BundleTask, filters map[string][]string) []*BundleTask {
	if len(filters) == 0 {
		return tasks
	}

	out := tasks[:0:0]
bundleLoop:
	for _, t := range tasks {
		for name, values := range filters {
			if !bundleTaskMatchesFilter(t, name, values) {
				continue bundleLoop
			}
		}

		out = append(out, t)
	}

	return out
}

func bundleTaskMatchesFilter(t *BundleTask, filterName string, values []string) bool {
	switch filterName {
	case "bundle-id":
		return anyEqual(t.BundleID, values)
	case "error-code":
		return anyEqual(t.ErrorCode, values)
	case "error-message":
		return anyEqual(t.ErrorMessage, values)
	case filterKeyInstanceID:
		return anyEqual(t.InstanceID, values)
	case "progress":
		return anyEqual(t.Progress, values)
	case "s3-bucket":
		return anyEqual(t.S3Bucket, values)
	case "s3-prefix":
		return anyEqual(t.S3Prefix, values)
	case filterKeyState:
		return anyEqual(t.State, values)
	}

	return true
}

// ---- CarrierGateway filters ----

// applyCarrierGatewayFilters supports carrier-gateway-id, state, owner-id,
// tag, tag-key, vpc-id (api_op_DescribeCarrierGateways.go).
func applyCarrierGatewayFilters(
	gws []*CarrierGateway,
	filters map[string][]string,
	b Backend,
) []*CarrierGateway {
	if len(filters) == 0 {
		return gws
	}

	out := gws[:0:0]
cgwLoop:
	for _, gw := range gws {
		for name, values := range filters {
			if !carrierGatewayMatchesFilter(gw, name, values, b) {
				continue cgwLoop
			}
		}

		out = append(out, gw)
	}

	return out
}

func carrierGatewayMatchesFilter(gw *CarrierGateway, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "carrier-gateway-id":
		return anyEqual(gw.CarrierGatewayID, values)
	case filterKeyState:
		return anyEqual(gw.State, values)
	case filterKeyOwnerID:
		return anyEqual(gw.OwnerID, values)
	case filterKeyVPCID:
		return anyEqual(gw.VpcID, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(gw.CarrierGatewayID, tagKey, values, b)
		}
	}

	return true
}

// ---- FlowLog filters ----

// applyFlowLogFilters supports deliver-log-status, log-destination-type,
// flow-log-id, log-group-name, resource-id, traffic-type, tag, tag-key
// (api_op_DescribeFlowLogs.go). log-group-name is documented but left: this
// backend does not model CloudWatch Logs log-group destinations separately
// from LogDestination, so there is nothing distinct to match.
func applyFlowLogFilters(logs []*FlowLog, filters map[string][]string, b Backend) []*FlowLog {
	if len(filters) == 0 {
		return logs
	}

	out := logs[:0:0]
flowLogLoop:
	for _, fl := range logs {
		for name, values := range filters {
			if !flowLogMatchesFilter(fl, name, values, b) {
				continue flowLogLoop
			}
		}

		out = append(out, fl)
	}

	return out
}

func flowLogMatchesFilter(fl *FlowLog, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "deliver-log-status":
		return anyEqual(fl.FlowLogStatus, values)
	case "log-destination-type":
		return anyEqual(fl.LogDestinationType, values)
	case "flow-log-id":
		return anyEqual(fl.FlowLogID, values)
	case filterKeyResourceID:
		return anyEqual(fl.ResourceID, values)
	case "traffic-type":
		return anyEqual(fl.TrafficType, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(fl.FlowLogID, tagKey, values, b)
		}
	}

	return true
}

// ---- NetworkACL filters ----

// applyNetworkACLFilters supports network-acl-id, vpc-id, default,
// association.association-id, association.network-acl-id,
// association.subnet-id, entry.cidr, entry.protocol, entry.rule-action,
// entry.rule-number, entry.egress, entry.port-range.from,
// entry.port-range.to, tag, tag-key (api_op_DescribeNetworkAcls.go).
// entry.icmp.code/entry.icmp.type/entry.ipv6-cidr and owner-id are
// documented but left: NACLEntry has no ICMP or IPv6 fields, and NetworkACL
// has no per-resource owner (see applyDhcpOptionsFilters' owner-id note).
//
// association.association-id and association.subnet-id both key off
// AssociationIDs: AddSubnetAssociation (network_acls.go) appends the raw
// subnetID there, so that list already IS the set of associated subnet IDs
// this backend tracks; there is no separately-modeled association ID.
func applyNetworkACLFilters(acls []*NetworkACL, filters map[string][]string, b Backend) []*NetworkACL {
	if len(filters) == 0 {
		return acls
	}

	out := acls[:0:0]
naclLoop:
	for _, acl := range acls {
		for name, values := range filters {
			if !naclMatchesFilter(acl, name, values, b) {
				continue naclLoop
			}
		}

		out = append(out, acl)
	}

	return out
}

func naclMatchesFilter(acl *NetworkACL, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "network-acl-id":
		return anyEqual(acl.ID, values)
	case filterKeyVPCID:
		return anyEqual(acl.VPCID, values)
	case "default":
		want := anyEqual("true", values)

		return acl.IsDefault == want
	}

	if strings.HasPrefix(filterName, "association.") {
		return naclMatchesAssociationFilter(acl, filterName, values)
	}

	if strings.HasPrefix(filterName, "entry.") {
		return naclMatchesEntryFilter(acl, filterName, values)
	}

	if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
		return tagMatch(acl.ID, tagKey, values, b)
	}

	return true
}

func naclMatchesAssociationFilter(acl *NetworkACL, filterName string, values []string) bool {
	switch filterName {
	case "association.association-id", "association.subnet-id":
		for _, aid := range acl.AssociationIDs {
			if anyEqual(aid, values) {
				return true
			}
		}

		return false
	case "association.network-acl-id":
		return len(acl.AssociationIDs) > 0 && anyEqual(acl.ID, values)
	}

	return true
}

func naclMatchesEntryFilter(acl *NetworkACL, filterName string, values []string) bool {
	switch filterName {
	case "entry.cidr":
		return naclEntryAny(acl, values, func(e NACLEntry) string { return e.CIDRBlock })
	case "entry.protocol":
		return naclEntryAny(acl, values, func(e NACLEntry) string { return e.Protocol })
	case "entry.rule-action":
		return naclEntryAny(acl, values, func(e NACLEntry) string { return e.RuleAction })
	case "entry.rule-number":
		return naclEntryAny(acl, values, func(e NACLEntry) string { return itoa(e.RuleNumber) })
	case "entry.port-range.from":
		return naclEntryAny(acl, values, func(e NACLEntry) string { return itoa(e.FromPort) })
	case "entry.port-range.to":
		return naclEntryAny(acl, values, func(e NACLEntry) string { return itoa(e.ToPort) })
	case "entry.egress":
		want := anyEqual("true", values)
		for _, e := range acl.Entries {
			if e.Egress == want {
				return true
			}
		}

		return false
	}

	return true
}

// naclEntryAny returns true if field(e) matches any value for any entry.
func naclEntryAny(acl *NetworkACL, values []string, field func(NACLEntry) string) bool {
	for _, e := range acl.Entries {
		if anyEqual(field(e), values) {
			return true
		}
	}

	return false
}

// ---- DescribeInstanceStatus filters ----

// applyInstanceStatusFilters supports availability-zone, instance-state-code,
// instance-state-name, instance-status.reachability, instance-status.status,
// system-status.reachability, system-status.status
// (api_op_DescribeInstanceStatus.go). availability-zone-id, event.*,
// operator.*, attached-ebs-status.status, and application-status.status are
// documented but left: this backend models neither scheduled events,
// managed-instance operators, nor per-resource-type health independent of
// the single computed instance/system status below.
func applyInstanceStatusFilters(instances []*Instance, filters map[string][]string) []*Instance {
	if len(filters) == 0 {
		return instances
	}

	out := instances[:0:0]
statusLoop:
	for _, inst := range instances {
		health := instanceHealthForState(inst.State.Name)
		for name, values := range filters {
			if !instanceStatusMatchesFilter(inst, health, name, values) {
				continue statusLoop
			}
		}

		out = append(out, inst)
	}

	return out
}

func instanceStatusMatchesFilter(
	inst *Instance,
	health instanceStatusDetails,
	filterName string,
	values []string,
) bool {
	switch filterName {
	case filterKeyAvailabilityZone:
		return anyEqual(inst.Placement.AvailabilityZone, values)
	case "instance-state-code":
		return anyEqual(itoa(inst.State.Code), values)
	case "instance-state-name":
		return anyEqual(inst.State.Name, values)
	case "instance-status.status", "system-status.status":
		return anyEqual(health.Status, values)
	case "instance-status.reachability", "system-status.reachability":
		for _, d := range health.Details {
			if d.Name == "reachability" && anyEqual(d.Status, values) {
				return true
			}
		}

		return false
	}

	return true
}

// applyActiveFleetInstanceFilters filters DescribeFleetInstances' results.
// Supports "instance-type", the only filter DescribeFleetInstancesInput
// documents (ec2@v1.319.1 api_op_DescribeFleetInstances.go).
func applyActiveFleetInstanceFilters(
	instances []ActiveFleetInstance, filters map[string][]string,
) []ActiveFleetInstance {
	if len(filters) == 0 {
		return instances
	}

	out := instances[:0:0]

instanceLoop:
	for _, inst := range instances {
		for name, values := range filters {
			if name == filterKeyInstanceType && !anyEqual(inst.InstanceType, values) {
				continue instanceLoop
			}
		}

		out = append(out, inst)
	}

	return out
}

// applyCustomerGatewayFilters supports bgp-asn, customer-gateway-id,
// ip-address, state, type, and tag: (api_op_DescribeCustomerGateways.go).
// amazon-side-asn/tag-key are documented but not implemented here.
func applyCustomerGatewayFilters(
	gws []*CustomerGateway, filters map[string][]string, b Backend,
) []*CustomerGateway {
	if len(filters) == 0 {
		return gws
	}

	out := gws[:0:0]

cgwLoop:
	for _, gw := range gws {
		for name, values := range filters {
			if !customerGatewayMatchesFilter(gw, name, values, b) {
				continue cgwLoop
			}
		}

		out = append(out, gw)
	}

	return out
}

func customerGatewayMatchesFilter(gw *CustomerGateway, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "bgp-asn":
		return anyEqual(gw.BgpAsn, values)
	case "customer-gateway-id":
		return anyEqual(gw.CustomerGatewayID, values)
	case "ip-address":
		return anyEqual(gw.IPAddress, values)
	case filterKeyState:
		return anyEqual(gw.State, values)
	case filterKeyType:
		return anyEqual(gw.Type, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(gw.CustomerGatewayID, tagKey, values, b)
		}
	}

	return true
}

// applyVpnGatewayFilters supports attachment.state, attachment.vpc-id,
// state, type, vpn-gateway-id, and tag: (api_op_DescribeVpnGateways.go).
// amazon-side-asn/availability-zone/tag-key are documented but not tracked
// by this backend's VpnGateway struct, so are left unimplemented.
func applyVpnGatewayFilters(
	gws []*VpnGateway, filters map[string][]string, b Backend,
) []*VpnGateway {
	if len(filters) == 0 {
		return gws
	}

	out := gws[:0:0]

vgwLoop:
	for _, gw := range gws {
		for name, values := range filters {
			if !vpnGatewayMatchesFilter(gw, name, values, b) {
				continue vgwLoop
			}
		}

		out = append(out, gw)
	}

	return out
}

func vpnGatewayMatchesFilter(gw *VpnGateway, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "attachment.state":
		return anyEqual(gw.AttachmentState, values)
	case "attachment.vpc-id":
		return anyEqual(gw.AttachedVPCID, values)
	case filterKeyState:
		return anyEqual(gw.State, values)
	case filterKeyType:
		return anyEqual(gw.Type, values)
	case "vpn-gateway-id":
		return anyEqual(gw.VpnGatewayID, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(gw.VpnGatewayID, tagKey, values, b)
		}
	}

	return true
}

// anyContains returns true when any element of list equals any of values.
func anyContains(list []string, values []string) bool {
	for _, item := range list {
		if anyEqual(item, values) {
			return true
		}
	}

	return false
}

// applyClassicLinkInstanceFilters supports group-id, vpc-id, and tag:
// (api_op_DescribeClassicLinkInstances.go). tag-key is documented but not
// implemented, matching this file's existing convention.
func applyClassicLinkInstanceFilters(
	links []*ClassicLinkInstance, filters map[string][]string, b Backend,
) []*ClassicLinkInstance {
	if len(filters) == 0 {
		return links
	}

	out := links[:0:0]

clLoop:
	for _, link := range links {
		for name, values := range filters {
			if !classicLinkInstanceMatchesFilter(link, name, values, b) {
				continue clLoop
			}
		}

		out = append(out, link)
	}

	return out
}

func classicLinkInstanceMatchesFilter(link *ClassicLinkInstance, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "group-id":
		return anyContains(link.Groups, values)
	case filterKeyVPCID:
		return anyEqual(link.VpcID, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(link.InstanceID, tagKey, values, b)
		}
	}

	return true
}

// applySecondaryInterfaceFilters supports owner-id, status,
// secondary-interface-id, secondary-interface-arn, secondary-interface-type,
// secondary-network-id, secondary-network-type, secondary-subnet-id,
// attachment.instance-id, private-ipv4-addresses.private-ip-address, and tag:
// (api_op_DescribeSecondaryInterfaces.go). attachment.attachment-id,
// attachment.instance-owner-id, attachment.status, and tag-key are
// documented but not tracked by this backend's SecondaryInterface struct.
func applySecondaryInterfaceFilters(
	sis []*SecondaryInterface, filters map[string][]string, b Backend,
) []*SecondaryInterface {
	if len(filters) == 0 {
		return sis
	}

	out := sis[:0:0]

siLoop:
	for _, si := range sis {
		for name, values := range filters {
			if !secondaryInterfaceMatchesFilter(si, name, values, b) {
				continue siLoop
			}
		}

		out = append(out, si)
	}

	return out
}

func secondaryInterfaceMatchesFilter(si *SecondaryInterface, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyOwnerID:
		return anyEqual(si.OwnerID, values)
	case filterKeyStatus:
		return anyEqual(si.Status, values)
	case "secondary-interface-id":
		return anyEqual(si.SecondaryInterfaceID, values)
	case "secondary-interface-arn":
		return anyEqual(si.SecondaryInterfaceArn, values)
	case "secondary-interface-type":
		return anyEqual(si.SecondaryInterfaceType, values)
	case filterKeySecondaryNetID:
		return anyEqual(si.SecondaryNetworkID, values)
	case "secondary-network-type":
		return anyEqual(si.SecondaryNetworkType, values)
	case "secondary-subnet-id":
		return anyEqual(si.SecondarySubnetID, values)
	case filterKeyAttachInstanceID:
		return anyEqual(si.InstanceID, values)
	case "private-ipv4-addresses.private-ip-address":
		return anyContains(si.PrivateIpv4Addresses, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(si.SecondaryInterfaceID, tagKey, values, b)
		}
	}

	return true
}

// applySecondaryNetworkFilters supports owner-id, secondary-network-id,
// secondary-network-arn, state, type, ipv4-cidr-block-association.*, and tag:
// (api_op_DescribeSecondaryNetworks.go). tag-key is documented but not
// implemented, matching this file's existing convention.
func applySecondaryNetworkFilters(
	nets []*SecondaryNetwork, filters map[string][]string, b Backend,
) []*SecondaryNetwork {
	if len(filters) == 0 {
		return nets
	}

	out := nets[:0:0]

netLoop:
	for _, n := range nets {
		for name, values := range filters {
			if !secondaryNetworkMatchesFilter(n, name, values, b) {
				continue netLoop
			}
		}

		out = append(out, n)
	}

	return out
}

// secondaryNetworkCidrAssocField returns the association field matching
// filterName's "ipv4-cidr-block-association.*" suffix, and whether
// filterName was recognized as one of that family.
func secondaryNetworkCidrAssocField(assoc SecondaryNetworkCidrAssoc, filterName string) (string, bool) {
	switch filterName {
	case "ipv4-cidr-block-association.association-id":
		return assoc.AssociationID, true
	case "ipv4-cidr-block-association.cidr-block":
		return assoc.CidrBlock, true
	case "ipv4-cidr-block-association.state":
		return assoc.State, true
	default:
		return "", false
	}
}

func secondaryNetworkMatchesFilter(n *SecondaryNetwork, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyOwnerID:
		return anyEqual(n.OwnerID, values)
	case filterKeySecondaryNetID:
		return anyEqual(n.SecondaryNetworkID, values)
	case "secondary-network-arn":
		return anyEqual(n.SecondaryNetworkArn, values)
	case filterKeyState:
		return anyEqual(n.State, values)
	case filterKeyType:
		return anyEqual(n.Type, values)
	default:
		if _, recognized := secondaryNetworkCidrAssocField(SecondaryNetworkCidrAssoc{}, filterName); recognized {
			for _, assoc := range n.Ipv4CidrBlockAssociations {
				field, _ := secondaryNetworkCidrAssocField(assoc, filterName)
				if anyEqual(field, values) {
					return true
				}
			}

			return false
		}

		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(n.SecondaryNetworkID, tagKey, values, b)
		}
	}

	return true
}

// applySecondarySubnetFilters supports owner-id, secondary-network-id,
// secondary-network-type, secondary-subnet-id, secondary-subnet-arn, state,
// ipv4-cidr-block-association.*, and tag: (api_op_DescribeSecondarySubnets.go).
// tag-key is documented but not implemented, matching this file's existing
// convention.
func applySecondarySubnetFilters(
	subs []*SecondarySubnet, filters map[string][]string, b Backend,
) []*SecondarySubnet {
	if len(filters) == 0 {
		return subs
	}

	out := subs[:0:0]

subLoop:
	for _, s := range subs {
		for name, values := range filters {
			if !secondarySubnetMatchesFilter(s, name, values, b) {
				continue subLoop
			}
		}

		out = append(out, s)
	}

	return out
}

// secondarySubnetCidrAssocField returns the association field matching
// filterName's "ipv4-cidr-block-association.*" suffix, and whether
// filterName was recognized as one of that family.
func secondarySubnetCidrAssocField(assoc SecondarySubnetCidrAssoc, filterName string) (string, bool) {
	switch filterName {
	case "ipv4-cidr-block-association.association-id":
		return assoc.AssociationID, true
	case "ipv4-cidr-block-association.cidr-block":
		return assoc.CidrBlock, true
	case "ipv4-cidr-block-association.state":
		return assoc.State, true
	default:
		return "", false
	}
}

func secondarySubnetMatchesFilter(s *SecondarySubnet, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyOwnerID:
		return anyEqual(s.OwnerID, values)
	case filterKeySecondaryNetID:
		return anyEqual(s.SecondaryNetworkID, values)
	case "secondary-network-type":
		return anyEqual(s.SecondaryNetworkType, values)
	case "secondary-subnet-id":
		return anyEqual(s.SecondarySubnetID, values)
	case "secondary-subnet-arn":
		return anyEqual(s.SecondarySubnetArn, values)
	case filterKeyState:
		return anyEqual(s.State, values)
	default:
		if _, recognized := secondarySubnetCidrAssocField(SecondarySubnetCidrAssoc{}, filterName); recognized {
			for _, assoc := range s.Ipv4CidrBlockAssociations {
				field, _ := secondarySubnetCidrAssocField(assoc, filterName)
				if anyEqual(field, values) {
					return true
				}
			}

			return false
		}

		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(s.SecondarySubnetID, tagKey, values, b)
		}
	}

	return true
}

// applyServiceLinkVirtualInterfaceFilters supports owner-id, outpost-lag-id,
// outpost-arn, state, vlan, service-link-virtual-interface-id, and tag:
// (api_op_DescribeServiceLinkVirtualInterfaces.go). local-gateway-virtual-
// interface-id and tag-key are documented but not tracked by this backend's
// ServiceLinkVirtualInterface struct.
func applyServiceLinkVirtualInterfaceFilters(
	vifs []*ServiceLinkVirtualInterface, filters map[string][]string, b Backend,
) []*ServiceLinkVirtualInterface {
	if len(filters) == 0 {
		return vifs
	}

	out := vifs[:0:0]

vifLoop:
	for _, v := range vifs {
		for name, values := range filters {
			if !serviceLinkVirtualInterfaceMatchesFilter(v, name, values, b) {
				continue vifLoop
			}
		}

		out = append(out, v)
	}

	return out
}

func serviceLinkVirtualInterfaceMatchesFilter(
	v *ServiceLinkVirtualInterface, filterName string, values []string, b Backend,
) bool {
	switch filterName {
	case filterKeyOwnerID:
		return anyEqual(v.OwnerID, values)
	case "outpost-lag-id":
		return anyEqual(v.OutpostLagID, values)
	case filterKeyOutpostArn:
		return anyEqual(v.OutpostArn, values)
	case filterKeyState:
		return anyEqual(v.ConfigurationState, values)
	case "vlan":
		return anyEqual(strconv.Itoa(int(v.Vlan)), values)
	case "service-link-virtual-interface-id":
		return anyEqual(v.ServiceLinkVirtualInterfaceID, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(v.ServiceLinkVirtualInterfaceID, tagKey, values, b)
		}
	}

	return true
}

// applySQLHaHistoryFilters supports haStatus, sqlServerLicenseUsage, and
// tag: (api_op_DescribeInstanceSqlHaHistoryStates.go). tag-key is
// documented but not implemented, matching this file's existing convention.
func applySQLHaHistoryFilters(
	regs []*RegisteredSQLHaInstance, filters map[string][]string, b Backend,
) []*RegisteredSQLHaInstance {
	if len(filters) == 0 {
		return regs
	}

	out := regs[:0:0]

haLoop:
	for _, r := range regs {
		for name, values := range filters {
			if !sqlHaHistoryMatchesFilter(r, name, values, b) {
				continue haLoop
			}
		}

		out = append(out, r)
	}

	return out
}

func sqlHaHistoryMatchesFilter(r *RegisteredSQLHaInstance, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "haStatus":
		return anyEqual(r.HaStatus, values)
	case "sqlServerLicenseUsage":
		return anyEqual(r.SQLServerLicenseUsage, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(r.InstanceID, tagKey, values, b)
		}
	}

	return true
}

// applyImageUsageReportEntryFilters supports account-id, resource-type, and
// creation-time (api_op_DescribeImageUsageReportEntries.go). creation-time
// supports the documented "*" wildcard suffix (e.g. "2025-11-29*") to match
// an entire day/prefix, plus an exact RFC3339 match.
func applyImageUsageReportEntryFilters(
	entries []*UsageReportEntry, filters map[string][]string,
) []*UsageReportEntry {
	if len(filters) == 0 {
		return entries
	}

	out := entries[:0:0]

entryLoop:
	for _, e := range entries {
		for name, values := range filters {
			if !usageReportEntryMatchesFilter(e, name, values) {
				continue entryLoop
			}
		}

		out = append(out, e)
	}

	return out
}

// matchesWildcardTimeFilter matches a wire-formatted timestamp against filter
// values that may use the documented "*" day/prefix wildcard suffix (e.g.
// "2025-11-29*"), falling back to an exact match otherwise.
func matchesWildcardTimeFilter(wireTime string, values []string) bool {
	for _, v := range values {
		if prefix, ok := strings.CutSuffix(v, "*"); ok {
			if strings.HasPrefix(wireTime, prefix) {
				return true
			}

			continue
		}

		if wireTime == v {
			return true
		}
	}

	return false
}

func usageReportEntryMatchesFilter(e *UsageReportEntry, filterName string, values []string) bool {
	switch filterName {
	case "account-id":
		return anyEqual(e.AccountID, values)
	case filterKeyResourceType:
		return anyEqual(e.ResourceType, values)
	case "creation-time":
		// Must match toImageUsageReportEntryItem's wire format
		// (handler_image_ops.go) exactly, or an exact-match filter built
		// from the timestamp this API just returned never matches its own
		// record.
		return matchesWildcardTimeFilter(e.ReportCreationTime.UTC().Format(time.RFC3339), values)
	}

	return true
}

// applyCapacityReservationFilters supports the DescribeCapacityReservations
// filters this backend has data for: instance-type, owner-id,
// availability-zone, instance-platform, instance-match-criteria, tenancy,
// state (api_op_DescribeCapacityReservations.go doc comment). outpost-arn,
// placement-group-arn, start-date, end-date, and end-date-type are
// documented but unmodeled (no such fields on CapacityReservation).
func applyCapacityReservationFilters(
	reservations []*CapacityReservation, filters map[string][]string,
) []*CapacityReservation {
	if len(filters) == 0 {
		return reservations
	}

	out := reservations[:0:0]

crLoop:
	for _, cr := range reservations {
		for name, values := range filters {
			if !capacityReservationMatchesFilter(cr, name, values) {
				continue crLoop
			}
		}

		out = append(out, cr)
	}

	return out
}

func capacityReservationMatchesFilter(cr *CapacityReservation, filterName string, values []string) bool {
	switch filterName {
	case filterKeyInstanceType:
		return anyEqual(cr.InstanceType, values)
	case filterKeyOwnerID:
		return anyEqual(cr.OwnedBy, values)
	case filterKeyAvailabilityZone:
		return anyEqual(cr.AvailabilityZone, values)
	case "instance-platform":
		return anyEqual(cr.InstancePlatform, values)
	case "instance-match-criteria":
		return anyEqual(cr.InstanceMatchCriteria, values)
	case "tenancy":
		return anyEqual(cr.Tenancy, values)
	case filterKeyState:
		return anyEqual(cr.State, values)
	}

	return true
}

// applyTransitGatewayFilters supports the DescribeTransitGateways filters
// this backend has data for: owner-id, state, transit-gateway-id, tag-key,
// tag:<key>, and the options.* filters backed by TransitGatewayOptions
// (api_op_DescribeTransitGateways.go doc comment). options.propagation-
// default-route-table-id and options.association-default-route-table-id
// are documented but unmodeled on TransitGateway.
func applyTransitGatewayFilters(tgws []*TransitGateway, filters map[string][]string, b Backend) []*TransitGateway {
	if len(filters) == 0 {
		return tgws
	}

	out := tgws[:0:0]

tgwLoop:
	for _, tgw := range tgws {
		for name, values := range filters {
			if !transitGatewayMatchesFilter(tgw, name, values, b) {
				continue tgwLoop
			}
		}

		out = append(out, tgw)
	}

	return out
}

func transitGatewayMatchesFilter(tgw *TransitGateway, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyOwnerID:
		return anyEqual(tgw.OwnerID, values)
	case filterKeyState:
		return anyEqual(tgw.State, values)
	case filterKeyTransitGatewayID:
		return anyEqual(tgw.ID, values)
	case "options.amazon-side-asn":
		return anyEqual(strconv.FormatInt(tgw.Options.AmazonSideAsn, 10), values)
	case "options.auto-accept-shared-attachments":
		return anyEqual(tgw.Options.AutoAcceptSharedAttachments, values)
	case "options.default-route-table-association":
		return anyEqual(tgw.Options.DefaultRouteTableAssociation, values)
	case "options.default-route-table-propagation":
		return anyEqual(tgw.Options.DefaultRouteTablePropagation, values)
	case "options.dns-support":
		return anyEqual(tgw.Options.DNSSupport, values)
	case "options.vpn-ecmp-support":
		return anyEqual(tgw.Options.VpnEcmpSupport, values)
	case filterKeyTagKey:
		tags := b.TagsForResource(tgw.ID)
		for _, v := range values {
			if _, ok := tags[v]; ok {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(tgw.ID, tagKey, values, b)
		}
	}

	return true
}

// applyTGWVpcAttachmentFilters supports the DescribeTransitGatewayVpcAttachments
// filters (api_op_DescribeTransitGatewayVpcAttachments.go doc comment): state,
// transit-gateway-attachment-id, transit-gateway-id, vpc-id, tag:<key>, tag-key.
func applyTGWVpcAttachmentFilters(
	atts []*TransitGatewayVpcAttachment, filters map[string][]string, b Backend,
) []*TransitGatewayVpcAttachment {
	if len(filters) == 0 {
		return atts
	}

	out := atts[:0:0]

attLoop:
	for _, att := range atts {
		for name, values := range filters {
			if !tgwVpcAttachmentMatchesFilter(att, name, values, b) {
				continue attLoop
			}
		}

		out = append(out, att)
	}

	return out
}

func tgwVpcAttachmentMatchesFilter(
	att *TransitGatewayVpcAttachment, filterName string, values []string, b Backend,
) bool {
	switch filterName {
	case filterKeyState:
		return anyEqual(att.State, values)
	case "transit-gateway-attachment-id":
		return anyEqual(att.TransitGatewayAttachmentID, values)
	case filterKeyTransitGatewayID:
		return anyEqual(att.TransitGatewayID, values)
	case filterKeyVPCID:
		return anyEqual(att.VpcID, values)
	case filterKeyTagKey:
		for k := range b.TagsForResource(att.TransitGatewayAttachmentID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(att.TransitGatewayAttachmentID, tagKey, values, b)
		}
	}

	return true
}

// applyTGWAttachmentFilters supports the DescribeTransitGatewayAttachments
// filters this backend has data for: resource-id, resource-type, state,
// transit-gateway-attachment-id, transit-gateway-id, tag:<key>, tag-key
// (api_op_DescribeTransitGatewayAttachments.go doc comment). association.*,
// resource-owner-id, and transit-gateway-owner-id are documented but
// unmodeled.
func applyTGWAttachmentFilters(
	atts []*TransitGatewayAttachmentSummary, filters map[string][]string, b Backend,
) []*TransitGatewayAttachmentSummary {
	if len(filters) == 0 {
		return atts
	}

	out := atts[:0:0]

attLoop:
	for _, att := range atts {
		for name, values := range filters {
			if !tgwAttachmentMatchesFilter(att, name, values, b) {
				continue attLoop
			}
		}

		out = append(out, att)
	}

	return out
}

func tgwAttachmentMatchesFilter(
	att *TransitGatewayAttachmentSummary, filterName string, values []string, b Backend,
) bool {
	switch filterName {
	case filterKeyResourceID:
		return anyEqual(att.ResourceID, values)
	case filterKeyResourceType:
		return anyEqual(att.ResourceType, values)
	case filterKeyState:
		return anyEqual(att.State, values)
	case "transit-gateway-attachment-id":
		return anyEqual(att.TransitGatewayAttachmentID, values)
	case filterKeyTransitGatewayID:
		return anyEqual(att.TransitGatewayID, values)
	case filterKeyTagKey:
		for k := range b.TagsForResource(att.TransitGatewayAttachmentID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(att.TransitGatewayAttachmentID, tagKey, values, b)
		}
	}

	return true
}

// applyClientVpnEndpointFilters supports the DescribeClientVpnEndpoints
// filters this backend has data for: endpoint-id, transport-protocol,
// tag:<key>, tag-key (api_op_DescribeClientVpnEndpoints.go doc comment).
func applyClientVpnEndpointFilters(
	eps []*ClientVpnEndpoint, filters map[string][]string, b Backend,
) []*ClientVpnEndpoint {
	if len(filters) == 0 {
		return eps
	}

	out := eps[:0:0]

epLoop:
	for _, ep := range eps {
		for name, values := range filters {
			if !clientVpnEndpointMatchesFilter(ep, name, values, b) {
				continue epLoop
			}
		}

		out = append(out, ep)
	}

	return out
}

func clientVpnEndpointMatchesFilter(ep *ClientVpnEndpoint, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "endpoint-id":
		return anyEqual(ep.ClientVpnEndpointID, values)
	case "transport-protocol":
		return anyEqual(ep.TransportProtocol, values)
	case filterKeyTagKey:
		for k := range b.TagsForResource(ep.ClientVpnEndpointID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(ep.ClientVpnEndpointID, tagKey, values, b)
		}
	}

	return true
}

// applyVpnConnectionFilters supports the DescribeVpnConnections filters this
// backend has data for: customer-gateway-id, state, option.static-routes-only,
// type, vpn-connection-id, vpn-gateway-id, tag:<key>, tag-key
// (api_op_DescribeVpnConnections.go doc comment).
// customer-gateway-configuration, route.destination-cidr-block, and bgp-asn
// are documented but unmodeled or unsuitable for equality filtering.
// transit-gateway-id is documented but unmodeled: CreateVpnConnection only
// ever attaches to a VpnGatewayId, never a TransitGatewayId, so
// VpnConnection.TransitGatewayID is never populated (PARITY.md).
func applyVpnConnectionFilters(
	conns []*VpnConnection, filters map[string][]string, b Backend,
) []*VpnConnection {
	if len(filters) == 0 {
		return conns
	}

	out := conns[:0:0]

connLoop:
	for _, c := range conns {
		for name, values := range filters {
			if !vpnConnectionMatchesFilter(c, name, values, b) {
				continue connLoop
			}
		}

		out = append(out, c)
	}

	return out
}

func vpnConnectionMatchesFilter(c *VpnConnection, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "vpn-connection-id":
		return anyEqual(c.VpnConnectionID, values)
	case filterKeyState:
		return anyEqual(c.State, values)
	case filterKeyType:
		return anyEqual(c.Type, values)
	case "customer-gateway-id":
		return anyEqual(c.CustomerGatewayID, values)
	case "vpn-gateway-id":
		return anyEqual(c.VpnGatewayID, values)
	case "option.static-routes-only":
		want := anyEqual("true", values)

		return c.Options.StaticRoutesOnly == want
	case filterKeyTagKey:
		for k := range b.TagsForResource(c.VpnConnectionID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(c.VpnConnectionID, tagKey, values, b)
		}
	}

	return true
}

// applyTGWRouteTableFilters supports the DescribeTransitGatewayRouteTables
// filters (api_op_DescribeTransitGatewayRouteTables.go doc comment):
// default-association-route-table, default-propagation-route-table, state,
// transit-gateway-id, transit-gateway-route-table-id.
func applyTGWRouteTableFilters(
	rts []*TransitGatewayRouteTable, filters map[string][]string,
) []*TransitGatewayRouteTable {
	if len(filters) == 0 {
		return rts
	}

	out := rts[:0:0]

rtLoop:
	for _, rt := range rts {
		for name, values := range filters {
			if !tgwRouteTableMatchesFilter(rt, name, values) {
				continue rtLoop
			}
		}

		out = append(out, rt)
	}

	return out
}

func tgwRouteTableMatchesFilter(rt *TransitGatewayRouteTable, filterName string, values []string) bool {
	switch filterName {
	case "default-association-route-table":
		want := anyEqual("true", values)

		return rt.DefaultAssociation == want
	case "default-propagation-route-table":
		want := anyEqual("true", values)

		return rt.DefaultPropagation == want
	case filterKeyState:
		return anyEqual(rt.State, values)
	case filterKeyTransitGatewayID:
		return anyEqual(rt.TransitGatewayID, values)
	case "transit-gateway-route-table-id":
		return anyEqual(rt.RouteTableID, values)
	}

	return true
}

// applyLaunchTemplateVersionFilters supports the DescribeLaunchTemplateVersions
// filters this backend has data for: image-id, instance-type, and
// is-default-version (api_op_DescribeLaunchTemplateVersions.go doc comment).
// The other documented filters (create-time, ebs-optimized, http-endpoint,
// etc.) have no backing field on launchTemplateVersionItem.
func applyLaunchTemplateVersionFilters(
	items []launchTemplateVersionItem, filters map[string][]string,
) []launchTemplateVersionItem {
	if len(filters) == 0 {
		return items
	}

	out := items[:0:0]

itemLoop:
	for _, item := range items {
		for name, values := range filters {
			if !launchTemplateVersionMatchesFilter(item, name, values) {
				continue itemLoop
			}
		}

		out = append(out, item)
	}

	return out
}

func launchTemplateVersionMatchesFilter(item launchTemplateVersionItem, filterName string, values []string) bool {
	switch filterName {
	case filterKeyImageID:
		return anyEqual(item.LaunchTemplateData.ImageID, values)
	case filterKeyInstanceType:
		return anyEqual(item.LaunchTemplateData.InstanceType, values)
	case "is-default-version":
		want := anyEqual("true", values)

		return item.DefaultVersion == want
	}

	return true
}

// applyReservedInstancesOfferingFilters supports the
// DescribeReservedInstancesOfferings filters this backend has data for:
// availability-zone, duration, fixed-price, instance-type,
// product-description, reserved-instances-offering-id, usage-price
// (api_op_DescribeReservedInstancesOfferings.go doc comment). marketplace,
// availability-zone-id, and scope are documented but unmodeled.
func applyReservedInstancesOfferingFilters(
	offerings []*ReservedInstancesOffering, filters map[string][]string,
) []*ReservedInstancesOffering {
	if len(filters) == 0 {
		return offerings
	}

	out := offerings[:0:0]

offerLoop:
	for _, o := range offerings {
		for name, values := range filters {
			if !reservedInstancesOfferingMatchesFilter(o, name, values) {
				continue offerLoop
			}
		}

		out = append(out, o)
	}

	return out
}

func reservedInstancesOfferingMatchesFilter(o *ReservedInstancesOffering, filterName string, values []string) bool {
	switch filterName {
	case filterKeyAvailabilityZone:
		return anyEqual(o.AvailabilityZone, values)
	case "duration":
		return anyEqual(strconv.FormatInt(o.Duration, 10), values)
	case "fixed-price":
		return anyEqual(strconv.FormatFloat(o.FixedPrice, 'f', -1, 64), values)
	case filterKeyInstanceType:
		return anyEqual(o.InstanceType, values)
	case filterKeyProductDesc:
		return anyEqual(o.ProductDescription, values)
	case "reserved-instances-offering-id":
		return anyEqual(o.ReservedInstancesOfferingID, values)
	case "usage-price":
		return anyEqual(strconv.FormatFloat(o.UsagePrice, 'f', -1, 64), values)
	}

	return true
}

// ---- VPC Endpoint filters ----

// applyVpcEndpointFilters implements DescribeVpcEndpoints' documented
// filters (vpc-id, vpc-endpoint-state, vpc-endpoint-type, service-name,
// tag:<key>, tag-key). Previously handleDescribeVpcEndpoints ignored
// Filters entirely, so e.g. a tag:Name filter returned every endpoint in
// the account instead of just the matching one.
func applyVpcEndpointFilters(endpoints []*VpcEndpoint, filters map[string][]string, b Backend) []*VpcEndpoint {
	if len(filters) == 0 {
		return endpoints
	}

	out := endpoints[:0:0]

epLoop:
	for _, ep := range endpoints {
		for name, values := range filters {
			if !vpcEndpointMatchesFilter(ep, name, values, b) {
				continue epLoop
			}
		}

		out = append(out, ep)
	}

	return out
}

func vpcEndpointMatchesFilter(ep *VpcEndpoint, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyVPCID:
		return anyEqual(ep.VPCID, values)
	case "vpc-endpoint-state":
		return anyEqual(ep.State, values)
	case "vpc-endpoint-type":
		return anyEqual(ep.VpcEndpointType, values)
	case "service-name":
		return anyEqual(ep.ServiceName, values)
	case filterKeyTagKey:
		for k := range b.TagsForResource(ep.ID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(ep.ID, tagKey, values, b)
		}
	}

	return true
}

// ---- IPAM Scope filters ----

// applyIpamScopeFilters implements DescribeIpamScopes' documented filters
// (ipam-arn, ipam-scope-type, is-default, owner-id, tag:<key>). Previously
// handleDescribeIpamScopes ignored Filters entirely, so an is-default=false
// filter still returned every scope (including the 2 account defaults).
func applyIpamScopeFilters(scopes []*IpamScope, filters map[string][]string, b Backend) []*IpamScope {
	if len(filters) == 0 {
		return scopes
	}

	out := scopes[:0:0]

scopeLoop:
	for _, s := range scopes {
		for name, values := range filters {
			if !ipamScopeMatchesFilter(s, name, values, b) {
				continue scopeLoop
			}
		}

		out = append(out, s)
	}

	return out
}

func ipamScopeMatchesFilter(s *IpamScope, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "ipam-arn":
		return anyEqual(s.IpamARN, values)
	case "ipam-id":
		return anyEqual(s.IpamID, values)
	case "ipam-scope-type":
		return anyEqual(s.IpamScopeType, values)
	case filterKeyIsDefault:
		want := anyEqual("true", values)

		return s.IsDefault == want
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(s.IpamScopeID, tagKey, values, b)
		}
	}

	return true
}

// ---- IPAM Resource Discovery filters ----

// applyIpamResourceDiscoveryFilters implements DescribeIpamResourceDiscoveries'
// documented filters (owner-id, is-default, tag:<key>). Previously
// handleDescribeIpamResourceDiscoveries ignored Filters entirely, so a
// tag:Name filter still returned the account's own default resource
// discovery alongside the matching one.
func applyIpamResourceDiscoveryFilters(
	discoveries []*IpamResourceDiscovery, filters map[string][]string, b Backend,
) []*IpamResourceDiscovery {
	if len(filters) == 0 {
		return discoveries
	}

	out := discoveries[:0:0]

discoveryLoop:
	for _, d := range discoveries {
		for name, values := range filters {
			if !ipamResourceDiscoveryMatchesFilter(d, name, values, b) {
				continue discoveryLoop
			}
		}

		out = append(out, d)
	}

	return out
}

func ipamResourceDiscoveryMatchesFilter(d *IpamResourceDiscovery, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "owner-id":
		return anyEqual(d.OwnerID, values)
	case filterKeyIsDefault:
		want := anyEqual("true", values)

		return d.IsDefault == want
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(d.IpamResourceDiscoveryID, tagKey, values, b)
		}
	}

	return true
}

// ---- Transit Gateway Connect filters ----

// applyTGWConnectFilters supports the DescribeTransitGatewayConnects filters
// this backend has data for: options.protocol, state,
// transit-gateway-attachment-id, transit-gateway-id,
// transport-transit-gateway-attachment-id (api_op_DescribeTransitGatewayConnects.go
// doc comment; this op documents no tag:/tag-key filter).
func applyTGWConnectFilters(conns []*TransitGatewayConnect, filters map[string][]string) []*TransitGatewayConnect {
	return applyFilterList(conns, filters, tgwConnectMatchesFilter)
}

func tgwConnectMatchesFilter(c *TransitGatewayConnect, filterName string, values []string) bool {
	switch filterName {
	case "options.protocol":
		return anyEqual(c.Protocol, values)
	case filterKeyState:
		return anyEqual(c.State, values)
	case filterKeyTGWAttachmentID:
		return anyEqual(c.TransitGatewayAttachmentID, values)
	case filterKeyTransitGatewayID:
		return anyEqual(c.TransitGatewayID, values)
	case "transport-transit-gateway-attachment-id":
		return anyEqual(c.TransportTransitGatewayAttachmentID, values)
	}

	return true
}

// applyTGWConnectPeerFilters supports the DescribeTransitGatewayConnectPeers
// filters this backend has data for: state, transit-gateway-attachment-id,
// transit-gateway-connect-peer-id (api_op_DescribeTransitGatewayConnectPeers.go
// doc comment; this op documents no tag:/tag-key filter).
func applyTGWConnectPeerFilters(
	peers []*TransitGatewayConnectPeer, filters map[string][]string,
) []*TransitGatewayConnectPeer {
	return applyFilterList(peers, filters, tgwConnectPeerMatchesFilter)
}

func tgwConnectPeerMatchesFilter(p *TransitGatewayConnectPeer, filterName string, values []string) bool {
	switch filterName {
	case filterKeyState:
		return anyEqual(p.State, values)
	case filterKeyTGWAttachmentID:
		return anyEqual(p.TransitGatewayAttachmentID, values)
	case "transit-gateway-connect-peer-id":
		return anyEqual(p.TransitGatewayConnectPeerID, values)
	}

	return true
}

// ---- Transit Gateway Multicast Domain filters ----

// applyTGWMulticastDomainFilters supports the
// DescribeTransitGatewayMulticastDomains filters this backend has data for:
// state, transit-gateway-id, transit-gateway-multicast-domain-id
// (api_op_DescribeTransitGatewayMulticastDomains.go doc comment; this op
// documents no tag:/tag-key filter).
func applyTGWMulticastDomainFilters(
	domains []*TransitGatewayMulticastDomain, filters map[string][]string,
) []*TransitGatewayMulticastDomain {
	return applyFilterList(domains, filters, tgwMulticastDomainMatchesFilter)
}

func tgwMulticastDomainMatchesFilter(d *TransitGatewayMulticastDomain, filterName string, values []string) bool {
	switch filterName {
	case filterKeyState:
		return anyEqual(d.State, values)
	case filterKeyTransitGatewayID:
		return anyEqual(d.TransitGatewayID, values)
	case "transit-gateway-multicast-domain-id":
		return anyEqual(d.ID, values)
	}

	return true
}

// applyTGWMulticastDomainAssociationFilters supports the
// GetTransitGatewayMulticastDomainAssociations filters this backend has data
// for: resource-id, resource-type, state, subnet-id,
// transit-gateway-attachment-id (api_op_GetTransitGatewayMulticastDomainAssociations.go
// doc comment).
func applyTGWMulticastDomainAssociationFilters(
	assocs []*TransitGatewayMulticastDomainAssociation, filters map[string][]string,
) []*TransitGatewayMulticastDomainAssociation {
	return applyFilterList(assocs, filters, tgwMulticastDomainAssociationMatchesFilter)
}

func tgwMulticastDomainAssociationMatchesFilter(
	a *TransitGatewayMulticastDomainAssociation, filterName string, values []string,
) bool {
	if matched, handled := matchesTGWResourceFilter(
		filterName, values, a.ResourceID, a.ResourceType, a.TransitGatewayAttachmentID,
	); handled {
		return matched
	}

	switch filterName {
	case filterKeyState:
		return anyEqual(a.State, values)
	case filterKeySubnetID:
		return anyEqual(a.SubnetID, values)
	}

	return true
}

// ---- Transit Gateway Peering Attachment filters ----

// applyTGWPeeringAttachmentFilters supports the
// DescribeTransitGatewayPeeringAttachments filters this backend has data
// for: local-owner-id, remote-owner-id, state, tag:<key>, tag-key,
// transit-gateway-attachment-id, transit-gateway-id
// (api_op_DescribeTransitGatewayPeeringAttachments.go doc comment).
func applyTGWPeeringAttachmentFilters(
	atts []*TransitGatewayPeeringAttachment, filters map[string][]string, b Backend,
) []*TransitGatewayPeeringAttachment {
	return applyFilterList(atts, filters, func(a *TransitGatewayPeeringAttachment, name string, values []string) bool {
		return tgwPeeringAttachmentMatchesFilter(a, name, values, b)
	})
}

func tgwPeeringAttachmentMatchesFilter(
	a *TransitGatewayPeeringAttachment, filterName string, values []string, b Backend,
) bool {
	switch filterName {
	case filterKeyTGWAttachmentID:
		return anyEqual(a.TransitGatewayAttachmentID, values)
	case "local-owner-id":
		return anyEqual(a.RequesterOwnerID, values)
	case "remote-owner-id":
		return anyEqual(a.AccepterOwnerID, values)
	case filterKeyState:
		return anyEqual(a.State, values)
	case filterKeyTransitGatewayID:
		return anyEqual(a.RequesterTransitGatewayID, values)
	case filterKeyTagKey:
		for k := range b.TagsForResource(a.TransitGatewayAttachmentID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(a.TransitGatewayAttachmentID, tagKey, values, b)
		}
	}

	return true
}

// ---- Transit Gateway route table / attachment propagation & association filters ----

// applyTGWAttachmentPropagationFilters supports the
// GetTransitGatewayAttachmentPropagations filters this backend has data for:
// transit-gateway-route-table-id (api_op_GetTransitGatewayAttachmentPropagations.go
// doc comment).
func applyTGWAttachmentPropagationFilters(
	props []*TransitGatewayAttachmentPropagation, filters map[string][]string,
) []*TransitGatewayAttachmentPropagation {
	return applyFilterList(props, filters, tgwAttachmentPropagationMatchesFilter)
}

func tgwAttachmentPropagationMatchesFilter(
	p *TransitGatewayAttachmentPropagation, filterName string, values []string,
) bool {
	if filterName == "transit-gateway-route-table-id" {
		return anyEqual(p.TransitGatewayRouteTableID, values)
	}

	return true
}

// applyTGWPrefixListRefFilters supports the GetTransitGatewayPrefixListReferences
// filters this backend has data for: attachment.transit-gateway-attachment-id,
// is-blackhole, prefix-list-id, state (api_op_GetTransitGatewayPrefixListReferences.go
// doc comment). attachment.resource-id, attachment.resource-type, and
// prefix-list-owner-id are documented but unmodeled.
func applyTGWPrefixListRefFilters(
	refs []*TransitGatewayPrefixListReference, filters map[string][]string,
) []*TransitGatewayPrefixListReference {
	return applyFilterList(refs, filters, tgwPrefixListRefMatchesFilter)
}

func tgwPrefixListRefMatchesFilter(r *TransitGatewayPrefixListReference, filterName string, values []string) bool {
	switch filterName {
	case "attachment.transit-gateway-attachment-id":
		return anyEqual(r.TransitGatewayAttachmentID, values)
	case "is-blackhole":
		want := anyEqual("true", values)

		return r.Blackhole == want
	case filterKeyPrefixListID:
		return anyEqual(r.PrefixListID, values)
	case filterKeyState:
		return anyEqual(r.State, values)
	}

	return true
}

// applyTGWRTAssociationFilters supports the GetTransitGatewayRouteTableAssociations
// filters this backend has data for: resource-type, transit-gateway-attachment-id
// (api_op_GetTransitGatewayRouteTableAssociations.go doc comment). resource-id
// is documented and modeled (TransitGatewayRouteTableAssociation.ResourceID)
// but AssociateTransitGatewayRouteTable never populates it (unlike
// EnableTransitGatewayRouteTablePropagation, which does), so it stays
// unimplemented rather than filtering against a field nothing ever sets.
func applyTGWRTAssociationFilters(
	assocs []*TransitGatewayRouteTableAssociation, filters map[string][]string,
) []*TransitGatewayRouteTableAssociation {
	return applyFilterList(assocs, filters, tgwRTAssociationMatchesFilter)
}

func tgwRTAssociationMatchesFilter(a *TransitGatewayRouteTableAssociation, filterName string, values []string) bool {
	switch filterName {
	case filterKeyResourceType:
		return anyEqual(a.ResourceType, values)
	case filterKeyTGWAttachmentID:
		return anyEqual(a.TransitGatewayAttachmentID, values)
	}

	return true
}

// applyTGWRTPropagationFilters supports the GetTransitGatewayRouteTablePropagations
// filters this backend has data for: resource-id, resource-type,
// transit-gateway-attachment-id (api_op_GetTransitGatewayRouteTablePropagations.go
// doc comment).
func applyTGWRTPropagationFilters(
	props []*TransitGatewayRouteTablePropagation, filters map[string][]string,
) []*TransitGatewayRouteTablePropagation {
	return applyFilterList(props, filters, tgwRTPropagationMatchesFilter)
}

func tgwRTPropagationMatchesFilter(p *TransitGatewayRouteTablePropagation, filterName string, values []string) bool {
	if matched, handled := matchesTGWResourceFilter(
		filterName, values, p.ResourceID, p.ResourceType, p.TransitGatewayAttachmentID,
	); handled {
		return matched
	}

	return true
}

// ---- VPC Endpoint Connection / Notification / Service Configuration / Permission filters ----

// applyVpcEndpointConnectionFilters supports the DescribeVpcEndpointConnections
// filters this backend has data for: service-id, vpc-endpoint-id,
// vpc-endpoint-state (api_op_DescribeVpcEndpointConnections.go doc comment).
// ip-address-type, vpc-endpoint-owner, and vpc-endpoint-region are documented
// but unmodeled.
func applyVpcEndpointConnectionFilters(
	conns []*VpcEndpointConnection, filters map[string][]string,
) []*VpcEndpointConnection {
	return applyFilterList(conns, filters, vpcEndpointConnectionMatchesFilter)
}

func vpcEndpointConnectionMatchesFilter(c *VpcEndpointConnection, filterName string, values []string) bool {
	switch filterName {
	case filterKeyServiceID:
		return anyEqual(c.ServiceID, values)
	case "vpc-endpoint-id":
		return anyEqual(c.VpcEndpointID, values)
	case "vpc-endpoint-state":
		return anyEqual(c.State, values)
	}

	return true
}

// applyVpcEndpointConnNotifFilters supports the
// DescribeVpcEndpointConnectionNotifications filters this backend has data
// for: connection-notification-arn, connection-notification-id,
// connection-notification-state, connection-notification-type, service-id,
// vpc-endpoint-id (api_op_DescribeVpcEndpointConnectionNotifications.go doc
// comment).
func applyVpcEndpointConnNotifFilters(
	notifs []*VpcEndpointConnectionNotification, filters map[string][]string,
) []*VpcEndpointConnectionNotification {
	return applyFilterList(notifs, filters, vpcEndpointConnNotifMatchesFilter)
}

func vpcEndpointConnNotifMatchesFilter(
	n *VpcEndpointConnectionNotification, filterName string, values []string,
) bool {
	switch filterName {
	case "connection-notification-arn":
		return anyEqual(n.ConnectionNotificationARN, values)
	case "connection-notification-id":
		return anyEqual(n.ConnectionNotificationID, values)
	case "connection-notification-state":
		return anyEqual(n.ConnectionNotificationState, values)
	case "connection-notification-type":
		return anyEqual(n.ConnectionNotificationType, values)
	case filterKeyServiceID:
		return anyEqual(n.ServiceID, values)
	case "vpc-endpoint-id":
		return anyEqual(n.VpcEndpointID, values)
	}

	return true
}

// applyVpcEndpointServiceConfigFilters supports the
// DescribeVpcEndpointServiceConfigurations filters this backend has data
// for: service-id, service-name, service-state, tag:<key>, tag-key
// (api_op_DescribeVpcEndpointServiceConfigurations.go doc comment).
// supported-ip-address-types is documented but unmodeled.
func applyVpcEndpointServiceConfigFilters(
	cfgs []*VpcEndpointServiceConfig, filters map[string][]string, b Backend,
) []*VpcEndpointServiceConfig {
	return applyFilterList(cfgs, filters, func(c *VpcEndpointServiceConfig, name string, values []string) bool {
		return vpcEndpointServiceConfigMatchesFilter(c, name, values, b)
	})
}

func vpcEndpointServiceConfigMatchesFilter(
	c *VpcEndpointServiceConfig, filterName string, values []string, b Backend,
) bool {
	switch filterName {
	case "service-name":
		return anyEqual(c.ServiceName, values)
	case filterKeyServiceID:
		return anyEqual(c.ServiceID, values)
	case "service-state":
		return anyEqual(c.ServiceState, values)
	case filterKeyTagKey:
		for k := range b.TagsForResource(c.ServiceID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(c.ServiceID, tagKey, values, b)
		}
	}

	return true
}

// applyVpcEndpointServicePermissionFilters supports the
// DescribeVpcEndpointServicePermissions filters this backend has data for:
// principal, principal-type (api_op_DescribeVpcEndpointServicePermissions.go
// doc comment).
func applyVpcEndpointServicePermissionFilters(principals []string, filters map[string][]string) []string {
	return applyFilterList(principals, filters, vpcEndpointServicePermissionMatchesFilter)
}

func vpcEndpointServicePermissionMatchesFilter(principal string, filterName string, values []string) bool {
	switch filterName {
	case "principal":
		return anyEqual(principal, values)
	case "principal-type":
		return anyEqual(principalTypeFor(principal), values)
	}

	return true
}

// ---- Client VPN Authorization Rule / Route / Target Network filters ----

// applyClientVpnAuthRuleFilters supports the
// DescribeClientVpnAuthorizationRules filters this backend has data for:
// description, destination-cidr (api_op_DescribeClientVpnAuthorizationRules.go
// doc comment). group-id is documented and modeled (ClientVpnAuthRule.GroupID)
// but AuthorizeClientVpnIngress never reads a GroupId off the wire to
// populate it (every rule is created with AccessAll: true), so it stays
// unimplemented rather than filtering against a field nothing ever sets.
func applyClientVpnAuthRuleFilters(
	rules []ClientVpnAuthRule, filters map[string][]string,
) []ClientVpnAuthRule {
	return applyFilterList(rules, filters, clientVpnAuthRuleMatchesFilter)
}

func clientVpnAuthRuleMatchesFilter(r ClientVpnAuthRule, filterName string, values []string) bool {
	switch filterName {
	case filterKeyDescription:
		return anyEqual(r.Description, values)
	case filterKeyDestinationCidr:
		return anyEqual(r.Cidr, values)
	}

	return true
}

// applyClientVpnRouteFilters supports the DescribeClientVpnRoutes filters
// this backend has data for: destination-cidr, origin, target-subnet
// (api_op_DescribeClientVpnRoutes.go doc comment).
func applyClientVpnRouteFilters(routes []ClientVpnRoute, filters map[string][]string) []ClientVpnRoute {
	return applyFilterList(routes, filters, clientVpnRouteMatchesFilter)
}

func clientVpnRouteMatchesFilter(r ClientVpnRoute, filterName string, values []string) bool {
	switch filterName {
	case filterKeyDestinationCidr:
		return anyEqual(r.DestinationCidr, values)
	case "origin":
		return anyEqual(r.Origin, values)
	case "target-subnet":
		return anyEqual(r.TargetSubnet, values)
	}

	return true
}

// applyClientVpnTargetNetworkFilters supports the
// DescribeClientVpnTargetNetworks filters this backend has data for:
// association-id, target-network-id, vpc-id
// (api_op_DescribeClientVpnTargetNetworks.go doc comment).
func applyClientVpnTargetNetworkFilters(
	networks []*ClientVpnTargetNetwork, filters map[string][]string,
) []*ClientVpnTargetNetwork {
	return applyFilterList(networks, filters, clientVpnTargetNetworkMatchesFilter)
}

func clientVpnTargetNetworkMatchesFilter(n *ClientVpnTargetNetwork, filterName string, values []string) bool {
	switch filterName {
	case "association-id":
		return anyEqual(n.AssociationID, values)
	case "target-network-id":
		return anyEqual(n.SubnetID, values)
	case filterKeyVPCID:
		return anyEqual(n.VPCID, values)
	}

	return true
}

// ---- Dedicated Host filters ----

// applyHostFilters supports the DescribeHosts filters this backend has data
// for: auto-placement, availability-zone, instance-type, state, tag-key
// (api_op_DescribeHosts.go doc comment). client-token and host-reservation-id
// are documented but unmodeled.
func applyHostFilters(hosts []*Host, filters map[string][]string, b Backend) []*Host {
	return applyFilterList(hosts, filters, func(host *Host, name string, values []string) bool {
		return hostMatchesFilter(host, name, values, b)
	})
}

func hostMatchesFilter(host *Host, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "auto-placement":
		return anyEqual(host.AutoPlacement, values)
	case filterKeyAvailabilityZone:
		return anyEqual(host.AvailabilityZone, values)
	case filterKeyInstanceType:
		return anyEqual(host.InstanceType, values)
	case filterKeyState:
		return anyEqual(host.State, values)
	case filterKeyTagKey:
		for k := range b.TagsForResource(host.HostID) {
			if anyEqual(k, values) {
				return true
			}
		}

		return false
	}

	return true
}

// ---- Placement Group filters ----

// applyPlacementGroupFilters supports the DescribePlacementGroups filters
// this backend has data for: group-name, state, strategy, tag:<key>, tag-key
// (api_op_DescribePlacementGroups.go doc comment). group-arn and
// spread-level are documented but unmodeled (PlacementGroup has no ARN or
// spread-level field).
func applyPlacementGroupFilters(pgs []*PlacementGroup, filters map[string][]string, b Backend) []*PlacementGroup {
	return applyFilterList(pgs, filters, func(pg *PlacementGroup, name string, values []string) bool {
		return placementGroupMatchesFilter(pg, name, values, b)
	})
}

func placementGroupMatchesFilter(pg *PlacementGroup, filterName string, values []string, b Backend) bool {
	if matched, handled := matchesTagFilter(pg.Name, filterName, values, b); handled {
		return matched
	}

	switch filterName {
	case filterKeyGroupName:
		return anyEqual(pg.Name, values)
	case filterKeyState:
		return anyEqual(pg.State, values)
	case "strategy":
		return anyEqual(pg.Strategy, values)
	}

	return true
}

// ---- Fleet filters ----

// applyFleetFilters supports the DescribeFleets filters this backend has
// data for: fleet-state, type (api_op_DescribeFleets.go doc comment).
// activity-status and replace-unhealthy-instances are documented but
// unmodeled (Fleet has no backing field for either); excess-capacity-
// termination-policy is documented as a true/false value but this backend
// stores the real no-termination/termination enum, so it is left unmodeled
// rather than fabricating a value mapping never verified against the wire.
func applyFleetFilters(fleets []*Fleet, filters map[string][]string) []*Fleet {
	return applyFilterList(fleets, filters, fleetMatchesFilter)
}

func fleetMatchesFilter(f *Fleet, filterName string, values []string) bool {
	switch filterName {
	case "fleet-state":
		return anyEqual(f.FleetState, values)
	case filterKeyType:
		return anyEqual(f.FleetType, values)
	}

	return true
}

// ---- Spot Price History filters ----

// applySpotPriceFilters supports the DescribeSpotPriceHistory filters this
// backend has data for: availability-zone, instance-type,
// product-description, spot-price (api_op_DescribeSpotPriceHistory.go doc
// comment). availability-zone-id is documented but unmodeled; timestamp is
// documented as wildcard-matchable, which this backend does not implement,
// so it is left unmodeled rather than an incorrect exact-match-only version.
func applySpotPriceFilters(records []SpotPriceRecord, filters map[string][]string) []SpotPriceRecord {
	return applyFilterList(records, filters, spotPriceRecordMatchesFilter)
}

func spotPriceRecordMatchesFilter(r SpotPriceRecord, filterName string, values []string) bool {
	switch filterName {
	case filterKeyAvailabilityZone:
		return anyEqual(r.AvailabilityZone, values)
	case filterKeyInstanceType:
		return anyEqual(r.InstanceType, values)
	case filterKeyProductDesc:
		return anyEqual(r.ProductDescription, values)
	case "spot-price":
		return anyEqual(r.SpotPrice, values)
	}

	return true
}

// ---- Reserved Instances filters ----

// applyReservedInstanceFilters supports the DescribeReservedInstances
// filters this backend has data for: availability-zone, duration, end,
// fixed-price, instance-type, product-description, reserved-instances-id,
// start, state, tag:<key>, tag-key, usage-price
// (api_op_DescribeReservedInstances.go doc comment). availability-zone-id
// and scope are documented but unmodeled (ReservedInstance has no backing
// field for either).
func applyReservedInstanceFilters(
	ris []*ReservedInstance, filters map[string][]string, b Backend,
) []*ReservedInstance {
	return applyFilterList(ris, filters, func(ri *ReservedInstance, name string, values []string) bool {
		return reservedInstanceMatchesFilter(ri, name, values, b)
	})
}

func reservedInstanceMatchesFilter(ri *ReservedInstance, filterName string, values []string, b Backend) bool {
	if matched, handled := matchesTagFilter(ri.ReservedInstancesID, filterName, values, b); handled {
		return matched
	}

	switch filterName {
	case filterKeyAvailabilityZone:
		return anyEqual(ri.AvailabilityZone, values)
	case "duration":
		return anyEqual(strconv.FormatInt(ri.Duration, 10), values)
	case "end":
		return anyEqual(ri.End.UTC().Format(time.RFC3339), values)
	case "fixed-price":
		return anyEqual(strconv.FormatFloat(ri.FixedPrice, 'f', -1, 64), values)
	case filterKeyInstanceType:
		return anyEqual(ri.InstanceType, values)
	case filterKeyProductDesc:
		return anyEqual(ri.ProductDescription, values)
	case "reserved-instances-id":
		return anyEqual(ri.ReservedInstancesID, values)
	case "start":
		return anyEqual(ri.Start.UTC().Format(time.RFC3339), values)
	case filterKeyState:
		return anyEqual(ri.State, values)
	case "usage-price":
		return anyEqual(strconv.FormatFloat(ri.UsagePrice, 'f', -1, 64), values)
	}

	return true
}

// ---- Traffic Mirror filters ----

// applyTrafficMirrorFilterFilters supports the DescribeTrafficMirrorFilters
// filters (description, traffic-mirror-filter-id --
// api_op_DescribeTrafficMirrorFilters.go doc comment); both are backed.
func applyTrafficMirrorFilterFilters(
	fs []*TrafficMirrorFilter, filters map[string][]string,
) []*TrafficMirrorFilter {
	return applyFilterList(fs, filters, trafficMirrorFilterMatchesFilter)
}

func trafficMirrorFilterMatchesFilter(f *TrafficMirrorFilter, filterName string, values []string) bool {
	switch filterName {
	case filterKeyDescription:
		return anyEqual(f.Description, values)
	case "traffic-mirror-filter-id":
		return anyEqual(f.TrafficMirrorFilterID, values)
	}

	return true
}

// applyTrafficMirrorSessionFilters supports the DescribeTrafficMirrorSessions
// filters (description, network-interface-id, owner-id, packet-length,
// session-number, traffic-mirror-filter-id, traffic-mirror-session-id,
// traffic-mirror-target-id, virtual-network-id --
// api_op_DescribeTrafficMirrorSessions.go doc comment); all nine are backed.
func applyTrafficMirrorSessionFilters(
	sessions []*TrafficMirrorSession, filters map[string][]string,
) []*TrafficMirrorSession {
	return applyFilterList(sessions, filters, trafficMirrorSessionMatchesFilter)
}

func trafficMirrorSessionMatchesFilter(s *TrafficMirrorSession, filterName string, values []string) bool {
	switch filterName {
	case filterKeyDescription:
		return anyEqual(s.Description, values)
	case filterKeyNetworkIfaceID:
		return anyEqual(s.NetworkInterfaceID, values)
	case filterKeyOwnerID:
		return anyEqual(s.OwnerID, values)
	case "packet-length":
		return anyEqual(strconv.Itoa(s.PacketLength), values)
	case "session-number":
		return anyEqual(strconv.Itoa(s.SessionNumber), values)
	case "traffic-mirror-filter-id":
		return anyEqual(s.TrafficMirrorFilterID, values)
	case "traffic-mirror-session-id":
		return anyEqual(s.TrafficMirrorSessionID, values)
	case "traffic-mirror-target-id":
		return anyEqual(s.TrafficMirrorTargetID, values)
	case "virtual-network-id":
		return anyEqual(strconv.Itoa(s.VirtualNetworkID), values)
	}

	return true
}

// applyTrafficMirrorTargetFilters supports the DescribeTrafficMirrorTargets
// filters (description, network-interface-id, network-load-balancer-arn,
// owner-id, traffic-mirror-target-id --
// api_op_DescribeTrafficMirrorTargets.go doc comment); all five are backed.
func applyTrafficMirrorTargetFilters(
	targets []*TrafficMirrorTarget, filters map[string][]string,
) []*TrafficMirrorTarget {
	return applyFilterList(targets, filters, trafficMirrorTargetMatchesFilter)
}

func trafficMirrorTargetMatchesFilter(t *TrafficMirrorTarget, filterName string, values []string) bool {
	switch filterName {
	case filterKeyDescription:
		return anyEqual(t.Description, values)
	case filterKeyNetworkIfaceID:
		return anyEqual(t.NetworkInterfaceID, values)
	case "network-load-balancer-arn":
		return anyEqual(t.NetworkLoadBalancerArn, values)
	case filterKeyOwnerID:
		return anyEqual(t.OwnerID, values)
	case "traffic-mirror-target-id":
		return anyEqual(t.TrafficMirrorTargetID, values)
	}

	return true
}

// ---- VPC Endpoint Association filters ----

// applyVpcEndpointAssociationFilters supports the
// DescribeVpcEndpointAssociations filters this backend has data for:
// vpc-endpoint-id (api_op_DescribeVpcEndpointAssociations.go doc comment).
// This backend models a VPC endpoint association as the endpoint itself
// rather than a real VPC Lattice service-network association record, so
// association-id, associated-resource-accessibility, associated-resource-id,
// service-network-arn, and resource-configuration-group-arn are documented
// but unmodeled.
func applyVpcEndpointAssociationFilters(eps []*VpcEndpoint, filters map[string][]string) []*VpcEndpoint {
	return applyFilterList(eps, filters, func(ep *VpcEndpoint, name string, values []string) bool {
		if name == filterKeyVpcEndpointID {
			return anyEqual(ep.ID, values)
		}

		return true
	})
}

// ---- Local Gateway Route Table filters ----

// applyLocalGatewayRouteTableFilters supports the
// DescribeLocalGatewayRouteTables filters (local-gateway-id,
// local-gateway-route-table-arn, local-gateway-route-table-id, outpost-arn,
// owner-id, state -- api_op_DescribeLocalGatewayRouteTables.go doc
// comment); all six are backed.
func applyLocalGatewayRouteTableFilters(
	rts []*LocalGatewayRouteTable, filters map[string][]string,
) []*LocalGatewayRouteTable {
	return applyFilterList(rts, filters, localGatewayRouteTableMatchesFilter)
}

func localGatewayRouteTableMatchesFilter(rt *LocalGatewayRouteTable, filterName string, values []string) bool {
	switch filterName {
	case filterKeyLocalGatewayID:
		return anyEqual(rt.LocalGatewayID, values)
	case filterKeyLGWRouteTableArn:
		return anyEqual(rt.LocalGatewayRouteTableArn, values)
	case filterKeyLGWRouteTableID:
		return anyEqual(rt.LocalGatewayRouteTableID, values)
	case filterKeyOutpostArn:
		return anyEqual(rt.OutpostArn, values)
	case filterKeyOwnerID:
		return anyEqual(rt.OwnerID, values)
	case filterKeyState:
		return anyEqual(rt.State, values)
	}

	return true
}

// applyLGWVifGroupAssocFilters supports the
// DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations filters
// (local-gateway-id, local-gateway-route-table-arn,
// local-gateway-route-table-id,
// local-gateway-route-table-virtual-interface-group-association-id,
// local-gateway-route-table-virtual-interface-group-id, owner-id, state --
// api_op_DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations.go
// doc comment); all seven are backed.
func applyLGWVifGroupAssocFilters(
	assocs []*LocalGatewayRouteTableVirtualInterfaceGroupAssociation, filters map[string][]string,
) []*LocalGatewayRouteTableVirtualInterfaceGroupAssociation {
	return applyFilterList(assocs, filters, lgwVifGroupAssocMatchesFilter)
}

func lgwVifGroupAssocMatchesFilter(
	a *LocalGatewayRouteTableVirtualInterfaceGroupAssociation, filterName string, values []string,
) bool {
	switch filterName {
	case filterKeyLocalGatewayID:
		return anyEqual(a.LocalGatewayID, values)
	case filterKeyLGWRouteTableArn:
		return anyEqual(a.LocalGatewayRouteTableArn, values)
	case filterKeyLGWRouteTableID:
		return anyEqual(a.LocalGatewayRouteTableID, values)
	case "local-gateway-route-table-virtual-interface-group-association-id":
		return anyEqual(a.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID, values)
	case "local-gateway-route-table-virtual-interface-group-id":
		return anyEqual(a.LocalGatewayVirtualInterfaceGroupID, values)
	case filterKeyOwnerID:
		return anyEqual(a.OwnerID, values)
	case filterKeyState:
		return anyEqual(a.State, values)
	}

	return true
}

// applyLocalGatewayRouteTableVpcAssociationFilters supports the
// DescribeLocalGatewayRouteTableVpcAssociations filters (local-gateway-id,
// local-gateway-route-table-arn, local-gateway-route-table-id,
// local-gateway-route-table-vpc-association-id, owner-id, state, vpc-id --
// api_op_DescribeLocalGatewayRouteTableVpcAssociations.go doc comment); all
// seven are backed.
func applyLocalGatewayRouteTableVpcAssociationFilters(
	assocs []*LocalGatewayRouteTableVpcAssociation, filters map[string][]string,
) []*LocalGatewayRouteTableVpcAssociation {
	return applyFilterList(assocs, filters, localGatewayRouteTableVpcAssociationMatchesFilter)
}

func localGatewayRouteTableVpcAssociationMatchesFilter(
	a *LocalGatewayRouteTableVpcAssociation, filterName string, values []string,
) bool {
	switch filterName {
	case filterKeyLocalGatewayID:
		return anyEqual(a.LocalGatewayID, values)
	case filterKeyLGWRouteTableArn:
		return anyEqual(a.LocalGatewayRouteTableArn, values)
	case filterKeyLGWRouteTableID:
		return anyEqual(a.LocalGatewayRouteTableID, values)
	case "local-gateway-route-table-vpc-association-id":
		return anyEqual(a.LocalGatewayRouteTableVpcAssociationID, values)
	case filterKeyOwnerID:
		return anyEqual(a.OwnerID, values)
	case filterKeyState:
		return anyEqual(a.State, values)
	case filterKeyVPCID:
		return anyEqual(a.VpcID, values)
	}

	return true
}

// ---- Network Insights filters ----

// applyNetworkInsightsPathFilters supports the DescribeNetworkInsightsPaths
// filters this backend has data for: destination, protocol, source
// (api_op_DescribeNetworkInsightsPaths.go doc comment). The
// filter-at-source.*/filter-at-destination.* sub-filters are documented but
// unmodeled: NetworkInsightsPath tracks no per-endpoint address/port-range
// filter data.
func applyNetworkInsightsPathFilters(
	paths []*NetworkInsightsPath, filters map[string][]string,
) []*NetworkInsightsPath {
	return applyFilterList(paths, filters, networkInsightsPathMatchesFilter)
}

func networkInsightsPathMatchesFilter(p *NetworkInsightsPath, filterName string, values []string) bool {
	switch filterName {
	case "destination":
		return anyEqual(p.DestinationID, values)
	case "protocol":
		return anyEqual(p.Protocol, values)
	case "source":
		return anyEqual(p.SourceID, values)
	}

	return true
}

// applyNetworkInsightsAnalysisFilters supports the
// DescribeNetworkInsightsAnalyses filters (path-found, status --
// api_op_DescribeNetworkInsightsAnalyses.go doc comment); both are backed.
func applyNetworkInsightsAnalysisFilters(
	analyses []*NetworkInsightsAnalysis, filters map[string][]string,
) []*NetworkInsightsAnalysis {
	return applyFilterList(analyses, filters, networkInsightsAnalysisMatchesFilter)
}

func networkInsightsAnalysisMatchesFilter(a *NetworkInsightsAnalysis, filterName string, values []string) bool {
	switch filterName {
	case "path-found":
		want := anyEqual("true", values)

		return a.NetworkPathFound == want
	case filterKeyStatus:
		return anyEqual(a.Status, values)
	}

	return true
}

// applyLaunchTemplateFilters supports the DescribeLaunchTemplates filters
// this backend has data for: create-time, launch-template-name, tag:<key>,
// tag-key (api_op_DescribeLaunchTemplates.go doc comment lists exactly these
// four).
func applyLaunchTemplateFilters(
	templates []*LaunchTemplate, filters map[string][]string, b Backend,
) []*LaunchTemplate {
	return applyFilterList(templates, filters, func(t *LaunchTemplate, name string, values []string) bool {
		return launchTemplateMatchesFilter(t, name, values, b)
	})
}

func launchTemplateMatchesFilter(t *LaunchTemplate, filterName string, values []string, b Backend) bool {
	if handled, ok := matchesTagFilter(t.ID, filterName, values, b); ok {
		return handled
	}

	switch filterName {
	case "launch-template-name":
		return anyEqual(t.Name, values)
	case "create-time":
		// Must match the wire format handleDescribeLaunchTemplates renders
		// (time.RFC3339), or an exact-match filter built from this API's own
		// output would never match its own record.
		return matchesWildcardTimeFilter(t.CreateTime.Format(time.RFC3339), values)
	}

	return true
}

// applyCoipPoolFilters supports the DescribeCoipPools filters this backend
// has data for: coip-pool.local-gateway-route-table-id, coip-pool.pool-id
// (api_op_DescribeCoipPools.go doc comment).
func applyCoipPoolFilters(pools []*CoipPool, filters map[string][]string) []*CoipPool {
	return applyFilterList(pools, filters, coipPoolMatchesFilter)
}

func coipPoolMatchesFilter(p *CoipPool, filterName string, values []string) bool {
	switch filterName {
	case "coip-pool.local-gateway-route-table-id":
		return anyEqual(p.LocalGatewayRouteTableID, values)
	case "coip-pool.pool-id":
		return anyEqual(p.PoolID, values)
	}

	return true
}

// applyLocalGatewayFilters supports the DescribeLocalGateways filters this
// backend has data for: local-gateway-id, outpost-arn, owner-id, state
// (api_op_DescribeLocalGateways.go doc comment lists exactly these four).
func applyLocalGatewayFilters(lgws []*LocalGateway, filters map[string][]string) []*LocalGateway {
	return applyFilterList(lgws, filters, localGatewayMatchesFilter)
}

func localGatewayMatchesFilter(lg *LocalGateway, filterName string, values []string) bool {
	switch filterName {
	case filterKeyLocalGatewayID:
		return anyEqual(lg.LocalGatewayID, values)
	case filterKeyOutpostArn:
		return anyEqual(lg.OutpostArn, values)
	case filterKeyOwnerID:
		return anyEqual(lg.OwnerID, values)
	case filterKeyState:
		return anyEqual(lg.State, values)
	}

	return true
}

// applyLocalGatewayVirtualInterfaceFilters supports the
// DescribeLocalGatewayVirtualInterfaces filters this backend has data for:
// local-address, local-bgp-asn, local-gateway-id,
// local-gateway-virtual-interface-id, owner-id, peer-address, peer-bgp-asn,
// vlan (api_op_DescribeLocalGatewayVirtualInterfaces.go doc comment lists
// exactly these eight).
func applyLocalGatewayVirtualInterfaceFilters(
	vifs []*LocalGatewayVirtualInterface, filters map[string][]string,
) []*LocalGatewayVirtualInterface {
	return applyFilterList(vifs, filters, localGatewayVirtualInterfaceMatchesFilter)
}

func localGatewayVirtualInterfaceMatchesFilter(
	vif *LocalGatewayVirtualInterface, filterName string, values []string,
) bool {
	switch filterName {
	case "local-address":
		return anyEqual(vif.LocalAddress, values)
	case "local-bgp-asn":
		return anyEqual(strconv.Itoa(int(vif.LocalBgpAsn)), values)
	case filterKeyLocalGatewayID:
		return anyEqual(vif.LocalGatewayID, values)
	case "local-gateway-virtual-interface-id":
		return anyEqual(vif.LocalGatewayVirtualInterfaceID, values)
	case filterKeyOwnerID:
		return anyEqual(vif.OwnerID, values)
	case "peer-address":
		return anyEqual(vif.PeerAddress, values)
	case "peer-bgp-asn":
		return anyEqual(strconv.Itoa(int(vif.PeerBgpAsn)), values)
	case "vlan":
		return anyEqual(strconv.Itoa(int(vif.Vlan)), values)
	}

	return true
}

// applyLocalGatewayVirtualInterfaceGroupFilters supports the
// DescribeLocalGatewayVirtualInterfaceGroups filters this backend has data
// for: local-gateway-id, local-gateway-virtual-interface-group-id,
// local-gateway-virtual-interface-id, owner-id
// (api_op_DescribeLocalGatewayVirtualInterfaceGroups.go doc comment lists
// exactly these four).
func applyLocalGatewayVirtualInterfaceGroupFilters(
	groups []*LocalGatewayVirtualInterfaceGroup, filters map[string][]string,
) []*LocalGatewayVirtualInterfaceGroup {
	return applyFilterList(groups, filters, localGatewayVirtualInterfaceGroupMatchesFilter)
}

func localGatewayVirtualInterfaceGroupMatchesFilter(
	g *LocalGatewayVirtualInterfaceGroup, filterName string, values []string,
) bool {
	switch filterName {
	case filterKeyLocalGatewayID:
		return anyEqual(g.LocalGatewayID, values)
	case "local-gateway-virtual-interface-group-id":
		return anyEqual(g.LocalGatewayVirtualInterfaceGroupID, values)
	case "local-gateway-virtual-interface-id":
		return anyContains(g.LocalGatewayVirtualInterfaceIDs, values)
	case filterKeyOwnerID:
		return anyEqual(g.OwnerID, values)
	}

	return true
}

// applyVolumeStatusFilters supports the DescribeVolumeStatus filter this
// backend has data for: availability-zone (api_op_DescribeVolumeStatus.go
// doc comment also documents action.*/event.*/volume-status.* filters, but
// this backend performs no real health-check pipeline -- VolumeStatus is
// always the constant "ok" with no per-event data behind it, so those stay
// unmodeled rather than filtering fields nothing ever varies).
func applyVolumeStatusFilters(items []VolumeStatusItem, filters map[string][]string) []VolumeStatusItem {
	return applyFilterList(items, filters, func(item VolumeStatusItem, name string, values []string) bool {
		if name == filterKeyAvailabilityZone {
			return anyEqual(item.AvailabilityZone, values)
		}

		return true
	})
}

// applyVolumeModificationFilters supports the DescribeVolumesModifications
// filters this backend has data for: modification-state, original-size,
// original-volume-type, start-time, target-iops, target-size,
// target-volume-type, volume-id (api_op_DescribeVolumesModifications.go doc
// comment). original-iops is documented but VolumeModification.OrigIops is
// never populated by ModifyVolume; originalMultiAttachEnabled/
// targetMultiAttachEnabled have no backing field at all -- both left
// unmodeled rather than fabricating data.
func applyVolumeModificationFilters(mods []*VolumeModification, filters map[string][]string) []*VolumeModification {
	return applyFilterList(mods, filters, volumeModificationMatchesFilter)
}

func volumeModificationMatchesFilter(mod *VolumeModification, filterName string, values []string) bool {
	switch filterName {
	case "modification-state":
		return anyEqual(mod.ModificationState, values)
	case "original-size":
		return anyEqual(strconv.Itoa(mod.OrigSize), values)
	case "original-volume-type":
		return anyEqual(mod.OrigVolumeType, values)
	case "start-time":
		// Must match the wire format handleDescribeVolumesModifications
		// renders, or an exact-match filter built from this API's own output
		// would never match its own record.
		return anyEqual(mod.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"), values)
	case "target-iops":
		return anyEqual(strconv.Itoa(mod.TargetIops), values)
	case "target-size":
		return anyEqual(strconv.Itoa(mod.TargetSize), values)
	case "target-volume-type":
		return anyEqual(mod.TargetVolumeType, values)
	case filterKeyVolumeID:
		return anyEqual(mod.VolumeID, values)
	}

	return true
}

// applyMacHostFilters supports the DescribeMacHosts filters this backend has
// data for: availability-zone, instance-type
// (api_op_DescribeMacHosts.go doc comment). MacHost itself (mac_hosts.go)
// carries neither field -- it's derived on read from the underlying
// Dedicated Host, so this cross-references Backend.DescribeHosts by HostID
// to reach AvailabilityZone/InstanceType instead of fabricating a match.
func applyMacHostFilters(hosts []*MacHost, filters map[string][]string, b Backend) []*MacHost {
	if len(filters) == 0 {
		return hosts
	}

	byID := make(map[string]*Host, len(hosts))
	for _, dh := range b.DescribeHosts(nil) {
		byID[dh.HostID] = dh
	}

	return applyFilterList(hosts, filters, func(mh *MacHost, name string, values []string) bool {
		dh := byID[mh.HostID]
		if dh == nil {
			return true
		}

		switch name {
		case filterKeyAvailabilityZone:
			return anyEqual(dh.AvailabilityZone, values)
		case filterKeyInstanceType:
			return anyEqual(dh.InstanceType, values)
		}

		return true
	})
}

// applyFpgaImageFilters supports the DescribeFpgaImages filters this backend
// has data for: create-time, fpga-image-id, fpga-image-global-id, name,
// owner-id, shell-version, state, tag:<key>, tag-key
// (api_op_DescribeFpgaImages.go doc comment). product-code is documented but
// FpgaImage.ProductCodes is never populated by CreateFpgaImage, so it stays
// unmodeled rather than filtering a field nothing ever sets.
func applyFpgaImageFilters(images []*FpgaImage, filters map[string][]string, b Backend) []*FpgaImage {
	return applyFilterList(images, filters, func(img *FpgaImage, name string, values []string) bool {
		return fpgaImageMatchesFilter(img, name, values, b)
	})
}

func fpgaImageMatchesFilter(img *FpgaImage, filterName string, values []string, b Backend) bool {
	if handled, ok := matchesTagFilter(img.FpgaImageID, filterName, values, b); ok {
		return handled
	}

	switch filterName {
	case "fpga-image-id":
		return anyEqual(img.FpgaImageID, values)
	case "fpga-image-global-id":
		return anyEqual(img.FpgaImageGlobalID, values)
	case "name":
		return anyEqual(img.Name, values)
	case filterKeyOwnerID:
		return anyEqual(img.OwnerID, values)
	case "shell-version":
		return anyEqual(img.ShellVersion, values)
	case filterKeyState:
		return anyEqual(img.State, values)
	case "create-time":
		// Must match toFpgaImageItemXML's wire format (time.RFC3339), or an
		// exact-match filter built from this API's own output would never
		// match its own record.
		return matchesWildcardTimeFilter(img.CreateTime.Format(time.RFC3339), values)
	}

	return true
}

// applyImportImageTaskFilters supports the DescribeImportImageTasks filter
// documented on the wire: task-state, matched against one of active |
// completed | deleting | deleted (api_op_DescribeImportImageTasks.go doc
// comment). ImportImage always leaves a task's Status at the constant
// "completed" (this backend runs imports synchronously), so this only ever
// keeps or drops entire result sets rather than distinguishing individual
// tasks -- still real filtering behavior on a real, populated field, not
// fabricated.
func applyImportImageTaskFilters(tasks []*ImageImportTask, filters map[string][]string) []*ImageImportTask {
	return applyFilterList(tasks, filters, func(t *ImageImportTask, name string, values []string) bool {
		if name == "task-state" {
			return anyEqual(t.Status, values)
		}

		return true
	})
}

// applyInstanceEventWindowFilters supports the DescribeInstanceEventWindows
// filters this backend has data for: dedicated-host-id, event-window-name,
// instance-id, tag:<key>, tag-key, tag-value
// (api_op_DescribeInstanceEventWindows.go doc comment). instance-tag/
// instance-tag-key/instance-tag-value (matched against the tags of an
// *associated instance*, not the event window itself) are documented but
// left unmodeled -- a real but more involved cross-resource lookup, out of
// scope for this pass.
func applyInstanceEventWindowFilters(
	ews []*InstanceEventWindow, filters map[string][]string, b Backend,
) []*InstanceEventWindow {
	return applyFilterList(ews, filters, func(ew *InstanceEventWindow, name string, values []string) bool {
		return instanceEventWindowMatchesFilter(ew, name, values, b)
	})
}

func instanceEventWindowMatchesFilter(ew *InstanceEventWindow, filterName string, values []string, b Backend) bool {
	if handled, ok := matchesTagFilter(ew.InstanceEventWindowID, filterName, values, b); ok {
		return handled
	}

	switch filterName {
	case "dedicated-host-id":
		return anyContains(ew.DedicatedHostIDs, values)
	case "event-window-name":
		return anyEqual(ew.Name, values)
	case filterKeyInstanceID:
		return anyContains(ew.InstanceIDs, values)
	case "tag-value":
		for _, v := range b.TagsForResource(ew.InstanceEventWindowID) {
			if anyEqual(v, values) {
				return true
			}
		}

		return false
	}

	return true
}

// applyInstanceCreditSpecFilters supports the
// DescribeInstanceCreditSpecifications filter documented on the wire:
// instance-id (api_op_DescribeInstanceCreditSpecifications.go doc comment
// lists only this one).
func applyInstanceCreditSpecFilters(specs []InstanceCreditSpec, filters map[string][]string) []InstanceCreditSpec {
	return applyFilterList(specs, filters, func(s InstanceCreditSpec, name string, values []string) bool {
		if name == filterKeyInstanceID {
			return anyEqual(s.InstanceID, values)
		}

		return true
	})
}

// applySnapshotLockFilters supports the DescribeLockedSnapshots filter
// documented on the wire: lock-state (api_op_DescribeLockedSnapshots.go doc
// comment lists only this one).
func applySnapshotLockFilters(locks []*SnapshotLock, filters map[string][]string) []*SnapshotLock {
	return applyFilterList(locks, filters, func(l *SnapshotLock, name string, values []string) bool {
		if name == "lock-state" {
			return anyEqual(l.LockState, values)
		}

		return true
	})
}
