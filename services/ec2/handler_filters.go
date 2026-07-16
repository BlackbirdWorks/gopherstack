package ec2

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
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
)

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

func vpcMatchesFilter(v *VPC, filterName string, values []string, b Backend) bool {
	switch filterName {
	case filterKeyVPCID:
		return anyEqual(v.ID, values)
	case "cidr", "cidr-block", "cidrBlock":
		return anyEqual(v.CIDRBlock, values)
	case "isDefault", "is-default":
		want := anyEqual("true", values)

		return v.IsDefault == want
	case filterKeyState:
		return anyEqual("available", values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(v.ID, tagKey, values, b)
		}
	}

	return true
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
	case "volume-id":
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
	case "attachment.instance-id":
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
	case "fingerprint":
		return anyEqual(kp.Fingerprint, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch("keypair-"+kp.Name, tagKey, values, b)
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
	case "volume-id":
		return anyEqual(s.VolumeID, values)
	case filterKeyStatus:
		return anyEqual(s.State, values)
	case "encrypted":
		want := anyEqual("true", values)

		return s.Encrypted == want
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(s.SnapshotID, tagKey, values, b)
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
	case "attachment.instance-id":
		return anyEqual(eni.InstanceID, values)
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
	case "image-id":
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
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			return tagMatch(a.ImageID, tagKey, values, b)
		}
	}

	return true
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

// parseRunInstancesCounts validates and returns MinCount and MaxCount from RunInstances params.
// MinCount defaults to 1 when absent. MaxCount defaults to MinCount when absent.
func parseRunInstancesCounts(vals url.Values) (int, int, error) {
	minCnt := 1
	if v := vals.Get("MinCount"); v != "" {
		if _, scanErr := fmt.Sscan(v, &minCnt); scanErr != nil || minCnt < 1 {
			return 0, 0, fmt.Errorf("%w: MinCount must be a positive integer", ErrInvalidParameter)
		}
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

	return minCnt, maxCnt, nil
}

// validateSecurityGroupIDs parses SecurityGroupId.N from vals and verifies each exists.
// Returns an error if any ID is not found.

// validateSecurityGroupIDs parses SecurityGroupId.N from vals and verifies each exists.
// Returns an error if any ID is not found.
func (h *Handler) validateSecurityGroupIDs(vals url.Values) ([]string, error) {
	sgIDs := parseMemberList(vals, "SecurityGroupId")
	if len(sgIDs) == 0 {
		return nil, nil
	}

	existing := h.Backend.DescribeSecurityGroups(sgIDs)
	if len(existing) != len(sgIDs) {
		return nil, fmt.Errorf("%w: one or more SecurityGroupId values not found", ErrSecurityGroupNotFound)
	}

	return sgIDs, nil
}

// parseEC2Filters parses Filter.N.Name / Filter.N.Value.M from EC2 form values.
// Returns a map of filter name → list of accepted values (OR semantics per AWS).

// parseEC2Filters parses Filter.N.Name / Filter.N.Value.M from EC2 form values.
// Returns a map of filter name → list of accepted values (OR semantics per AWS).
func parseEC2Filters(vals url.Values) map[string][]string {
	filters := make(map[string][]string)

	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}

		var values []string
		for j := 1; ; j++ {
			v := vals.Get(fmt.Sprintf("Filter.%d.Value.%d", i, j))
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

// applyInstanceFilters returns only instances matching all filters (AND across filters,
// OR within each filter's values). Supports the following filter names:
//
//   - instance-state-name (already handled by backend but re-applied for multi-value)
//   - image-id
//   - vpc-id
//   - subnet-id
//   - instance-type
//   - key-name
//   - private-ip-address
//   - ip-address (public IP)
//   - tag:<key>

// applyInstanceFilters returns only instances matching all filters (AND across filters,
// OR within each filter's values). Supports the following filter names:
//
//   - instance-state-name (already handled by backend but re-applied for multi-value)
//   - image-id
//   - vpc-id
//   - subnet-id
//   - instance-type
//   - key-name
//   - private-ip-address
//   - ip-address (public IP)
//   - tag:<key>
func applyInstanceFilters(instances []*Instance, filters map[string][]string, b Backend) []*Instance {
	if len(filters) == 0 {
		return instances
	}

	out := instances[:0:0]

instanceLoop:
	for _, inst := range instances {
		for name, values := range filters {
			if !instanceMatchesFilter(inst, name, values, b) {
				continue instanceLoop
			}
		}

		out = append(out, inst)
	}

	return out
}

// instanceMatchesFilter returns true if the instance matches any value in the filter.

// instanceMatchesFilter returns true if the instance matches any value in the filter.
func instanceMatchesFilter(inst *Instance, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "instance-state-name":
		return anyEqual(inst.State.Name, values)
	case "image-id":
		return anyEqual(inst.ImageID, values)
	case filterKeyVPCID:
		return anyEqual(inst.VPCID, values)
	case filterKeySubnetID:
		return anyEqual(inst.SubnetID, values)
	case "instance-type":
		return anyEqual(inst.InstanceType, values)
	case "key-name":
		return anyEqual(inst.KeyName, values)
	case "private-ip-address":
		return anyEqual(inst.PrivateIP, values)
	case "ip-address":
		return anyEqual(inst.PublicIPAddress, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			tags := b.TagsForResource(inst.ID)
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
// Supported filter names: vpc-id, group-name, group-id.

// applySecurityGroupFilters filters security groups by named EC2 filter values.
// Supported filter names: vpc-id, group-name, group-id.
func applySecurityGroupFilters(groups []*SecurityGroup, filters map[string][]string) []*SecurityGroup {
	if len(filters) == 0 {
		return groups
	}

	out := groups[:0:0]

groupLoop:
	for _, sg := range groups {
		for name, values := range filters {
			if !sgMatchesFilter(sg, name, values) {
				continue groupLoop
			}
		}

		out = append(out, sg)
	}

	return out
}

// sgMatchesFilter returns true if the security group matches any value in the filter.

// sgMatchesFilter returns true if the security group matches any value in the filter.
func sgMatchesFilter(sg *SecurityGroup, filterName string, values []string) bool {
	switch filterName {
	case filterKeyVPCID:
		return anyEqual(sg.VPCID, values)
	case "group-name":
		return anyEqual(sg.Name, values)
	case "group-id":
		return anyEqual(sg.ID, values)
	}

	// Unknown filters: pass through (lenient).
	return true
}
