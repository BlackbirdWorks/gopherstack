package app

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccKeyTags  = "Tags"
	ccKeyVpcID = "VpcId"
)

func ccEC2Tags(m map[string]string) []any {
	out := make([]any, 0, len(m))
	for k, v := range m {
		out = append(out, map[string]any{"Key": k, "Value": v})
	}

	return out
}

func ccEC2TagSpec(rt string, tags map[string]string) []any {
	if len(tags) == 0 {
		return nil
	}

	return []any{map[string]any{"ResourceType": rt, "Tags": ccEC2Tags(tags)}}
}

func ccEC2ModelTags(tags []map[string]any) []map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[ccMapStr(t, "Key")] = ccMapStr(t, "Value")
	}

	return tagsProperty(m)
}

func ccEC2SyncTags(ctx context.Context, c *ccEC2Client, id string, current, desired map[string]any) error {
	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, err := c.call(ctx, "CreateTags", map[string]any{"Resources": []any{id}, "Tags": ccEC2Tags(m)})

			return err
		},
		func(keys []string) error {
			tags := make([]any, 0, len(keys))
			for _, k := range keys {
				tags = append(tags, map[string]any{"Key": k})
			}

			_, err := c.call(ctx, "DeleteTags", map[string]any{"Resources": []any{id}, "Tags": tags})

			return err
		})
}

func ccBool(m map[string]any, key string) (bool, bool) {
	b, ok := m[key].(bool)

	return b, ok
}

// --- AWS::EC2::VPC ---

type ccVPC struct{ client *ccEC2Client }

func (h *ccVPC) Create(ctx context.Context, desired map[string]any) (string, error) {
	cidr := ccString(desired, "CidrBlock")
	if cidr == "" {
		return "", fmt.Errorf("%w: CidrBlock is required", cloudcontrolbackend.ErrValidation)
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := map[string]any{"CidrBlock": cidr}
	if ts := ccEC2TagSpec("vpc", tags); ts != nil {
		in["TagSpecifications"] = ts
	}

	ccSetStr(in, "InstanceTenancy", ccString(desired, "InstanceTenancy"))

	out, err := h.client.call(ctx, "CreateVpc", in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := ccMapStr(ccMapObj(out, "Vpc"), "VpcId")

	if err = h.setDNSAttrs(ctx, id, desired); err != nil {
		return "", err
	}

	return id, nil
}

func (h *ccVPC) setDNSAttrs(ctx context.Context, id string, desired map[string]any) error {
	for _, k := range []string{"EnableDnsSupport", "EnableDnsHostnames"} {
		v, ok := ccBool(desired, k)
		if !ok {
			continue
		}

		if _, err := h.client.call(ctx, "ModifyVpcAttribute", map[string]any{
			"VpcId": id, k: map[string]any{"Value": v},
		}); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccVPC) dnsAttr(ctx context.Context, id, attr string) (bool, bool) {
	out, err := h.client.call(ctx, "DescribeVpcAttribute", map[string]any{
		"VpcId": id, "Attribute": strings.ToLower(attr[:1]) + attr[1:],
	})
	if err != nil {
		return false, false
	}

	// The enum value is the output member name with a lowercase first letter.
	if v := ccMapObj(out, attr); v != nil {
		return ccMapBool(v, "Value"), true
	}

	return false, false
}

func (h *ccVPC) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.call(ctx, "DescribeVpcs", map[string]any{"VpcIds": []any{id}})
	if err != nil {
		return nil, ccMapError(err)
	}

	vpcs := ccMapList(out, "Vpcs")
	if len(vpcs) == 0 {
		return nil, fmt.Errorf("%w: vpc %s", cloudcontrolbackend.ErrNotFound, id)
	}

	v := vpcs[0]
	model := map[string]any{
		ccKeyVpcID: id, "CidrBlock": ccMapStr(v, "CidrBlock"), "InstanceTenancy": ccMapStr(v, "InstanceTenancy"),
	}

	for _, attr := range []string{"EnableDnsSupport", "EnableDnsHostnames"} {
		if val, ok := h.dnsAttr(ctx, id, attr); ok {
			model[attr] = val
		}
	}

	h.addDefaults(ctx, id, model)

	if ids := ccVPCAssociationIDs(v); len(ids) > 0 {
		model["CidrBlockAssociations"] = ids
	}

	if blocks := ccVPCIPv6Blocks(v); len(blocks) > 0 {
		model["Ipv6CidrBlocks"] = blocks
	}

	if tags := ccMapList(v, ccKeyTags); len(tags) > 0 {
		model[ccKeyTags] = ccEC2ModelTags(tags)
	}

	return model, nil
}

func ccVPCAssociationIDs(v map[string]any) []string {
	assocs := ccMapList(v, "CidrBlockAssociationSet")
	ids := make([]string, 0, len(assocs))

	for _, a := range assocs {
		ids = append(ids, ccMapStr(a, "AssociationId"))
	}

	return ids
}

func ccVPCIPv6Blocks(v map[string]any) []string {
	assocs := ccMapList(v, "Ipv6CidrBlockAssociationSet")
	blocks := make([]string, 0, len(assocs))

	for _, a := range assocs {
		blocks = append(blocks, ccMapStr(a, "Ipv6CidrBlock"))
	}

	return blocks
}

func (h *ccVPC) addDefaults(ctx context.Context, id string, model map[string]any) {
	vpcFilter := ccEC2Filter("vpc-id", id)

	if sgs, err := h.client.call(ctx, "DescribeSecurityGroups", map[string]any{
		"Filters": []any{vpcFilter, ccEC2Filter("group-name", "default")},
	}); err == nil {
		if l := ccMapList(sgs, "SecurityGroups"); len(l) > 0 {
			model["DefaultSecurityGroup"] = ccMapStr(l[0], "GroupId")
		}
	}

	if acls, err := h.client.call(ctx, "DescribeNetworkAcls", map[string]any{
		"Filters": []any{vpcFilter, ccEC2Filter("default", "true")},
	}); err == nil {
		if l := ccMapList(acls, "NetworkAcls"); len(l) > 0 {
			model["DefaultNetworkAcl"] = ccMapStr(l[0], "NetworkAclId")
		}
	}
}

func (h *ccVPC) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current,
		desired,
		ccKeyTags,
		"EnableDnsSupport",
		"EnableDnsHostnames",
	); err != nil {
		return err
	}

	if err := h.setDNSAttrs(
		ctx,
		id,
		ccChanged(current, desired, "EnableDnsSupport", "EnableDnsHostnames"),
	); err != nil {
		return err
	}

	return ccEC2SyncTags(ctx, h.client, id, current, desired)
}

// ccChanged returns the subset of desired for keys whose value differs from current.
func ccChanged(current, desired map[string]any, keys ...string) map[string]any {
	out := map[string]any{}

	for _, k := range keys {
		if v, ok := desired[k]; ok && !reflect.DeepEqual(current[k], v) {
			out[k] = v
		}
	}

	return out
}

func (h *ccVPC) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.call(ctx, "DeleteVpc", map[string]any{"VpcId": id})

	return ccMapError(err)
}

func (h *ccVPC) List(ctx context.Context) ([]string, error) {
	vpcs, err := h.client.describe(ctx, "DescribeVpcs", "Vpcs", nil)
	if err != nil {
		return nil, ccMapError(err)
	}

	var ids []string
	for _, v := range vpcs {
		ids = append(ids, ccMapStr(v, ccKeyVpcID))
	}

	return ids, nil
}

// --- AWS::EC2::Subnet ---

type ccSubnet struct{ client *ccEC2Client }

const (
	ccKeyDNS64    = "EnableDns64"
	ccKeyPublicIP = "MapPublicIpOnLaunch"
	ccKeyV6OnCr   = "AssignIpv6AddressOnCreation"
)

func ccSubnetAttrs() []string {
	return []string{ccKeyPublicIP, ccKeyDNS64, ccKeyV6OnCr}
}

func (h *ccSubnet) Create(ctx context.Context, desired map[string]any) (string, error) {
	vpc, cidr := ccString(desired, ccKeyVpcID), ccString(desired, "CidrBlock")
	native, _ := ccBool(desired, "Ipv6Native")

	if vpc == "" || (cidr == "" && !native) {
		return "", fmt.Errorf("%w: VpcId and CidrBlock are required", cloudcontrolbackend.ErrValidation)
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := map[string]any{ccKeyVpcID: vpc}
	ccSetStr(in, "CidrBlock", cidr)
	ccSetStr(in, "Ipv6CidrBlock", ccString(desired, "Ipv6CidrBlock"))
	ccSetStr(in, "AvailabilityZone", ccString(desired, "AvailabilityZone"))
	ccSetStr(in, "AvailabilityZoneId", ccString(desired, "AvailabilityZoneId"))
	ccSetStr(in, "OutpostArn", ccString(desired, "OutpostArn"))

	if ts := ccEC2TagSpec("subnet", tags); ts != nil {
		in["TagSpecifications"] = ts
	}

	if native {
		in["Ipv6Native"] = true
	}

	out, err := h.client.call(ctx, "CreateSubnet", in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := ccMapStr(ccMapObj(out, "Subnet"), "SubnetId")

	return id, h.setAttrs(ctx, id, desired)
}

func (h *ccSubnet) setAttrs(ctx context.Context, id string, desired map[string]any) error {
	for _, k := range ccSubnetAttrs() {
		v, ok := ccBool(desired, k)
		if !ok {
			continue
		}

		if _, err := h.client.call(ctx, "ModifySubnetAttribute", map[string]any{
			"SubnetId": id, k: map[string]any{"Value": v},
		}); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccSubnet) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.call(ctx, "DescribeSubnets", map[string]any{"SubnetIds": []any{id}})
	if err != nil {
		return nil, ccMapError(err)
	}

	subnets := ccMapList(out, "Subnets")
	if len(subnets) == 0 {
		return nil, fmt.Errorf("%w: subnet %s", cloudcontrolbackend.ErrNotFound, id)
	}

	s := subnets[0]
	model := map[string]any{
		"SubnetId": id, ccKeyVpcID: ccMapStr(s, ccKeyVpcID),
		"AvailabilityZone": ccMapStr(s, "AvailabilityZone"), ccKeyPublicIP: ccMapBool(s, ccKeyPublicIP),
	}

	if v := ccMapStr(s, "CidrBlock"); v != "" {
		model["CidrBlock"] = v
	}

	if _, present := s["AvailabilityZoneId"]; present {
		model["AvailabilityZoneId"] = ccMapStr(s, "AvailabilityZoneId")
	}

	if v := ccMapStr(s, "OutpostArn"); v != "" {
		model["OutpostArn"] = v
	}

	var v6 []string

	for _, a := range ccMapList(s, "Ipv6CidrBlockAssociationSet") {
		if ccMapStr(ccMapObj(a, "Ipv6CidrBlockState"), "State") == "associated" {
			v6 = append(v6, ccMapStr(a, "Ipv6CidrBlock"))
		}
	}

	if len(v6) > 0 {
		model["Ipv6CidrBlocks"] = v6
		model["Ipv6CidrBlock"] = v6[0]
	}

	for _, k := range []string{"Ipv6Native", ccKeyDNS64, ccKeyV6OnCr} {
		if ccMapBool(s, k) {
			model[k] = true
		}
	}

	if tags := ccMapList(s, ccKeyTags); len(tags) > 0 {
		model[ccKeyTags] = ccEC2ModelTags(tags)
	}

	return model, nil
}

func (h *ccSubnet) Update(ctx context.Context, id string, current, desired map[string]any) error {
	allowed := append([]string{ccKeyTags}, ccSubnetAttrs()...)
	if err := ccRejectUnsupportedChanges(current, desired, allowed...); err != nil {
		return err
	}

	if err := h.setAttrs(ctx, id, ccChanged(current, desired, ccSubnetAttrs()...)); err != nil {
		return err
	}

	return ccEC2SyncTags(ctx, h.client, id, current, desired)
}

func (h *ccSubnet) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.call(ctx, "DeleteSubnet", map[string]any{"SubnetId": id})

	return ccMapError(err)
}

func (h *ccSubnet) List(ctx context.Context) ([]string, error) {
	subnets, err := h.client.describe(ctx, "DescribeSubnets", "Subnets", nil)
	if err != nil {
		return nil, ccMapError(err)
	}

	var ids []string
	for _, s := range subnets {
		ids = append(ids, ccMapStr(s, "SubnetId"))
	}

	return ids, nil
}
