package main

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccKeyTags  = "Tags"
	ccKeyVpcID = "VpcId"
)

func ccEC2Tags(m map[string]string) []ec2types.Tag {
	out := make([]ec2types.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, ec2types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func ccEC2TagSpec(rt ec2types.ResourceType, tags map[string]string) []ec2types.TagSpecification {
	if len(tags) == 0 {
		return nil
	}

	return []ec2types.TagSpecification{{ResourceType: rt, Tags: ccEC2Tags(tags)}}
}

func ccEC2ModelTags(tags []ec2types.Tag) []map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}

	return tagsProperty(m)
}

func ccEC2SyncTags(ctx context.Context, c *ec2.Client, id string, current, desired map[string]any) error {
	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, err := c.CreateTags(ctx, &ec2.CreateTagsInput{Resources: []string{id}, Tags: ccEC2Tags(m)})

			return err
		},
		func(keys []string) error {
			tags := make([]ec2types.Tag, 0, len(keys))
			for _, k := range keys {
				tags = append(tags, ec2types.Tag{Key: aws.String(k)})
			}

			_, err := c.DeleteTags(ctx, &ec2.DeleteTagsInput{Resources: []string{id}, Tags: tags})

			return err
		})
}

func ccBool(m map[string]any, key string) (bool, bool) {
	b, ok := m[key].(bool)

	return b, ok
}

// --- AWS::EC2::VPC ---

type ccVPC struct{ client *ec2.Client }

func (h *ccVPC) Create(ctx context.Context, desired map[string]any) (string, error) {
	cidr := ccString(desired, "CidrBlock")
	if cidr == "" {
		return "", fmt.Errorf("%w: CidrBlock is required", cloudcontrolbackend.ErrValidation)
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &ec2.CreateVpcInput{
		CidrBlock:         aws.String(cidr),
		TagSpecifications: ccEC2TagSpec(ec2types.ResourceTypeVpc, tags),
	}
	if t := ccString(desired, "InstanceTenancy"); t != "" {
		in.InstanceTenancy = ec2types.Tenancy(t)
	}

	out, err := h.client.CreateVpc(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := aws.ToString(out.Vpc.VpcId)

	if err = h.setDNSAttrs(ctx, id, desired); err != nil {
		return "", err
	}

	return id, nil
}

func (h *ccVPC) setDNSAttrs(ctx context.Context, id string, desired map[string]any) error {
	if v, ok := ccBool(desired, "EnableDnsSupport"); ok {
		if _, err := h.client.ModifyVpcAttribute(ctx, &ec2.ModifyVpcAttributeInput{
			VpcId: aws.String(id), EnableDnsSupport: &ec2types.AttributeBooleanValue{Value: aws.Bool(v)},
		}); err != nil {
			return ccMapError(err)
		}
	}

	if v, ok := ccBool(desired, "EnableDnsHostnames"); ok {
		if _, err := h.client.ModifyVpcAttribute(ctx, &ec2.ModifyVpcAttributeInput{
			VpcId: aws.String(id), EnableDnsHostnames: &ec2types.AttributeBooleanValue{Value: aws.Bool(v)},
		}); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccVPC) dnsAttr(ctx context.Context, id string, attr ec2types.VpcAttributeName) (bool, bool) {
	out, err := h.client.DescribeVpcAttribute(ctx, &ec2.DescribeVpcAttributeInput{
		VpcId: aws.String(id), Attribute: attr,
	})
	if err != nil {
		return false, false
	}

	if attr == ec2types.VpcAttributeNameEnableDnsSupport && out.EnableDnsSupport != nil {
		return aws.ToBool(out.EnableDnsSupport.Value), true
	}

	if attr == ec2types.VpcAttributeNameEnableDnsHostnames && out.EnableDnsHostnames != nil {
		return aws.ToBool(out.EnableDnsHostnames.Value), true
	}

	return false, false
}

func (h *ccVPC) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{id}})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.Vpcs) == 0 {
		return nil, fmt.Errorf("%w: vpc %s", cloudcontrolbackend.ErrNotFound, id)
	}

	v := out.Vpcs[0]
	model := map[string]any{
		ccKeyVpcID: id, "CidrBlock": aws.ToString(v.CidrBlock), "InstanceTenancy": string(v.InstanceTenancy),
	}

	if val, ok := h.dnsAttr(ctx, id, ec2types.VpcAttributeNameEnableDnsSupport); ok {
		model["EnableDnsSupport"] = val
	}

	if val, ok := h.dnsAttr(ctx, id, ec2types.VpcAttributeNameEnableDnsHostnames); ok {
		model["EnableDnsHostnames"] = val
	}

	h.addDefaults(ctx, id, model)

	if ids := ccVPCAssociationIDs(v); len(ids) > 0 {
		model["CidrBlockAssociations"] = ids
	}

	if blocks := ccVPCIPv6Blocks(v); len(blocks) > 0 {
		model["Ipv6CidrBlocks"] = blocks
	}

	if len(v.Tags) > 0 {
		model[ccKeyTags] = ccEC2ModelTags(v.Tags)
	}

	return model, nil
}

func ccVPCAssociationIDs(v ec2types.Vpc) []string {
	ids := make([]string, 0, len(v.CidrBlockAssociationSet))

	for _, a := range v.CidrBlockAssociationSet {
		ids = append(ids, aws.ToString(a.AssociationId))
	}

	return ids
}

func ccVPCIPv6Blocks(v ec2types.Vpc) []string {
	blocks := make([]string, 0, len(v.Ipv6CidrBlockAssociationSet))

	for _, a := range v.Ipv6CidrBlockAssociationSet {
		blocks = append(blocks, aws.ToString(a.Ipv6CidrBlock))
	}

	return blocks
}

func (h *ccVPC) addDefaults(ctx context.Context, id string, model map[string]any) {
	filter := []ec2types.Filter{{Name: aws.String("vpc-id"), Values: []string{id}}}

	if sgs, err := h.client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: append(filter, ec2types.Filter{Name: aws.String("group-name"), Values: []string{"default"}}),
	}); err == nil && len(sgs.SecurityGroups) > 0 {
		model["DefaultSecurityGroup"] = aws.ToString(sgs.SecurityGroups[0].GroupId)
	}

	if acls, err := h.client.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{
		Filters: append(filter, ec2types.Filter{Name: aws.String("default"), Values: []string{"true"}}),
	}); err == nil && len(acls.NetworkAcls) > 0 {
		model["DefaultNetworkAcl"] = aws.ToString(acls.NetworkAcls[0].NetworkAclId)
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

	_, err := h.client.DeleteVpc(ctx, &ec2.DeleteVpcInput{VpcId: aws.String(id)})

	return ccMapError(err)
}

func (h *ccVPC) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := ec2.NewDescribeVpcsPaginator(h.client, &ec2.DescribeVpcsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, v := range out.Vpcs {
			ids = append(ids, aws.ToString(v.VpcId))
		}
	}

	return ids, nil
}

// --- AWS::EC2::Subnet ---

type ccSubnet struct{ client *ec2.Client }

const (
	ccKeyDNS64    = "EnableDns64"
	ccKeyPublicIP = "MapPublicIpOnLaunch"
)

func ccSubnetAttrs() []string {
	return []string{ccKeyPublicIP, ccKeyDNS64, "AssignIpv6AddressOnCreation"}
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

	in := &ec2.CreateSubnetInput{
		VpcId: aws.String(vpc), CidrBlock: ccOptional(cidr),
		Ipv6CidrBlock:      ccOptional(ccString(desired, "Ipv6CidrBlock")),
		AvailabilityZone:   ccOptional(ccString(desired, "AvailabilityZone")),
		AvailabilityZoneId: ccOptional(ccString(desired, "AvailabilityZoneId")),
		OutpostArn:         ccOptional(ccString(desired, "OutpostArn")),
		TagSpecifications:  ccEC2TagSpec(ec2types.ResourceTypeSubnet, tags),
	}
	if native {
		in.Ipv6Native = aws.Bool(true)
	}

	out, err := h.client.CreateSubnet(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := aws.ToString(out.Subnet.SubnetId)

	return id, h.setAttrs(ctx, id, desired)
}

func (h *ccSubnet) setAttrs(ctx context.Context, id string, desired map[string]any) error {
	for _, k := range ccSubnetAttrs() {
		v, ok := ccBool(desired, k)
		if !ok {
			continue
		}

		val := &ec2types.AttributeBooleanValue{Value: aws.Bool(v)}
		in := &ec2.ModifySubnetAttributeInput{SubnetId: aws.String(id)}

		switch k {
		case ccKeyPublicIP:
			in.MapPublicIpOnLaunch = val
		case ccKeyDNS64:
			in.EnableDns64 = val
		default:
			in.AssignIpv6AddressOnCreation = val
		}

		if _, err := h.client.ModifySubnetAttribute(ctx, in); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccSubnet) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: []string{id}})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.Subnets) == 0 {
		return nil, fmt.Errorf("%w: subnet %s", cloudcontrolbackend.ErrNotFound, id)
	}

	s := out.Subnets[0]
	model := map[string]any{
		"SubnetId": id, ccKeyVpcID: aws.ToString(s.VpcId),
		"AvailabilityZone": aws.ToString(s.AvailabilityZone), "MapPublicIpOnLaunch": aws.ToBool(s.MapPublicIpOnLaunch),
	}

	if s.CidrBlock != nil && *s.CidrBlock != "" {
		model["CidrBlock"] = aws.ToString(s.CidrBlock)
	}

	if s.AvailabilityZoneId != nil {
		model["AvailabilityZoneId"] = aws.ToString(s.AvailabilityZoneId)
	}

	if s.OutpostArn != nil && *s.OutpostArn != "" {
		model["OutpostArn"] = aws.ToString(s.OutpostArn)
	}

	var v6 []string

	for _, a := range s.Ipv6CidrBlockAssociationSet {
		if a.Ipv6CidrBlockState != nil && a.Ipv6CidrBlockState.State == ec2types.SubnetCidrBlockStateCodeAssociated {
			v6 = append(v6, aws.ToString(a.Ipv6CidrBlock))
		}
	}

	if len(v6) > 0 {
		model["Ipv6CidrBlocks"] = v6
		model["Ipv6CidrBlock"] = v6[0]
	}

	for k, on := range map[string]*bool{
		"Ipv6Native": s.Ipv6Native, ccKeyDNS64: s.EnableDns64,
		"AssignIpv6AddressOnCreation": s.AssignIpv6AddressOnCreation,
	} {
		if aws.ToBool(on) {
			model[k] = true
		}
	}

	if len(s.Tags) > 0 {
		model[ccKeyTags] = ccEC2ModelTags(s.Tags)
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

	_, err := h.client.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: aws.String(id)})

	return ccMapError(err)
}

func (h *ccSubnet) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := ec2.NewDescribeSubnetsPaginator(h.client, &ec2.DescribeSubnetsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, s := range out.Subnets {
			ids = append(ids, aws.ToString(s.SubnetId))
		}
	}

	return ids, nil
}
