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
	ccKeyIngress = "SecurityGroupIngress"
	ccKeyEgress  = "SecurityGroupEgress"
	ccKeyGroupID = "GroupId"
)

// ccSGRule is one AWS::EC2::SecurityGroup Ingress/Egress rule property.
type ccSGRule struct {
	FromPort                   *int32 `json:"FromPort,omitempty"`
	ToPort                     *int32 `json:"ToPort,omitempty"`
	IPProtocol                 string `json:"IpProtocol"`
	CidrIP                     string `json:"CidrIp,omitempty"`
	CidrIpv6                   string `json:"CidrIpv6,omitempty"`
	Description                string `json:"Description,omitempty"`
	SourceSecurityGroupID      string `json:"SourceSecurityGroupId,omitempty"`
	DestinationSecurityGroupID string `json:"DestinationSecurityGroupId,omitempty"`
	SourcePrefixListID         string `json:"SourcePrefixListId,omitempty"`
	DestinationPrefixListID    string `json:"DestinationPrefixListId,omitempty"`
}

func (r ccSGRule) permission() ec2types.IpPermission {
	p := ec2types.IpPermission{IpProtocol: aws.String(r.IPProtocol), FromPort: r.FromPort, ToPort: r.ToPort}
	desc := aws.String(r.Description)

	if r.Description == "" {
		desc = nil
	}

	if r.CidrIP != "" {
		p.IpRanges = []ec2types.IpRange{{CidrIp: aws.String(r.CidrIP), Description: desc}}
	}

	if r.CidrIpv6 != "" {
		p.Ipv6Ranges = []ec2types.Ipv6Range{{CidrIpv6: aws.String(r.CidrIpv6), Description: desc}}
	}

	for _, g := range []string{r.SourceSecurityGroupID, r.DestinationSecurityGroupID} {
		if g != "" {
			p.UserIdGroupPairs = append(
				p.UserIdGroupPairs,
				ec2types.UserIdGroupPair{GroupId: aws.String(g), Description: desc},
			)
		}
	}

	for _, pl := range []string{r.SourcePrefixListID, r.DestinationPrefixListID} {
		if pl != "" {
			p.PrefixListIds = append(
				p.PrefixListIds,
				ec2types.PrefixListId{PrefixListId: aws.String(pl), Description: desc},
			)
		}
	}

	return p
}

func ccSGRules(m map[string]any, key string) ([]ccSGRule, error) {
	raw, ok := m[key]
	if !ok || raw == nil {
		return nil, nil
	}

	return ccDecode[[]ccSGRule](raw)
}

func ccSGPermissions(rules []ccSGRule) []ec2types.IpPermission {
	out := make([]ec2types.IpPermission, 0, len(rules))
	for _, r := range rules {
		out = append(out, r.permission())
	}

	return out
}

func ccSGModelRules(perms []ec2types.IpPermission, egress bool) []ccSGRule {
	var out []ccSGRule

	for _, p := range perms {
		base := ccSGRule{FromPort: p.FromPort, ToPort: p.ToPort, IPProtocol: aws.ToString(p.IpProtocol)}

		for _, r := range p.IpRanges {
			rule := base
			rule.CidrIP, rule.Description = aws.ToString(r.CidrIp), aws.ToString(r.Description)
			out = append(out, rule)
		}

		for _, r := range p.Ipv6Ranges {
			rule := base
			rule.CidrIpv6, rule.Description = aws.ToString(r.CidrIpv6), aws.ToString(r.Description)
			out = append(out, rule)
		}

		for _, g := range p.UserIdGroupPairs {
			rule := base
			rule.Description = aws.ToString(g.Description)

			if egress {
				rule.DestinationSecurityGroupID = aws.ToString(g.GroupId)
			} else {
				rule.SourceSecurityGroupID = aws.ToString(g.GroupId)
			}

			out = append(out, rule)
		}

		for _, pl := range p.PrefixListIds {
			rule := base
			rule.Description = aws.ToString(pl.Description)

			if egress {
				rule.DestinationPrefixListID = aws.ToString(pl.PrefixListId)
			} else {
				rule.SourcePrefixListID = aws.ToString(pl.PrefixListId)
			}

			out = append(out, rule)
		}
	}

	return out
}

// --- AWS::EC2::SecurityGroup ---

type ccSecurityGroup struct{ client *ec2.Client }

func (h *ccSecurityGroup) Create(ctx context.Context, desired map[string]any) (string, error) {
	desc := ccString(desired, "GroupDescription")
	if desc == "" {
		return "", fmt.Errorf("%w: GroupDescription is required", cloudcontrolbackend.ErrValidation)
	}

	name := ccString(desired, "GroupName")
	if name == "" {
		name = ccGeneratedName("cc-sg-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &ec2.CreateSecurityGroupInput{
		GroupName: aws.String(name), Description: aws.String(desc),
		TagSpecifications: ccEC2TagSpec(ec2types.ResourceTypeSecurityGroup, tags),
	}
	if vpc := ccString(desired, ccKeyVpcID); vpc != "" {
		in.VpcId = aws.String(vpc)
	}

	out, err := h.client.CreateSecurityGroup(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := aws.ToString(out.GroupId)

	if err = h.applyRules(ctx, id, nil, desired); err != nil {
		return "", err
	}

	return id, nil
}

func (h *ccSecurityGroup) applyRules(ctx context.Context, id string, current, desired map[string]any) error {
	if err := h.syncRules(ctx, id, current, desired, ccKeyIngress, false); err != nil {
		return err
	}

	return h.syncRules(ctx, id, current, desired, ccKeyEgress, true)
}

func (h *ccSecurityGroup) syncRules(
	ctx context.Context, id string, current, desired map[string]any, key string, egress bool,
) error {
	want, err := ccSGRules(desired, key)
	if err != nil {
		return err
	}

	have, err := ccSGRules(current, key)
	if err != nil {
		return err
	}

	if _, given := desired[key]; !given || reflect.DeepEqual(normalizeJSON(have), normalizeJSON(want)) {
		return nil
	}

	if len(have) > 0 {
		if err = h.revoke(ctx, id, ccSGPermissions(have), egress); err != nil {
			return err
		}
	}

	if len(want) == 0 {
		return nil
	}

	return h.authorize(ctx, id, ccSGPermissions(want), egress)
}

func (h *ccSecurityGroup) authorize(ctx context.Context, id string, perms []ec2types.IpPermission, egress bool) error {
	var err error

	if egress {
		_, err = h.client.AuthorizeSecurityGroupEgress(ctx, &ec2.AuthorizeSecurityGroupEgressInput{
			GroupId: aws.String(id), IpPermissions: perms,
		})
	} else {
		_, err = h.client.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
			GroupId: aws.String(id), IpPermissions: perms,
		})
	}

	return ccMapError(err)
}

func (h *ccSecurityGroup) revoke(ctx context.Context, id string, perms []ec2types.IpPermission, egress bool) error {
	var err error

	if egress {
		_, err = h.client.RevokeSecurityGroupEgress(ctx, &ec2.RevokeSecurityGroupEgressInput{
			GroupId: aws.String(id), IpPermissions: perms,
		})
	} else {
		_, err = h.client.RevokeSecurityGroupIngress(ctx, &ec2.RevokeSecurityGroupIngressInput{
			GroupId: aws.String(id), IpPermissions: perms,
		})
	}

	return ccMapError(err)
}

func (h *ccSecurityGroup) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{id}})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.SecurityGroups) == 0 {
		return nil, fmt.Errorf("%w: security group %s", cloudcontrolbackend.ErrNotFound, id)
	}

	g := out.SecurityGroups[0]
	model := map[string]any{
		"Id": id, ccKeyGroupID: id, "GroupName": aws.ToString(g.GroupName),
		"GroupDescription": aws.ToString(g.Description),
	}

	if g.VpcId != nil {
		model[ccKeyVpcID] = aws.ToString(g.VpcId)
	}

	if rules := ccSGModelRules(g.IpPermissions, false); len(rules) > 0 {
		model[ccKeyIngress] = rules
	}

	if rules := ccSGModelRules(g.IpPermissionsEgress, true); len(rules) > 0 {
		model[ccKeyEgress] = rules
	}

	if len(g.Tags) > 0 {
		model[ccKeyTags] = ccEC2ModelTags(g.Tags)
	}

	normalized, _ := normalizeJSON(model).(map[string]any)

	return normalized, nil
}

func (h *ccSecurityGroup) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, ccKeyTags, ccKeyIngress, ccKeyEgress); err != nil {
		return err
	}

	if err := h.applyRules(ctx, id, current, desired); err != nil {
		return err
	}

	return ccEC2SyncTags(ctx, h.client, id, current, desired)
}

func (h *ccSecurityGroup) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: aws.String(id)})

	return ccMapError(err)
}

func (h *ccSecurityGroup) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := ec2.NewDescribeSecurityGroupsPaginator(h.client, &ec2.DescribeSecurityGroupsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, g := range out.SecurityGroups {
			ids = append(ids, aws.ToString(g.GroupId))
		}
	}

	return ids, nil
}
