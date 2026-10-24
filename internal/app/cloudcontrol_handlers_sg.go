package app

import (
	"context"
	"fmt"
	"reflect"

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

func (r ccSGRule) permission() map[string]any {
	p := map[string]any{"IpProtocol": r.IPProtocol}
	if r.FromPort != nil {
		p["FromPort"] = float64(*r.FromPort)
	}

	if r.ToPort != nil {
		p["ToPort"] = float64(*r.ToPort)
	}

	withDesc := func(m map[string]any) map[string]any {
		ccSetStr(m, "Description", r.Description)

		return m
	}

	if r.CidrIP != "" {
		p["IpRanges"] = []any{withDesc(map[string]any{"CidrIp": r.CidrIP})}
	}

	if r.CidrIpv6 != "" {
		p["Ipv6Ranges"] = []any{withDesc(map[string]any{"CidrIpv6": r.CidrIpv6})}
	}

	var pairs, prefixes []any

	for _, g := range []string{r.SourceSecurityGroupID, r.DestinationSecurityGroupID} {
		if g != "" {
			pairs = append(pairs, withDesc(map[string]any{ccKeyGroupID: g}))
		}
	}

	for _, pl := range []string{r.SourcePrefixListID, r.DestinationPrefixListID} {
		if pl != "" {
			prefixes = append(prefixes, withDesc(map[string]any{"PrefixListId": pl}))
		}
	}

	if pairs != nil {
		p["UserIdGroupPairs"] = pairs
	}

	if prefixes != nil {
		p["PrefixListIds"] = prefixes
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

func ccSGPermissions(rules []ccSGRule) []any {
	out := make([]any, 0, len(rules))
	for _, r := range rules {
		out = append(out, r.permission())
	}

	return out
}

func ccPort(m map[string]any, key string) *int32 {
	f, ok := m[key].(float64)
	if !ok {
		return nil
	}

	v := int32(f)

	return &v
}

func ccSGModelRules(perms []map[string]any, egress bool) []ccSGRule {
	var out []ccSGRule

	for _, p := range perms {
		base := ccSGRule{FromPort: ccPort(p, "FromPort"), ToPort: ccPort(p, "ToPort"), IPProtocol: ccMapStr(p, "IpProtocol")}

		for _, r := range ccMapList(p, "IpRanges") {
			rule := base
			rule.CidrIP, rule.Description = ccMapStr(r, "CidrIp"), ccMapStr(r, "Description")
			out = append(out, rule)
		}

		for _, r := range ccMapList(p, "Ipv6Ranges") {
			rule := base
			rule.CidrIpv6, rule.Description = ccMapStr(r, "CidrIpv6"), ccMapStr(r, "Description")
			out = append(out, rule)
		}

		for _, g := range ccMapList(p, "UserIdGroupPairs") {
			rule := base
			rule.Description = ccMapStr(g, "Description")

			if egress {
				rule.DestinationSecurityGroupID = ccMapStr(g, ccKeyGroupID)
			} else {
				rule.SourceSecurityGroupID = ccMapStr(g, ccKeyGroupID)
			}

			out = append(out, rule)
		}

		for _, pl := range ccMapList(p, "PrefixListIds") {
			rule := base
			rule.Description = ccMapStr(pl, "Description")

			if egress {
				rule.DestinationPrefixListID = ccMapStr(pl, "PrefixListId")
			} else {
				rule.SourcePrefixListID = ccMapStr(pl, "PrefixListId")
			}

			out = append(out, rule)
		}
	}

	return out
}

// --- AWS::EC2::SecurityGroup ---

type ccSecurityGroup struct{ client *ccEC2Client }

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

	in := map[string]any{"GroupName": name, "Description": desc}
	if ts := ccEC2TagSpec("security-group", tags); ts != nil {
		in["TagSpecifications"] = ts
	}

	ccSetStr(in, ccKeyVpcID, ccString(desired, ccKeyVpcID))

	out, err := h.client.call(ctx, "CreateSecurityGroup", in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := ccMapStr(out, ccKeyGroupID)

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

func (h *ccSecurityGroup) authorize(ctx context.Context, id string, perms []any, egress bool) error {
	return h.permissions(ctx, "Authorize", id, perms, egress)
}

func (h *ccSecurityGroup) revoke(ctx context.Context, id string, perms []any, egress bool) error {
	return h.permissions(ctx, "Revoke", id, perms, egress)
}

func (h *ccSecurityGroup) permissions(ctx context.Context, verb, id string, perms []any, egress bool) error {
	op := verb + "SecurityGroupIngress"
	if egress {
		op = verb + "SecurityGroupEgress"
	}

	_, err := h.client.call(ctx, op, map[string]any{ccKeyGroupID: id, "IpPermissions": perms})

	return ccMapError(err)
}

func (h *ccSecurityGroup) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.call(ctx, "DescribeSecurityGroups", map[string]any{"GroupIds": []any{id}})
	if err != nil {
		return nil, ccMapError(err)
	}

	groups := ccMapList(out, "SecurityGroups")
	if len(groups) == 0 {
		return nil, fmt.Errorf("%w: security group %s", cloudcontrolbackend.ErrNotFound, id)
	}

	g := groups[0]
	model := map[string]any{
		"Id": id, ccKeyGroupID: id, "GroupName": ccMapStr(g, "GroupName"),
		"GroupDescription": ccMapStr(g, "Description"),
	}

	if _, present := g[ccKeyVpcID]; present {
		model[ccKeyVpcID] = ccMapStr(g, ccKeyVpcID)
	}

	if rules := ccSGModelRules(ccMapList(g, "IpPermissions"), false); len(rules) > 0 {
		model[ccKeyIngress] = rules
	}

	if rules := ccSGModelRules(ccMapList(g, "IpPermissionsEgress"), true); len(rules) > 0 {
		model[ccKeyEgress] = rules
	}

	if tags := ccMapList(g, ccKeyTags); len(tags) > 0 {
		model[ccKeyTags] = ccEC2ModelTags(tags)
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

	_, err := h.client.call(ctx, "DeleteSecurityGroup", map[string]any{ccKeyGroupID: id})

	return ccMapError(err)
}

func (h *ccSecurityGroup) List(ctx context.Context) ([]string, error) {
	groups, err := h.client.describe(ctx, "DescribeSecurityGroups", "SecurityGroups", nil)
	if err != nil {
		return nil, ccMapError(err)
	}

	var ids []string
	for _, g := range groups {
		ids = append(ids, ccMapStr(g, ccKeyGroupID))
	}

	return ids, nil
}
