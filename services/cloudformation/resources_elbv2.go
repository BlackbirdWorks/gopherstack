package cloudformation

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

// ---- ELBv2 LoadBalancer ----

func (rc *ResourceCreator) createELBv2LoadBalancer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	lbType := strProp(props, "Type", params, physicalIDs)
	if lbType == "" {
		lbType = "application"
	}

	subnets, mappings := elbv2SubnetProps(props, params, physicalIDs)

	lb, err := rc.backends.ELBv2.Backend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
		Name:                         name,
		Type:                         lbType,
		Scheme:                       strProp(props, "Scheme", params, physicalIDs),
		IPAddressType:                strProp(props, "IpAddressType", params, physicalIDs),
		EnablePrefixForIpv6SourceNat: strProp(props, "EnablePrefixForIpv6SourceNat", params, physicalIDs),
		IPv4IPAMPoolID:               strProp(props, "Ipv4IpamPoolId", params, physicalIDs),
		Subnets:                      subnets,
		SubnetMappings:               mappings,
		SecurityGroups:               strSliceProp(props["SecurityGroups"], params, physicalIDs),
		Tags:                         elbv2TagKVs(tagListProp(props, params, physicalIDs)),
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 load balancer %s: %w", name, err)
	}

	if attrs := elbv2LBAttributes(props, params, physicalIDs); len(attrs) > 0 {
		if _, err = rc.backends.ELBv2.Backend.ModifyLoadBalancerAttributes(lb.LoadBalancerArn, attrs); err != nil {
			_ = rc.backends.ELBv2.Backend.DeleteLoadBalancer(lb.LoadBalancerArn)

			return "", fmt.Errorf("set ELBv2 load balancer %s attributes: %w", name, err)
		}
	}

	return lb.LoadBalancerArn, nil
}

// elbv2SubnetProps decodes Subnets and SubnetMappings as the CloudFormation reference documents them.
func elbv2SubnetProps(
	props map[string]any, params, physicalIDs map[string]string,
) ([]string, []elbv2backend.SubnetMapping) {
	subnets := strSliceProp(props["Subnets"], params, physicalIDs)

	raw, _ := props["SubnetMappings"].([]any)
	mappings := make([]elbv2backend.SubnetMapping, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		mappings = append(mappings, elbv2backend.SubnetMapping{
			SubnetID:            strProp(m, "SubnetId", params, physicalIDs),
			AllocationID:        strProp(m, "AllocationId", params, physicalIDs),
			PrivateIPv4Address:  strProp(m, "PrivateIPv4Address", params, physicalIDs),
			IPv6Address:         strProp(m, "IPv6Address", params, physicalIDs),
			SourceNatIpv6Prefix: strProp(m, "SourceNatIpv6Prefix", params, physicalIDs),
		})
	}

	return subnets, mappings
}

func elbv2LBAttributes(props map[string]any, params, physicalIDs map[string]string) map[string]string {
	raw, _ := props["LoadBalancerAttributes"].([]any)
	out := make(map[string]string, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		if k := strProp(m, "Key", params, physicalIDs); k != "" {
			out[k] = strProp(m, "Value", params, physicalIDs)
		}
	}

	return out
}

func elbv2TagKVs(m map[string]string) []tags.KV {
	out := make([]tags.KV, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, tags.KV{Key: k, Value: m[k]})
	}

	return out
}

// updateELBv2LoadBalancer applies the "No interruption" properties in place; Name, Type and Scheme
// force replacement (requiresRecreation) and never reach here.
func (rc *ResourceCreator) updateELBv2LoadBalancer(
	arn string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	be := rc.backends.ELBv2.Backend

	subnets, mappings := elbv2SubnetProps(props, params, physicalIDs)
	if len(mappings) == 0 {
		mappings = make([]elbv2backend.SubnetMapping, len(subnets))
		for i, id := range subnets {
			mappings[i] = elbv2backend.SubnetMapping{SubnetID: id}
		}
	}

	if len(mappings) > 0 {
		sourceNat := strProp(props, "EnablePrefixForIpv6SourceNat", params, physicalIDs)
		if _, err := be.SetSubnets(arn, mappings, sourceNat, ""); err != nil {
			return fmt.Errorf("update ELBv2 load balancer subnets: %w", err)
		}
	}

	if sgs, ok := props["SecurityGroups"]; ok {
		_, err := be.SetSecurityGroups(arn, strSliceProp(sgs, params, physicalIDs), "")
		if err != nil {
			return fmt.Errorf("update ELBv2 load balancer security groups: %w", err)
		}
	}

	if ipType := strProp(props, "IpAddressType", params, physicalIDs); ipType != "" {
		if _, err := be.SetIPAddressType(arn, ipType); err != nil {
			return fmt.Errorf("update ELBv2 load balancer IP address type: %w", err)
		}
	}

	if attrs := elbv2LBAttributes(props, params, physicalIDs); len(attrs) > 0 {
		if _, err := be.ModifyLoadBalancerAttributes(arn, attrs); err != nil {
			return fmt.Errorf("update ELBv2 load balancer attributes: %w", err)
		}
	}

	return rc.syncELBv2Tags(arn, tagListProp(props, params, physicalIDs))
}

func (rc *ResourceCreator) syncELBv2Tags(arn string, want map[string]string) error {
	be := rc.backends.ELBv2.Backend

	cur, err := be.DescribeTags([]string{arn})
	if err != nil {
		return fmt.Errorf("describe ELBv2 load balancer tags: %w", err)
	}

	var stale []string

	for _, kv := range cur[arn] {
		if _, keep := want[kv.Key]; !keep {
			stale = append(stale, kv.Key)
		}
	}

	if len(stale) > 0 {
		if err = be.RemoveTags([]string{arn}, stale); err != nil {
			return fmt.Errorf("remove ELBv2 load balancer tags: %w", err)
		}
	}

	if len(want) > 0 {
		if err = be.AddTags([]string{arn}, elbv2TagKVs(want)); err != nil {
			return fmt.Errorf("add ELBv2 load balancer tags: %w", err)
		}
	}

	return nil
}

func (rc *ResourceCreator) deleteELBv2LoadBalancer(arn string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteLoadBalancer(arn)
}

// ---- ELBv2 TargetGroup ----

func (rc *ResourceCreator) createELBv2TargetGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	tg, err := rc.backends.ELBv2.Backend.CreateTargetGroup(elbv2backend.CreateTargetGroupInput{
		Name:       name,
		Protocol:   strProp(props, "Protocol", params, physicalIDs),
		VpcID:      strProp(props, "VpcId", params, physicalIDs),
		TargetType: strProp(props, "TargetType", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 target group %s: %w", name, err)
	}

	return tg.TargetGroupArn, nil
}

func (rc *ResourceCreator) deleteELBv2TargetGroup(arn string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteTargetGroup(arn)
}

// ---- ELBv2 Listener ----

func (rc *ResourceCreator) createELBv2Listener(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	lbArn := strProp(props, "LoadBalancerArn", params, physicalIDs)
	protocol := strProp(props, "Protocol", params, physicalIDs)

	listener, err := rc.backends.ELBv2.Backend.CreateListener(elbv2backend.CreateListenerInput{
		LoadBalancerArn: lbArn,
		Protocol:        protocol,
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 listener on %s: %w", lbArn, err)
	}

	return listener.ListenerArn, nil
}

func (rc *ResourceCreator) deleteELBv2Listener(arn string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteListener(arn)
}

// ---- ELBv2 ListenerRule ----

// createELBv2SupplementalResource handles ELBv2 ListenerRule resource creation.
func (rc *ResourceCreator) createELBv2SupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != "AWS::ElasticLoadBalancingV2::ListenerRule" {
		return "", false, nil
	}
	id, err := rc.createELBv2ListenerRule(logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteELBv2SupplementalResource handles ELBv2 ListenerRule resource deletion.
func (rc *ResourceCreator) deleteELBv2SupplementalResource(
	resourceType, physicalID string,
) (bool, error) {
	if resourceType != "AWS::ElasticLoadBalancingV2::ListenerRule" {
		return false, nil
	}

	return true, rc.deleteELBv2ListenerRule(physicalID)
}

func (rc *ResourceCreator) createELBv2ListenerRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}
	listenerARN := strProp(props, "ListenerArn", params, physicalIDs)
	priority := strProp(props, "Priority", params, physicalIDs)
	actions := parseELBv2CFNActions(props, params, physicalIDs)
	conditions := parseELBv2CFNConditions(props, params, physicalIDs)

	rule, err := rc.backends.ELBv2.Backend.CreateRule(elbv2backend.CreateRuleInput{
		ListenerArn: listenerARN,
		Priority:    priority,
		Actions:     actions,
		Conditions:  conditions,
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 listener rule: %w", err)
	}

	return rule.RuleArn, nil
}

func (rc *ResourceCreator) deleteELBv2ListenerRule(physicalID string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteRule(physicalID)
}

func parseELBv2CFNActions(
	props map[string]any,
	params, physicalIDs map[string]string,
) []elbv2backend.Action {
	raw, _ := props["Actions"].([]any)
	out := make([]elbv2backend.Action, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, elbv2backend.Action{
			Type:           resolve(m["Type"], params, physicalIDs),
			TargetGroupArn: resolve(m["TargetGroupArn"], params, physicalIDs),
		})
	}

	return out
}

func parseELBv2CFNConditions(
	props map[string]any,
	params, physicalIDs map[string]string,
) []elbv2backend.Condition {
	raw, _ := props["Conditions"].([]any)
	out := make([]elbv2backend.Condition, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		c := elbv2backend.Condition{Field: resolve(m["Field"], params, physicalIDs)}
		if vals, ok2 := m["Values"].([]any); ok2 {
			for _, v := range vals {
				c.Values = append(c.Values, fmt.Sprintf("%v", v))
			}
		}
		out = append(out, c)
	}

	return out
}
