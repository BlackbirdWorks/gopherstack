package cloudformation

import (
	"fmt"
	"strings"

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

	scheme := strProp(props, "Scheme", params, physicalIDs)

	lb, err := rc.backends.ELBv2.Backend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
		Name:   name,
		Type:   lbType,
		Scheme: scheme,
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 load balancer %s: %w", name, err)
	}

	return lb.LoadBalancerArn, nil
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
