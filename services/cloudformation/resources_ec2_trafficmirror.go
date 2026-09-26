package cloudformation

import (
	"fmt"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	resTypeEC2TrafficMirrorFilter     = "AWS::EC2::TrafficMirrorFilter"
	resTypeEC2TrafficMirrorFilterRule = "AWS::EC2::TrafficMirrorFilterRule"
	resTypeEC2TrafficMirrorTarget     = "AWS::EC2::TrafficMirrorTarget"
	resTypeEC2TrafficMirrorSession    = "AWS::EC2::TrafficMirrorSession"
)

// createEC2TrafficMirrorResource handles the Traffic Mirror resource types
// listed above. Returns handled=false otherwise.
func (rc *ResourceCreator) createEC2TrafficMirrorResource(
	_, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2TrafficMirrorFilter:
		id, err := rc.createEC2TrafficMirrorFilter(props, params, physicalIDs)

		return id, true, err
	case resTypeEC2TrafficMirrorFilterRule:
		id, err := rc.createEC2TrafficMirrorFilterRule(props, params, physicalIDs)

		return id, true, err
	case resTypeEC2TrafficMirrorTarget:
		id, err := rc.createEC2TrafficMirrorTarget(props, params, physicalIDs)

		return id, true, err
	case resTypeEC2TrafficMirrorSession:
		id, err := rc.createEC2TrafficMirrorSession(props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteEC2TrafficMirrorResource handles deletion for the types created above.
func (rc *ResourceCreator) deleteEC2TrafficMirrorResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.EC2 == nil {
		switch resourceType {
		case resTypeEC2TrafficMirrorFilter, resTypeEC2TrafficMirrorFilterRule,
			resTypeEC2TrafficMirrorTarget, resTypeEC2TrafficMirrorSession:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeEC2TrafficMirrorFilter:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteTrafficMirrorFilter(physicalID), ec2backend.ErrTrafficMirrorFilterNotFound,
		)
	case resTypeEC2TrafficMirrorFilterRule:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteTrafficMirrorFilterRule(physicalID),
			ec2backend.ErrTrafficMirrorFilterRuleNotFound,
		)
	case resTypeEC2TrafficMirrorTarget:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteTrafficMirrorTarget(physicalID), ec2backend.ErrTrafficMirrorTargetNotFound,
		)
	case resTypeEC2TrafficMirrorSession:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteTrafficMirrorSession(physicalID), ec2backend.ErrTrafficMirrorSessionNotFound,
		)
	default:
		return false, nil
	}
}

// ---- AWS::EC2::TrafficMirrorFilter ----
// Ref returns the ID of the filter (documented, no Fn::GetAtt section).

func (rc *ResourceCreator) createEC2TrafficMirrorFilter(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "tmf-stub", nil
	}

	f, err := rc.backends.EC2.Backend.CreateTrafficMirrorFilter(
		strProp(props, "Description", params, physicalIDs), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create traffic mirror filter: %w", err)
	}

	return f.TrafficMirrorFilterID, nil
}

// ---- AWS::EC2::TrafficMirrorFilterRule ----
// Ref returns the ID of the filter rule (documented, Fn::GetAtt returns the
// same ID).

func (rc *ResourceCreator) createEC2TrafficMirrorFilterRule(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "tmfr-stub", nil
	}

	rule, err := rc.backends.EC2.Backend.CreateTrafficMirrorFilterRule(
		strProp(props, "TrafficMirrorFilterId", params, physicalIDs),
		strProp(props, "TrafficDirection", params, physicalIDs),
		strProp(props, "RuleAction", params, physicalIDs),
		strProp(props, "SourceCidrBlock", params, physicalIDs),
		strProp(props, "DestinationCidrBlock", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		intProp(props, "RuleNumber"),
		intProp(props, "Protocol"),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create traffic mirror filter rule: %w", err)
	}

	return rule.TrafficMirrorFilterRuleID, nil
}

// ---- AWS::EC2::TrafficMirrorTarget ----
// Ref returns the ID of the target (documented, no Fn::GetAtt section).

func (rc *ResourceCreator) createEC2TrafficMirrorTarget(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "tmt-stub", nil
	}

	t, err := rc.backends.EC2.Backend.CreateTrafficMirrorTarget(
		strProp(props, "NetworkInterfaceId", params, physicalIDs),
		strProp(props, "NetworkLoadBalancerArn", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
		strProp(props, "GatewayLoadBalancerEndpointId", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create traffic mirror target: %w", err)
	}

	return t.TrafficMirrorTargetID, nil
}

// ---- AWS::EC2::TrafficMirrorSession ----
// Ref returns the ID of the session (documented, no Fn::GetAtt section).

func (rc *ResourceCreator) createEC2TrafficMirrorSession(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "tms-stub", nil
	}

	s, err := rc.backends.EC2.Backend.CreateTrafficMirrorSession(
		strProp(props, "NetworkInterfaceId", params, physicalIDs),
		strProp(props, "TrafficMirrorTargetId", params, physicalIDs),
		strProp(props, "TrafficMirrorFilterId", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		intProp(props, "SessionNumber"),
		tagListProp(props, params, physicalIDs),
		intProp(props, "PacketLength"),
	)
	if err != nil {
		return "", fmt.Errorf("create traffic mirror session: %w", err)
	}

	return s.TrafficMirrorSessionID, nil
}
