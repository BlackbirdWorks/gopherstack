package ec2

import (
	"fmt"
	"net"
	"slices"
	"strconv"
)

// Port and ICMP bounds per the EC2 API.
const (
	minPort     = 0
	maxPort     = 65535
	minICMPType = -1
	maxICMPType = 255
	maxProtoNum = 255

	protoAll = "all"
)

// validateSecurityGroupRules validates a batch of ingress/egress rules the way
// the EC2 API does at authorize time: protocol must be recognized, port ranges
// must be well-formed for port-based protocols, and any CIDR must parse. It also
// rejects a rule that duplicates one already present on the group
// (InvalidPermission.Duplicate). It does NOT emulate packet evaluation — this is
// the validation layer the audit cites, not a network-path simulation.
func validateSecurityGroupRules(existing, incoming []SecurityGroupRule) error {
	for i := range incoming {
		rule := incoming[i]
		if err := validateSecurityGroupRule(rule); err != nil {
			return err
		}

		if ruleExists(existing, rule) {
			return fmt.Errorf("%w: the specified rule already exists", ErrDuplicatePermission)
		}

		// A rule duplicated within the same request is also rejected by AWS.
		for j := range incoming[:i] {
			if incoming[j] == rule {
				return fmt.Errorf("%w: the specified rule already exists", ErrDuplicatePermission)
			}
		}
	}

	return nil
}

// validateSecurityGroupRule validates a single rule's protocol, ports and CIDR.
func validateSecurityGroupRule(rule SecurityGroupRule) error {
	portBased, protoErr := validateProtocol(rule.Protocol)
	if protoErr != nil {
		return protoErr
	}

	if portErr := validateRulePorts(rule, portBased); portErr != nil {
		return portErr
	}

	if rule.IPRange != "" {
		if _, _, cidrErr := net.ParseCIDR(rule.IPRange); cidrErr != nil {
			return fmt.Errorf("%w: %q is not a valid CIDR block", ErrInvalidParameter, rule.IPRange)
		}
	}

	return nil
}

// validateProtocol validates the protocol and reports whether it is port-based
// (tcp/udp). AWS accepts the names tcp/udp/icmp/icmpv6, the wildcard "-1"/"all",
// or a numeric IP protocol number (0-255). It returns an error for anything else.
func validateProtocol(proto string) (bool, error) {
	switch proto {
	case "tcp", "udp", "6", "17":
		return true, nil
	case "icmp", "icmpv6", "1", "58":
		return false, nil
	case "-1", protoAll, "":
		// Empty protocol is treated as "all" (AWS defaults a missing protocol
		// to -1); not port-based.
		return false, nil
	}

	// Numeric IP protocol number (e.g. "50" for ESP) is accepted.
	if n, convErr := strconv.Atoi(proto); convErr == nil && n >= 0 && n <= maxProtoNum {
		return false, nil
	}

	return false, fmt.Errorf("%w: invalid IP protocol %q", ErrInvalidParameter, proto)
}

// validateRulePorts validates the FromPort/ToPort fields for a rule.
func validateRulePorts(rule SecurityGroupRule, portBased bool) error {
	if portBased {
		if rule.FromPort < minPort || rule.FromPort > maxPort ||
			rule.ToPort < minPort || rule.ToPort > maxPort {
			return fmt.Errorf("%w: port must be between %d and %d", ErrInvalidParameter, minPort, maxPort)
		}

		if rule.FromPort > rule.ToPort {
			return fmt.Errorf("%w: FromPort (%d) must not exceed ToPort (%d)",
				ErrInvalidParameter, rule.FromPort, rule.ToPort)
		}

		return nil
	}

	// ICMP uses FromPort=type, ToPort=code; both -1..255.
	if rule.FromPort < minICMPType || rule.FromPort > maxICMPType ||
		rule.ToPort < minICMPType || rule.ToPort > maxICMPType {
		return fmt.Errorf("%w: ICMP type/code must be between %d and %d",
			ErrInvalidParameter, minICMPType, maxICMPType)
	}

	return nil
}

// ruleExists reports whether target is already present in rules.
func ruleExists(rules []SecurityGroupRule, target SecurityGroupRule) bool {
	return slices.Contains(rules, target)
}
