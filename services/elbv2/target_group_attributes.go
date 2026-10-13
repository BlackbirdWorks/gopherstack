package elbv2

const (
	protoGeneve = "GENEVE"
	protoUDP    = "UDP"
	protoTCPUDP = "TCP_UDP"
	protoTCP    = "TCP"
	attrOff     = "off"
)

type tgFamily int

const (
	tgFamilyALB tgFamily = iota
	tgFamilyNLB
	tgFamilyGWLB
)

func targetGroupFamily(proto string) tgFamily {
	switch proto {
	case protoGeneve:
		return tgFamilyGWLB
	case protoTCP, protoTLS, protoUDP, protoTCPUDP:
		return tgFamilyNLB
	default:
		return tgFamilyALB
	}
}

// defaultTargetGroupAttributes returns the attribute keys and defaults
// documented on elbv2@v1.58.5 types.TargetGroupAttribute.Key for the target
// group's load balancer family and target type.
func defaultTargetGroupAttributes(proto, targetType string) map[string]string {
	family := targetGroupFamily(proto)
	attrs := map[string]string{"stickiness.enabled": attrValueFalse}

	if targetType != targetTypeLambda {
		attrs["deregistration_delay.timeout_seconds"] = "300"
	}

	switch family {
	case tgFamilyGWLB:
		attrs["stickiness.type"] = "source_ip_dest_ip"
		attrs["target_failover.on_deregistration"] = "no_rebalance"
		attrs["target_failover.on_unhealthy"] = "no_rebalance"

		return attrs
	case tgFamilyNLB:
		attrs["stickiness.type"] = "source_ip"
		addNLBTargetGroupAttributes(attrs, proto, targetType)
	case tgFamilyALB:
		attrs["stickiness.type"] = "lb_cookie"
		addALBTargetGroupAttributes(attrs, targetType)
	}

	attrs[attrCrossZoneLoadBalancingEnabled] = "use_load_balancer_configuration"
	attrs["target_group_health.dns_failover.minimum_healthy_targets.count"] = "1"
	attrs["target_group_health.dns_failover.minimum_healthy_targets.percentage"] = attrOff
	attrs["target_group_health.unhealthy_state_routing.minimum_healthy_targets.count"] = "1"
	attrs["target_group_health.unhealthy_state_routing.minimum_healthy_targets.percentage"] = attrOff

	return attrs
}

func addALBTargetGroupAttributes(attrs map[string]string, targetType string) {
	if targetType == targetTypeLambda {
		attrs["lambda.multi_value_headers.enabled"] = attrValueFalse

		return
	}

	attrs["load_balancing.algorithm.type"] = "round_robin"
	attrs["load_balancing.algorithm.anomaly_mitigation"] = attrOff
	attrs["slow_start.duration_seconds"] = "0"
	attrs["stickiness.app_cookie.cookie_name"] = ""
	attrs["stickiness.app_cookie.duration_seconds"] = "86400"
	attrs["stickiness.lb_cookie.duration_seconds"] = "86400"
}

func addNLBTargetGroupAttributes(attrs map[string]string, proto, targetType string) {
	udpLike := proto == protoUDP || proto == protoTCPUDP

	attrs["deregistration_delay.connection_termination.enabled"] = boolAttr(udpLike)
	attrs["preserve_client_ip.enabled"] = boolAttr(targetType != "ip" || (proto != protoTCP && proto != protoTLS))
	attrs["proxy_protocol_v2.enabled"] = attrValueFalse
	attrs["target_health_state.unhealthy.connection_termination.enabled"] = attrValueTrue
	attrs["target_health_state.unhealthy.draining_interval_seconds"] = "0"
}

func boolAttr(v bool) string {
	if v {
		return attrValueTrue
	}

	return attrValueFalse
}
