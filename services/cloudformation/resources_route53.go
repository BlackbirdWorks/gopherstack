package cloudformation

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"

	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
)

// ---- Route53 HealthCheck ----

func (rc *ResourceCreator) createRoute53HealthCheck(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53 == nil {
		return logicalID + "-stub", nil
	}

	callerRef := strProp(props, "CallerReference", params, physicalIDs)
	if callerRef == "" {
		callerRef = uuid.New().String()
	}

	hcType := route53backend.HealthCheckType(strProp(props, "Type", params, physicalIDs))
	if hcType == "" {
		hcType = route53backend.HealthCheckTypeHTTPS
	}

	cfg := route53backend.HealthCheckConfig{Type: hcType}
	applyHealthCheckConfigProps(props, params, physicalIDs, &cfg)

	hc, err := rc.backends.Route53.Backend.CreateHealthCheck(callerRef, cfg)
	if err != nil {
		return "", fmt.Errorf("create Route53 health check: %w", err)
	}

	return hc.ID, nil
}

// applyHealthCheckConfigProps fills in the HealthCheckConfig from the HealthCheckConfig property bag.
func applyHealthCheckConfigProps(
	props map[string]any,
	params, physicalIDs map[string]string,
	cfg *route53backend.HealthCheckConfig,
) {
	hcc, ok := props["HealthCheckConfig"].(map[string]any)
	if !ok {
		return
	}

	cfg.FullyQualifiedDomainName = resolve(hcc["FullyQualifiedDomainName"], params, physicalIDs)
	cfg.IPAddress = resolve(hcc["IPAddress"], params, physicalIDs)

	if portVal := resolve(hcc["Port"], params, physicalIDs); portVal != "" {
		if p, err := strconv.Atoi(portVal); err == nil {
			cfg.Port = p
		}
	}

	if resourcePath := resolve(hcc["ResourcePath"], params, physicalIDs); resourcePath != "" {
		cfg.ResourcePath = resourcePath
	}

	if hcTypeStr := resolve(hcc["Type"], params, physicalIDs); hcTypeStr != "" {
		cfg.Type = route53backend.HealthCheckType(hcTypeStr)
	}
}

func (rc *ResourceCreator) deleteRoute53HealthCheck(id string) error {
	if rc.backends.Route53 == nil {
		return nil
	}

	return rc.backends.Route53.Backend.DeleteHealthCheck(id)
}

// ---- Route53Resolver ----

func (rc *ResourceCreator) createRoute53ResolverEndpoint(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	direction := strProp(props, "Direction", params, physicalIDs)
	if direction == "" {
		direction = "INBOUND"
	}

	ep, err := rc.backends.Route53Resolver.Backend.CreateResolverEndpoint(
		ctx,
		name,
		direction,
		"",
		nil,
		nil,
		"",
		nil,
		"",
		"",
		"",
		false,
		false,
	)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver endpoint %s: %w", name, err)
	}

	return ep.ID, nil
}

func (rc *ResourceCreator) deleteRoute53ResolverEndpoint(ctx context.Context, id string) error {
	if rc.backends.Route53Resolver == nil {
		return nil
	}

	return rc.backends.Route53Resolver.Backend.DeleteResolverEndpoint(ctx, id)
}

func (rc *ResourceCreator) createRoute53ResolverRule(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	domainName := strProp(props, "DomainName", params, physicalIDs)
	ruleType := strProp(props, "RuleType", params, physicalIDs)
	if ruleType == "" {
		ruleType = "FORWARD"
	}

	endpointID := strProp(props, "ResolverEndpointId", params, physicalIDs)

	rule, err := rc.backends.Route53Resolver.Backend.CreateResolverRule(
		ctx,
		name,
		domainName,
		ruleType,
		endpointID,
		"",
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver rule %s: %w", name, err)
	}

	return rule.ID, nil
}

func (rc *ResourceCreator) deleteRoute53ResolverRule(ctx context.Context, id string) error {
	if rc.backends.Route53Resolver == nil {
		return nil
	}

	return rc.backends.Route53Resolver.Backend.DeleteResolverRule(ctx, id)
}
