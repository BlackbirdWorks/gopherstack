package apigatewayv2

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
)

func applyDomainNameDefaults(
	in []DomainNameConfiguration,
	domain, region string,
) []DomainNameConfiguration {
	configs := make([]DomainNameConfiguration, len(in))
	copy(configs, in)

	for i := range configs {
		if configs[i].DomainNameStatus == "" {
			configs[i].DomainNameStatus = "AVAILABLE"
		}

		if configs[i].SecurityPolicy == "" {
			configs[i].SecurityPolicy = "TLS_1_2"
		}

		if configs[i].EndpointType == "" {
			configs[i].EndpointType = "REGIONAL"
		}

		if configs[i].APIGatewayDomainName == "" {
			configs[i].APIGatewayDomainName = domain + ".execute-api." + region + ".amazonaws.com"
		}

		if configs[i].HostedZoneID == "" {
			configs[i].HostedZoneID = "Z2FDTNDATAQYW2"
		}
	}

	return configs
}

// cloneMutualTLSAuthentication returns a deep copy of cfg, or nil if cfg is
// nil. TruststoreWarnings is never populated by the caller (it is an
// output-only field computed by API Gateway while validating the truststore);
// the emulator has no truststore to validate against S3, so it is left empty,
// matching the "no warnings" case real AWS returns for a well-formed request.
func cloneMutualTLSAuthentication(cfg *MutualTLSAuthentication) *MutualTLSAuthentication {
	if cfg == nil {
		return nil
	}

	cp := *cfg
	cp.TruststoreWarnings = nil

	return &cp
}

// validateRoutingMode returns ErrBadRequest if routingMode is not one of the
// modeled enum values (API_MAPPING_ONLY, ROUTING_RULE_ONLY,
// ROUTING_RULE_THEN_API_MAPPING).
func validateRoutingMode(routingMode string) error {
	switch routingMode {
	case routingModeAPIMappingOnly, routingModeRoutingRuleOnly, routingModeRoutingRuleThenAPIMapping:
		return nil
	default:
		return fmt.Errorf(
			"%w: routingMode must be one of API_MAPPING_ONLY, ROUTING_RULE_ONLY, ROUTING_RULE_THEN_API_MAPPING",
			ErrBadRequest,
		)
	}
}

// CreateDomainName creates a new custom domain name.
func (b *InMemoryBackend) CreateDomainName(
	ctx context.Context,
	input CreateDomainNameInput,
) (*DomainName, error) {
	if input.DomainNameValue == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrBadRequest)
	}

	// Apply AWS-realistic default RoutingMode ("API_MAPPING_ONLY") when not
	// provided.
	routingMode := input.RoutingMode
	if routingMode == "" {
		routingMode = routingModeAPIMappingOnly
	} else if err := validateRoutingMode(routingMode); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if b.domainNames.Has(input.DomainNameValue) {
		return nil, fmt.Errorf("%w: domain name %q already exists", ErrAlreadyExists, input.DomainNameValue)
	}

	domainNameConfigs := []DomainNameConfiguration{}
	if len(input.DomainNameConfigurations) > 0 {
		domainNameConfigs = applyDomainNameDefaults(
			input.DomainNameConfigurations, input.DomainNameValue, regionFromCtx(ctx))
	}

	domainNameArn := "arn:aws:apigateway:" + regionFromCtx(ctx) + "::/domainnames/" + input.DomainNameValue

	dn := &DomainName{
		DomainNameValue:          input.DomainNameValue,
		DomainNameArn:            domainNameArn,
		RoutingMode:              routingMode,
		Tags:                     copyTags(input.Tags),
		DomainNameConfigurations: domainNameConfigs,
		MutualTLSAuthentication:  cloneMutualTLSAuthentication(input.MutualTLSAuthentication),
	}

	b.domainNames.Put(dn)

	cp := *dn

	return &cp, nil
}

// CreateRoutingRule creates a routing rule under a domain name.
func (b *InMemoryBackend) CreateRoutingRule(
	ctx context.Context,
	domainName string,
	input CreateRoutingRuleInput,
) (*RoutingRule, error) {
	b.mu.Lock("CreateRoutingRule")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	id := randomID()
	rule := &RoutingRule{
		RoutingRuleID: id,
		RoutingRuleARN: "arn:aws:apigateway:" + regionFromCtx(ctx) +
			"::/domainnames/" + domainName + "/routingrules/" + id,
		DomainName: domainName,
		Priority:   input.Priority,
		Actions:    input.Actions,
		Conditions: input.Conditions,
	}
	b.routingRules.Put(rule)

	cp := *rule

	return &cp, nil
}

// GetRoutingRule retrieves a routing rule.
func (b *InMemoryBackend) GetRoutingRule(domainName, routingRuleID string) (*RoutingRule, error) {
	b.mu.RLock("GetRoutingRule")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	rule, ok := b.routingRules.Get(routingRuleKey(domainName, routingRuleID))
	if !ok {
		return nil, ErrRoutingRuleNotFound
	}

	cp := *rule

	return &cp, nil
}

// ListRoutingRules lists routing rules for a domain.
func (b *InMemoryBackend) ListRoutingRules(domainName string) ([]RoutingRule, error) {
	b.mu.RLock("ListRoutingRules")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	rules := b.routingRulesByDomain.Get(domainName)
	out := make([]RoutingRule, 0, len(rules))

	for _, rule := range rules {
		out = append(out, *rule)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RoutingRuleID < out[j].RoutingRuleID })

	return out, nil
}

// PutRoutingRule updates an existing routing rule.
func (b *InMemoryBackend) PutRoutingRule(
	domainName, routingRuleID string,
	input PutRoutingRuleInput,
) (*RoutingRule, error) {
	b.mu.Lock("PutRoutingRule")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	rule, ok := b.routingRules.Get(routingRuleKey(domainName, routingRuleID))
	if !ok {
		return nil, ErrRoutingRuleNotFound
	}
	rule.Priority = input.Priority
	rule.Actions = input.Actions
	rule.Conditions = input.Conditions

	cp := *rule

	return &cp, nil
}

// DeleteRoutingRule deletes a routing rule.
func (b *InMemoryBackend) DeleteRoutingRule(domainName, routingRuleID string) error {
	b.mu.Lock("DeleteRoutingRule")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return ErrDomainNameNotFound
	}

	if !b.routingRules.Delete(routingRuleKey(domainName, routingRuleID)) {
		return ErrRoutingRuleNotFound
	}

	return nil
}

// GetDomainName retrieves a domain name by name.
func (b *InMemoryBackend) GetDomainName(domainName string) (*DomainName, error) {
	b.mu.RLock("GetDomainName")
	defer b.mu.RUnlock()

	dn, ok := b.domainNames.Get(domainName)
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	cp := *dn

	return &cp, nil
}

// GetDomainNames retrieves all custom domain names.
func (b *InMemoryBackend) GetDomainNames() ([]DomainName, error) {
	b.mu.RLock("GetDomainNames")
	defer b.mu.RUnlock()

	all := b.domainNames.All()
	result := make([]DomainName, 0, len(all))

	for _, dn := range all {
		result = append(result, *dn)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DomainNameValue < result[j].DomainNameValue
	})

	return result, nil
}

// DeleteDomainName removes a custom domain name and all its API mappings.
func (b *InMemoryBackend) DeleteDomainName(domainName string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()

	if !b.domainNames.Delete(domainName) {
		return ErrDomainNameNotFound
	}

	for _, m := range slices.Clone(b.apiMappingsByDomain.Get(domainName)) {
		b.apiMappings.Delete(apiMappingKey(domainName, m.APIMappingID))
	}

	for _, r := range slices.Clone(b.routingRulesByDomain.Get(domainName)) {
		b.routingRules.Delete(routingRuleKey(domainName, r.RoutingRuleID))
	}

	return nil
}

// UpdateDomainName updates fields on an existing domain name.
func (b *InMemoryBackend) UpdateDomainName(domainName string, input UpdateDomainNameInput) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	dn, ok := b.domainNames.Get(domainName)
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	if input.Tags != nil {
		if dn.Tags == nil {
			dn.Tags = make(map[string]string)
		}
		maps.Copy(dn.Tags, input.Tags)
	}

	if len(input.DomainNameConfigurations) > 0 {
		configs := make([]DomainNameConfiguration, len(input.DomainNameConfigurations))
		copy(configs, input.DomainNameConfigurations)
		dn.DomainNameConfigurations = configs
	}

	if input.MutualTLSAuthentication != nil {
		dn.MutualTLSAuthentication = cloneMutualTLSAuthentication(input.MutualTLSAuthentication)
	}

	if input.RoutingMode != "" {
		if err := validateRoutingMode(input.RoutingMode); err != nil {
			return nil, err
		}

		dn.RoutingMode = input.RoutingMode
	}

	cp := *dn

	return &cp, nil
}
