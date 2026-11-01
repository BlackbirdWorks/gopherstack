package elbv2

import (
	"cmp"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// validateLBType validates CreateLoadBalancerInput.Type, defaulting to
// "application" when omitted.
func validateLBType(t string) (string, error) {
	switch t {
	case "", lbTypeApplication:
		return lbTypeApplication, nil
	case lbTypeNetwork, lbTypeGateway:
		return t, nil
	default:
		return "", fmt.Errorf(
			"%w: invalid Type %q; must be application, network, or gateway", ErrInvalidParameter, t,
		)
	}
}

// validateOnOffFlag validates an on/off-enum request field, returning
// defaultVal when v is empty ("" means the caller omitted the field).
func validateOnOffFlag(fieldName, v, defaultVal string) (string, error) {
	switch v {
	case "":
		return defaultVal, nil
	case onOffValueOn, onOffValueOff:
		return v, nil
	default:
		return "", fmt.Errorf("%w: invalid %s %q; must be on or off", ErrInvalidParameter, fieldName, v)
	}
}

// validateLBName applies load-balancer-specific name rules on top of validateResourceName:
// underscores are not allowed, and the name must be at least 2 characters.
func validateLBName(name string) error {
	if err := validateResourceName(name, "load balancer"); err != nil {
		return err
	}

	if strings.HasPrefix(name, "internal-") {
		return fmt.Errorf("%w: load balancer name cannot begin with \"internal-\"", ErrInvalidParameter)
	}

	if len(name) < minLBNameLength {
		return fmt.Errorf(
			"%w: load balancer name must be at least 2 characters",
			ErrInvalidParameter,
		)
	}

	for _, c := range name {
		if c == '_' {
			return fmt.Errorf(
				"%w: load balancer name cannot contain underscores",
				ErrInvalidParameter,
			)
		}
	}

	return nil
}

// canonicalHostedZoneIDForLB returns the canonical hosted-zone ID for the given LB type and region.
// Returns a sensible default when the combination is not in the known map.
func canonicalHostedZoneIDForLB(lbType, region string) string {
	// ALB hosted zone IDs per region (partial list — falls back to us-east-1).
	albZones := map[string]string{
		"us-east-1":      "Z35SXDOTRQ7X7K",
		"us-east-2":      "Z3AADJGX6KTTL2",
		"us-west-1":      "Z368ELLRRE2KJ0",
		"us-west-2":      "Z1H1FL5HABSF5",
		"eu-west-1":      "Z32O12XQLNTSW2",
		"eu-west-2":      "ZHURV8PSTC4K8",
		"eu-central-1":   "Z215JYRZR1TBD5",
		"ap-southeast-1": "Z1LMS91P8CMLE5",
		"ap-southeast-2": "Z1GM3OXH4ZPM65",
		"ap-northeast-1": "Z14GRHDCWA56QT",
		"ap-south-1":     "ZP97RAFLXTNZK",
		"sa-east-1":      "ZTK26PT1VY4CU",
		"ca-central-1":   "ZQSVJUPU6J1EY",
	}
	// NLB hosted zone IDs per region.
	nlbZones := map[string]string{
		"us-east-1":      "Z26RNL4JYFTOTI",
		"us-east-2":      "ZLMOA37VPKANP",
		"us-west-1":      "Z24FKFUX50B4VW",
		"us-west-2":      "Z18D5FSROUN65G",
		"eu-west-1":      "Z2IFOLAFXWLO4F",
		"eu-west-2":      "ZD4D7Y8KGAS4G",
		"eu-central-1":   "Z3F0SRJ5LGBH90",
		"ap-southeast-1": "ZKVM6ETL9YU8P",
		"ap-southeast-2": "ZCT6FZBF4DROD",
		"ap-northeast-1": "Z31USIVHYNEOWT",
		"ap-south-1":     "ZVDDRBQ08TROA",
		"sa-east-1":      "ZTK26PT1VY4CU",
		"ca-central-1":   "Z2EPGBWURTVAEP",
	}

	switch lbType {
	case lbTypeNetwork:
		if id, ok := nlbZones[region]; ok {
			return id
		}

		return "Z26RNL4JYFTOTI"
	default:
		if id, ok := albZones[region]; ok {
			return id
		}

		return "Z35SXDOTRQ7X7K"
	}
}

// lbDNSName returns the DNS name for a load balancer following the real AWS format.
// ALB/GWLB: {name}-{id}.{region}.elb.amazonaws.com
// NLB:      {name}-{id}.elb.{region}.amazonaws.com.
func lbDNSName(name, lbType, region, id string) string {
	suffix := dnsSuffix(id)

	switch lbType {
	case lbTypeNetwork:
		return fmt.Sprintf("%s-%s.elb.%s.amazonaws.com", name, suffix, region)
	default:
		return fmt.Sprintf("%s-%s.%s.elb.amazonaws.com", name, suffix, region)
	}
}

func (b *InMemoryBackend) lbARN(name, lbType, id string) string {
	return arn.Build(
		"elasticloadbalancing",
		b.region,
		b.accountID,
		"loadbalancer/"+lbARNSegment(lbType)+"/"+name+"/"+id,
	)
}

func lbARNSegment(lbType string) string {
	switch lbType {
	case lbTypeNetwork:
		return "net"
	case lbTypeGateway:
		return "gwy"
	default:
		return "app"
	}
}

// subnetMappingsToAZs converts SubnetMapping slices into AvailabilityZone structs.
// When only plain subnet IDs are given (no rich mappings), each is wrapped in a SubnetMapping first.
// Zone names are synthesised from the region + index since we have no real VPC service.
func subnetMappingsToAZs(region string, mappings []SubnetMapping) []AvailabilityZone {
	azLetters := "abcdef"
	azs := make([]AvailabilityZone, 0, len(mappings))

	for i, m := range mappings {
		zoneName := region + string(azLetters[i%len(azLetters)])
		azs = append(azs, AvailabilityZone{ZoneName: zoneName, SubnetID: m.SubnetID})
	}

	return azs
}

// placeSubnets returns the AZs for mappings and the VPC they sit in. With an EC2 resolver that
// implements SubnetPlacementResolver the real subnet AZ and VPC are used; otherwise AZ names are
// synthesised and the VPC is empty.
func (b *InMemoryBackend) placeSubnets(mappings []SubnetMapping) ([]AvailabilityZone, string) {
	azs := subnetMappingsToAZs(b.region, mappings)

	if regional, ok := b.ec2Resolver.(RegionalSubnetPlacementResolver); ok {
		return b.placeSubnetsIn(regional, azs, mappings)
	}

	placer, ok := b.ec2Resolver.(SubnetPlacementResolver)
	if !ok {
		return azs, ""
	}

	vpcID := ""

	for i, m := range mappings {
		if az := placer.SubnetAZ(m.SubnetID); az != "" {
			azs[i].ZoneName = az
		}

		if vpcID == "" {
			vpcID = placer.SubnetVPC(m.SubnetID)
		}
	}

	return azs, vpcID
}

func (b *InMemoryBackend) placeSubnetsIn(
	r RegionalSubnetPlacementResolver, azs []AvailabilityZone, mappings []SubnetMapping,
) ([]AvailabilityZone, string) {
	vpcID := ""

	for i, m := range mappings {
		if az := r.SubnetAZIn(b.region, m.SubnetID); az != "" {
			azs[i].ZoneName = az
		}

		if vpcID == "" {
			vpcID = r.SubnetVPCIn(b.region, m.SubnetID)
		}
	}

	return azs, vpcID
}

// subnetsToMappings converts plain subnet ID strings into SubnetMapping values.
func subnetsToMappings(subnets []string) []SubnetMapping {
	out := make([]SubnetMapping, len(subnets))
	for i, s := range subnets {
		out[i] = SubnetMapping{SubnetID: s}
	}

	return out
}

// validateNetworkRefs checks CreateLoadBalancer's SecurityGroups and subnet mappings
// against the wired EC2Resolver. Callers must hold b.mu. A nil ec2Resolver (the
// default) accepts every security-group/subnet id unvalidated.
func (b *InMemoryBackend) validateNetworkRefs(sgs []string, mappings []SubnetMapping) error {
	if b.ec2Resolver == nil {
		return nil
	}

	for _, sg := range sgs {
		if !b.ec2Resolver.SecurityGroupExists(sg) {
			return fmt.Errorf("%w: %s", ErrInvalidSecurityGroup, sg)
		}
	}

	for _, m := range mappings {
		if !b.ec2Resolver.SubnetExists(m.SubnetID) {
			return fmt.Errorf("%w: %s", ErrSubnetNotFound, m.SubnetID)
		}
	}

	return nil
}

// existingLBLocked returns the load balancer already named input.Name when input repeats its
// settings, ErrLoadBalancerAlreadyExists when the settings differ, and nil, nil when the name is free.
func (b *InMemoryBackend) existingLBLocked(lbType string, input CreateLoadBalancerInput) (*LoadBalancer, error) {
	for _, lb := range b.loadBalancers.All() {
		if lb.LoadBalancerName != input.Name {
			continue
		}

		if !sameLBSettings(lb, lbType, input) {
			return nil, ErrLoadBalancerAlreadyExists
		}

		cp := *lb

		return &cp, nil
	}

	return nil, nil //nolint:nilnil // name is free
}

// sameLBSettings reports whether input describes lb exactly, in which case CreateLoadBalancer
// returns the existing load balancer instead of DuplicateLoadBalancerName.
func sameLBSettings(lb *LoadBalancer, lbType string, input CreateLoadBalancerInput) bool {
	mappings := input.SubnetMappings
	if len(mappings) == 0 {
		mappings = subnetsToMappings(input.Subnets)
	}

	subnets := make([]string, 0, len(mappings))
	for _, m := range mappings {
		subnets = append(subnets, m.SubnetID)
	}

	existing := make([]string, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		existing = append(existing, az.SubnetID)
	}

	scheme := cmp.Or(input.Scheme, "internet-facing")
	ipType := cmp.Or(input.IPAddressType, ipAddressTypeIPv4)

	return lb.Type == lbType && lb.Scheme == scheme && lb.IPAddressType == ipType &&
		sameStringSet(subnets, existing) && sameStringSet(input.SecurityGroups, lb.SecurityGroups)
}

func sameStringSet(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)

	return slices.Equal(slices.Compact(x), slices.Compact(y))
}

// defaultLBVpcID is the VPC reported when subnet placement cannot be resolved.
const defaultLBVpcID = "vpc-00000000"

// CreateLoadBalancer creates a new load balancer.
func (b *InMemoryBackend) CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error) {
	b.mu.Lock("CreateLoadBalancer")
	defer b.mu.Unlock()

	lbType, err := validateCreateLBInput(input)
	if err != nil {
		return nil, err
	}

	if existing, dupErr := b.existingLBLocked(lbType, input); existing != nil || dupErr != nil {
		return existing, dupErr
	}

	lbID, err := b.resolveLBID(input.PinnedID)
	if err != nil {
		return nil, err
	}

	lbArn := b.lbARN(input.Name, lbType, lbID)

	scheme := input.Scheme
	if scheme == "" {
		scheme = "internet-facing"
	}

	ipType := input.IPAddressType
	if ipType == "" {
		ipType = ipAddressTypeIPv4
	}

	sourceNat, sourceNatErr := validateOnOffFlag(
		"EnablePrefixForIpv6SourceNat", input.EnablePrefixForIpv6SourceNat, onOffValueOff,
	)
	if sourceNatErr != nil {
		return nil, sourceNatErr
	}

	t := tags.New("elbv2.lb." + input.Name + ".tags")
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	var defaultAttrs map[string]string
	switch lbType {
	case lbTypeNetwork, lbTypeGateway:
		defaultAttrs = map[string]string{
			attrAccessLogsS3Enabled:           attrValueFalse,
			attrDeletionProtectionEnabled:     attrValueFalse,
			attrCrossZoneLoadBalancingEnabled: attrValueFalse,
		}
	default: // lbTypeApplication
		defaultAttrs = albDefaultAttributes()
	}

	mappings := input.SubnetMappings
	if len(mappings) == 0 {
		mappings = subnetsToMappings(input.Subnets)
	}

	if netErr := b.validateNetworkRefs(input.SecurityGroups, mappings); netErr != nil {
		return nil, netErr
	}

	azs, vpcID := b.placeSubnets(mappings)
	if vpcID == "" {
		vpcID = defaultLBVpcID
	}

	lb := &LoadBalancer{
		LoadBalancerArn:              lbArn,
		LoadBalancerName:             input.Name,
		DNSName:                      lbDNSName(input.Name, lbType, b.region, lbID),
		CanonicalHostedZoneID:        canonicalHostedZoneIDForLB(lbType, b.region),
		CreatedTime:                  time.Now().UTC(),
		Scheme:                       scheme,
		Type:                         lbType,
		IPAddressType:                ipType,
		EnablePrefixForIpv6SourceNat: sourceNat,
		IPv4IPAMPoolID:               input.IPv4IPAMPoolID,
		CustomerOwnedIPv4Pool:        input.CustomerOwnedIPv4Pool,
		VpcID:                        vpcID,
		AvailabilityZones:            azs,
		SecurityGroups:               input.SecurityGroups,
		State: LoadBalancerState{
			Code:        "active",
			Description: "",
		},
		Attributes: defaultAttrs,
		Tags:       t,
	}

	b.loadBalancers.Put(lb)

	cp := *lb

	return &cp, nil
}

// checkAllArnsFound returns ErrLoadBalancerNotFound if any of the queried ARNs are absent from result.
func checkAllArnsFound(arns []string, result []LoadBalancer) error {
	for _, a := range arns {
		found := false
		for _, lb := range result {
			if lb.LoadBalancerArn == a {
				found = true

				break
			}
		}

		if !found {
			return ErrLoadBalancerNotFound
		}
	}

	return nil
}

// checkAllLBNamesFound returns ErrLoadBalancerNotFound if any of the queried names are absent from result.
func checkAllLBNamesFound(names []string, result []LoadBalancer) error {
	for _, n := range names {
		found := false
		for _, lb := range result {
			if lb.LoadBalancerName == n {
				found = true

				break
			}
		}

		if !found {
			return ErrLoadBalancerNotFound
		}
	}

	return nil
}

// DescribeLoadBalancers returns load balancers filtered by ARNs and/or names.
// The returned LoadBalancer values contain a Tags pointer that is backend-owned; callers must treat it as read-only.
//
// Fast path: when only ARNs are supplied (no names), look them up directly in
// the ARN-keyed map instead of scanning every load balancer in the backend.
func (b *InMemoryBackend) DescribeLoadBalancers(
	arns []string,
	names []string,
) ([]LoadBalancer, error) {
	b.mu.RLock("DescribeLoadBalancers")
	defer b.mu.RUnlock()

	if len(arns) > 0 && len(names) == 0 {
		result := make([]LoadBalancer, 0, len(arns))

		for _, a := range arns {
			if lb, ok := b.loadBalancers.Get(a); ok {
				result = append(result, *lb)
			}
		}

		sortLoadBalancersByName(result)

		if err := checkAllArnsFound(arns, result); err != nil {
			return nil, err
		}

		return result, nil
	}

	result := b.filterLoadBalancersLocked(arns, names)
	sortLoadBalancersByName(result)

	if len(arns) > 0 {
		if err := checkAllArnsFound(arns, result); err != nil {
			return nil, err
		}
	}

	if len(names) > 0 {
		if err := checkAllLBNamesFound(names, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// filterLoadBalancersLocked scans all load balancers and returns those whose
// ARN is in arns (when non-empty) and whose name is in names (when non-empty).
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) filterLoadBalancersLocked(arns, names []string) []LoadBalancer {
	arnSet := make(map[string]bool, len(arns))
	for _, a := range arns {
		arnSet[a] = true
	}

	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	result := make([]LoadBalancer, 0, b.loadBalancers.Len())

	for _, lb := range b.loadBalancers.All() {
		if len(arns) > 0 && !arnSet[lb.LoadBalancerArn] {
			continue
		}

		if len(names) > 0 && !nameSet[lb.LoadBalancerName] {
			continue
		}

		result = append(result, *lb)
	}

	return result
}

// sortLoadBalancersByName sorts load balancers by name in ascending order.
func sortLoadBalancersByName(result []LoadBalancer) {
	sort.Slice(result, func(i, j int) bool {
		return result[i].LoadBalancerName < result[j].LoadBalancerName
	})
}

// deleteLoadBalancer deletes a load balancer by ARN.
func (b *InMemoryBackend) deleteLoadBalancer(lbArn string) error {
	b.mu.Lock("DeleteLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return ErrLoadBalancerNotFound
	}

	if lb.Attributes[attrDeletionProtectionEnabled] == attrValueTrue {
		return fmt.Errorf(
			"%w: load balancer cannot be deleted because deletion protection is enabled",
			ErrOperationNotPermitted,
		)
	}

	// Cascade: delete all listeners and their rules. The index lookups are
	// copied into fresh slices first because Table.Delete mutates the very
	// index groups Index.Get returns; iterating the live group while deleting
	// from it would corrupt the in-progress scan.
	listenersToDelete := append([]*Listener(nil), b.listenersByLB.Get(lbArn)...)
	for _, l := range listenersToDelete {
		rulesToDelete := append([]*Rule(nil), b.rulesByListener.Get(l.ListenerArn)...)
		for _, r := range rulesToDelete {
			r.Tags.Close()
			b.rules.Delete(r.RuleArn)
		}

		l.Tags.Close()
		b.listeners.Delete(l.ListenerArn)
	}

	lb.Tags.Close()
	delete(b.resourcePolicies, lbArn)
	b.loadBalancers.Delete(lbArn)

	return nil
}

// ModifyLoadBalancerAttributes updates attributes on a load balancer.
func (b *InMemoryBackend) ModifyLoadBalancerAttributes(
	lbArn string,
	attrs map[string]string,
) (*LoadBalancer, error) {
	b.mu.Lock("ModifyLoadBalancerAttributes")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if err := validateLBAttributeValues(attrs); err != nil {
		return nil, err
	}

	if lb.Attributes == nil {
		lb.Attributes = make(map[string]string)
	}

	maps.Copy(lb.Attributes, attrs)

	cp := *lb

	return &cp, nil
}

const (
	maxIdleTimeoutSeconds = 4000
	maxKeepAliveSeconds   = 604800
	minKeepAliveSeconds   = 60
)

// validateLBAttributeValues range-checks the typed attributes whose bounds are documented
// (LoadBalancerAttribute): boolean ".enabled" keys, idle timeout 1-4000s, keep-alive 60-604800s.
// Unknown keys are accepted unchanged.
func validateLBAttributeValues(attrs map[string]string) error {
	for k, v := range attrs {
		switch {
		case strings.HasSuffix(k, ".enabled"):
			if v != attrValueTrue && v != attrValueFalse {
				return fmt.Errorf("%w: '%s' is not a valid value for attribute '%s'", ErrInvalidParameter, v, k)
			}
		case k == "idle_timeout.timeout_seconds":
			if err := checkIntAttrRange(k, v, 1, maxIdleTimeoutSeconds); err != nil {
				return err
			}
		case k == "client_keep_alive.seconds":
			if err := checkIntAttrRange(k, v, minKeepAliveSeconds, maxKeepAliveSeconds); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkIntAttrRange(key, v string, minV, maxV int) error {
	n, err := strconv.Atoi(v)
	if err != nil || n < minV || n > maxV {
		return fmt.Errorf(
			"%w: attribute '%s' must be an integer between %d and %d",
			ErrInvalidParameter,
			key,
			minV,
			maxV,
		)
	}

	return nil
}

// SetSecurityGroups updates the security groups associated with a load balancer.
func (b *InMemoryBackend) SetSecurityGroups(
	lbArn string,
	sgs []string,
	enforceInboundRulesOnPrivateLink string,
) (*LoadBalancer, error) {
	b.mu.Lock("SetSecurityGroups")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if lb.Type != lbTypeApplication {
		return nil, fmt.Errorf(
			"%w: security groups cannot be associated with Network or Gateway Load Balancers",
			ErrInvalidConfigurationRequest,
		)
	}

	if netErr := b.validateNetworkRefs(sgs, nil); netErr != nil {
		return nil, netErr
	}

	// Real AWS default is "on" (SetSecurityGroupsInput.
	// EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic doc comment); an
	// omitted flag on a later call leaves the load balancer's current value
	// (if already set) unchanged rather than resetting it.
	keepOrDefault := lb.EnforceSGInboundRulesOnPrivateLink
	if keepOrDefault == "" {
		keepOrDefault = onOffValueOn
	}

	enforceVal, err := validateOnOffFlag(
		"EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic", enforceInboundRulesOnPrivateLink, keepOrDefault,
	)
	if err != nil {
		return nil, err
	}

	lb.EnforceSGInboundRulesOnPrivateLink = enforceVal

	lb.SecurityGroups = sgs
	cp := *lb

	return &cp, nil
}

// SetSubnets updates the availability zones / subnets associated with a load balancer.
func (b *InMemoryBackend) SetSubnets(
	lbArn string,
	mappings []SubnetMapping,
	enablePrefixForIpv6SourceNat, ipAddressType string,
) (*LoadBalancer, error) {
	b.mu.Lock("SetSubnets")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if netErr := b.validateNetworkRefs(nil, mappings); netErr != nil {
		return nil, netErr
	}

	// An omitted flag leaves the load balancer's current value unchanged.
	sourceNat, err := validateOnOffFlag(
		"EnablePrefixForIpv6SourceNat", enablePrefixForIpv6SourceNat, lb.EnablePrefixForIpv6SourceNat,
	)
	if err != nil {
		return nil, err
	}

	if ipAddressType != "" {
		if ipErr := validateLBIPAddressType(ipAddressType); ipErr != nil {
			return nil, ipErr
		}

		lb.IPAddressType = ipAddressType
	}

	lb.EnablePrefixForIpv6SourceNat = sourceNat

	azs, vpcID := b.placeSubnets(mappings)
	lb.AvailabilityZones = azs

	if vpcID != "" {
		lb.VpcID = vpcID
	}

	cp := *lb

	return &cp, nil
}

// SetIPAddressType updates the IP address type of a load balancer.
func (b *InMemoryBackend) SetIPAddressType(lbArn string, ipType string) (*LoadBalancer, error) {
	b.mu.Lock("SetIPAddressType")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if err := validateLBIPAddressType(ipType); err != nil {
		return nil, err
	}

	lb.IPAddressType = ipType
	cp := *lb

	return &cp, nil
}

func validateLBIPAddressType(ipType string) error {
	switch ipType {
	case ipAddressTypeIPv4, "dualstack", "dualstack-without-public-ipv4":
		return nil
	default:
		return fmt.Errorf(
			"%w: invalid IpAddressType %q; must be ipv4, dualstack, or dualstack-without-public-ipv4",
			ErrInvalidParameter,
			ipType,
		)
	}
}

// ModifyIPPools updates the IPAM pool configuration on a load balancer.
func (b *InMemoryBackend) ModifyIPPools(
	lbArn string, ipv4PoolID *string, removeIPv4 bool,
) (*LoadBalancer, error) {
	b.mu.Lock("ModifyIPPools")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if removeIPv4 {
		lb.IPv4IPAMPoolID = ""
	}

	if ipv4PoolID != nil {
		lb.IPv4IPAMPoolID = *ipv4PoolID
	}

	cp := *lb

	return &cp, nil
}

var lbIDPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// resolveLBID returns the pinned load balancer ARN suffix when set, else a random one. Callers hold b.mu.
func (b *InMemoryBackend) resolveLBID(pinned string) (string, error) {
	if pinned == "" {
		return newHexID(), nil
	}

	if err := tags.ValidatePinnedID(pinned, lbIDPattern, "16 lowercase hex digits"); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidParameter, err)
	}

	for _, existing := range b.loadBalancers.All() {
		if strings.HasSuffix(existing.LoadBalancerArn, "/"+pinned) {
			return "", fmt.Errorf("%w: load balancer ID %q already in use", ErrLoadBalancerAlreadyExists, pinned)
		}
	}

	return pinned, nil
}

func validateCreateLBInput(input CreateLoadBalancerInput) (string, error) {
	if input.Name == "" {
		return "", fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if err := validateLBName(input.Name); err != nil {
		return "", err
	}

	return validateLBType(input.Type)
}
