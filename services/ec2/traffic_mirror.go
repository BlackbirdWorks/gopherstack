package ec2

import (
	"fmt"
	"hash/fnv"
	"slices"
	"sort"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) CreateTrafficMirrorFilter(
	description string, tags map[string]string,
) (*TrafficMirrorFilter, error) {
	b.mu.Lock("CreateTrafficMirrorFilter")
	defer b.mu.Unlock()

	id := "tmf-" + uuid.New().String()[:8]
	f := &TrafficMirrorFilter{
		TrafficMirrorFilterID: id,
		Description:           description,
	}
	b.trafficMirrorFilters.Put(f)
	b.setTagsLocked(id, tags)

	cp := *f

	return &cp, nil
}

func (b *InMemoryBackend) DeleteTrafficMirrorFilter(id string) error {
	b.mu.Lock("DeleteTrafficMirrorFilter")
	defer b.mu.Unlock()

	if _, ok := b.trafficMirrorFilters.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorFilterNotFound, id)
	}
	b.trafficMirrorFilters.Delete(id)
	delete(b.tags, id)

	return nil
}

func (b *InMemoryBackend) DescribeTrafficMirrorFilters(ids []string) []*TrafficMirrorFilter {
	b.mu.RLock("DescribeTrafficMirrorFilters")
	defer b.mu.RUnlock()

	var result []*TrafficMirrorFilter

	for _, f := range b.trafficMirrorFilters.All() {
		if len(ids) > 0 && !slices.Contains(ids, f.TrafficMirrorFilterID) {
			continue
		}

		cp := *f
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TrafficMirrorFilterID < result[j].TrafficMirrorFilterID
	})

	return result
}

func (b *InMemoryBackend) ModifyTrafficMirrorFilterNetworkServices(
	id string, add, remove []string,
) (*TrafficMirrorFilter, error) {
	b.mu.Lock("ModifyTrafficMirrorFilterNetworkServices")
	defer b.mu.Unlock()

	f, ok := b.trafficMirrorFilters.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTrafficMirrorFilterNotFound, id)
	}

	services := make(map[string]bool)
	for _, s := range f.NetworkServices {
		services[s] = true
	}

	for _, s := range add {
		services[s] = true
	}

	for _, s := range remove {
		delete(services, s)
	}

	f.NetworkServices = nil
	for s := range services {
		f.NetworkServices = append(f.NetworkServices, s)
	}

	sort.Strings(f.NetworkServices)

	cp := *f

	return &cp, nil
}

func (b *InMemoryBackend) CreateTrafficMirrorFilterRule(
	filterID, direction, action, srcCIDR, dstCIDR, description string,
	ruleNumber, protocol int,
	tags map[string]string,
	ports ...TrafficMirrorPortRangePair,
) (*TrafficMirrorFilterRule, error) {
	b.mu.Lock("CreateTrafficMirrorFilterRule")
	defer b.mu.Unlock()

	f, ok := b.trafficMirrorFilters.Get(filterID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTrafficMirrorFilterNotFound, filterID)
	}

	id := "tmfr-" + uuid.New().String()[:8]
	rule := &TrafficMirrorFilterRule{
		TrafficMirrorFilterRuleID: id,
		TrafficMirrorFilterID:     filterID,
		RuleNumber:                ruleNumber,
		RuleAction:                action,
		TrafficDirection:          direction,
		Protocol:                  protocol,
		SourceCidrBlock:           srcCIDR,
		DestinationCidrBlock:      dstCIDR,
		Description:               description,
	}

	if len(ports) > 0 {
		rule.SourcePortRange = ports[0].Source
		rule.DestinationPortRange = ports[0].Destination
	}

	if direction == "egress" {
		f.EgressFilterRules = append(f.EgressFilterRules, rule)
	} else {
		f.IngressFilterRules = append(f.IngressFilterRules, rule)
	}
	b.trafficMirrorFilterRules.Put(rule)
	b.setTagsLocked(id, tags)

	cp := *rule

	return &cp, nil
}

func (b *InMemoryBackend) DeleteTrafficMirrorFilterRule(id string) error {
	b.mu.Lock("DeleteTrafficMirrorFilterRule")
	defer b.mu.Unlock()

	rule, ok := b.trafficMirrorFilterRules.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorFilterRuleNotFound, id)
	}

	if f, found := b.trafficMirrorFilters.Get(rule.TrafficMirrorFilterID); found {
		f.IngressFilterRules = removeTrafficMirrorFilterRule(f.IngressFilterRules, id)
		f.EgressFilterRules = removeTrafficMirrorFilterRule(f.EgressFilterRules, id)
	}
	b.trafficMirrorFilterRules.Delete(id)
	delete(b.tags, id)

	return nil
}

func removeTrafficMirrorFilterRule(rules []*TrafficMirrorFilterRule, id string) []*TrafficMirrorFilterRule {
	out := rules[:0]
	for _, r := range rules {
		if r.TrafficMirrorFilterRuleID != id {
			out = append(out, r)
		}
	}

	return out
}

func (b *InMemoryBackend) DescribeTrafficMirrorFilterRules(filterID string) ([]*TrafficMirrorFilterRule, error) {
	b.mu.RLock("DescribeTrafficMirrorFilterRules")
	defer b.mu.RUnlock()

	f, ok := b.trafficMirrorFilters.Get(filterID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTrafficMirrorFilterNotFound, filterID)
	}

	var result []*TrafficMirrorFilterRule

	for _, r := range f.IngressFilterRules {
		cp := *r
		result = append(result, &cp)
	}

	for _, r := range f.EgressFilterRules {
		cp := *r
		result = append(result, &cp)
	}

	return result, nil
}

func (b *InMemoryBackend) ModifyTrafficMirrorFilterRule(
	id, action, description string,
) (*TrafficMirrorFilterRule, error) {
	b.mu.Lock("ModifyTrafficMirrorFilterRule")
	defer b.mu.Unlock()

	rule, ok := b.trafficMirrorFilterRules.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTrafficMirrorFilterRuleNotFound, id)
	}

	if action != "" {
		rule.RuleAction = action
	}

	if description != "" {
		rule.Description = description
	}

	cp := *rule

	return &cp, nil
}

func (b *InMemoryBackend) CreateTrafficMirrorSession(
	networkInterfaceID, targetID, filterID, description string,
	sessionNumber int,
	tags map[string]string,
	packetLength ...int,
) (*TrafficMirrorSession, error) {
	b.mu.Lock("CreateTrafficMirrorSession")
	defer b.mu.Unlock()

	pl := 0
	if len(packetLength) > 0 {
		pl = packetLength[0]
	}

	id := "tms-" + uuid.New().String()[:8]
	s := &TrafficMirrorSession{
		TrafficMirrorSessionID: id,
		NetworkInterfaceID:     networkInterfaceID,
		OwnerID:                b.AccountID,
		TrafficMirrorTargetID:  targetID,
		TrafficMirrorFilterID:  filterID,
		SessionNumber:          sessionNumber,
		Description:            description,
		PacketLength:           pl,
		VirtualNetworkID:       trafficMirrorSessionVNI(id),
	}
	b.trafficMirrorSessions.Put(s)
	b.setTagsLocked(id, tags)

	cp := *s

	return &cp, nil
}

// maxTrafficMirrorVNI is the highest valid VXLAN virtual network identifier.
const maxTrafficMirrorVNI = 16777215

// trafficMirrorSessionVNI derives a deterministic pseudo-random VXLAN
// virtual network identifier (1-maxTrafficMirrorVNI) from the session ID.
func trafficMirrorSessionVNI(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))

	return int(h.Sum32()%maxTrafficMirrorVNI) + 1
}

func (b *InMemoryBackend) DeleteTrafficMirrorSession(id string) error {
	b.mu.Lock("DeleteTrafficMirrorSession")
	defer b.mu.Unlock()

	if _, ok := b.trafficMirrorSessions.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorSessionNotFound, id)
	}
	b.trafficMirrorSessions.Delete(id)
	delete(b.tags, id)

	return nil
}

func (b *InMemoryBackend) DescribeTrafficMirrorSessions(ids []string) []*TrafficMirrorSession {
	b.mu.RLock("DescribeTrafficMirrorSessions")
	defer b.mu.RUnlock()

	var result []*TrafficMirrorSession

	for _, s := range b.trafficMirrorSessions.All() {
		if len(ids) > 0 && !slices.Contains(ids, s.TrafficMirrorSessionID) {
			continue
		}

		cp := *s
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TrafficMirrorSessionID < result[j].TrafficMirrorSessionID
	})

	return result
}

func (b *InMemoryBackend) ModifyTrafficMirrorSession(
	id, targetID, filterID, description string,
) (*TrafficMirrorSession, error) {
	b.mu.Lock("ModifyTrafficMirrorSession")
	defer b.mu.Unlock()

	s, ok := b.trafficMirrorSessions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTrafficMirrorSessionNotFound, id)
	}

	if targetID != "" {
		s.TrafficMirrorTargetID = targetID
	}

	if filterID != "" {
		s.TrafficMirrorFilterID = filterID
	}

	if description != "" {
		s.Description = description
	}

	cp := *s

	return &cp, nil
}

func (b *InMemoryBackend) CreateTrafficMirrorTarget(
	networkInterfaceID, networkLoadBalancerArn, description string,
	tags map[string]string,
	gatewayLoadBalancerEndpointID ...string,
) (*TrafficMirrorTarget, error) {
	b.mu.Lock("CreateTrafficMirrorTarget")
	defer b.mu.Unlock()

	glbEndpointID := ""
	if len(gatewayLoadBalancerEndpointID) > 0 {
		glbEndpointID = gatewayLoadBalancerEndpointID[0]
	}

	id := "tmt-" + uuid.New().String()[:8]
	t := &TrafficMirrorTarget{
		TrafficMirrorTargetID:         id,
		NetworkInterfaceID:            networkInterfaceID,
		NetworkLoadBalancerArn:        networkLoadBalancerArn,
		GatewayLoadBalancerEndpointID: glbEndpointID,
		OwnerID:                       b.AccountID,
		Type: trafficMirrorTargetType(
			networkInterfaceID,
			networkLoadBalancerArn,
			glbEndpointID,
		),
		Description: description,
	}
	b.trafficMirrorTargets.Put(t)
	b.setTagsLocked(id, tags)

	cp := *t

	return &cp, nil
}

// trafficMirrorTargetType derives the Traffic Mirror target type from
// whichever destination identifier was supplied.
func trafficMirrorTargetType(networkInterfaceID, networkLoadBalancerArn, gatewayLoadBalancerEndpointID string) string {
	switch {
	case networkInterfaceID != "":
		return resourceTypeENI
	case networkLoadBalancerArn != "":
		return "network-load-balancer"
	case gatewayLoadBalancerEndpointID != "":
		return "gateway-load-balancer-endpoint"
	default:
		return ""
	}
}

func (b *InMemoryBackend) DeleteTrafficMirrorTarget(id string) error {
	b.mu.Lock("DeleteTrafficMirrorTarget")
	defer b.mu.Unlock()

	if _, ok := b.trafficMirrorTargets.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorTargetNotFound, id)
	}
	b.trafficMirrorTargets.Delete(id)
	delete(b.tags, id)

	return nil
}

func (b *InMemoryBackend) DescribeTrafficMirrorTargets(ids []string) []*TrafficMirrorTarget {
	b.mu.RLock("DescribeTrafficMirrorTargets")
	defer b.mu.RUnlock()

	var result []*TrafficMirrorTarget

	for _, t := range b.trafficMirrorTargets.All() {
		if len(ids) > 0 && !slices.Contains(ids, t.TrafficMirrorTargetID) {
			continue
		}

		cp := *t
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TrafficMirrorTargetID < result[j].TrafficMirrorTargetID
	})

	return result
}

// ---- EC2 Fleet backend methods ----
