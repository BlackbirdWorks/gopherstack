package ec2

import (
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

var (
	ErrTrafficMirrorFilterNotFound       = errors.New("InvalidTrafficMirrorFilterId.NotFound")
	ErrTrafficMirrorFilterRuleNotFound   = errors.New("InvalidTrafficMirrorFilterRuleId.NotFound")
	ErrTrafficMirrorSessionNotFound      = errors.New("InvalidTrafficMirrorSessionId.NotFound")
	ErrTrafficMirrorTargetNotFound       = errors.New("InvalidTrafficMirrorTargetId.NotFound")
	ErrFleetNotFound                     = errors.New("InvalidFleetId.NotFound")
	ErrNetworkInsightsPathNotFound       = errors.New("InvalidNetworkInsightsPathId.NotFound")
	ErrNetworkInsightsAnalysisNotFound   = errors.New("InvalidNetworkInsightsAnalysisId.NotFound")
	ErrNetworkInsightsAccessScopeNF      = errors.New("InvalidNetworkInsightsAccessScopeId.NotFound")
	ErrNetworkInsightsAccessScopeAnaNF   = errors.New("InvalidNetworkInsightsAccessScopeAnalysisId.NotFound")
	ErrCarrierGatewayNotFound           = errors.New("InvalidCarrierGatewayId.NotFound")
	ErrReservedInstancesListingNotFound = errors.New("InvalidReservedInstancesListingId.NotFound")
)

// ---- Traffic Mirror ----

// TrafficMirrorFilter holds a traffic mirror filter.
type TrafficMirrorFilter struct {
	TrafficMirrorFilterID string                    `json:"trafficMirrorFilterId"`
	Description           string                    `json:"description"`
	NetworkServices       []string                  `json:"networkServices"`
	IngressFilterRules    []*TrafficMirrorFilterRule `json:"ingressFilterRules"`
	EgressFilterRules     []*TrafficMirrorFilterRule `json:"egressFilterRules"`
}

// TrafficMirrorFilterRule holds a single traffic mirror filter rule.
type TrafficMirrorFilterRule struct {
	TrafficMirrorFilterRuleID string `json:"trafficMirrorFilterRuleId"`
	TrafficMirrorFilterID     string `json:"trafficMirrorFilterId"`
	RuleNumber                int    `json:"ruleNumber"`
	RuleAction                string `json:"ruleAction"`
	TrafficDirection          string `json:"trafficDirection"`
	Protocol                  int    `json:"protocol"`
	DestinationCidrBlock      string `json:"destinationCidrBlock"`
	SourceCidrBlock           string `json:"sourceCidrBlock"`
	Description               string `json:"description"`
}

// TrafficMirrorSession holds a traffic mirror session.
type TrafficMirrorSession struct {
	TrafficMirrorSessionID string `json:"trafficMirrorSessionId"`
	NetworkInterfaceID     string `json:"networkInterfaceId"`
	TrafficMirrorTargetID  string `json:"trafficMirrorTargetId"`
	TrafficMirrorFilterID  string `json:"trafficMirrorFilterId"`
	SessionNumber          int    `json:"sessionNumber"`
	Description            string `json:"description"`
}

// TrafficMirrorTarget holds a traffic mirror target.
type TrafficMirrorTarget struct {
	TrafficMirrorTargetID string `json:"trafficMirrorTargetId"`
	NetworkInterfaceID    string `json:"networkInterfaceId"`
	NetworkLoadBalancerArn string `json:"networkLoadBalancerArn"`
	Description           string `json:"description"`
}

// ---- EC2 Fleet ----

// Fleet holds an EC2 Fleet.
type Fleet struct {
	FleetID               string `json:"fleetId"`
	FleetState            string `json:"fleetState"`
	FleetType             string `json:"fleetType"`
	TargetCapacityUnitType string `json:"targetCapacityUnitType"`
	TotalTargetCapacity   int    `json:"totalTargetCapacity"`
	OnDemandTargetCapacity int   `json:"onDemandTargetCapacity"`
	SpotTargetCapacity    int    `json:"spotTargetCapacity"`
	ExcessCapacityTerminationPolicy string `json:"excessCapacityTerminationPolicy"`
	TerminateInstancesWithExpiration bool  `json:"terminateInstancesWithExpiration"`
}

// ---- Network Insights ----

// NetworkInsightsPath holds a network insights path.
type NetworkInsightsPath struct {
	NetworkInsightsPathID  string `json:"networkInsightsPathId"`
	NetworkInsightsPathArn string `json:"networkInsightsPathArn"`
	SourceID               string `json:"sourceId"`
	DestinationID          string `json:"destinationId"`
	Protocol               string `json:"protocol"`
	DestinationPort        int    `json:"destinationPort"`
}

// NetworkInsightsAnalysis holds a network insights analysis.
type NetworkInsightsAnalysis struct {
	NetworkInsightsAnalysisID  string `json:"networkInsightsAnalysisId"`
	NetworkInsightsPathID      string `json:"networkInsightsPathId"`
	Status                     string `json:"status"`
	NetworkPathFound           bool   `json:"networkPathFound"`
}

// NetworkInsightsAccessScope holds a network insights access scope.
type NetworkInsightsAccessScope struct {
	NetworkInsightsAccessScopeID  string `json:"networkInsightsAccessScopeId"`
	NetworkInsightsAccessScopeArn string `json:"networkInsightsAccessScopeArn"`
}

// NetworkInsightsAccessScopeAnalysis holds an access scope analysis.
type NetworkInsightsAccessScopeAnalysis struct {
	NetworkInsightsAccessScopeAnalysisID  string `json:"networkInsightsAccessScopeAnalysisId"`
	NetworkInsightsAccessScopeID          string `json:"networkInsightsAccessScopeId"`
	Status                                string `json:"status"`
	AnalyzedEniCount                      int    `json:"analyzedEniCount"`
}

// ---- Carrier Gateways ----

// CarrierGateway holds a carrier gateway.
type CarrierGateway struct {
	CarrierGatewayID string `json:"carrierGatewayId"`
	VpcID            string `json:"vpcId"`
	State            string `json:"state"`
	OwnerID          string `json:"ownerId"`
}

// ---- Reserved Instances ----

// ReservedInstance holds a reserved instance.
type ReservedInstance struct {
	ReservedInstancesID string `json:"reservedInstancesId"`
	InstanceType        string `json:"instanceType"`
	AvailabilityZone    string `json:"availabilityZone"`
	InstanceCount       int    `json:"instanceCount"`
	ProductDescription  string `json:"productDescription"`
	State               string `json:"state"`
	OfferingType        string `json:"offeringType"`
	Duration            int64  `json:"duration"`
	FixedPrice          float64 `json:"fixedPrice"`
	UsagePrice          float64 `json:"usagePrice"`
}

// ReservedInstancesOffering holds a reserved instances offering.
type ReservedInstancesOffering struct {
	ReservedInstancesOfferingID string  `json:"reservedInstancesOfferingId"`
	InstanceType                string  `json:"instanceType"`
	AvailabilityZone            string  `json:"availabilityZone"`
	ProductDescription          string  `json:"productDescription"`
	OfferingType                string  `json:"offeringType"`
	Duration                    int64   `json:"duration"`
	FixedPrice                  float64 `json:"fixedPrice"`
	UsagePrice                  float64 `json:"usagePrice"`
}

// ReservedInstancesListing holds a reserved instances listing.
type ReservedInstancesListing struct {
	ReservedInstancesListingID string `json:"reservedInstancesListingId"`
	ReservedInstancesID        string `json:"reservedInstancesId"`
	Status                     string `json:"status"`
	StatusMessage              string `json:"statusMessage"`
}

// ReservedInstancesModification holds a reserved instances modification.
type ReservedInstancesModification struct {
	ReservedInstancesModificationID string `json:"reservedInstancesModificationId"`
	Status                          string `json:"status"`
	StatusMessage                   string `json:"statusMessage"`
}

// ---- Traffic Mirror backend methods ----

func (b *InMemoryBackend) CreateTrafficMirrorFilter(description string) (*TrafficMirrorFilter, error) {
	b.mu.Lock("CreateTrafficMirrorFilter")
	defer b.mu.Unlock()

	id := "tmf-" + uuid.New().String()[:8]
	f := &TrafficMirrorFilter{
		TrafficMirrorFilterID: id,
		Description:           description,
	}
	b.trafficMirrorFilters[id] = f

	cp := *f

	return &cp, nil
}

func (b *InMemoryBackend) DeleteTrafficMirrorFilter(id string) error {
	b.mu.Lock("DeleteTrafficMirrorFilter")
	defer b.mu.Unlock()

	if _, ok := b.trafficMirrorFilters[id]; !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorFilterNotFound, id)
	}

	delete(b.trafficMirrorFilters, id)

	return nil
}

func (b *InMemoryBackend) DescribeTrafficMirrorFilters(ids []string) []*TrafficMirrorFilter {
	b.mu.RLock("DescribeTrafficMirrorFilters")
	defer b.mu.RUnlock()

	var result []*TrafficMirrorFilter

	for _, f := range b.trafficMirrorFilters {
		if len(ids) > 0 && !strInSlice(f.TrafficMirrorFilterID, ids) {
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

func (b *InMemoryBackend) ModifyTrafficMirrorFilterNetworkServices(id string, add, remove []string) error {
	b.mu.Lock("ModifyTrafficMirrorFilterNetworkServices")
	defer b.mu.Unlock()

	f, ok := b.trafficMirrorFilters[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorFilterNotFound, id)
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

	return nil
}

func (b *InMemoryBackend) CreateTrafficMirrorFilterRule(
	filterID, direction, action, srcCIDR, dstCIDR, description string,
	ruleNumber, protocol int,
) (*TrafficMirrorFilterRule, error) {
	b.mu.Lock("CreateTrafficMirrorFilterRule")
	defer b.mu.Unlock()

	f, ok := b.trafficMirrorFilters[filterID]
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

	if direction == "egress" {
		f.EgressFilterRules = append(f.EgressFilterRules, rule)
	} else {
		f.IngressFilterRules = append(f.IngressFilterRules, rule)
	}

	b.trafficMirrorFilterRules[id] = rule

	cp := *rule

	return &cp, nil
}

func (b *InMemoryBackend) DeleteTrafficMirrorFilterRule(id string) error {
	b.mu.Lock("DeleteTrafficMirrorFilterRule")
	defer b.mu.Unlock()

	rule, ok := b.trafficMirrorFilterRules[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorFilterRuleNotFound, id)
	}

	if f, ok := b.trafficMirrorFilters[rule.TrafficMirrorFilterID]; ok {
		f.IngressFilterRules = removeTrafficMirrorFilterRule(f.IngressFilterRules, id)
		f.EgressFilterRules = removeTrafficMirrorFilterRule(f.EgressFilterRules, id)
	}

	delete(b.trafficMirrorFilterRules, id)

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

	f, ok := b.trafficMirrorFilters[filterID]
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

func (b *InMemoryBackend) ModifyTrafficMirrorFilterRule(id, action, description string) error {
	b.mu.Lock("ModifyTrafficMirrorFilterRule")
	defer b.mu.Unlock()

	rule, ok := b.trafficMirrorFilterRules[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorFilterRuleNotFound, id)
	}

	if action != "" {
		rule.RuleAction = action
	}

	if description != "" {
		rule.Description = description
	}

	return nil
}

func (b *InMemoryBackend) CreateTrafficMirrorSession(
	networkInterfaceID, targetID, filterID, description string,
	sessionNumber int,
) (*TrafficMirrorSession, error) {
	b.mu.Lock("CreateTrafficMirrorSession")
	defer b.mu.Unlock()

	id := "tms-" + uuid.New().String()[:8]
	s := &TrafficMirrorSession{
		TrafficMirrorSessionID: id,
		NetworkInterfaceID:     networkInterfaceID,
		TrafficMirrorTargetID:  targetID,
		TrafficMirrorFilterID:  filterID,
		SessionNumber:          sessionNumber,
		Description:            description,
	}
	b.trafficMirrorSessions[id] = s

	cp := *s

	return &cp, nil
}

func (b *InMemoryBackend) DeleteTrafficMirrorSession(id string) error {
	b.mu.Lock("DeleteTrafficMirrorSession")
	defer b.mu.Unlock()

	if _, ok := b.trafficMirrorSessions[id]; !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorSessionNotFound, id)
	}

	delete(b.trafficMirrorSessions, id)

	return nil
}

func (b *InMemoryBackend) DescribeTrafficMirrorSessions(ids []string) []*TrafficMirrorSession {
	b.mu.RLock("DescribeTrafficMirrorSessions")
	defer b.mu.RUnlock()

	var result []*TrafficMirrorSession

	for _, s := range b.trafficMirrorSessions {
		if len(ids) > 0 && !strInSlice(s.TrafficMirrorSessionID, ids) {
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

func (b *InMemoryBackend) ModifyTrafficMirrorSession(id, targetID, filterID, description string) error {
	b.mu.Lock("ModifyTrafficMirrorSession")
	defer b.mu.Unlock()

	s, ok := b.trafficMirrorSessions[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorSessionNotFound, id)
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

	return nil
}

func (b *InMemoryBackend) CreateTrafficMirrorTarget(
	networkInterfaceID, networkLoadBalancerArn, description string,
) (*TrafficMirrorTarget, error) {
	b.mu.Lock("CreateTrafficMirrorTarget")
	defer b.mu.Unlock()

	id := "tmt-" + uuid.New().String()[:8]
	t := &TrafficMirrorTarget{
		TrafficMirrorTargetID:  id,
		NetworkInterfaceID:     networkInterfaceID,
		NetworkLoadBalancerArn: networkLoadBalancerArn,
		Description:            description,
	}
	b.trafficMirrorTargets[id] = t

	cp := *t

	return &cp, nil
}

func (b *InMemoryBackend) DeleteTrafficMirrorTarget(id string) error {
	b.mu.Lock("DeleteTrafficMirrorTarget")
	defer b.mu.Unlock()

	if _, ok := b.trafficMirrorTargets[id]; !ok {
		return fmt.Errorf("%w: %s", ErrTrafficMirrorTargetNotFound, id)
	}

	delete(b.trafficMirrorTargets, id)

	return nil
}

func (b *InMemoryBackend) DescribeTrafficMirrorTargets(ids []string) []*TrafficMirrorTarget {
	b.mu.RLock("DescribeTrafficMirrorTargets")
	defer b.mu.RUnlock()

	var result []*TrafficMirrorTarget

	for _, t := range b.trafficMirrorTargets {
		if len(ids) > 0 && !strInSlice(t.TrafficMirrorTargetID, ids) {
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

func (b *InMemoryBackend) CreateFleet(fleetType string, totalTargetCapacity int) (*Fleet, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if fleetType == "" {
		fleetType = "maintain"
	}

	id := "fleet-" + uuid.New().String()[:8]
	f := &Fleet{
		FleetID:             id,
		FleetState:          "active",
		FleetType:           fleetType,
		TotalTargetCapacity: totalTargetCapacity,
		ExcessCapacityTerminationPolicy: "termination",
	}
	b.fleets[id] = f

	cp := *f

	return &cp, nil
}

func (b *InMemoryBackend) DeleteFleets(ids []string) []string {
	b.mu.Lock("DeleteFleets")
	defer b.mu.Unlock()

	var deleted []string

	for _, id := range ids {
		if _, ok := b.fleets[id]; ok {
			b.fleets[id].FleetState = "deleted"
			delete(b.fleets, id)
			deleted = append(deleted, id)
		}
	}

	return deleted
}

func (b *InMemoryBackend) DescribeFleets(ids []string) []*Fleet {
	b.mu.RLock("DescribeFleets")
	defer b.mu.RUnlock()

	var result []*Fleet

	for _, f := range b.fleets {
		if len(ids) > 0 && !strInSlice(f.FleetID, ids) {
			continue
		}

		cp := *f
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].FleetID < result[j].FleetID
	})

	return result
}

func (b *InMemoryBackend) ModifyFleet(id string, totalTargetCapacity int, excessPolicy string) error {
	b.mu.Lock("ModifyFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrFleetNotFound, id)
	}

	if totalTargetCapacity > 0 {
		f.TotalTargetCapacity = totalTargetCapacity
	}

	if excessPolicy != "" {
		f.ExcessCapacityTerminationPolicy = excessPolicy
	}

	return nil
}

// ---- Network Insights backend methods ----

func (b *InMemoryBackend) CreateNetworkInsightsPath(
	sourceID, destinationID, protocol string,
	destinationPort int,
) (*NetworkInsightsPath, error) {
	b.mu.Lock("CreateNetworkInsightsPath")
	defer b.mu.Unlock()

	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceId is required", ErrInvalidParameter)
	}

	id := "nip-" + uuid.New().String()[:8]
	p := &NetworkInsightsPath{
		NetworkInsightsPathID:  id,
		NetworkInsightsPathArn: "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":network-insights-path/" + id,
		SourceID:               sourceID,
		DestinationID:          destinationID,
		Protocol:               protocol,
		DestinationPort:        destinationPort,
	}
	b.networkInsightsPaths[id] = p

	cp := *p

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsPath(id string) error {
	b.mu.Lock("DeleteNetworkInsightsPath")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsPaths[id]; !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsPathNotFound, id)
	}

	delete(b.networkInsightsPaths, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsPaths(ids []string) []*NetworkInsightsPath {
	b.mu.RLock("DescribeNetworkInsightsPaths")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsPath

	for _, p := range b.networkInsightsPaths {
		if len(ids) > 0 && !strInSlice(p.NetworkInsightsPathID, ids) {
			continue
		}

		cp := *p
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsPathID < result[j].NetworkInsightsPathID
	})

	return result
}

func (b *InMemoryBackend) StartNetworkInsightsAnalysis(pathID string) (*NetworkInsightsAnalysis, error) {
	b.mu.Lock("StartNetworkInsightsAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsPaths[pathID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInsightsPathNotFound, pathID)
	}

	id := "nia-" + uuid.New().String()[:8]
	a := &NetworkInsightsAnalysis{
		NetworkInsightsAnalysisID: id,
		NetworkInsightsPathID:     pathID,
		Status:                    "succeeded",
		NetworkPathFound:          true,
	}
	b.networkInsightsAnalyses[id] = a

	cp := *a

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAnalysis(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAnalyses[id]; !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAnalysisNotFound, id)
	}

	delete(b.networkInsightsAnalyses, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAnalyses(ids []string) []*NetworkInsightsAnalysis {
	b.mu.RLock("DescribeNetworkInsightsAnalyses")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAnalysis

	for _, a := range b.networkInsightsAnalyses {
		if len(ids) > 0 && !strInSlice(a.NetworkInsightsAnalysisID, ids) {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsAnalysisID < result[j].NetworkInsightsAnalysisID
	})

	return result
}

func (b *InMemoryBackend) CreateNetworkInsightsAccessScope() (*NetworkInsightsAccessScope, error) {
	b.mu.Lock("CreateNetworkInsightsAccessScope")
	defer b.mu.Unlock()

	id := "nias-" + uuid.New().String()[:8]
	s := &NetworkInsightsAccessScope{
		NetworkInsightsAccessScopeID:  id,
		NetworkInsightsAccessScopeArn: "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":network-insights-access-scope/" + id,
	}
	b.networkInsightsAccessScopes[id] = s

	cp := *s

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAccessScope(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAccessScope")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopes[id]; !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeNF, id)
	}

	delete(b.networkInsightsAccessScopes, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAccessScopes(ids []string) []*NetworkInsightsAccessScope {
	b.mu.RLock("DescribeNetworkInsightsAccessScopes")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAccessScope

	for _, s := range b.networkInsightsAccessScopes {
		if len(ids) > 0 && !strInSlice(s.NetworkInsightsAccessScopeID, ids) {
			continue
		}

		cp := *s
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsAccessScopeID < result[j].NetworkInsightsAccessScopeID
	})

	return result
}

func (b *InMemoryBackend) StartNetworkInsightsAccessScopeAnalysis(
	scopeID string,
) (*NetworkInsightsAccessScopeAnalysis, error) {
	b.mu.Lock("StartNetworkInsightsAccessScopeAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopes[scopeID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeNF, scopeID)
	}

	id := "niasa-" + uuid.New().String()[:8]
	a := &NetworkInsightsAccessScopeAnalysis{
		NetworkInsightsAccessScopeAnalysisID: id,
		NetworkInsightsAccessScopeID:         scopeID,
		Status:                               "succeeded",
		AnalyzedEniCount:                     0,
	}
	b.networkInsightsAccessScopeAnalyses[id] = a

	cp := *a

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAccessScopeAnalysis(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAccessScopeAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopeAnalyses[id]; !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeAnaNF, id)
	}

	delete(b.networkInsightsAccessScopeAnalyses, id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAccessScopeAnalyses(
	ids []string,
) []*NetworkInsightsAccessScopeAnalysis {
	b.mu.RLock("DescribeNetworkInsightsAccessScopeAnalyses")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAccessScopeAnalysis

	for _, a := range b.networkInsightsAccessScopeAnalyses {
		if len(ids) > 0 && !strInSlice(a.NetworkInsightsAccessScopeAnalysisID, ids) {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NetworkInsightsAccessScopeAnalysisID < result[j].NetworkInsightsAccessScopeAnalysisID
	})

	return result
}

// ---- BYOIP backend methods ----

func (b *InMemoryBackend) ProvisionByoipCidr(cidr, description string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("ProvisionByoipCidr")
	defer b.mu.Unlock()

	entry := &ByoipCidr{
		Cidr:          cidr,
		State:         "pending-provision",
		StatusMessage: description,
	}
	b.byoipCidrs[cidr] = entry

	cp := *entry

	return &cp, nil
}

func (b *InMemoryBackend) DeprovisionByoipCidr(cidr string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionByoipCidr")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs[cidr]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, cidr)
	}

	entry.State = "pending-deprovision"
	delete(b.byoipCidrs, cidr)

	cp := *entry

	return &cp, nil
}

func (b *InMemoryBackend) WithdrawByoipCidr(cidr string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("WithdrawByoipCidr")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs[cidr]
	if !ok {
		entry = &ByoipCidr{Cidr: cidr}
		b.byoipCidrs[cidr] = entry
	}

	entry.State = "advertised"

	cp := *entry

	return &cp, nil
}

// ---- Carrier Gateways backend methods ----

func (b *InMemoryBackend) CreateCarrierGateway(vpcID string) (*CarrierGateway, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCarrierGateway")
	defer b.mu.Unlock()

	id := "cagw-" + uuid.New().String()[:8]
	gw := &CarrierGateway{
		CarrierGatewayID: id,
		VpcID:            vpcID,
		State:            "available",
		OwnerID:          b.AccountID,
	}
	b.carrierGateways[id] = gw

	cp := *gw

	return &cp, nil
}

func (b *InMemoryBackend) DeleteCarrierGateway(id string) error {
	b.mu.Lock("DeleteCarrierGateway")
	defer b.mu.Unlock()

	if _, ok := b.carrierGateways[id]; !ok {
		return fmt.Errorf("%w: %s", ErrCarrierGatewayNotFound, id)
	}

	delete(b.carrierGateways, id)

	return nil
}

func (b *InMemoryBackend) DescribeCarrierGateways(ids []string) []*CarrierGateway {
	b.mu.RLock("DescribeCarrierGateways")
	defer b.mu.RUnlock()

	var result []*CarrierGateway

	for _, gw := range b.carrierGateways {
		if len(ids) > 0 && !strInSlice(gw.CarrierGatewayID, ids) {
			continue
		}

		cp := *gw
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CarrierGatewayID < result[j].CarrierGatewayID
	})

	return result
}

// ---- Reserved Instances backend methods ----

func (b *InMemoryBackend) DescribeReservedInstances(ids []string) []*ReservedInstance {
	b.mu.RLock("DescribeReservedInstances")
	defer b.mu.RUnlock()

	var result []*ReservedInstance

	for _, ri := range b.reservedInstances {
		if len(ids) > 0 && !strInSlice(ri.ReservedInstancesID, ids) {
			continue
		}

		cp := *ri
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesID < result[j].ReservedInstancesID
	})

	return result
}

func (b *InMemoryBackend) DescribeReservedInstancesOfferings(instanceType, az, productDesc string) []*ReservedInstancesOffering {
	b.mu.RLock("DescribeReservedInstancesOfferings")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesOffering

	for _, o := range b.reservedInstancesOfferings {
		if instanceType != "" && o.InstanceType != instanceType {
			continue
		}

		if az != "" && o.AvailabilityZone != az {
			continue
		}

		if productDesc != "" && o.ProductDescription != productDesc {
			continue
		}

		cp := *o
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesOfferingID < result[j].ReservedInstancesOfferingID
	})

	return result
}

func (b *InMemoryBackend) PurchaseReservedInstancesOffering(
	offeringID string,
	instanceCount int,
) (*ReservedInstance, error) {
	b.mu.Lock("PurchaseReservedInstancesOffering")
	defer b.mu.Unlock()

	offering, ok := b.reservedInstancesOfferings[offeringID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrReservedInstancesNotFound, offeringID)
	}

	id := "r-" + uuid.New().String()[:8]
	ri := &ReservedInstance{
		ReservedInstancesID: id,
		InstanceType:        offering.InstanceType,
		AvailabilityZone:    offering.AvailabilityZone,
		ProductDescription:  offering.ProductDescription,
		OfferingType:        offering.OfferingType,
		Duration:            offering.Duration,
		FixedPrice:          offering.FixedPrice,
		UsagePrice:          offering.UsagePrice,
		InstanceCount:       instanceCount,
		State:               "active",
	}
	b.reservedInstances[id] = ri

	// Seed a default offering if not present
	if _, ok := b.reservedInstancesOfferings[offeringID]; !ok {
		b.reservedInstancesOfferings[offeringID] = &ReservedInstancesOffering{
			ReservedInstancesOfferingID: offeringID,
		}
	}

	cp := *ri

	return &cp, nil
}

func (b *InMemoryBackend) CreateReservedInstancesListing(
	reservedInstancesID string,
	instanceCount int,
) (*ReservedInstancesListing, error) {
	b.mu.Lock("CreateReservedInstancesListing")
	defer b.mu.Unlock()

	id := "rsl-" + uuid.New().String()[:8]
	l := &ReservedInstancesListing{
		ReservedInstancesListingID: id,
		ReservedInstancesID:        reservedInstancesID,
		Status:                     "active",
	}
	b.reservedInstancesListings[id] = l

	cp := *l

	return &cp, nil
}

func (b *InMemoryBackend) CancelReservedInstancesListing(id string) error {
	b.mu.Lock("CancelReservedInstancesListing")
	defer b.mu.Unlock()

	l, ok := b.reservedInstancesListings[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrReservedInstancesListingNotFound, id)
	}

	l.Status = "cancelled"

	return nil
}

func (b *InMemoryBackend) DescribeReservedInstancesListings(ids []string) []*ReservedInstancesListing {
	b.mu.RLock("DescribeReservedInstancesListings")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesListing

	for _, l := range b.reservedInstancesListings {
		if len(ids) > 0 && !strInSlice(l.ReservedInstancesListingID, ids) {
			continue
		}

		cp := *l
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesListingID < result[j].ReservedInstancesListingID
	})

	return result
}

func (b *InMemoryBackend) DescribeReservedInstancesModifications(ids []string) []*ReservedInstancesModification {
	b.mu.RLock("DescribeReservedInstancesModifications")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesModification

	for _, m := range b.reservedInstancesModifications {
		if len(ids) > 0 && !strInSlice(m.ReservedInstancesModificationID, ids) {
			continue
		}

		cp := *m
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesModificationID < result[j].ReservedInstancesModificationID
	})

	return result
}

func (b *InMemoryBackend) ModifyReservedInstances(
	reservedInstancesIDs []string,
	targetInstanceType string,
	targetCount int,
) (*ReservedInstancesModification, error) {
	b.mu.Lock("ModifyReservedInstances")
	defer b.mu.Unlock()

	id := "rimod-" + uuid.New().String()[:8]
	m := &ReservedInstancesModification{
		ReservedInstancesModificationID: id,
		Status:                          "fulfilled",
		StatusMessage:                   "Modification fulfilled",
	}
	b.reservedInstancesModifications[id] = m

	cp := *m

	return &cp, nil
}

func (b *InMemoryBackend) DeleteQueuedReservedInstances(ids []string) {
	b.mu.Lock("DeleteQueuedReservedInstances")
	defer b.mu.Unlock()

	for _, id := range ids {
		delete(b.reservedInstances, id)
	}
}

// strInSlice returns true if s is in slice.
func strInSlice(s string, slice []string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}

	return false
}
