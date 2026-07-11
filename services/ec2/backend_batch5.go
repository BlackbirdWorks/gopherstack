package ec2

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"

	"github.com/google/uuid"
)

const (
	stateByoipAdvertised   = "advertised"
	stateAnalysisSucceeded = "succeeded"
	fleetTypeDefault       = "maintain"
)

var (
	ErrTrafficMirrorFilterNotFound      = errors.New("InvalidTrafficMirrorFilterId.NotFound")
	ErrTrafficMirrorFilterRuleNotFound  = errors.New("InvalidTrafficMirrorFilterRuleId.NotFound")
	ErrTrafficMirrorSessionNotFound     = errors.New("InvalidTrafficMirrorSessionId.NotFound")
	ErrTrafficMirrorTargetNotFound      = errors.New("InvalidTrafficMirrorTargetId.NotFound")
	ErrFleetNotFound                    = errors.New("InvalidFleetId.NotFound")
	ErrNetworkInsightsPathNotFound      = errors.New("InvalidNetworkInsightsPathId.NotFound")
	ErrNetworkInsightsAnalysisNotFound  = errors.New("InvalidNetworkInsightsAnalysisId.NotFound")
	ErrNetworkInsightsAccessScopeNF     = errors.New("InvalidNetworkInsightsAccessScopeId.NotFound")
	ErrNetworkInsightsAccessScopeAnaNF  = errors.New("InvalidNetworkInsightsAccessScopeAnalysisId.NotFound")
	ErrCarrierGatewayNotFound           = errors.New("InvalidCarrierGatewayId.NotFound")
	ErrReservedInstancesListingNotFound = errors.New("InvalidReservedInstancesListingId.NotFound")
)

// ---- Traffic Mirror ----

// TrafficMirrorFilter holds a traffic mirror filter.
type TrafficMirrorFilter struct {
	TrafficMirrorFilterID string                     `json:"trafficMirrorFilterId,omitempty"`
	Description           string                     `json:"description,omitempty"`
	NetworkServices       []string                   `json:"networkServices,omitempty"`
	IngressFilterRules    []*TrafficMirrorFilterRule `json:"ingressFilterRules,omitempty"`
	EgressFilterRules     []*TrafficMirrorFilterRule `json:"egressFilterRules,omitempty"`
}

// TrafficMirrorPortRange holds a source or destination port range assigned
// to a traffic mirror filter rule.
type TrafficMirrorPortRange struct {
	FromPort int `json:"fromPort,omitempty"`
	ToPort   int `json:"toPort,omitempty"`
}

// TrafficMirrorPortRangePair carries the optional source and destination
// port ranges supplied when creating a traffic mirror filter rule.
type TrafficMirrorPortRangePair struct {
	Source      *TrafficMirrorPortRange
	Destination *TrafficMirrorPortRange
}

// TrafficMirrorFilterRule holds a single traffic mirror filter rule.
type TrafficMirrorFilterRule struct {
	DestinationPortRange      *TrafficMirrorPortRange `json:"destinationPortRange,omitempty"`
	SourcePortRange           *TrafficMirrorPortRange `json:"sourcePortRange,omitempty"`
	TrafficMirrorFilterRuleID string                  `json:"trafficMirrorFilterRuleId,omitempty"`
	TrafficMirrorFilterID     string                  `json:"trafficMirrorFilterId,omitempty"`
	RuleAction                string                  `json:"ruleAction,omitempty"`
	TrafficDirection          string                  `json:"trafficDirection,omitempty"`
	DestinationCidrBlock      string                  `json:"destinationCidrBlock,omitempty"`
	SourceCidrBlock           string                  `json:"sourceCidrBlock,omitempty"`
	Description               string                  `json:"description,omitempty"`
	RuleNumber                int                     `json:"ruleNumber,omitempty"`
	Protocol                  int                     `json:"protocol,omitempty"`
}

// TrafficMirrorSession holds a traffic mirror session.
type TrafficMirrorSession struct {
	TrafficMirrorSessionID string `json:"trafficMirrorSessionId,omitempty"`
	NetworkInterfaceID     string `json:"networkInterfaceId,omitempty"`
	OwnerID                string `json:"ownerId,omitempty"`
	TrafficMirrorTargetID  string `json:"trafficMirrorTargetId,omitempty"`
	TrafficMirrorFilterID  string `json:"trafficMirrorFilterId,omitempty"`
	Description            string `json:"description,omitempty"`
	SessionNumber          int    `json:"sessionNumber,omitempty"`
	PacketLength           int    `json:"packetLength,omitempty"`
	VirtualNetworkID       int    `json:"virtualNetworkId,omitempty"`
}

// TrafficMirrorTarget holds a traffic mirror target.
type TrafficMirrorTarget struct {
	TrafficMirrorTargetID         string `json:"trafficMirrorTargetId,omitempty"`
	NetworkInterfaceID            string `json:"networkInterfaceId,omitempty"`
	NetworkLoadBalancerArn        string `json:"networkLoadBalancerArn,omitempty"`
	GatewayLoadBalancerEndpointID string `json:"gatewayLoadBalancerEndpointId,omitempty"`
	OwnerID                       string `json:"ownerId,omitempty"`
	Type                          string `json:"type,omitempty"`
	Description                   string `json:"description,omitempty"`
}

// ---- EC2 Fleet ----

// Fleet holds an EC2 Fleet.
type Fleet struct {
	FleetID                          string `json:"fleetId,omitempty"`
	FleetState                       string `json:"fleetState,omitempty"`
	FleetType                        string `json:"fleetType,omitempty"`
	TargetCapacityUnitType           string `json:"targetCapacityUnitType,omitempty"`
	ExcessCapacityTerminationPolicy  string `json:"excessCapacityTerminationPolicy,omitempty"`
	TotalTargetCapacity              int    `json:"totalTargetCapacity,omitempty"`
	OnDemandTargetCapacity           int    `json:"onDemandTargetCapacity,omitempty"`
	SpotTargetCapacity               int    `json:"spotTargetCapacity,omitempty"`
	TerminateInstancesWithExpiration bool   `json:"terminateInstancesWithExpiration,omitempty"`
}

// ---- Network Insights ----

// NetworkInsightsPath holds a network insights path.
type NetworkInsightsPath struct {
	NetworkInsightsPathID  string `json:"networkInsightsPathId,omitempty"`
	NetworkInsightsPathArn string `json:"networkInsightsPathArn,omitempty"`
	SourceID               string `json:"sourceId,omitempty"`
	DestinationID          string `json:"destinationId,omitempty"`
	Protocol               string `json:"protocol,omitempty"`
	DestinationPort        int    `json:"destinationPort,omitempty"`
}

// NetworkInsightsAnalysis holds a network insights analysis.
type NetworkInsightsAnalysis struct {
	NetworkInsightsAnalysisID string `json:"networkInsightsAnalysisId,omitempty"`
	NetworkInsightsPathID     string `json:"networkInsightsPathId,omitempty"`
	Status                    string `json:"status,omitempty"`
	NetworkPathFound          bool   `json:"networkPathFound,omitempty"`
}

// NetworkInsightsAccessScope holds a network insights access scope.
type NetworkInsightsAccessScope struct {
	NetworkInsightsAccessScopeID  string `json:"networkInsightsAccessScopeId,omitempty"`
	NetworkInsightsAccessScopeArn string `json:"networkInsightsAccessScopeArn,omitempty"`
}

// NetworkInsightsAccessScopeAnalysis holds an access scope analysis.
type NetworkInsightsAccessScopeAnalysis struct {
	NetworkInsightsAccessScopeAnalysisID string `json:"networkInsightsAccessScopeAnalysisId,omitempty"`
	NetworkInsightsAccessScopeID         string `json:"networkInsightsAccessScopeId,omitempty"`
	Status                               string `json:"status,omitempty"`
	AnalyzedEniCount                     int    `json:"analyzedEniCount,omitempty"`
}

// ---- Carrier Gateways ----

// CarrierGateway holds a carrier gateway.
type CarrierGateway struct {
	CarrierGatewayID string `json:"carrierGatewayId,omitempty"`
	VpcID            string `json:"vpcId,omitempty"`
	State            string `json:"state,omitempty"`
	OwnerID          string `json:"ownerId,omitempty"`
}

// ---- Reserved Instances ----

// ReservedInstance holds a reserved instance.
type ReservedInstance struct {
	ReservedInstancesID string  `json:"reservedInstancesId,omitempty"`
	InstanceType        string  `json:"instanceType,omitempty"`
	AvailabilityZone    string  `json:"availabilityZone,omitempty"`
	ProductDescription  string  `json:"productDescription,omitempty"`
	State               string  `json:"state,omitempty"`
	OfferingType        string  `json:"offeringType,omitempty"`
	InstanceCount       int     `json:"instanceCount,omitempty"`
	Duration            int64   `json:"duration"`
	FixedPrice          float64 `json:"fixedPrice"`
	UsagePrice          float64 `json:"usagePrice"`
}

// ReservedInstancesOffering holds a reserved instances offering.
type ReservedInstancesOffering struct {
	ReservedInstancesOfferingID string  `json:"reservedInstancesOfferingId,omitempty"`
	InstanceType                string  `json:"instanceType,omitempty"`
	AvailabilityZone            string  `json:"availabilityZone,omitempty"`
	ProductDescription          string  `json:"productDescription,omitempty"`
	OfferingType                string  `json:"offeringType,omitempty"`
	Duration                    int64   `json:"duration"`
	FixedPrice                  float64 `json:"fixedPrice"`
	UsagePrice                  float64 `json:"usagePrice"`
}

// ReservedInstancesListing holds a reserved instances listing.
type ReservedInstancesListing struct {
	ReservedInstancesListingID string `json:"reservedInstancesListingId,omitempty"`
	ReservedInstancesID        string `json:"reservedInstancesId,omitempty"`
	Status                     string `json:"status,omitempty"`
	StatusMessage              string `json:"statusMessage,omitempty"`
}

// ReservedInstancesModification holds a reserved instances modification.
type ReservedInstancesModification struct {
	ReservedInstancesModificationID string `json:"reservedInstancesModificationId,omitempty"`
	Status                          string `json:"status,omitempty"`
	StatusMessage                   string `json:"statusMessage,omitempty"`
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
	b.trafficMirrorFilters.Put(f)

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

func (b *InMemoryBackend) ModifyTrafficMirrorFilterNetworkServices(id string, add, remove []string) error {
	b.mu.Lock("ModifyTrafficMirrorFilterNetworkServices")
	defer b.mu.Unlock()

	f, ok := b.trafficMirrorFilters.Get(id)
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

func (b *InMemoryBackend) ModifyTrafficMirrorFilterRule(id, action, description string) error {
	b.mu.Lock("ModifyTrafficMirrorFilterRule")
	defer b.mu.Unlock()

	rule, ok := b.trafficMirrorFilterRules.Get(id)
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

func (b *InMemoryBackend) ModifyTrafficMirrorSession(id, targetID, filterID, description string) error {
	b.mu.Lock("ModifyTrafficMirrorSession")
	defer b.mu.Unlock()

	s, ok := b.trafficMirrorSessions.Get(id)
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

	cp := *t

	return &cp, nil
}

// trafficMirrorTargetType derives the Traffic Mirror target type from
// whichever destination identifier was supplied.
func trafficMirrorTargetType(networkInterfaceID, networkLoadBalancerArn, gatewayLoadBalancerEndpointID string) string {
	switch {
	case networkInterfaceID != "":
		return "network-interface"
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

func (b *InMemoryBackend) CreateFleet(fleetType string, totalTargetCapacity int) (*Fleet, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if fleetType == "" {
		fleetType = fleetTypeDefault
	}

	id := "fleet-" + uuid.New().String()[:8]
	f := &Fleet{
		FleetID:                         id,
		FleetState:                      SpotFleetStateActive,
		FleetType:                       fleetType,
		TotalTargetCapacity:             totalTargetCapacity,
		ExcessCapacityTerminationPolicy: "termination",
	}
	b.fleets.Put(f)

	cp := *f

	return &cp, nil
}

func (b *InMemoryBackend) DeleteFleets(ids []string) []string {
	b.mu.Lock("DeleteFleets")
	defer b.mu.Unlock()

	var deleted []string

	for _, id := range ids {
		if f, ok := b.fleets.Get(id); ok {
			f.FleetState = tgwRouteStateDeleted
			b.fleets.Delete(id)
			deleted = append(deleted, id)
		}
	}

	return deleted
}

func (b *InMemoryBackend) DescribeFleets(ids []string) []*Fleet {
	b.mu.RLock("DescribeFleets")
	defer b.mu.RUnlock()

	var result []*Fleet

	for _, f := range b.fleets.All() {
		if len(ids) > 0 && !slices.Contains(ids, f.FleetID) {
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

	f, ok := b.fleets.Get(id)
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
	b.networkInsightsPaths.Put(p)

	cp := *p

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsPath(id string) error {
	b.mu.Lock("DeleteNetworkInsightsPath")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsPaths.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsPathNotFound, id)
	}
	b.networkInsightsPaths.Delete(id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsPaths(ids []string) []*NetworkInsightsPath {
	b.mu.RLock("DescribeNetworkInsightsPaths")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsPath

	for _, p := range b.networkInsightsPaths.All() {
		if len(ids) > 0 && !slices.Contains(ids, p.NetworkInsightsPathID) {
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

	if _, ok := b.networkInsightsPaths.Get(pathID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInsightsPathNotFound, pathID)
	}

	id := "nia-" + uuid.New().String()[:8]
	a := &NetworkInsightsAnalysis{
		NetworkInsightsAnalysisID: id,
		NetworkInsightsPathID:     pathID,
		Status:                    stateAnalysisSucceeded,
		NetworkPathFound:          true,
	}
	b.networkInsightsAnalyses.Put(a)

	cp := *a

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAnalysis(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAnalyses.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAnalysisNotFound, id)
	}
	b.networkInsightsAnalyses.Delete(id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAnalyses(ids []string) []*NetworkInsightsAnalysis {
	b.mu.RLock("DescribeNetworkInsightsAnalyses")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAnalysis

	for _, a := range b.networkInsightsAnalyses.All() {
		if len(ids) > 0 && !slices.Contains(ids, a.NetworkInsightsAnalysisID) {
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
	b.networkInsightsAccessScopes.Put(s)

	cp := *s

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAccessScope(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAccessScope")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopes.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeNF, id)
	}
	b.networkInsightsAccessScopes.Delete(id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAccessScopes(ids []string) []*NetworkInsightsAccessScope {
	b.mu.RLock("DescribeNetworkInsightsAccessScopes")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAccessScope

	for _, s := range b.networkInsightsAccessScopes.All() {
		if len(ids) > 0 && !slices.Contains(ids, s.NetworkInsightsAccessScopeID) {
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

	if _, ok := b.networkInsightsAccessScopes.Get(scopeID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeNF, scopeID)
	}

	id := "niasa-" + uuid.New().String()[:8]
	a := &NetworkInsightsAccessScopeAnalysis{
		NetworkInsightsAccessScopeAnalysisID: id,
		NetworkInsightsAccessScopeID:         scopeID,
		Status:                               stateAnalysisSucceeded,
		AnalyzedEniCount:                     0,
	}
	b.networkInsightsAccessScopeAnalyses.Put(a)

	cp := *a

	return &cp, nil
}

func (b *InMemoryBackend) DeleteNetworkInsightsAccessScopeAnalysis(id string) error {
	b.mu.Lock("DeleteNetworkInsightsAccessScopeAnalysis")
	defer b.mu.Unlock()

	if _, ok := b.networkInsightsAccessScopeAnalyses.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInsightsAccessScopeAnaNF, id)
	}
	b.networkInsightsAccessScopeAnalyses.Delete(id)

	return nil
}

func (b *InMemoryBackend) DescribeNetworkInsightsAccessScopeAnalyses(
	ids []string,
) []*NetworkInsightsAccessScopeAnalysis {
	b.mu.RLock("DescribeNetworkInsightsAccessScopeAnalyses")
	defer b.mu.RUnlock()

	var result []*NetworkInsightsAccessScopeAnalysis

	for _, a := range b.networkInsightsAccessScopeAnalyses.All() {
		if len(ids) > 0 && !slices.Contains(ids, a.NetworkInsightsAccessScopeAnalysisID) {
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
	b.byoipCidrs.Put(entry)

	cp := *entry

	return &cp, nil
}

func (b *InMemoryBackend) DeprovisionByoipCidr(cidr string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionByoipCidr")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs.Get(cidr)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, cidr)
	}

	entry.State = "pending-deprovision"
	b.byoipCidrs.Delete(cidr)

	cp := *entry

	return &cp, nil
}

func (b *InMemoryBackend) WithdrawByoipCidr(cidr string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("WithdrawByoipCidr")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs.Get(cidr)
	if !ok {
		entry = &ByoipCidr{Cidr: cidr}
		b.byoipCidrs.Put(entry)
	}

	entry.State = stateByoipAdvertised

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
		State:            stateAvailableImg,
		OwnerID:          b.AccountID,
	}
	b.carrierGateways.Put(gw)

	cp := *gw

	return &cp, nil
}

func (b *InMemoryBackend) DeleteCarrierGateway(id string) error {
	b.mu.Lock("DeleteCarrierGateway")
	defer b.mu.Unlock()

	if _, ok := b.carrierGateways.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrCarrierGatewayNotFound, id)
	}
	b.carrierGateways.Delete(id)

	return nil
}

func (b *InMemoryBackend) DescribeCarrierGateways(ids []string) []*CarrierGateway {
	b.mu.RLock("DescribeCarrierGateways")
	defer b.mu.RUnlock()

	var result []*CarrierGateway

	for _, gw := range b.carrierGateways.All() {
		if len(ids) > 0 && !slices.Contains(ids, gw.CarrierGatewayID) {
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

	for _, ri := range b.reservedInstances.All() {
		if len(ids) > 0 && !slices.Contains(ids, ri.ReservedInstancesID) {
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

func (b *InMemoryBackend) DescribeReservedInstancesOfferings(
	instanceType, az, productDesc string,
) []*ReservedInstancesOffering {
	b.mu.RLock("DescribeReservedInstancesOfferings")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesOffering

	for _, o := range b.reservedInstancesOfferings.All() {
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

	offering, ok := b.reservedInstancesOfferings.Get(offeringID)
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
		State:               SpotFleetStateActive,
	}
	b.reservedInstances.Put(ri)

	cp := *ri

	return &cp, nil
}

func (b *InMemoryBackend) CreateReservedInstancesListing(
	reservedInstancesID string,
	_ int,
) (*ReservedInstancesListing, error) {
	b.mu.Lock("CreateReservedInstancesListing")
	defer b.mu.Unlock()

	id := "rsl-" + uuid.New().String()[:8]
	l := &ReservedInstancesListing{
		ReservedInstancesListingID: id,
		ReservedInstancesID:        reservedInstancesID,
		Status:                     SpotFleetStateActive,
	}
	b.reservedInstancesListings.Put(l)

	cp := *l

	return &cp, nil
}

func (b *InMemoryBackend) CancelReservedInstancesListing(id string) error {
	b.mu.Lock("CancelReservedInstancesListing")
	defer b.mu.Unlock()

	l, ok := b.reservedInstancesListings.Get(id)
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

	for _, l := range b.reservedInstancesListings.All() {
		if len(ids) > 0 && !slices.Contains(ids, l.ReservedInstancesListingID) {
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

	for _, m := range b.reservedInstancesModifications.All() {
		if len(ids) > 0 && !slices.Contains(ids, m.ReservedInstancesModificationID) {
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
	_ []string,
	_ string,
	_ int,
) (*ReservedInstancesModification, error) {
	b.mu.Lock("ModifyReservedInstances")
	defer b.mu.Unlock()

	id := "rimod-" + uuid.New().String()[:8]
	m := &ReservedInstancesModification{
		ReservedInstancesModificationID: id,
		Status:                          "fulfilled",
		StatusMessage:                   "Modification fulfilled",
	}
	b.reservedInstancesModifications.Put(m)

	cp := *m

	return &cp, nil
}

func (b *InMemoryBackend) DeleteQueuedReservedInstances(ids []string) {
	b.mu.Lock("DeleteQueuedReservedInstances")
	defer b.mu.Unlock()

	for _, id := range ids {
		b.reservedInstances.Delete(id)
	}
}
