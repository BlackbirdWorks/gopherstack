// Package elbv2 provides an in-memory implementation of the AWS Elastic Load
// Balancing v2 (Application Load Balancer / Network Load Balancer) service.
package elbv2

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrLoadBalancerNotFound is returned when the requested load balancer does not exist.
	ErrLoadBalancerNotFound = awserr.New("LoadBalancerNotFound", awserr.ErrNotFound)
	// ErrTargetGroupNotFound is returned when the requested target group does not exist.
	ErrTargetGroupNotFound = awserr.New("TargetGroupNotFound", awserr.ErrNotFound)
	// ErrListenerNotFound is returned when the requested listener does not exist.
	ErrListenerNotFound = awserr.New("ListenerNotFound", awserr.ErrNotFound)
	// ErrRuleNotFound is returned when the requested rule does not exist.
	ErrRuleNotFound = awserr.New("RuleNotFound", awserr.ErrNotFound)
	// ErrTrustStoreNotFound is returned when the requested trust store does not exist.
	ErrTrustStoreNotFound = awserr.New("TrustStoreNotFound", awserr.ErrNotFound)
	// ErrLoadBalancerAlreadyExists is returned when a load balancer with that name already exists.
	ErrLoadBalancerAlreadyExists = awserr.New("DuplicateLoadBalancerName", awserr.ErrAlreadyExists)
	// ErrTargetGroupAlreadyExists is returned when a target group with that name already exists.
	ErrTargetGroupAlreadyExists = awserr.New("DuplicateTargetGroupName", awserr.ErrAlreadyExists)
	// ErrTrustStoreAlreadyExists is returned when a trust store with that name already exists.
	ErrTrustStoreAlreadyExists = awserr.New("DuplicateTrustStoreName", awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned when a request parameter is invalid or missing.
	ErrInvalidParameter = awserr.New("ValidationError", awserr.ErrInvalidParameter)
	// ErrUnknownAction is returned when the requested action is not recognized.
	ErrUnknownAction = awserr.New("InvalidAction", awserr.ErrInvalidParameter)
)

// LoadBalancerState represents the state of a load balancer.
type LoadBalancerState struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

// LoadBalancer represents an ELBv2 load balancer.
type LoadBalancer struct {
	CreatedTime           time.Time         `json:"createdTime"`
	State                 LoadBalancerState `json:"state"`
	Tags                  *tags.Tags        `json:"tags,omitempty"`
	LoadBalancerArn       string            `json:"loadBalancerArn"`
	LoadBalancerName      string            `json:"loadBalancerName"`
	DNSName               string            `json:"dnsName"`
	CanonicalHostedZoneID string            `json:"canonicalHostedZoneId"`
	VpcID                 string            `json:"vpcId"`
	Scheme                string            `json:"scheme"`
	Type                  string            `json:"type"`
	IPAddressType         string            `json:"ipAddressType"`
	AvailabilityZones     []string          `json:"availabilityZones"`
	SecurityGroups        []string          `json:"securityGroups"`
}

// TargetGroup represents an ELBv2 target group.
type TargetGroup struct {
	Tags                *tags.Tags `json:"tags,omitempty"`
	TargetGroupArn      string     `json:"targetGroupArn"`
	TargetGroupName     string     `json:"targetGroupName"`
	Protocol            string     `json:"protocol"`
	VpcID               string     `json:"vpcId"`
	TargetType          string     `json:"targetType"`
	HealthCheckProtocol string     `json:"healthCheckProtocol"`
	HealthCheckPort     string     `json:"healthCheckPort"`
	HealthCheckPath     string     `json:"healthCheckPath"`
	Targets             []Target   `json:"targets"`
	Port                int32      `json:"port"`
	HealthCheckEnabled  bool       `json:"healthCheckEnabled"`
}

// Target represents a registered target in a target group.
type Target struct {
	ID   string `json:"id"`
	Port int32  `json:"port"`
}

// Action represents a listener or rule action.
type Action struct {
	Type           string `json:"type"`
	TargetGroupArn string `json:"targetGroupArn"`
}

// Listener represents an ELBv2 listener.
type Listener struct {
	Tags            *tags.Tags `json:"tags,omitempty"`
	ListenerArn     string     `json:"listenerArn"`
	LoadBalancerArn string     `json:"loadBalancerArn"`
	Protocol        string     `json:"protocol"`
	DefaultActions  []Action   `json:"defaultActions"`
	Certificates    []string   `json:"certificates,omitempty"`
	Port            int32      `json:"port"`
}

// Rule represents an ELBv2 listener rule.
type Rule struct {
	Tags        *tags.Tags `json:"tags,omitempty"`
	RuleArn     string     `json:"ruleArn"`
	ListenerArn string     `json:"listenerArn"`
	Priority    string     `json:"priority"`
	Actions     []Action   `json:"actions"`
	IsDefault   bool       `json:"isDefault"`
}

// TrustStore represents an ELBv2 trust store.
type TrustStore struct {
	Tags          *tags.Tags `json:"tags,omitempty"`
	TrustStoreArn string     `json:"trustStoreArn"`
	Name          string     `json:"name"`
	Status        string     `json:"status"`
	Revocations   []string   `json:"revocations,omitempty"`
}

// StorageBackend is the interface for ELBv2 storage operations.
type StorageBackend interface {
	CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error)
	DescribeLoadBalancers(arns []string, names []string) ([]LoadBalancer, error)
	DeleteLoadBalancer(lbArn string) error
	ModifyLoadBalancerAttributes(lbArn string) (*LoadBalancer, error)
	CreateTargetGroup(input CreateTargetGroupInput) (*TargetGroup, error)
	DescribeTargetGroups(arns []string, names []string, lbArn string) ([]TargetGroup, error)
	DeleteTargetGroup(tgArn string) error
	RegisterTargets(tgArn string, targets []Target) error
	DeregisterTargets(tgArn string, targets []Target) error
	DescribeTargetHealth(tgArn string) ([]Target, error)
	CreateListener(input CreateListenerInput) (*Listener, error)
	DescribeListeners(lbArn string, listenerArns []string) ([]Listener, error)
	DeleteListener(listenerArn string) error
	CreateRule(input CreateRuleInput) (*Rule, error)
	DescribeRules(listenerArn string, ruleArns []string) ([]Rule, error)
	DeleteRule(ruleArn string) error
	AddTags(resourceArns []string, kvs []tags.KV) error
	RemoveTags(resourceArns []string, keys []string) error
	DescribeTags(resourceArns []string) (map[string][]tags.KV, error)
	// TrustStore operations.
	CreateTrustStore(name string, kvs []tags.KV) (*TrustStore, error)
	DescribeTrustStores(arns []string, names []string) ([]TrustStore, error)
	DeleteTrustStore(trustStoreArn string) error
	ModifyTrustStore(trustStoreArn, name string) (*TrustStore, error)
	AddTrustStoreRevocations(trustStoreArn string, revocations []string) error
	RemoveTrustStoreRevocations(trustStoreArn string, revocationIDs []string) error
	DescribeTrustStoreRevocations(trustStoreArn string) ([]string, error)
	DescribeTrustStoreAssociations(trustStoreArn string) ([]string, error)
	// Rule priority operations.
	SetRulePriorities(priorities []RulePriority) ([]Rule, error)
	// Listener certificate operations.
	AddListenerCertificates(listenerArn string, certArns []string) error
	DescribeListenerCertificates(listenerArn string) ([]string, error)
	RemoveListenerCertificates(listenerArn string, certArns []string) error
}

// CreateLoadBalancerInput holds the parameters for creating a load balancer.
type CreateLoadBalancerInput struct {
	Name              string
	Scheme            string
	Type              string
	IPAddressType     string
	AvailabilityZones []string
	SecurityGroups    []string
	Tags              []tags.KV
}

// CreateTargetGroupInput holds the parameters for creating a target group.
type CreateTargetGroupInput struct {
	Name       string
	Protocol   string
	VpcID      string
	TargetType string
	Tags       []tags.KV
	Port       int32
}

// CreateListenerInput holds the parameters for creating a listener.
type CreateListenerInput struct {
	LoadBalancerArn string
	Protocol        string
	DefaultActions  []Action
	Tags            []tags.KV
	Port            int32
}

// CreateRuleInput holds the parameters for creating a listener rule.
type CreateRuleInput struct {
	ListenerArn string
	Priority    string
	Actions     []Action
	Tags        []tags.KV
}

// RulePriority holds an ARN-to-priority mapping used by SetRulePriorities.
type RulePriority struct {
	RuleArn  string
	Priority string
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	loadBalancers map[string]*LoadBalancer // keyed by ARN
	targetGroups  map[string]*TargetGroup  // keyed by ARN
	listeners     map[string]*Listener     // keyed by ARN
	rules         map[string]*Rule         // keyed by ARN
	trustStores   map[string]*TrustStore   // keyed by ARN
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new in-memory ELBv2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		loadBalancers: make(map[string]*LoadBalancer),
		targetGroups:  make(map[string]*TargetGroup),
		listeners:     make(map[string]*Listener),
		rules:         make(map[string]*Rule),
		trustStores:   make(map[string]*TrustStore),
		accountID:     accountID,
		region:        region,
		mu:            lockmetrics.New("elbv2"),
	}
}

func (b *InMemoryBackend) lbARN(name string) string {
	return arn.Build("elasticloadbalancing", b.region, b.accountID, "loadbalancer/app/"+name+"/0123456789abcdef")
}

func (b *InMemoryBackend) tgARN(name string) string {
	return arn.Build("elasticloadbalancing", b.region, b.accountID, "targetgroup/"+name+"/0123456789abcdef")
}

func (b *InMemoryBackend) listenerARN(lbName string, port int32) string {
	return arn.Build(
		"elasticloadbalancing",
		b.region,
		b.accountID,
		fmt.Sprintf("listener/app/%s/0123456789abcdef/%d", lbName, port),
	)
}

func (b *InMemoryBackend) ruleARN(listenerArn, idx string) string {
	// Extract the load-balancer/listener path from the listener ARN resource to build
	// a rule ARN in the standard form: listener-rule/app/<lb-name>/<lb-id>/<listener-id>/<rule-id>.
	// The listener ARN resource looks like: listener/app/<lb-name>/<lb-id>/<listener-port>.
	resource := "listener-rule/app/lb/0123456789abcdef/0000000000000000/" + idx
	if listenerArn != "" {
		if i := strings.Index(listenerArn, ":listener/"); i >= 0 {
			path := listenerArn[i+len(":"):]
			resource = "listener-rule/" + strings.TrimPrefix(path, "listener/") + "/" + idx
		}
	}

	return arn.Build("elasticloadbalancing", b.region, b.accountID, resource)
}

func (b *InMemoryBackend) trustStoreARN(id string) string {
	return arn.Build("elasticloadbalancing", b.region, b.accountID, "truststore/"+id)
}

// CreateLoadBalancer creates a new load balancer.
func (b *InMemoryBackend) CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error) {
	b.mu.Lock("CreateLoadBalancer")
	defer b.mu.Unlock()

	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	for _, lb := range b.loadBalancers {
		if lb.LoadBalancerName == input.Name {
			return nil, ErrLoadBalancerAlreadyExists
		}
	}

	lbArn := b.lbARN(input.Name)

	lbType := input.Type
	if lbType == "" {
		lbType = "application"
	}

	scheme := input.Scheme
	if scheme == "" {
		scheme = "internet-facing"
	}

	ipType := input.IPAddressType
	if ipType == "" {
		ipType = "ipv4"
	}

	t := tags.New("elbv2.lb." + input.Name + ".tags")
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	lb := &LoadBalancer{
		LoadBalancerArn:       lbArn,
		LoadBalancerName:      input.Name,
		DNSName:               fmt.Sprintf("%s-%s.%s.elb.amazonaws.com", input.Name, b.region, b.region),
		CanonicalHostedZoneID: "Z35SXDOTRQ7X7K",
		CreatedTime:           time.Now().UTC(),
		Scheme:                scheme,
		Type:                  lbType,
		IPAddressType:         ipType,
		VpcID:                 "vpc-00000000",
		AvailabilityZones:     input.AvailabilityZones,
		SecurityGroups:        input.SecurityGroups,
		State: LoadBalancerState{
			Code:        "active",
			Description: "",
		},
		Tags: t,
	}

	b.loadBalancers[lbArn] = lb

	cp := *lb

	return &cp, nil
}

// DescribeLoadBalancers returns load balancers filtered by ARNs and/or names.
// The returned LoadBalancer values contain a Tags pointer that is backend-owned; callers must treat it as read-only.
func (b *InMemoryBackend) DescribeLoadBalancers(arns []string, names []string) ([]LoadBalancer, error) {
	b.mu.RLock("DescribeLoadBalancers")
	defer b.mu.RUnlock()

	arnSet := make(map[string]bool, len(arns))
	for _, a := range arns {
		arnSet[a] = true
	}

	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	result := make([]LoadBalancer, 0)

	for _, lb := range b.loadBalancers {
		if len(arns) > 0 && !arnSet[lb.LoadBalancerArn] {
			continue
		}

		if len(names) > 0 && !nameSet[lb.LoadBalancerName] {
			continue
		}

		result = append(result, *lb)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LoadBalancerName < result[j].LoadBalancerName
	})

	return result, nil
}

// DeleteLoadBalancer deletes a load balancer by ARN.
func (b *InMemoryBackend) DeleteLoadBalancer(lbArn string) error {
	b.mu.Lock("DeleteLoadBalancer")
	defer b.mu.Unlock()

	if _, ok := b.loadBalancers[lbArn]; !ok {
		return ErrLoadBalancerNotFound
	}

	b.loadBalancers[lbArn].Tags.Close()
	delete(b.loadBalancers, lbArn)

	return nil
}

// ModifyLoadBalancerAttributes is a no-op returning the existing load balancer.
func (b *InMemoryBackend) ModifyLoadBalancerAttributes(lbArn string) (*LoadBalancer, error) {
	b.mu.RLock("ModifyLoadBalancerAttributes")
	defer b.mu.RUnlock()

	lb, ok := b.loadBalancers[lbArn]
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	cp := *lb

	return &cp, nil
}

// CreateTargetGroup creates a new target group.
func (b *InMemoryBackend) CreateTargetGroup(input CreateTargetGroupInput) (*TargetGroup, error) {
	b.mu.Lock("CreateTargetGroup")
	defer b.mu.Unlock()

	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	for _, tg := range b.targetGroups {
		if tg.TargetGroupName == input.Name {
			return nil, ErrTargetGroupAlreadyExists
		}
	}

	tgArn := b.tgARN(input.Name)

	proto := input.Protocol
	if proto == "" {
		proto = "HTTP"
	}

	targetType := input.TargetType
	if targetType == "" {
		targetType = "instance"
	}

	t := tags.New("elbv2.tg." + input.Name + ".tags")
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	tg := &TargetGroup{
		TargetGroupArn:      tgArn,
		TargetGroupName:     input.Name,
		Protocol:            proto,
		Port:                input.Port,
		VpcID:               input.VpcID,
		TargetType:          targetType,
		HealthCheckProtocol: proto,
		HealthCheckPort:     "traffic-port",
		HealthCheckPath:     "/",
		HealthCheckEnabled:  true,
		Targets:             []Target{},
		Tags:                t,
	}

	b.targetGroups[tgArn] = tg

	cp := *tg

	return &cp, nil
}

// DescribeTargetGroups returns target groups filtered by ARNs, names, or load balancer ARN.
// The returned TargetGroup values contain a Tags pointer that is backend-owned; callers must treat it as read-only.
func (b *InMemoryBackend) DescribeTargetGroups(arns []string, names []string, _ string) ([]TargetGroup, error) {
	b.mu.RLock("DescribeTargetGroups")
	defer b.mu.RUnlock()

	arnSet := make(map[string]bool, len(arns))
	for _, a := range arns {
		arnSet[a] = true
	}

	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	result := make([]TargetGroup, 0)

	for _, tg := range b.targetGroups {
		if len(arns) > 0 && !arnSet[tg.TargetGroupArn] {
			continue
		}

		if len(names) > 0 && !nameSet[tg.TargetGroupName] {
			continue
		}

		result = append(result, *tg)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TargetGroupName < result[j].TargetGroupName
	})

	return result, nil
}

// DeleteTargetGroup deletes a target group by ARN.
func (b *InMemoryBackend) DeleteTargetGroup(tgArn string) error {
	b.mu.Lock("DeleteTargetGroup")
	defer b.mu.Unlock()

	if _, ok := b.targetGroups[tgArn]; !ok {
		return ErrTargetGroupNotFound
	}

	b.targetGroups[tgArn].Tags.Close()
	delete(b.targetGroups, tgArn)

	return nil
}

// RegisterTargets registers targets with a target group.
func (b *InMemoryBackend) RegisterTargets(tgArn string, targets []Target) error {
	b.mu.Lock("RegisterTargets")
	defer b.mu.Unlock()

	tg, ok := b.targetGroups[tgArn]
	if !ok {
		return ErrTargetGroupNotFound
	}

	existing := make(map[string]bool)
	for _, t := range tg.Targets {
		existing[t.ID] = true
	}

	for _, t := range targets {
		if !existing[t.ID] {
			tg.Targets = append(tg.Targets, t)
		}
	}

	return nil
}

// DeregisterTargets removes targets from a target group.
func (b *InMemoryBackend) DeregisterTargets(tgArn string, targets []Target) error {
	b.mu.Lock("DeregisterTargets")
	defer b.mu.Unlock()

	tg, ok := b.targetGroups[tgArn]
	if !ok {
		return ErrTargetGroupNotFound
	}

	remove := make(map[string]bool)
	for _, t := range targets {
		remove[t.ID] = true
	}

	remaining := make([]Target, 0, len(tg.Targets))

	for _, t := range tg.Targets {
		if !remove[t.ID] {
			remaining = append(remaining, t)
		}
	}

	tg.Targets = remaining

	return nil
}

// DescribeTargetHealth returns targets registered with the target group.
func (b *InMemoryBackend) DescribeTargetHealth(tgArn string) ([]Target, error) {
	b.mu.RLock("DescribeTargetHealth")
	defer b.mu.RUnlock()

	tg, ok := b.targetGroups[tgArn]
	if !ok {
		return nil, ErrTargetGroupNotFound
	}

	result := make([]Target, len(tg.Targets))
	copy(result, tg.Targets)

	return result, nil
}

// CreateListener creates a new listener on a load balancer.
func (b *InMemoryBackend) CreateListener(input CreateListenerInput) (*Listener, error) {
	b.mu.Lock("CreateListener")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers[input.LoadBalancerArn]
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	listenerArn := b.listenerARN(lb.LoadBalancerName, input.Port)

	t := tags.New(fmt.Sprintf("elbv2.listener.%s.%d.tags", lb.LoadBalancerName, input.Port))
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	listener := &Listener{
		ListenerArn:     listenerArn,
		LoadBalancerArn: input.LoadBalancerArn,
		Protocol:        input.Protocol,
		Port:            input.Port,
		DefaultActions:  input.DefaultActions,
		Tags:            t,
	}

	b.listeners[listenerArn] = listener

	cp := *listener

	return &cp, nil
}

// DescribeListeners returns listeners filtered by load balancer ARN and/or listener ARNs.
// The returned Listener values contain a Tags pointer that is backend-owned; callers must treat it as read-only.
func (b *InMemoryBackend) DescribeListeners(lbArn string, listenerArns []string) ([]Listener, error) {
	b.mu.RLock("DescribeListeners")
	defer b.mu.RUnlock()

	arnSet := make(map[string]bool, len(listenerArns))
	for _, a := range listenerArns {
		arnSet[a] = true
	}

	result := make([]Listener, 0)

	for _, l := range b.listeners {
		if lbArn != "" && l.LoadBalancerArn != lbArn {
			continue
		}

		if len(listenerArns) > 0 && !arnSet[l.ListenerArn] {
			continue
		}

		result = append(result, *l)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ListenerArn < result[j].ListenerArn
	})

	return result, nil
}

// DeleteListener deletes a listener by ARN.
func (b *InMemoryBackend) DeleteListener(listenerArn string) error {
	b.mu.Lock("DeleteListener")
	defer b.mu.Unlock()

	if _, ok := b.listeners[listenerArn]; !ok {
		return ErrListenerNotFound
	}

	b.listeners[listenerArn].Tags.Close()
	delete(b.listeners, listenerArn)

	return nil
}

// CreateRule creates a new rule on a listener.
func (b *InMemoryBackend) CreateRule(input CreateRuleInput) (*Rule, error) {
	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	if _, ok := b.listeners[input.ListenerArn]; !ok {
		return nil, ErrListenerNotFound
	}

	idx := strconv.Itoa(len(b.rules))
	ruleArn := b.ruleARN(input.ListenerArn, idx)

	t := tags.New("elbv2.rule." + ruleArn + ".tags")
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	rule := &Rule{
		RuleArn:     ruleArn,
		ListenerArn: input.ListenerArn,
		Priority:    input.Priority,
		IsDefault:   false,
		Actions:     input.Actions,
		Tags:        t,
	}

	b.rules[ruleArn] = rule

	cp := *rule

	return &cp, nil
}

// DescribeRules returns rules filtered by listener ARN and/or rule ARNs.
func (b *InMemoryBackend) DescribeRules(listenerArn string, ruleArns []string) ([]Rule, error) {
	b.mu.RLock("DescribeRules")
	defer b.mu.RUnlock()

	arnSet := make(map[string]bool, len(ruleArns))
	for _, a := range ruleArns {
		arnSet[a] = true
	}

	result := make([]Rule, 0)

	for _, r := range b.rules {
		if listenerArn != "" && r.ListenerArn != listenerArn {
			continue
		}

		if len(ruleArns) > 0 && !arnSet[r.RuleArn] {
			continue
		}

		result = append(result, *r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RuleArn < result[j].RuleArn
	})

	return result, nil
}

// DeleteRule deletes a rule by ARN.
func (b *InMemoryBackend) DeleteRule(ruleArn string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	if _, ok := b.rules[ruleArn]; !ok {
		return ErrRuleNotFound
	}

	rule := b.rules[ruleArn]
	rule.Tags.Close()
	delete(b.rules, ruleArn)

	return nil
}

// findTagsLocked returns the *tags.Tags for the given resource ARN.
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) findTagsLocked(resArn string) *tags.Tags {
	if lb, ok := b.loadBalancers[resArn]; ok {
		return lb.Tags
	}

	if tg, ok := b.targetGroups[resArn]; ok {
		return tg.Tags
	}

	if l, ok := b.listeners[resArn]; ok {
		return l.Tags
	}

	if r, ok := b.rules[resArn]; ok {
		return r.Tags
	}

	return nil
}

// AddTags adds or updates tags on ELBv2 resources.
func (b *InMemoryBackend) AddTags(resourceArns []string, kvs []tags.KV) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	for _, resArn := range resourceArns {
		t := b.findTagsLocked(resArn)
		if t == nil {
			continue
		}

		for _, kv := range kvs {
			t.Set(kv.Key, kv.Value)
		}
	}

	return nil
}

// RemoveTags removes tags from ELBv2 resources.
func (b *InMemoryBackend) RemoveTags(resourceArns []string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	for _, resArn := range resourceArns {
		t := b.findTagsLocked(resArn)
		if t != nil {
			t.DeleteKeys(keys)
		}
	}

	return nil
}

func tagsToKVs(t *tags.Tags) []tags.KV {
	kvs := make([]tags.KV, 0, t.Len())
	t.Range(func(k, v string) bool {
		kvs = append(kvs, tags.KV{Key: k, Value: v})

		return true
	})

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	return kvs
}

// DescribeTags returns tags for the specified resource ARNs.
func (b *InMemoryBackend) DescribeTags(resourceArns []string) (map[string][]tags.KV, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string][]tags.KV, len(resourceArns))

	for _, resArn := range resourceArns {
		t := b.findTagsLocked(resArn)
		if t != nil {
			result[resArn] = tagsToKVs(t)
		} else {
			result[resArn] = []tags.KV{}
		}
	}

	return result, nil
}

// CreateTrustStore creates a new trust store.
func (b *InMemoryBackend) CreateTrustStore(name string, kvs []tags.KV) (*TrustStore, error) {
	b.mu.Lock("CreateTrustStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	for _, ts := range b.trustStores {
		if ts.Name == name {
			return nil, ErrTrustStoreAlreadyExists
		}
	}

	id := name + "/" + uuid.New().String()
	tsArn := b.trustStoreARN(id)

	t := tags.New("elbv2.ts." + name + ".tags")
	for _, kv := range kvs {
		t.Set(kv.Key, kv.Value)
	}

	ts := &TrustStore{
		TrustStoreArn: tsArn,
		Name:          name,
		Status:        "ACTIVE",
		Revocations:   []string{},
		Tags:          t,
	}

	b.trustStores[tsArn] = ts

	cp := *ts

	return &cp, nil
}

// DescribeTrustStores returns trust stores filtered by ARNs and/or names.
func (b *InMemoryBackend) DescribeTrustStores(arns []string, names []string) ([]TrustStore, error) {
	b.mu.RLock("DescribeTrustStores")
	defer b.mu.RUnlock()

	filterArns := len(arns) > 0
	filterNames := len(names) > 0

	var wantArn map[string]struct{}
	if filterArns {
		wantArn = make(map[string]struct{}, len(arns))
		for _, a := range arns {
			wantArn[a] = struct{}{}
		}
	}

	var wantName map[string]struct{}
	if filterNames {
		wantName = make(map[string]struct{}, len(names))
		for _, n := range names {
			wantName[n] = struct{}{}
		}
	}

	result := make([]TrustStore, 0, len(b.trustStores))

	for _, ts := range b.trustStores {
		if filterArns {
			if _, ok := wantArn[ts.TrustStoreArn]; !ok {
				continue
			}
		}

		if filterNames {
			if _, ok := wantName[ts.Name]; !ok {
				continue
			}
		}

		result = append(result, *ts)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteTrustStore deletes a trust store by ARN.
func (b *InMemoryBackend) DeleteTrustStore(trustStoreArn string) error {
	b.mu.Lock("DeleteTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[trustStoreArn]
	if !ok {
		return ErrTrustStoreNotFound
	}

	ts.Tags.Close()
	delete(b.trustStores, trustStoreArn)

	return nil
}

// AddTrustStoreRevocations appends revocation entries to a trust store.
func (b *InMemoryBackend) AddTrustStoreRevocations(trustStoreArn string, revocations []string) error {
	b.mu.Lock("AddTrustStoreRevocations")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[trustStoreArn]
	if !ok {
		return ErrTrustStoreNotFound
	}

	ts.Revocations = append(ts.Revocations, revocations...)

	return nil
}

// DescribeTrustStoreAssociations returns listener ARNs associated with a trust store.
// In this implementation, trust stores are not automatically linked to listeners,
// so we return an empty list.
func (b *InMemoryBackend) DescribeTrustStoreAssociations(trustStoreArn string) ([]string, error) {
	b.mu.RLock("DescribeTrustStoreAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.trustStores[trustStoreArn]; !ok {
		return nil, ErrTrustStoreNotFound
	}

	return []string{}, nil
}

// AddListenerCertificates adds ACM certificate ARNs to a listener.
func (b *InMemoryBackend) AddListenerCertificates(listenerArn string, certArns []string) error {
	b.mu.Lock("AddListenerCertificates")
	defer b.mu.Unlock()

	listener, ok := b.listeners[listenerArn]
	if !ok {
		return ErrListenerNotFound
	}

	existing := make(map[string]bool, len(listener.Certificates))
	for _, c := range listener.Certificates {
		existing[c] = true
	}

	for _, c := range certArns {
		if !existing[c] {
			listener.Certificates = append(listener.Certificates, c)
			existing[c] = true
		}
	}

	return nil
}

// DescribeListenerCertificates returns certificate ARNs on a listener.
func (b *InMemoryBackend) DescribeListenerCertificates(listenerArn string) ([]string, error) {
	b.mu.RLock("DescribeListenerCertificates")
	defer b.mu.RUnlock()

	listener, ok := b.listeners[listenerArn]
	if !ok {
		return nil, ErrListenerNotFound
	}

	result := make([]string, len(listener.Certificates))
	copy(result, listener.Certificates)

	return result, nil
}

// RemoveListenerCertificates removes certificate ARNs from a listener.
func (b *InMemoryBackend) RemoveListenerCertificates(listenerArn string, certArns []string) error {
	b.mu.Lock("RemoveListenerCertificates")
	defer b.mu.Unlock()

	listener, ok := b.listeners[listenerArn]
	if !ok {
		return ErrListenerNotFound
	}

	remove := make(map[string]bool, len(certArns))
	for _, c := range certArns {
		remove[c] = true
	}

	remaining := make([]string, 0, len(listener.Certificates))
	for _, c := range listener.Certificates {
		if !remove[c] {
			remaining = append(remaining, c)
		}
	}

	listener.Certificates = remaining

	return nil
}

// ModifyTrustStore updates a trust store's name.
func (b *InMemoryBackend) ModifyTrustStore(trustStoreArn, name string) (*TrustStore, error) {
	b.mu.Lock("ModifyTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[trustStoreArn]
	if !ok {
		return nil, ErrTrustStoreNotFound
	}

	if name != "" {
		ts.Name = name
	}

	cp := *ts

	return &cp, nil
}

// RemoveTrustStoreRevocations removes revocation entries from a trust store by index value.
func (b *InMemoryBackend) RemoveTrustStoreRevocations(trustStoreArn string, revocationIDs []string) error {
	b.mu.Lock("RemoveTrustStoreRevocations")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[trustStoreArn]
	if !ok {
		return ErrTrustStoreNotFound
	}

	remove := make(map[string]bool, len(revocationIDs))
	for _, id := range revocationIDs {
		remove[id] = true
	}

	remaining := make([]string, 0, len(ts.Revocations))
	for _, r := range ts.Revocations {
		if !remove[r] {
			remaining = append(remaining, r)
		}
	}

	ts.Revocations = remaining

	return nil
}

// DescribeTrustStoreRevocations returns revocation entries for a trust store.
func (b *InMemoryBackend) DescribeTrustStoreRevocations(trustStoreArn string) ([]string, error) {
	b.mu.RLock("DescribeTrustStoreRevocations")
	defer b.mu.RUnlock()

	ts, ok := b.trustStores[trustStoreArn]
	if !ok {
		return nil, ErrTrustStoreNotFound
	}

	result := make([]string, len(ts.Revocations))
	copy(result, ts.Revocations)

	return result, nil
}

// SetRulePriorities updates the priorities of one or more rules.
func (b *InMemoryBackend) SetRulePriorities(priorities []RulePriority) ([]Rule, error) {
	b.mu.Lock("SetRulePriorities")
	defer b.mu.Unlock()

	result := make([]Rule, 0, len(priorities))

	for _, p := range priorities {
		r, ok := b.rules[p.RuleArn]
		if !ok {
			return nil, ErrRuleNotFound
		}

		r.Priority = p.Priority
		result = append(result, *r)
	}

	return result, nil
}
