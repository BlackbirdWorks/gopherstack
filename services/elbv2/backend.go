// Package elbv2 provides an in-memory implementation of the AWS Elastic Load
// Balancing v2 (Application Load Balancer / Network Load Balancer) service.
package elbv2

import (
"fmt"
"sort"
"time"

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
// ErrLoadBalancerAlreadyExists is returned when a load balancer with that name already exists.
ErrLoadBalancerAlreadyExists = awserr.New("DuplicateLoadBalancerName", awserr.ErrAlreadyExists)
// ErrTargetGroupAlreadyExists is returned when a target group with that name already exists.
ErrTargetGroupAlreadyExists = awserr.New("DuplicateTargetGroupName", awserr.ErrAlreadyExists)
// ErrInvalidParameter is returned when a request parameter is invalid or missing.
ErrInvalidParameter = awserr.New("ValidationError", awserr.ErrInvalidParameter)
// ErrUnknownAction is returned when the requested action is not recognized.
ErrUnknownAction = awserr.New("InvalidAction", awserr.ErrInvalidParameter)
)

// LoadBalancerState represents the state of a load balancer.
type LoadBalancerState struct {
Code        string
Description string
}

// LoadBalancer represents an ELBv2 load balancer.
type LoadBalancer struct {
CreatedTime           time.Time
State                 LoadBalancerState
Tags                  *tags.Tags
LoadBalancerArn       string
LoadBalancerName      string
DNSName               string
CanonicalHostedZoneID string
VpcID                 string
Scheme                string
Type                  string
IPAddressType         string
AvailabilityZones     []string
SecurityGroups        []string
}

// TargetGroup represents an ELBv2 target group.
type TargetGroup struct {
Tags                *tags.Tags
TargetGroupArn      string
TargetGroupName     string
Protocol            string
VpcID               string
TargetType          string
HealthCheckProtocol string
HealthCheckPort     string
HealthCheckPath     string
Targets             []Target
Port                int32
HealthCheckEnabled  bool
}

// Target represents a registered target in a target group.
type Target struct {
ID   string
Port int32
}

// Action represents a listener or rule action.
type Action struct {
Type           string
TargetGroupArn string
}

// Listener represents an ELBv2 listener.
type Listener struct {
Tags            *tags.Tags
ListenerArn     string
LoadBalancerArn string
Protocol        string
Port            int32
DefaultActions  []Action
}

// Rule represents an ELBv2 listener rule.
type Rule struct {
RuleArn     string
ListenerArn string
Priority    string
IsDefault   bool
Actions     []Action
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
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
loadBalancers map[string]*LoadBalancer // keyed by ARN
targetGroups  map[string]*TargetGroup  // keyed by ARN
listeners     map[string]*Listener     // keyed by ARN
rules         map[string]*Rule         // keyed by ARN
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
return arn.Build("elasticloadbalancing", b.region, b.accountID, fmt.Sprintf("listener/app/%s/0123456789abcdef/%d", lbName, port))
}

func (b *InMemoryBackend) ruleARN(listenerArn, idx string) string {
return arn.Build("elasticloadbalancing", b.region, b.accountID, "listener-rule/app/rule/"+listenerArn+"/"+idx)
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
func (b *InMemoryBackend) DescribeTargetGroups(arns []string, names []string, lbArn string) ([]TargetGroup, error) {
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

idx := fmt.Sprintf("%d", len(b.rules))
ruleArn := b.ruleARN(input.ListenerArn, idx)

rule := &Rule{
RuleArn:     ruleArn,
ListenerArn: input.ListenerArn,
Priority:    input.Priority,
IsDefault:   false,
Actions:     input.Actions,
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

delete(b.rules, ruleArn)

return nil
}

// AddTags adds or updates tags on ELBv2 resources.
func (b *InMemoryBackend) AddTags(resourceArns []string, kvs []tags.KV) error {
b.mu.Lock("AddTags")
defer b.mu.Unlock()

for _, resArn := range resourceArns {
if lb, ok := b.loadBalancers[resArn]; ok {
for _, kv := range kvs {
lb.Tags.Set(kv.Key, kv.Value)
}

continue
}

if tg, ok := b.targetGroups[resArn]; ok {
for _, kv := range kvs {
tg.Tags.Set(kv.Key, kv.Value)
}

continue
}

if l, ok := b.listeners[resArn]; ok {
for _, kv := range kvs {
l.Tags.Set(kv.Key, kv.Value)
}

continue
}
}

return nil
}

// RemoveTags removes tags from ELBv2 resources.
func (b *InMemoryBackend) RemoveTags(resourceArns []string, keys []string) error {
b.mu.Lock("RemoveTags")
defer b.mu.Unlock()

for _, resArn := range resourceArns {
if lb, ok := b.loadBalancers[resArn]; ok {
lb.Tags.DeleteKeys(keys)
continue
}

if tg, ok := b.targetGroups[resArn]; ok {
tg.Tags.DeleteKeys(keys)
continue
}

if l, ok := b.listeners[resArn]; ok {
l.Tags.DeleteKeys(keys)
continue
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
if lb, ok := b.loadBalancers[resArn]; ok {
result[resArn] = tagsToKVs(lb.Tags)
continue
}

if tg, ok := b.targetGroups[resArn]; ok {
result[resArn] = tagsToKVs(tg.Tags)
continue
}

if l, ok := b.listeners[resArn]; ok {
result[resArn] = tagsToKVs(l.Tags)
continue
}

result[resArn] = []tags.KV{}
}

return result, nil
}
