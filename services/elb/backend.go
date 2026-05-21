// Package elb provides an in-memory implementation of the AWS Classic Elastic
// Load Balancing (ELB) service.
package elb

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	policyTypeAppCookie = "AppCookieStickinessPolicyType"
	policyTypeLBCookie  = "LBCookieStickinessPolicyType"
	notApplicable       = "N/A"
)

var (
	// ErrLoadBalancerNotFound is returned when the requested load balancer does not exist.
	ErrLoadBalancerNotFound = awserr.New("LoadBalancerNotFound", awserr.ErrNotFound)
	// ErrLoadBalancerAlreadyExists is returned when a load balancer with that name already exists.
	ErrLoadBalancerAlreadyExists = awserr.New("DuplicateLoadBalancerName", awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned when a request parameter is invalid or missing.
	ErrInvalidParameter = awserr.New("ValidationError", awserr.ErrInvalidParameter)
	// ErrUnknownAction is returned when the requested action is not recognized.
	ErrUnknownAction = awserr.New("InvalidAction", awserr.ErrInvalidParameter)
	// ErrPolicyNotFound is returned when a policy does not exist.
	ErrPolicyNotFound = awserr.New("PolicyNotFound", awserr.ErrNotFound)
	// ErrPolicyAlreadyExists is returned when a policy with that name already exists.
	ErrPolicyAlreadyExists = awserr.New("DuplicatePolicyName", awserr.ErrAlreadyExists)
	// ErrValidation is a generic validation error sentinel mapped to HTTP 400.
	ErrValidation = awserr.New("ValidationError", awserr.ErrInvalidParameter)
	// ErrListenerNotFound is returned when a listener on the requested port does not exist.
	ErrListenerNotFound = awserr.New("ListenerNotFound", awserr.ErrNotFound)
	// ErrInvalidInstance is returned when a specified instance is not registered with the LB.
	ErrInvalidInstance = awserr.New("InvalidInstance", awserr.ErrInvalidParameter)
	// ErrDuplicateListener is returned when a listener already exists on the requested port.
	ErrDuplicateListener = awserr.New("DuplicateListener", awserr.ErrAlreadyExists)

	// lbNameRe matches valid Classic ELB names: 1-32 chars, alphanumeric + hyphens,
	// must start and end with alphanumeric.
	lbNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\-]{0,30}[a-zA-Z0-9]$|^[a-zA-Z0-9]$`)

	// policyNameRe matches valid Classic ELB policy names: 1-32 chars, alphanumeric + hyphens,
	// must start and end with alphanumeric.
	policyNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\-]{0,30}[a-zA-Z0-9]$|^[a-zA-Z0-9]$`)

	// instanceIDRe matches valid EC2 instance IDs: i- followed by 8-17 lowercase hex chars.
	instanceIDRe = regexp.MustCompile(`^i-[a-f0-9]{8,17}$`)

	// knownPolicyTypes is the set of built-in Classic ELB policy type names.
	knownPolicyTypes = map[string]struct{}{ //nolint:gochecknoglobals // immutable lookup table
		policyTypeAppCookie:                     {},
		policyTypeLBCookie:                      {},
		"ProxyProtocolPolicyType":               {},
		"SSLNegotiationPolicyType":              {},
		"BackendServerAuthenticationPolicyType": {},
	}
)

const (
	// defaultConnectionDrainingTimeout is the default connection-draining timeout in seconds.
	defaultConnectionDrainingTimeout int32 = 300
	// defaultIdleTimeout is the default idle connection timeout in seconds.
	defaultIdleTimeout int32 = 60
	// defaultAccessLogEmitInterval is the default access log emit interval in minutes.
	defaultAccessLogEmitInterval int32 = 60
	// canonicalHostedZoneID is the real AWS Classic ELB hosted zone ID (us-east-1).
	// In production AWS uses per-region values; for a local emulator one constant suffices.
	canonicalHostedZoneID = "Z35SXDOTRQ7X7K"
)

// Listener is a single protocol/port mapping on a load balancer.
type Listener struct {
	Protocol         string   `json:"protocol"`
	InstanceProtocol string   `json:"instanceProtocol"`
	SSLCertificateID string   `json:"sslCertificateId,omitempty"`
	PolicyNames      []string `json:"policyNames,omitempty"`
	LoadBalancerPort int32    `json:"loadBalancerPort"`
	InstancePort     int32    `json:"instancePort"`
}

// BackendServerDescription maps an instance port to the policies applied to it.
type BackendServerDescription struct {
	PolicyNames  []string `json:"policyNames"`
	InstancePort int32    `json:"instancePort"`
}

// AccessLog holds access-log configuration for a Classic ELB.
type AccessLog struct {
	S3BucketName   string `json:"s3BucketName"`
	S3BucketPrefix string `json:"s3BucketPrefix"`
	EmitInterval   int32  `json:"emitInterval"`
	Enabled        bool   `json:"enabled"`
}

// LoadBalancerAttributes holds tunable attributes for a Classic ELB.
type LoadBalancerAttributes struct {
	DesyncMitigationMode      string    `json:"desyncMitigationMode"`
	AccessLog                 AccessLog `json:"accessLog"`
	ConnectionDrainingTimeout int32     `json:"connectionDrainingTimeout"`
	IdleTimeout               int32     `json:"idleTimeout"`
	CrossZoneLoadBalancing    bool      `json:"crossZoneLoadBalancing"`
	ConnectionDraining        bool      `json:"connectionDraining"`
}

// defaultLBAttributes returns the default LoadBalancerAttributes used at
// creation time, matching the AWS service defaults.
func defaultLBAttributes() LoadBalancerAttributes {
	return LoadBalancerAttributes{
		CrossZoneLoadBalancing:    false,
		ConnectionDraining:        false,
		ConnectionDrainingTimeout: defaultConnectionDrainingTimeout,
		IdleTimeout:               defaultIdleTimeout,
		DesyncMitigationMode:      "defensive",
		AccessLog:                 AccessLog{Enabled: false, EmitInterval: defaultAccessLogEmitInterval},
	}
}

// HealthCheck holds health-check configuration for a load balancer.
type HealthCheck struct {
	Target             string `json:"target"`
	Interval           int32  `json:"interval"`
	Timeout            int32  `json:"timeout"`
	UnhealthyThreshold int32  `json:"unhealthyThreshold"`
	HealthyThreshold   int32  `json:"healthyThreshold"`
}

// Instance is an EC2 instance registered with a load balancer.
type Instance struct {
	InstanceID string `json:"instanceId"`
}

// LoadBalancer represents a Classic ELB load balancer.
type LoadBalancer struct {
	CreatedTime               time.Time
	HealthCheck               *HealthCheck
	Tags                      *tags.Tags
	ARN                       string
	VPCId                     string
	Region                    string
	CanonicalHostedZoneName   string
	CanonicalHostedZoneNameID string
	Scheme                    string
	LoadBalancerName          string
	AccountID                 string
	DNSName                   string
	Listeners                 []Listener
	Instances                 []Instance
	BackendServerDescriptions []BackendServerDescription
	AvailabilityZones         []string
	SecurityGroups            []string
	Subnets                   []string
	Attributes                LoadBalancerAttributes
}

// CreateLoadBalancerInput holds input for CreateLoadBalancer.
type CreateLoadBalancerInput struct {
	LoadBalancerName  string
	Scheme            string
	AvailabilityZones []string
	SecurityGroups    []string
	Subnets           []string
	Listeners         []Listener
}

// PolicyAttribute is a single attribute for a load balancer policy.
type PolicyAttribute struct {
	AttributeName  string `json:"attributeName"`
	AttributeValue string `json:"attributeValue"`
}

// LoadBalancerPolicy represents a Classic ELB policy.
type LoadBalancerPolicy struct {
	PolicyName                  string            `json:"policyName"`
	PolicyTypeName              string            `json:"policyTypeName"`
	LoadBalancerName            string            `json:"loadBalancerName"`
	PolicyAttributeDescriptions []PolicyAttribute `json:"policyAttributeDescriptions"`
}

// InstanceState represents the health state of a registered instance.
type InstanceState struct {
	InstanceID  string
	State       string
	ReasonCode  string
	Description string
}

// AccountLimit represents a single ELB account limit.
type AccountLimit struct {
	Name string
	Max  string
}

// PolicyAttributeTypeDescription describes the attributes of a policy type.
type PolicyAttributeTypeDescription struct {
	AttributeName string
	AttributeType string
	Cardinality   string
	DefaultValue  string
	Description   string
}

// PolicyTypeDescription describes a Classic ELB policy type.
type PolicyTypeDescription struct {
	PolicyTypeName                  string
	Description                     string
	PolicyAttributeTypeDescriptions []PolicyAttributeTypeDescription
}

// StorageBackend is the interface for the ELB in-memory store.
type StorageBackend interface {
	Reset()

	CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error)
	DeleteLoadBalancer(name string) error
	DescribeLoadBalancers(names []string) ([]LoadBalancer, error)

	CreateLoadBalancerListeners(name string, listeners []Listener) error
	DeleteLoadBalancerListeners(name string, ports []int32) error

	RegisterInstancesWithLoadBalancer(name string, instances []Instance) ([]Instance, error)
	DeregisterInstancesFromLoadBalancer(name string, instances []Instance) ([]Instance, error)

	ConfigureHealthCheck(name string, hc HealthCheck) (*HealthCheck, error)

	ModifyLoadBalancerAttributes(name string, attrs LoadBalancerAttributes) (*LoadBalancerAttributes, error)
	DescribeLoadBalancerAttributes(name string) (*LoadBalancerAttributes, error)

	AddTags(names []string, kvs []tags.KV) error
	DescribeTags(names []string) (map[string][]tags.KV, error)
	RemoveTags(names []string, keys []string) error

	ApplySecurityGroupsToLoadBalancer(name string, securityGroups []string) ([]string, error)
	AttachLoadBalancerToSubnets(name string, subnets []string) ([]string, error)
	DetachLoadBalancerFromSubnets(name string, subnets []string) ([]string, error)
	EnableAvailabilityZonesForLoadBalancer(name string, azs []string) ([]string, error)
	DisableAvailabilityZonesForLoadBalancer(name string, azs []string) ([]string, error)
	SetLoadBalancerListenerSSLCertificate(name string, port int32, certID string) error
	SetLoadBalancerPoliciesOfListener(name string, port int32, policyNames []string) error
	SetLoadBalancerPoliciesForBackendServer(name string, instancePort int32, policyNames []string) error

	CreateAppCookieStickinessPolicy(name, policyName, cookieName string) error
	CreateLBCookieStickinessPolicy(name, policyName string, cookieExpirationPeriod int64) error
	CreateLoadBalancerPolicy(name, policyName, policyTypeName string, attrs []PolicyAttribute) error
	DeleteLoadBalancerPolicy(name, policyName string) error

	DescribeAccountLimits() ([]AccountLimit, error)
	DescribeInstanceHealth(name string, instances []Instance) ([]InstanceState, error)
	DescribeLoadBalancerPolicies(name string, policyNames []string) ([]LoadBalancerPolicy, error)
	DescribeLoadBalancerPolicyTypes(policyTypeNames []string) ([]PolicyTypeDescription, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	lbs map[string]*LoadBalancer
	// policies stores load balancer policies keyed by "loadBalancerName/policyName".
	policies  map[string]*LoadBalancerPolicy
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		lbs:       make(map[string]*LoadBalancer),
		policies:  make(map[string]*LoadBalancerPolicy),
		mu:        lockmetrics.New("elb"),
		accountID: accountID,
		region:    region,
	}
}

// Reset clears all backend state. All Tags registries are closed to avoid metric leaks.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, lb := range b.lbs {
		if lb.Tags != nil {
			lb.Tags.Close()
		}
	}

	b.lbs = make(map[string]*LoadBalancer)
	b.policies = make(map[string]*LoadBalancerPolicy)
}

// lbCopy returns a deep copy of a LoadBalancer, excluding the Tags pointer (which is
// shared and safe for concurrent reads through its own sync primitives).
func lbCopy(lb *LoadBalancer) LoadBalancer {
	cp := *lb

	cp.Listeners = make([]Listener, len(lb.Listeners))
	for i, l := range lb.Listeners {
		lCopy := l
		if l.PolicyNames != nil {
			lCopy.PolicyNames = make([]string, len(l.PolicyNames))
			copy(lCopy.PolicyNames, l.PolicyNames)
		}

		cp.Listeners[i] = lCopy
	}

	cp.Instances = make([]Instance, len(lb.Instances))
	copy(cp.Instances, lb.Instances)

	cp.AvailabilityZones = make([]string, len(lb.AvailabilityZones))
	copy(cp.AvailabilityZones, lb.AvailabilityZones)

	cp.SecurityGroups = make([]string, len(lb.SecurityGroups))
	copy(cp.SecurityGroups, lb.SecurityGroups)

	cp.Subnets = make([]string, len(lb.Subnets))
	copy(cp.Subnets, lb.Subnets)

	cp.BackendServerDescriptions = make([]BackendServerDescription, len(lb.BackendServerDescriptions))
	for i, bsd := range lb.BackendServerDescriptions {
		bsdCopy := bsd
		bsdCopy.PolicyNames = make([]string, len(bsd.PolicyNames))
		copy(bsdCopy.PolicyNames, bsd.PolicyNames)
		cp.BackendServerDescriptions[i] = bsdCopy
	}

	if lb.HealthCheck != nil {
		hc := *lb.HealthCheck
		cp.HealthCheck = &hc
	}

	return cp
}

// AddLoadBalancerInternal inserts a pre-built LoadBalancer for seeding test state.
// The lb is deep-copied on insertion. Tags is initialised if nil.
func (b *InMemoryBackend) AddLoadBalancerInternal(lb LoadBalancer) {
	b.mu.Lock("AddLoadBalancerInternal")
	defer b.mu.Unlock()

	if lb.Tags == nil {
		lb.Tags = tags.New("elb." + lb.LoadBalancerName)
	}

	if lb.Listeners == nil {
		lb.Listeners = []Listener{}
	}

	if lb.Instances == nil {
		lb.Instances = []Instance{}
	}

	if lb.BackendServerDescriptions == nil {
		lb.BackendServerDescriptions = []BackendServerDescription{}
	}

	if lb.AvailabilityZones == nil {
		lb.AvailabilityZones = []string{}
	}

	if lb.SecurityGroups == nil {
		lb.SecurityGroups = []string{}
	}

	if lb.Subnets == nil {
		lb.Subnets = []string{}
	}

	cp := lbCopy(&lb)
	b.lbs[lb.LoadBalancerName] = &cp
}

// CreateLoadBalancer creates a new Classic ELB load balancer.
func (b *InMemoryBackend) CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error) {
	b.mu.Lock("CreateLoadBalancer")
	defer b.mu.Unlock()

	if input.LoadBalancerName == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	if !lbNameRe.MatchString(input.LoadBalancerName) {
		return nil, fmt.Errorf(
			"%w: LoadBalancerName must be 1-32 alphanumeric characters or hyphens, starting and ending with alphanumeric",
			ErrInvalidParameter,
		)
	}

	if _, exists := b.lbs[input.LoadBalancerName]; exists {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerAlreadyExists, input.LoadBalancerName)
	}

	scheme := input.Scheme
	if scheme == "" {
		scheme = "internet-facing"
	}

	if scheme != "internet-facing" && scheme != "internal" {
		return nil, fmt.Errorf("%w: Scheme must be 'internet-facing' or 'internal'", ErrInvalidParameter)
	}

	dnsName := input.LoadBalancerName + "." + b.region + ".elb.amazonaws.com"
	lbARN := arn.Build("elasticloadbalancing", b.region, b.accountID, "loadbalancer/"+input.LoadBalancerName)

	// Ensure non-nil slices so callers never have to nil-check.
	azs := input.AvailabilityZones
	if azs == nil {
		azs = []string{}
	}

	sgs := input.SecurityGroups
	if sgs == nil {
		sgs = []string{}
	}

	subnets := input.Subnets
	if subnets == nil {
		subnets = []string{}
	}

	listeners := input.Listeners
	if listeners == nil {
		listeners = []Listener{}
	}

	// Derive VPCId: if subnets are provided (VPC-mode LB) use a stable synthetic ID.
	// The first 8 characters of the account ID make a reasonably unique VPC identifier.
	const vpcSuffixLen = 8

	vpcID := ""
	if len(subnets) > 0 {
		acctSuffix := b.accountID
		if len(acctSuffix) > vpcSuffixLen {
			acctSuffix = acctSuffix[:vpcSuffixLen]
		}

		vpcID = "vpc-" + acctSuffix
	}

	lb := &LoadBalancer{
		LoadBalancerName:          input.LoadBalancerName,
		ARN:                       lbARN,
		DNSName:                   dnsName,
		CanonicalHostedZoneName:   dnsName,
		CanonicalHostedZoneNameID: canonicalHostedZoneID,
		CreatedTime:               time.Now(),
		Scheme:                    scheme,
		AvailabilityZones:         azs,
		SecurityGroups:            sgs,
		Subnets:                   subnets,
		VPCId:                     vpcID,
		Listeners:                 listeners,
		Instances:                 []Instance{},
		BackendServerDescriptions: []BackendServerDescription{},
		Tags:                      tags.New("elb." + input.LoadBalancerName),
		AccountID:                 b.accountID,
		Region:                    b.region,
		Attributes:                defaultLBAttributes(),
	}

	b.lbs[input.LoadBalancerName] = lb

	cp := lbCopy(lb)

	return &cp, nil
}

// DeleteLoadBalancer removes a load balancer by name and all of its policies.
func (b *InMemoryBackend) DeleteLoadBalancer(name string) error {
	b.mu.Lock("DeleteLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.Tags.Close()
	delete(b.lbs, name)

	// Cascade-delete all policies that belong to this load balancer.
	prefix := name + "/"
	for k := range b.policies {
		if strings.HasPrefix(k, prefix) {
			delete(b.policies, k)
		}
	}

	return nil
}

// DescribeLoadBalancers returns load balancers, optionally filtered by name.
func (b *InMemoryBackend) DescribeLoadBalancers(names []string) ([]LoadBalancer, error) {
	b.mu.RLock("DescribeLoadBalancers")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		result := make([]LoadBalancer, 0, len(names))

		for _, name := range names {
			lb, ok := b.lbs[name]
			if !ok {
				return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
			}

			result = append(result, lbCopy(lb))
		}

		return result, nil
	}

	result := make([]LoadBalancer, 0, len(b.lbs))
	for _, lb := range b.lbs {
		result = append(result, lbCopy(lb))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LoadBalancerName < result[j].LoadBalancerName
	})

	return result, nil
}

// RegisterInstancesWithLoadBalancer registers EC2 instances with a load balancer.
func (b *InMemoryBackend) RegisterInstancesWithLoadBalancer(name string, instances []Instance) ([]Instance, error) {
	b.mu.Lock("RegisterInstancesWithLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	for _, inst := range instances {
		if !instanceIDRe.MatchString(inst.InstanceID) {
			return nil, fmt.Errorf(
				"%w: invalid instance ID format %q; must match i-[a-f0-9]{8,17}",
				ErrInvalidParameter,
				inst.InstanceID,
			)
		}
	}

	existing := make(map[string]bool, len(lb.Instances))
	for _, inst := range lb.Instances {
		existing[inst.InstanceID] = true
	}

	for _, inst := range instances {
		if !existing[inst.InstanceID] {
			lb.Instances = append(lb.Instances, inst)
			existing[inst.InstanceID] = true
		}
	}

	result := make([]Instance, len(lb.Instances))
	copy(result, lb.Instances)

	return result, nil
}

// DeregisterInstancesFromLoadBalancer removes EC2 instances from a load balancer.
func (b *InMemoryBackend) DeregisterInstancesFromLoadBalancer(name string, instances []Instance) ([]Instance, error) {
	b.mu.Lock("DeregisterInstancesFromLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	remove := make(map[string]bool, len(instances))
	for _, inst := range instances {
		remove[inst.InstanceID] = true
	}

	kept := lb.Instances[:0]
	for _, inst := range lb.Instances {
		if !remove[inst.InstanceID] {
			kept = append(kept, inst)
		}
	}

	lb.Instances = kept

	result := make([]Instance, len(lb.Instances))
	copy(result, lb.Instances)

	return result, nil
}

// ConfigureHealthCheck sets the health-check configuration on a load balancer.
func (b *InMemoryBackend) ConfigureHealthCheck(name string, hc HealthCheck) (*HealthCheck, error) {
	b.mu.Lock("ConfigureHealthCheck")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.HealthCheck = &hc
	cp := hc

	return &cp, nil
}

// AddTags adds or updates tags on one or more load balancers.
func (b *InMemoryBackend) AddTags(names []string, kvs []tags.KV) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	const maxTagKeyLen = 128
	const maxTagValueLen = 256
	const maxTagsPerLB = 10

	// Validate tag key/value lengths before mutating any LB.
	for _, kv := range kvs {
		if kv.Key == "" || len(kv.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrInvalidParameter, maxTagKeyLen)
		}

		if len(kv.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0-%d characters", ErrInvalidParameter, maxTagValueLen)
		}
	}

	for _, name := range names {
		lb, ok := b.lbs[name]
		if !ok {
			return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		// Count existing tags that won't be overwritten.
		newKeys := make(map[string]struct{}, len(kvs))
		for _, kv := range kvs {
			newKeys[kv.Key] = struct{}{}
		}

		existingCount := 0
		lb.Tags.Range(func(k, _ string) bool {
			if _, isNew := newKeys[k]; !isNew {
				existingCount++
			}

			return true
		})

		if existingCount+len(newKeys) > maxTagsPerLB {
			return fmt.Errorf("%w: cannot have more than %d tags on a load balancer", ErrInvalidParameter, maxTagsPerLB)
		}

		for _, kv := range kvs {
			lb.Tags.Set(kv.Key, kv.Value)
		}
	}

	return nil
}

// DescribeTags returns the tags for the given load balancers.
func (b *InMemoryBackend) DescribeTags(names []string) (map[string][]tags.KV, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string][]tags.KV, len(names))

	for _, name := range names {
		lb, ok := b.lbs[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		kvs := make([]tags.KV, 0, lb.Tags.Len())
		lb.Tags.Range(func(k, v string) bool {
			kvs = append(kvs, tags.KV{Key: k, Value: v})

			return true
		})

		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

		result[name] = kvs
	}

	return result, nil
}

// RemoveTags removes the specified tag keys from one or more load balancers.
func (b *InMemoryBackend) RemoveTags(names []string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	for _, name := range names {
		lb, ok := b.lbs[name]
		if !ok {
			return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}

		lb.Tags.DeleteKeys(keys)
	}

	return nil
}

// CreateLoadBalancerListeners adds listeners to an existing load balancer.
// Idempotent: if a listener on the same port already exists with identical settings,
// it is a no-op. Returns DuplicateListener if the port is in use with different settings.
func (b *InMemoryBackend) CreateLoadBalancerListeners(name string, listeners []Listener) error {
	b.mu.Lock("CreateLoadBalancerListeners")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	existing := make(map[int32]*Listener, len(lb.Listeners))
	for i := range lb.Listeners {
		existing[lb.Listeners[i].LoadBalancerPort] = &lb.Listeners[i]
	}

	// Validate all incoming listeners: port conflict with different config = DuplicateListener.
	seen := make(map[int32]bool, len(listeners))
	for _, l := range listeners {
		ex, portTaken := existing[l.LoadBalancerPort]
		if portTaken {
			if ex.Protocol != l.Protocol || ex.InstancePort != l.InstancePort ||
				ex.InstanceProtocol != l.InstanceProtocol {
				return fmt.Errorf(
					"%w: conflicting listener on port %d",
					ErrDuplicateListener,
					l.LoadBalancerPort,
				)
			}
			// Exact match: idempotent no-op.
			continue
		}

		if seen[l.LoadBalancerPort] {
			return fmt.Errorf("%w: duplicate port %d in request", ErrDuplicateListener, l.LoadBalancerPort)
		}

		seen[l.LoadBalancerPort] = true
	}

	for _, l := range listeners {
		if _, alreadyExists := existing[l.LoadBalancerPort]; !alreadyExists {
			lb.Listeners = append(lb.Listeners, l)
		}
	}

	return nil
}

// DeleteLoadBalancerListeners removes listeners by port from an existing load balancer.
func (b *InMemoryBackend) DeleteLoadBalancerListeners(name string, ports []int32) error {
	b.mu.Lock("DeleteLoadBalancerListeners")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	remove := make(map[int32]bool, len(ports))
	for _, p := range ports {
		remove[p] = true
	}

	kept := lb.Listeners[:0]
	for _, l := range lb.Listeners {
		if !remove[l.LoadBalancerPort] {
			kept = append(kept, l)
		}
	}

	lb.Listeners = kept

	return nil
}

// ModifyLoadBalancerAttributes updates the tunable attributes for a load balancer.
func (b *InMemoryBackend) ModifyLoadBalancerAttributes(
	name string,
	attrs LoadBalancerAttributes,
) (*LoadBalancerAttributes, error) {
	b.mu.Lock("ModifyLoadBalancerAttributes")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.Attributes = attrs
	cp := attrs

	return &cp, nil
}

// DescribeLoadBalancerAttributes returns the tunable attributes for a load balancer.
func (b *InMemoryBackend) DescribeLoadBalancerAttributes(name string) (*LoadBalancerAttributes, error) {
	b.mu.RLock("DescribeLoadBalancerAttributes")
	defer b.mu.RUnlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	cp := lb.Attributes

	return &cp, nil
}

// policyKey returns the compound key used to look up a policy in the policies map.
func policyKey(lbName, policyName string) string {
	return lbName + "/" + policyName
}

// validatePolicyName returns ErrInvalidParameter if the policy name is empty or does not
// match the allowed format (1-32 alphanumeric + hyphen chars, start/end alphanumeric).
func validatePolicyName(policyName string) error {
	if policyName == "" {
		return fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	if !policyNameRe.MatchString(policyName) {
		return fmt.Errorf(
			"%w: PolicyName must be 1-32 alphanumeric characters or hyphens, starting and ending with alphanumeric",
			ErrInvalidParameter,
		)
	}

	return nil
}

// ApplySecurityGroupsToLoadBalancer replaces the security groups for a VPC load balancer.
func (b *InMemoryBackend) ApplySecurityGroupsToLoadBalancer(name string, securityGroups []string) ([]string, error) {
	b.mu.Lock("ApplySecurityGroupsToLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	cp := make([]string, len(securityGroups))
	copy(cp, securityGroups)
	sort.Strings(cp)
	lb.SecurityGroups = cp

	return cp, nil
}

// AttachLoadBalancerToSubnets adds subnets to an existing load balancer.
func (b *InMemoryBackend) AttachLoadBalancerToSubnets(name string, subnets []string) ([]string, error) {
	b.mu.Lock("AttachLoadBalancerToSubnets")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	existing := make(map[string]bool, len(lb.Subnets))
	for _, s := range lb.Subnets {
		existing[s] = true
	}

	for _, s := range subnets {
		if !existing[s] {
			lb.Subnets = append(lb.Subnets, s)
			existing[s] = true
		}
	}

	result := make([]string, len(lb.Subnets))
	copy(result, lb.Subnets)
	sort.Strings(result)

	return result, nil
}

// DetachLoadBalancerFromSubnets removes subnets from an existing load balancer.
func (b *InMemoryBackend) DetachLoadBalancerFromSubnets(name string, subnets []string) ([]string, error) {
	b.mu.Lock("DetachLoadBalancerFromSubnets")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	remove := make(map[string]bool, len(subnets))
	for _, s := range subnets {
		remove[s] = true
	}

	kept := lb.Subnets[:0]
	for _, s := range lb.Subnets {
		if !remove[s] {
			kept = append(kept, s)
		}
	}

	lb.Subnets = kept

	result := make([]string, len(lb.Subnets))
	copy(result, lb.Subnets)
	sort.Strings(result)

	return result, nil
}

// EnableAvailabilityZonesForLoadBalancer adds availability zones to an existing load balancer.
func (b *InMemoryBackend) EnableAvailabilityZonesForLoadBalancer(name string, azs []string) ([]string, error) {
	b.mu.Lock("EnableAvailabilityZonesForLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	existing := make(map[string]bool, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		existing[az] = true
	}

	for _, az := range azs {
		if !existing[az] {
			lb.AvailabilityZones = append(lb.AvailabilityZones, az)
			existing[az] = true
		}
	}

	result := make([]string, len(lb.AvailabilityZones))
	copy(result, lb.AvailabilityZones)
	sort.Strings(result)

	return result, nil
}

// DisableAvailabilityZonesForLoadBalancer removes availability zones from an existing load balancer.
func (b *InMemoryBackend) DisableAvailabilityZonesForLoadBalancer(name string, azs []string) ([]string, error) {
	b.mu.Lock("DisableAvailabilityZonesForLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// No-op when no AZs provided.
	if len(azs) == 0 {
		result := make([]string, len(lb.AvailabilityZones))
		copy(result, lb.AvailabilityZones)
		sort.Strings(result)

		return result, nil
	}

	remove := make(map[string]bool, len(azs))
	for _, az := range azs {
		remove[az] = true
	}

	kept := make([]string, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		if !remove[az] {
			kept = append(kept, az)
		}
	}

	if len(kept) == 0 {
		return nil, fmt.Errorf(
			"%w: cannot remove all availability zones; at least one must remain",
			ErrInvalidParameter,
		)
	}

	lb.AvailabilityZones = kept

	result := make([]string, len(lb.AvailabilityZones))
	copy(result, lb.AvailabilityZones)
	sort.Strings(result)

	return result, nil
}

// SetLoadBalancerListenerSSLCertificate sets the SSL certificate for an existing listener.
func (b *InMemoryBackend) SetLoadBalancerListenerSSLCertificate(name string, port int32, certID string) error {
	b.mu.Lock("SetLoadBalancerListenerSSLCertificate")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	for i := range lb.Listeners {
		if lb.Listeners[i].LoadBalancerPort == port {
			lb.Listeners[i].SSLCertificateID = certID

			return nil
		}
	}

	return fmt.Errorf("%w: no listener on port %d", ErrListenerNotFound, port)
}

// SetLoadBalancerPoliciesOfListener sets the policies for an existing listener.
func (b *InMemoryBackend) SetLoadBalancerPoliciesOfListener(name string, port int32, policyNames []string) error {
	b.mu.Lock("SetLoadBalancerPoliciesOfListener")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// Validate each policy exists for this LB.
	for _, p := range policyNames {
		if _, exists := b.policies[policyKey(name, p)]; !exists {
			return fmt.Errorf("%w: %q", ErrPolicyNotFound, p)
		}
	}

	for i := range lb.Listeners {
		if lb.Listeners[i].LoadBalancerPort == port {
			cp := make([]string, len(policyNames))
			copy(cp, policyNames)
			lb.Listeners[i].PolicyNames = cp

			return nil
		}
	}

	return fmt.Errorf("%w: no listener on port %d", ErrListenerNotFound, port)
}

// SetLoadBalancerPoliciesForBackendServer sets the policies for a backend server instance port.
func (b *InMemoryBackend) SetLoadBalancerPoliciesForBackendServer(
	name string,
	instancePort int32,
	policyNames []string,
) error {
	b.mu.Lock("SetLoadBalancerPoliciesForBackendServer")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// Validate each policy exists for this LB.
	for _, p := range policyNames {
		if _, exists := b.policies[policyKey(name, p)]; !exists {
			return fmt.Errorf("%w: %q", ErrPolicyNotFound, p)
		}
	}

	cp := make([]string, len(policyNames))
	copy(cp, policyNames)

	for i := range lb.BackendServerDescriptions {
		if lb.BackendServerDescriptions[i].InstancePort == instancePort {
			lb.BackendServerDescriptions[i].PolicyNames = cp

			return nil
		}
	}

	lb.BackendServerDescriptions = append(lb.BackendServerDescriptions, BackendServerDescription{
		InstancePort: instancePort,
		PolicyNames:  cp,
	})

	return nil
}

// CreateAppCookieStickinessPolicy creates an application-cookie stickiness policy.
func (b *InMemoryBackend) CreateAppCookieStickinessPolicy(name, policyName, cookieName string) error {
	if err := validatePolicyName(policyName); err != nil {
		return err
	}

	if cookieName == "" {
		return fmt.Errorf("%w: CookieName is required for AppCookieStickinessPolicy", ErrInvalidParameter)
	}

	b.mu.Lock("CreateAppCookieStickinessPolicy")
	defer b.mu.Unlock()

	if _, ok := b.lbs[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, ok := b.policies[k]; ok {
		return fmt.Errorf("%w: %q", ErrPolicyAlreadyExists, policyName)
	}

	b.policies[k] = &LoadBalancerPolicy{
		PolicyName:       policyName,
		PolicyTypeName:   policyTypeAppCookie,
		LoadBalancerName: name,
		PolicyAttributeDescriptions: []PolicyAttribute{
			{AttributeName: "CookieName", AttributeValue: cookieName},
		},
	}

	return nil
}

// CreateLBCookieStickinessPolicy creates an LB-cookie stickiness policy.
func (b *InMemoryBackend) CreateLBCookieStickinessPolicy(name, policyName string, cookieExpirationPeriod int64) error {
	if err := validatePolicyName(policyName); err != nil {
		return err
	}

	b.mu.Lock("CreateLBCookieStickinessPolicy")
	defer b.mu.Unlock()

	if _, ok := b.lbs[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, ok := b.policies[k]; ok {
		return fmt.Errorf("%w: %q", ErrPolicyAlreadyExists, policyName)
	}

	expStr := ""
	if cookieExpirationPeriod > 0 {
		expStr = strconv.FormatInt(cookieExpirationPeriod, 10)
	}

	b.policies[k] = &LoadBalancerPolicy{
		PolicyName:       policyName,
		PolicyTypeName:   policyTypeLBCookie,
		LoadBalancerName: name,
		PolicyAttributeDescriptions: []PolicyAttribute{
			{AttributeName: "CookieExpirationPeriod", AttributeValue: expStr},
		},
	}

	return nil
}

// CreateLoadBalancerPolicy creates a policy with custom attributes.
func (b *InMemoryBackend) CreateLoadBalancerPolicy(
	name, policyName, policyTypeName string,
	attrs []PolicyAttribute,
) error {
	if err := validatePolicyName(policyName); err != nil {
		return err
	}

	b.mu.Lock("CreateLoadBalancerPolicy")
	defer b.mu.Unlock()

	if _, ok := b.lbs[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, ok := b.policies[k]; ok {
		return fmt.Errorf("%w: %q", ErrPolicyAlreadyExists, policyName)
	}

	attrCopy := make([]PolicyAttribute, len(attrs))
	copy(attrCopy, attrs)

	b.policies[k] = &LoadBalancerPolicy{
		PolicyName:                  policyName,
		PolicyTypeName:              policyTypeName,
		LoadBalancerName:            name,
		PolicyAttributeDescriptions: attrCopy,
	}

	return nil
}

// DeleteLoadBalancerPolicy removes a policy from a load balancer.
func (b *InMemoryBackend) DeleteLoadBalancerPolicy(name, policyName string) error {
	b.mu.Lock("DeleteLoadBalancerPolicy")
	defer b.mu.Unlock()

	lb, ok := b.lbs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, exists := b.policies[k]; !exists {
		return fmt.Errorf("%w: %q", ErrPolicyNotFound, policyName)
	}

	// Reject deletion if the policy is currently attached to a listener.
	for _, l := range lb.Listeners {
		if slices.Contains(l.PolicyNames, policyName) {
			return fmt.Errorf(
				"%w: policy %q is still in use by listener on port %d",
				ErrValidation,
				policyName,
				l.LoadBalancerPort,
			)
		}
	}

	// Reject deletion if the policy is currently attached to a backend server.
	for _, bsd := range lb.BackendServerDescriptions {
		if slices.Contains(bsd.PolicyNames, policyName) {
			return fmt.Errorf(
				"%w: policy %q is still in use by backend server on port %d",
				ErrValidation,
				policyName,
				bsd.InstancePort,
			)
		}
	}

	delete(b.policies, k)

	return nil
}

// DescribeAccountLimits returns the current ELB account limits.
func (b *InMemoryBackend) DescribeAccountLimits() ([]AccountLimit, error) {
	b.mu.RLock("DescribeAccountLimits")
	defer b.mu.RUnlock()

	return []AccountLimit{
		{Name: "classic-load-balancers", Max: "20"},
		{Name: "classic-listeners", Max: "100"},
		{Name: "classic-registered-instances", Max: "1000"},
	}, nil
}

// DescribeInstanceHealth returns the health state of registered instances.
func (b *InMemoryBackend) DescribeInstanceHealth(name string, instances []Instance) ([]InstanceState, error) {
	b.mu.RLock("DescribeInstanceHealth")
	defer b.mu.RUnlock()

	lb, ok := b.lbs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// If specific instances requested, validate them and return their health.
	if len(instances) > 0 {
		registered := make(map[string]bool, len(lb.Instances))
		for _, inst := range lb.Instances {
			registered[inst.InstanceID] = true
		}

		result := make([]InstanceState, 0, len(instances))
		for _, inst := range instances {
			if !registered[inst.InstanceID] {
				return nil, fmt.Errorf(
					"%w: instance %q is not registered with load balancer %q",
					ErrInvalidInstance,
					inst.InstanceID,
					name,
				)
			}

			result = append(result, InstanceState{
				InstanceID:  inst.InstanceID,
				State:       "InService",
				ReasonCode:  notApplicable,
				Description: notApplicable,
			})
		}

		return result, nil
	}

	// Return all registered instances as InService.
	result := make([]InstanceState, 0, len(lb.Instances))
	for _, inst := range lb.Instances {
		result = append(result, InstanceState{
			InstanceID:  inst.InstanceID,
			State:       "InService",
			ReasonCode:  notApplicable,
			Description: notApplicable,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].InstanceID < result[j].InstanceID
	})

	return result, nil
}

// DescribeLoadBalancerPolicies returns policies associated with the given load balancer,
// optionally filtered by policy names.
func (b *InMemoryBackend) DescribeLoadBalancerPolicies(
	name string,
	policyNames []string,
) ([]LoadBalancerPolicy, error) {
	b.mu.RLock("DescribeLoadBalancerPolicies")
	defer b.mu.RUnlock()

	// When a load balancer name is given, validate it exists.
	if name != "" {
		if _, ok := b.lbs[name]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}
	}

	filterNames := make(map[string]bool, len(policyNames))
	for _, n := range policyNames {
		filterNames[n] = true
	}

	result := make([]LoadBalancerPolicy, 0, len(b.policies))
	for _, p := range b.policies {
		if name != "" && p.LoadBalancerName != name {
			continue
		}

		if len(filterNames) > 0 && !filterNames[p.PolicyName] {
			continue
		}

		cp := *p
		attrCopy := make([]PolicyAttribute, len(p.PolicyAttributeDescriptions))
		copy(attrCopy, p.PolicyAttributeDescriptions)
		cp.PolicyAttributeDescriptions = attrCopy
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PolicyName < result[j].PolicyName
	})

	return result, nil
}

// builtinPolicyTypes returns the built-in Classic ELB policy type descriptions.
func builtinPolicyTypes() []PolicyTypeDescription {
	return []PolicyTypeDescription{
		{
			PolicyTypeName: policyTypeAppCookie,
			Description:    "Stickiness policy with sticky session lifetimes controlled by the application-generated cookie.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{
				{
					AttributeName: "CookieName",
					AttributeType: "String",
					Cardinality:   "ONE",
					Description:   "The name of the application cookie used for stickiness.",
				},
			},
		},
		{
			PolicyTypeName: policyTypeLBCookie,
			Description:    "Stickiness policy with sticky session lifetimes controlled by the browser or an expiration period.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{
				{
					AttributeName: "CookieExpirationPeriod",
					AttributeType: "Long",
					Cardinality:   "ZERO_OR_ONE",
					Description:   "The time period, in seconds, after which the cookie should be considered stale.",
				},
			},
		},
		{
			PolicyTypeName:                  "ProxyProtocolPolicyType",
			Description:                     "Policy that enables Proxy Protocol on the load balancer.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{},
		},
		{
			PolicyTypeName:                  "PublicKeyPolicyType",
			Description:                     "Policy that holds a public key for back-end server authentication.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{},
		},
		{
			PolicyTypeName: "BackendServerAuthenticationPolicyType",
			Description: "Policy that enables authentication between the load balancer " +
				"and back-end instances.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{},
		},
		{
			PolicyTypeName: "SSLNegotiationPolicyType",
			Description: "Policy that configures front-end connections using the protocols " +
				"and ciphers available in the OpenSSL library.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{},
		},
	}
}

// DescribeLoadBalancerPolicyTypes returns the specified policy type descriptions.
// If policyTypeNames is non-empty, an error is returned for any unknown type name.
func (b *InMemoryBackend) DescribeLoadBalancerPolicyTypes(policyTypeNames []string) ([]PolicyTypeDescription, error) {
	all := builtinPolicyTypes()

	if len(policyTypeNames) == 0 {
		return all, nil
	}

	byName := make(map[string]PolicyTypeDescription, len(all))
	for _, pt := range all {
		byName[pt.PolicyTypeName] = pt
	}

	result := make([]PolicyTypeDescription, 0, len(policyTypeNames))

	for _, typeName := range policyTypeNames {
		pt, ok := byName[typeName]
		if !ok {
			return nil, fmt.Errorf("%w: policy type %q not found", ErrPolicyNotFound, typeName)
		}

		result = append(result, pt)
	}

	return result, nil
}
