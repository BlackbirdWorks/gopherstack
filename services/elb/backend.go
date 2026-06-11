// Package elb provides an in-memory implementation of the AWS Classic Elastic
// Load Balancing (ELB) service.
package elb

import (
	"context"
	"fmt"
	"hash/fnv"
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

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Classic ELB load balancers are isolated per region: every backend operation
// resolves the caller's region from the request context and operates only on
// that region's nested store. A Classic ELB and all of its listeners, policies,
// instances, and tags live entirely within a single region, so cross-region
// references never occur and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	policyTypeAppCookie   = "AppCookieStickinessPolicyType"
	policyTypeLBCookie    = "LBCookieStickinessPolicyType"
	policyTypeSSLNeg      = "SSLNegotiationPolicyType"
	notApplicable         = "N/A"
	protoHTTP             = "HTTP"
	protoHTTPS            = "HTTPS"
	protoTCP              = "TCP"
	protoSSL              = "SSL"
	attrRefSecurityPolicy = "Reference-Security-Policy"
	attrProtocolTLS12     = "Protocol-TLSv1.2"
	attrProtocolTLS11     = "Protocol-TLSv1.1"
	attrProtocolTLS10     = "Protocol-TLSv1"
	attrServerCipherOrder = "Server-Defined-Cipher-Order"
	attrTypeBoolean       = "Boolean"
	attrTypeString        = "String"
	cardinalityZeroOrOne  = "ZERO_OR_ONE"

	// dnsHashModLB is the modulus for generating deterministic DNS host IDs.
	dnsHashModLB = 10_000_000_000
	// dnsHashModHZ is the modulus for generating deterministic hosted zone IDs.
	dnsHashModHZ = 1_000_000_000

	// smtpPort is SMTP port 25.
	smtpPort = 25
	// httpPort is the standard HTTP port.
	httpPort = 80
	// httpsPort is the standard HTTPS port.
	httpsPort = 443
	// smtpsPort is the SMTPS port.
	smtpsPort = 465
	// submissionPort is the mail submission port.
	submissionPort = 587
	// minDynamicPort is the lowest user/dynamic port.
	minDynamicPort = 1024
	// maxPort is the maximum TCP/UDP port number.
	maxPort = 65535
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
	// ErrInvalidConfiguration is returned when an operation is not valid for the LB's configuration.
	ErrInvalidConfiguration = awserr.New("InvalidConfigurationRequest", awserr.ErrInvalidParameter)

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
		policyTypeSSLNeg:                        {},
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
)

// regionHostedZoneIDs maps AWS region names to the Classic ELB hosted zone ID for that region.
var regionHostedZoneIDs = map[string]string{ //nolint:gochecknoglobals // immutable lookup table
	"us-east-1":      "Z35SXDOTRQ7X7K",
	"us-east-2":      "Z3AADJGX6KTTL2",
	"us-west-1":      "Z368ELLRRE2KJ0",
	"us-west-2":      "Z1H1FL5HABSF5",
	"eu-west-1":      "Z32O12XQLNTSW2",
	"eu-west-2":      "ZHURV8PSTC4K8",
	"eu-central-1":   "Z215JYRZR1TBD5",
	"ap-northeast-1": "Z14GRHDCWA56QT",
	"ap-southeast-1": "Z1LMS91P8CMLE5",
	"ap-southeast-2": "Z1GM3OXH4ZPM65",
}

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

// dnsNameSuffix returns a stable 10-digit numeric suffix for an ELB DNS name,
// derived from a hash of the account ID and LB name.
func dnsNameSuffix(accountID, lbName string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(accountID + ":" + lbName))

	return fmt.Sprintf("%010d", h.Sum64()%dnsHashModLB)
}

// canonicalHostedZoneIDForRegion returns the Classic ELB hosted zone ID for the given region.
// Falls back to a synthetic ID for unknown regions.
func canonicalHostedZoneIDForRegion(region string) string {
	if id, ok := regionHostedZoneIDs[region]; ok {
		return id
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte("elb-hzid:" + region))

	return fmt.Sprintf("Z%09d", h.Sum32()%dnsHashModHZ)
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
	IsVPC                     bool
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
//
// Every operation that touches per-load-balancer state takes a context.Context
// so the backend can resolve the caller's AWS region and route the operation to
// that region's isolated store. Region returns the backend's default region for
// callers (e.g. the HTTP handler) that need a fallback when the request omits a
// region.
type StorageBackend interface {
	Reset()
	Region() string

	CreateLoadBalancer(ctx context.Context, input CreateLoadBalancerInput) (*LoadBalancer, error)
	DeleteLoadBalancer(ctx context.Context, name string) error
	DescribeLoadBalancers(ctx context.Context, names []string) ([]LoadBalancer, error)

	CreateLoadBalancerListeners(ctx context.Context, name string, listeners []Listener) error
	DeleteLoadBalancerListeners(ctx context.Context, name string, ports []int32) error

	RegisterInstancesWithLoadBalancer(ctx context.Context, name string, instances []Instance) ([]Instance, error)
	DeregisterInstancesFromLoadBalancer(ctx context.Context, name string, instances []Instance) ([]Instance, error)

	ConfigureHealthCheck(ctx context.Context, name string, hc HealthCheck) (*HealthCheck, error)

	ModifyLoadBalancerAttributes(
		ctx context.Context, name string, attrs LoadBalancerAttributes,
	) (*LoadBalancerAttributes, error)
	DescribeLoadBalancerAttributes(ctx context.Context, name string) (*LoadBalancerAttributes, error)

	AddTags(ctx context.Context, names []string, kvs []tags.KV) error
	DescribeTags(ctx context.Context, names []string) (map[string][]tags.KV, error)
	RemoveTags(ctx context.Context, names []string, keys []string) error

	ApplySecurityGroupsToLoadBalancer(ctx context.Context, name string, securityGroups []string) ([]string, error)
	AttachLoadBalancerToSubnets(ctx context.Context, name string, subnets []string) ([]string, error)
	DetachLoadBalancerFromSubnets(ctx context.Context, name string, subnets []string) ([]string, error)
	EnableAvailabilityZonesForLoadBalancer(ctx context.Context, name string, azs []string) ([]string, error)
	DisableAvailabilityZonesForLoadBalancer(ctx context.Context, name string, azs []string) ([]string, error)
	SetLoadBalancerListenerSSLCertificate(ctx context.Context, name string, port int32, certID string) error
	SetLoadBalancerPoliciesOfListener(ctx context.Context, name string, port int32, policyNames []string) error
	SetLoadBalancerPoliciesForBackendServer(
		ctx context.Context, name string, instancePort int32, policyNames []string,
	) error

	CreateAppCookieStickinessPolicy(ctx context.Context, name, policyName, cookieName string) error
	CreateLBCookieStickinessPolicy(ctx context.Context, name, policyName string, cookieExpirationPeriod int64) error
	CreateLoadBalancerPolicy(
		ctx context.Context,
		name, policyName, policyTypeName string,
		attrs []PolicyAttribute,
	) error
	DeleteLoadBalancerPolicy(ctx context.Context, name, policyName string) error

	DescribeAccountLimits(ctx context.Context) ([]AccountLimit, error)
	DescribeInstanceHealth(ctx context.Context, name string, instances []Instance) ([]InstanceState, error)
	DescribeLoadBalancerPolicies(ctx context.Context, name string, policyNames []string) ([]LoadBalancerPolicy, error)
	DescribeLoadBalancerPolicyTypes(ctx context.Context, policyTypeNames []string) ([]PolicyTypeDescription, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// Both the lbs and policies maps are nested by region (outer key = region) so
// that same-named load balancers and policies are fully isolated across
// regions. The per-region inner maps are created lazily via the *Store helpers.
// Callers must hold b.mu while accessing the inner maps.
type InMemoryBackend struct {
	// lbs stores load balancers nested by region: lbs[region][loadBalancerName].
	lbs map[string]map[string]*LoadBalancer
	// policies stores load balancer policies nested by region and keyed by
	// "loadBalancerName/policyName": policies[region][key].
	policies  map[string]map[string]*LoadBalancerPolicy
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		lbs:       make(map[string]map[string]*LoadBalancer),
		policies:  make(map[string]map[string]*LoadBalancerPolicy),
		mu:        lockmetrics.New("elb"),
		accountID: accountID,
		region:    region,
	}
}

// Region returns the AWS region this backend was configured with. It is the
// fallback region used when a request context carries no region.
func (b *InMemoryBackend) Region() string { return b.region }

// lbsStore returns the per-region load balancer map, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) lbsStore(region string) map[string]*LoadBalancer {
	if b.lbs[region] == nil {
		b.lbs[region] = make(map[string]*LoadBalancer)
	}

	return b.lbs[region]
}

// policiesStore returns the per-region policy map, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) policiesStore(region string) map[string]*LoadBalancerPolicy {
	if b.policies[region] == nil {
		b.policies[region] = make(map[string]*LoadBalancerPolicy)
	}

	return b.policies[region]
}

// Reset clears all backend state across every region. All Tags registries are
// closed to avoid metric leaks.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, regionLBs := range b.lbs {
		for _, lb := range regionLBs {
			if lb.Tags != nil {
				lb.Tags.Close()
			}
		}
	}

	b.lbs = make(map[string]map[string]*LoadBalancer)
	b.policies = make(map[string]map[string]*LoadBalancerPolicy)
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
	b.lbsStore(b.region)[lb.LoadBalancerName] = &cp
}

// validateCreateLBName checks that the LB name is present and well-formed.
func validateCreateLBName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	if !lbNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: LoadBalancerName must be 1-32 alphanumeric characters or hyphens, "+
				"starting and ending with alphanumeric",
			ErrInvalidParameter,
		)
	}

	return nil
}

// resolveScheme normalises the scheme parameter, returning an error for invalid values.
func resolveScheme(scheme string) (string, error) {
	if scheme == "" {
		scheme = "internet-facing"
	}

	if scheme != "internet-facing" && scheme != "internal" {
		return "", fmt.Errorf(
			"%w: Scheme must be 'internet-facing' or 'internal'",
			ErrInvalidParameter,
		)
	}

	return scheme, nil
}

// validateCreateLBZones checks listener, AZ, and subnet constraints.
func validateCreateLBZones(input CreateLoadBalancerInput) error {
	if len(input.Listeners) == 0 {
		return fmt.Errorf(
			"%w: at least one Listener is required to create a load balancer",
			ErrInvalidParameter,
		)
	}

	if len(input.AvailabilityZones) > 0 && len(input.Subnets) > 0 {
		return fmt.Errorf(
			"%w: AvailabilityZones and Subnets are mutually exclusive",
			ErrInvalidConfiguration,
		)
	}

	if len(input.AvailabilityZones) == 0 && len(input.Subnets) == 0 {
		return fmt.Errorf(
			"%w: at least one AvailabilityZone or Subnet is required",
			ErrInvalidParameter,
		)
	}

	return nil
}

// nonNilStrings returns src, or an empty (non-nil) slice when src is nil, so
// stored load balancers never carry nil slices.
func nonNilStrings(src []string) []string {
	if src == nil {
		return []string{}
	}

	return src
}

// deriveVPCID returns the synthetic VPC ID for a load balancer. VPC-mode load
// balancers (those with subnets) get a stable ID derived from the first 8
// characters of the account ID; EC2-Classic load balancers get an empty ID.
func (b *InMemoryBackend) deriveVPCID(subnets []string) string {
	const vpcSuffixLen = 8

	if len(subnets) == 0 {
		return ""
	}

	acctSuffix := b.accountID
	if len(acctSuffix) > vpcSuffixLen {
		acctSuffix = acctSuffix[:vpcSuffixLen]
	}

	return "vpc-" + acctSuffix
}

// CreateLoadBalancer creates a new Classic ELB load balancer in the caller's region.
func (b *InMemoryBackend) CreateLoadBalancer(
	ctx context.Context,
	input CreateLoadBalancerInput,
) (*LoadBalancer, error) {
	if err := validateCreateLBName(input.LoadBalancerName); err != nil {
		return nil, err
	}

	scheme, err := resolveScheme(input.Scheme)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateLoadBalancer")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.lbsStore(region)

	if _, exists := store[input.LoadBalancerName]; exists {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerAlreadyExists, input.LoadBalancerName)
	}

	const maxLBs = 20
	if len(store) >= maxLBs {
		return nil, fmt.Errorf(
			"%w: classic-load-balancers limit of %d exceeded",
			ErrValidation, maxLBs,
		)
	}

	if zoneErr := validateCreateLBZones(input); zoneErr != nil {
		return nil, zoneErr
	}

	isVPC := len(input.Subnets) > 0

	suffix := dnsNameSuffix(b.accountID, input.LoadBalancerName)
	dnsPrefix := input.LoadBalancerName + "-" + suffix
	if scheme == "internal" {
		dnsPrefix = "internal-" + dnsPrefix
	}

	dnsName := dnsPrefix + "." + region + ".elb.amazonaws.com"
	lbARN := arn.Build("elasticloadbalancing", region, b.accountID, "loadbalancer/"+input.LoadBalancerName)

	// Ensure non-nil slices so callers never have to nil-check.
	azs := nonNilStrings(input.AvailabilityZones)
	sgs := nonNilStrings(input.SecurityGroups)
	subnets := nonNilStrings(input.Subnets)

	listeners := input.Listeners
	if listeners == nil {
		listeners = []Listener{}
	}

	vpcID := b.deriveVPCID(subnets)

	lb := &LoadBalancer{
		LoadBalancerName:          input.LoadBalancerName,
		ARN:                       lbARN,
		DNSName:                   dnsName,
		CanonicalHostedZoneName:   dnsName,
		CanonicalHostedZoneNameID: canonicalHostedZoneIDForRegion(region),
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
		Region:                    region,
		Attributes:                defaultLBAttributes(),
		IsVPC:                     isVPC,
	}

	store[input.LoadBalancerName] = lb

	cp := lbCopy(lb)

	return &cp, nil
}

// DeleteLoadBalancer removes a load balancer by name and all of its policies
// within the caller's region.
func (b *InMemoryBackend) DeleteLoadBalancer(ctx context.Context, name string) error {
	b.mu.Lock("DeleteLoadBalancer")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.lbsStore(region)

	lb, ok := store[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.Tags.Close()
	delete(store, name)

	// Cascade-delete all policies that belong to this load balancer.
	policies := b.policiesStore(region)
	prefix := name + "/"
	for k := range policies {
		if strings.HasPrefix(k, prefix) {
			delete(policies, k)
		}
	}

	return nil
}

// DescribeLoadBalancers returns load balancers in the caller's region,
// optionally filtered by name.
func (b *InMemoryBackend) DescribeLoadBalancers(ctx context.Context, names []string) ([]LoadBalancer, error) {
	b.mu.RLock("DescribeLoadBalancers")
	defer b.mu.RUnlock()

	store := b.lbsStore(getRegion(ctx, b.region))

	if len(names) > 0 {
		result := make([]LoadBalancer, 0, len(names))

		for _, name := range names {
			lb, ok := store[name]
			if !ok {
				return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
			}

			result = append(result, lbCopy(lb))
		}

		return result, nil
	}

	result := make([]LoadBalancer, 0, len(store))
	for _, lb := range store {
		result = append(result, lbCopy(lb))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LoadBalancerName < result[j].LoadBalancerName
	})

	return result, nil
}

// RegisterInstancesWithLoadBalancer registers EC2 instances with a load balancer.
func (b *InMemoryBackend) RegisterInstancesWithLoadBalancer(
	ctx context.Context, name string, instances []Instance,
) ([]Instance, error) {
	b.mu.Lock("RegisterInstancesWithLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
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

	const maxRegisteredInstances = 1000
	newCount := 0
	for _, inst := range instances {
		if !existing[inst.InstanceID] {
			newCount++
		}
	}

	if len(lb.Instances)+newCount > maxRegisteredInstances {
		return nil, fmt.Errorf(
			"%w: classic-registered-instances limit of %d exceeded",
			ErrValidation,
			maxRegisteredInstances,
		)
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
func (b *InMemoryBackend) DeregisterInstancesFromLoadBalancer(
	ctx context.Context, name string, instances []Instance,
) ([]Instance, error) {
	b.mu.Lock("DeregisterInstancesFromLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
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
func (b *InMemoryBackend) ConfigureHealthCheck(
	ctx context.Context, name string, hc HealthCheck,
) (*HealthCheck, error) {
	b.mu.Lock("ConfigureHealthCheck")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.HealthCheck = &hc
	cp := hc

	return &cp, nil
}

// AddTags adds or updates tags on one or more load balancers.
func (b *InMemoryBackend) AddTags(ctx context.Context, names []string, kvs []tags.KV) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	store := b.lbsStore(getRegion(ctx, b.region))

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
		lb, ok := store[name]
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
func (b *InMemoryBackend) DescribeTags(ctx context.Context, names []string) (map[string][]tags.KV, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	store := b.lbsStore(getRegion(ctx, b.region))
	result := make(map[string][]tags.KV, len(names))

	for _, name := range names {
		lb, ok := store[name]
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
func (b *InMemoryBackend) RemoveTags(ctx context.Context, names []string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	store := b.lbsStore(getRegion(ctx, b.region))

	for _, name := range names {
		lb, ok := store[name]
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
func (b *InMemoryBackend) CreateLoadBalancerListeners(ctx context.Context, name string, listeners []Listener) error {
	b.mu.Lock("CreateLoadBalancerListeners")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	const maxListeners = 100
	if len(lb.Listeners)+len(listeners) > maxListeners {
		return fmt.Errorf("%w: classic-listeners limit of %d exceeded", ErrValidation, maxListeners)
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
func (b *InMemoryBackend) DeleteLoadBalancerListeners(ctx context.Context, name string, ports []int32) error {
	b.mu.Lock("DeleteLoadBalancerListeners")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
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
	ctx context.Context,
	name string,
	attrs LoadBalancerAttributes,
) (*LoadBalancerAttributes, error) {
	b.mu.Lock("ModifyLoadBalancerAttributes")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.Attributes = attrs
	cp := attrs

	return &cp, nil
}

// DescribeLoadBalancerAttributes returns the tunable attributes for a load balancer.
func (b *InMemoryBackend) DescribeLoadBalancerAttributes(
	ctx context.Context, name string,
) (*LoadBalancerAttributes, error) {
	b.mu.RLock("DescribeLoadBalancerAttributes")
	defer b.mu.RUnlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
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
func (b *InMemoryBackend) ApplySecurityGroupsToLoadBalancer(
	ctx context.Context, name string, securityGroups []string,
) ([]string, error) {
	b.mu.Lock("ApplySecurityGroupsToLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if !lb.IsVPC {
		return nil, fmt.Errorf(
			"%w: ApplySecurityGroupsToLoadBalancer is only available for VPC load balancers",
			ErrInvalidConfiguration,
		)
	}

	cp := make([]string, len(securityGroups))
	copy(cp, securityGroups)
	sort.Strings(cp)
	lb.SecurityGroups = cp

	return cp, nil
}

// AttachLoadBalancerToSubnets adds subnets to an existing load balancer.
func (b *InMemoryBackend) AttachLoadBalancerToSubnets(
	ctx context.Context, name string, subnets []string,
) ([]string, error) {
	b.mu.Lock("AttachLoadBalancerToSubnets")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if !lb.IsVPC {
		return nil, fmt.Errorf("%w: cannot attach subnets to an EC2-Classic load balancer", ErrInvalidConfiguration)
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
func (b *InMemoryBackend) DetachLoadBalancerFromSubnets(
	ctx context.Context, name string, subnets []string,
) ([]string, error) {
	b.mu.Lock("DetachLoadBalancerFromSubnets")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
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
func (b *InMemoryBackend) EnableAvailabilityZonesForLoadBalancer(
	ctx context.Context, name string, azs []string,
) ([]string, error) {
	b.mu.Lock("EnableAvailabilityZonesForLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if lb.IsVPC {
		return nil, fmt.Errorf(
			"%w: cannot enable availability zones on a VPC load balancer; use AttachLoadBalancerToSubnets instead",
			ErrInvalidConfiguration,
		)
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
func (b *InMemoryBackend) DisableAvailabilityZonesForLoadBalancer(
	ctx context.Context, name string, azs []string,
) ([]string, error) {
	b.mu.Lock("DisableAvailabilityZonesForLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
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
func (b *InMemoryBackend) SetLoadBalancerListenerSSLCertificate(
	ctx context.Context, name string, port int32, certID string,
) error {
	b.mu.Lock("SetLoadBalancerListenerSSLCertificate")
	defer b.mu.Unlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	for i := range lb.Listeners {
		if lb.Listeners[i].LoadBalancerPort == port {
			proto := lb.Listeners[i].Protocol
			if proto != protoHTTPS && proto != protoSSL {
				return fmt.Errorf(
					"%w: SSL certificate can only be set on HTTPS or SSL listeners (port %d has protocol %s)",
					ErrInvalidConfiguration, port, proto,
				)
			}

			lb.Listeners[i].SSLCertificateID = certID

			return nil
		}
	}

	return fmt.Errorf("%w: no listener on port %d", ErrListenerNotFound, port)
}

// listenerProtocolForPort returns the protocol of the listener on the given port,
// or an empty string if no matching listener exists.
func listenerProtocolForPort(lb *LoadBalancer, port int32) string {
	for i := range lb.Listeners {
		if lb.Listeners[i].LoadBalancerPort == port {
			return lb.Listeners[i].Protocol
		}
	}

	return ""
}

// isStickinessPolicy returns true if the policy type is an app or LB cookie policy.
func isStickinessPolicy(pol *LoadBalancerPolicy) bool {
	return pol.PolicyTypeName == policyTypeAppCookie ||
		pol.PolicyTypeName == policyTypeLBCookie
}

// SetLoadBalancerPoliciesOfListener sets the policies for an existing listener.
func (b *InMemoryBackend) SetLoadBalancerPoliciesOfListener(
	ctx context.Context, name string, port int32, policyNames []string,
) error {
	b.mu.Lock("SetLoadBalancerPoliciesOfListener")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	policies := b.policiesStore(region)

	lb, ok := b.lbsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// Validate each policy exists for this LB.
	for _, p := range policyNames {
		if _, exists := policies[policyKey(name, p)]; !exists {
			return fmt.Errorf("%w: %q", ErrPolicyNotFound, p)
		}
	}

	// Validate stickiness policies are not attached to TCP/SSL listeners.
	proto := listenerProtocolForPort(lb, port)
	if proto == protoTCP || proto == protoSSL {
		for _, pName := range policyNames {
			if isStickinessPolicy(policies[policyKey(name, pName)]) {
				return fmt.Errorf(
					"%w: stickiness policies cannot be applied to TCP or SSL listeners",
					ErrInvalidConfiguration,
				)
			}
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
	ctx context.Context,
	name string,
	instancePort int32,
	policyNames []string,
) error {
	b.mu.Lock("SetLoadBalancerPoliciesForBackendServer")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	policies := b.policiesStore(region)

	lb, ok := b.lbsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// Validate each policy exists for this LB.
	for _, p := range policyNames {
		if _, exists := policies[policyKey(name, p)]; !exists {
			return fmt.Errorf("%w: %q", ErrPolicyNotFound, p)
		}
	}

	cp := make([]string, len(policyNames))
	copy(cp, policyNames)

	if len(policyNames) == 0 {
		// Remove the BSD entry when policy list is empty.
		kept := lb.BackendServerDescriptions[:0]
		for _, bsd := range lb.BackendServerDescriptions {
			if bsd.InstancePort != instancePort {
				kept = append(kept, bsd)
			}
		}
		lb.BackendServerDescriptions = kept

		return nil
	}

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
func (b *InMemoryBackend) CreateAppCookieStickinessPolicy(
	ctx context.Context,
	name, policyName, cookieName string,
) error {
	if err := validatePolicyName(policyName); err != nil {
		return err
	}

	if cookieName == "" {
		return fmt.Errorf("%w: CookieName is required for AppCookieStickinessPolicy", ErrInvalidParameter)
	}

	b.mu.Lock("CreateAppCookieStickinessPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	policies := b.policiesStore(region)

	if _, ok := b.lbsStore(region)[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, ok := policies[k]; ok {
		return fmt.Errorf("%w: %q", ErrPolicyAlreadyExists, policyName)
	}

	policies[k] = &LoadBalancerPolicy{
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
func (b *InMemoryBackend) CreateLBCookieStickinessPolicy(
	ctx context.Context, name, policyName string, cookieExpirationPeriod int64,
) error {
	if err := validatePolicyName(policyName); err != nil {
		return err
	}

	b.mu.Lock("CreateLBCookieStickinessPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	policies := b.policiesStore(region)

	if _, ok := b.lbsStore(region)[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, ok := policies[k]; ok {
		return fmt.Errorf("%w: %q", ErrPolicyAlreadyExists, policyName)
	}

	expStr := ""
	if cookieExpirationPeriod > 0 {
		expStr = strconv.FormatInt(cookieExpirationPeriod, 10)
	}

	policies[k] = &LoadBalancerPolicy{
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
	ctx context.Context,
	name, policyName, policyTypeName string,
	attrs []PolicyAttribute,
) error {
	if err := validatePolicyName(policyName); err != nil {
		return err
	}

	b.mu.Lock("CreateLoadBalancerPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	policies := b.policiesStore(region)

	if _, ok := b.lbsStore(region)[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, ok := policies[k]; ok {
		return fmt.Errorf("%w: %q", ErrPolicyAlreadyExists, policyName)
	}

	attrCopy := make([]PolicyAttribute, len(attrs))
	copy(attrCopy, attrs)

	policies[k] = &LoadBalancerPolicy{
		PolicyName:                  policyName,
		PolicyTypeName:              policyTypeName,
		LoadBalancerName:            name,
		PolicyAttributeDescriptions: attrCopy,
	}

	return nil
}

// DeleteLoadBalancerPolicy removes a policy from a load balancer.
func (b *InMemoryBackend) DeleteLoadBalancerPolicy(ctx context.Context, name, policyName string) error {
	b.mu.Lock("DeleteLoadBalancerPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	policies := b.policiesStore(region)

	lb, ok := b.lbsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	k := policyKey(name, policyName)
	if _, exists := policies[k]; !exists {
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

	delete(policies, k)

	return nil
}

// DescribeAccountLimits returns the current ELB account limits.
func (b *InMemoryBackend) DescribeAccountLimits(_ context.Context) ([]AccountLimit, error) {
	b.mu.RLock("DescribeAccountLimits")
	defer b.mu.RUnlock()

	return []AccountLimit{
		{Name: "classic-load-balancers", Max: "20"},
		{Name: "classic-listeners", Max: "100"},
		{Name: "classic-registered-instances", Max: "1000"},
	}, nil
}

// DescribeInstanceHealth returns the health state of registered instances.
func (b *InMemoryBackend) DescribeInstanceHealth(
	ctx context.Context, name string, instances []Instance,
) ([]InstanceState, error) {
	b.mu.RLock("DescribeInstanceHealth")
	defer b.mu.RUnlock()

	lb, ok := b.lbsStore(getRegion(ctx, b.region))[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// If specific instances requested, validate them and return their health.
	if len(instances) > 0 {
		for _, inst := range instances {
			if !instanceIDRe.MatchString(inst.InstanceID) {
				return nil, fmt.Errorf(
					"%w: invalid instance ID format %q; must match i-[a-f0-9]{8,17}",
					ErrInvalidInstance,
					inst.InstanceID,
				)
			}
		}

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
	ctx context.Context,
	name string,
	policyNames []string,
) ([]LoadBalancerPolicy, error) {
	b.mu.RLock("DescribeLoadBalancerPolicies")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	// When a load balancer name is given, validate it exists.
	if name != "" {
		if _, ok := b.lbsStore(region)[name]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
		}
	}

	filterNames := make(map[string]bool, len(policyNames))
	for _, n := range policyNames {
		filterNames[n] = true
	}

	if name == "" {
		samples := builtinSamplePolicies()
		result := make([]LoadBalancerPolicy, 0, len(samples))

		for _, p := range samples {
			if len(filterNames) > 0 && !filterNames[p.PolicyName] {
				continue
			}

			cp := p
			attrCopy := make([]PolicyAttribute, len(p.PolicyAttributeDescriptions))
			copy(attrCopy, p.PolicyAttributeDescriptions)
			cp.PolicyAttributeDescriptions = attrCopy
			result = append(result, cp)
		}

		sort.Slice(result, func(i, j int) bool { return result[i].PolicyName < result[j].PolicyName })

		return result, nil
	}

	policies := b.policiesStore(region)
	result := make([]LoadBalancerPolicy, 0, len(policies))
	for _, p := range policies {
		if p.LoadBalancerName != name {
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

// builtinSamplePolicies returns the predefined Classic ELB SSL/cipher reference policies
// that AWS ships by default. These are returned by DescribeLoadBalancerPolicies when no
// LoadBalancerName is specified.
func builtinSamplePolicies() []LoadBalancerPolicy {
	return []LoadBalancerPolicy{
		{
			PolicyName:       "ELBSecurityPolicy-2016-08",
			PolicyTypeName:   policyTypeSSLNeg,
			LoadBalancerName: "",
			PolicyAttributeDescriptions: []PolicyAttribute{
				{AttributeName: attrRefSecurityPolicy, AttributeValue: "ELBSecurityPolicy-2016-08"},
				{AttributeName: attrProtocolTLS12, AttributeValue: boolStrTrue},
				{AttributeName: attrProtocolTLS11, AttributeValue: boolStrTrue},
				{AttributeName: attrProtocolTLS10, AttributeValue: boolStrFalse},
				{AttributeName: attrServerCipherOrder, AttributeValue: boolStrTrue},
			},
		},
		{
			PolicyName:       "ELBSecurityPolicy-TLS-1-2-2017-01",
			PolicyTypeName:   policyTypeSSLNeg,
			LoadBalancerName: "",
			PolicyAttributeDescriptions: []PolicyAttribute{
				{AttributeName: attrRefSecurityPolicy, AttributeValue: "ELBSecurityPolicy-TLS-1-2-2017-01"},
				{AttributeName: attrProtocolTLS12, AttributeValue: boolStrTrue},
				{AttributeName: attrProtocolTLS11, AttributeValue: boolStrFalse},
				{AttributeName: attrProtocolTLS10, AttributeValue: boolStrFalse},
				{AttributeName: attrServerCipherOrder, AttributeValue: boolStrTrue},
			},
		},
		{
			PolicyName:     "ELBSample-ELBDefaultNegotiationPolicy",
			PolicyTypeName: policyTypeSSLNeg,
			PolicyAttributeDescriptions: []PolicyAttribute{
				{AttributeName: attrProtocolTLS12, AttributeValue: boolStrTrue},
				{AttributeName: attrProtocolTLS11, AttributeValue: boolStrTrue},
				{AttributeName: attrProtocolTLS10, AttributeValue: boolStrTrue},
				{AttributeName: attrServerCipherOrder, AttributeValue: boolStrFalse},
			},
		},
		{
			PolicyName:     "ELBSample-OpenSSLDefaultCipherPolicy",
			PolicyTypeName: policyTypeSSLNeg,
			PolicyAttributeDescriptions: []PolicyAttribute{
				{AttributeName: attrProtocolTLS12, AttributeValue: boolStrTrue},
				{AttributeName: attrProtocolTLS11, AttributeValue: boolStrTrue},
				{AttributeName: attrProtocolTLS10, AttributeValue: boolStrTrue},
				{AttributeName: attrServerCipherOrder, AttributeValue: boolStrFalse},
			},
		},
	}
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
					AttributeType: attrTypeString,
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
					Cardinality:   cardinalityZeroOrOne,
					Description:   "The time period, in seconds, after which the cookie should be considered stale.",
				},
			},
		},
		{
			PolicyTypeName: "ProxyProtocolPolicyType",
			Description:    "Policy that enables Proxy Protocol on the load balancer.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{
				{
					AttributeName: "ProxyProtocol", AttributeType: attrTypeBoolean,
					Cardinality: "ONE", DefaultValue: boolStrFalse,
					Description: "Enable or disable Proxy Protocol support.",
				},
			},
		},
		{
			PolicyTypeName: "PublicKeyPolicyType",
			Description:    "Policy that holds a public key for back-end server authentication.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{
				{
					AttributeName: "PublicKey", AttributeType: attrTypeString,
					Cardinality: "ONE_OR_MORE",
					Description: "The public key used to authenticate back-end servers.",
				},
			},
		},
		{
			PolicyTypeName: "BackendServerAuthenticationPolicyType",
			Description: "Policy that enables authentication between the load balancer " +
				"and back-end instances.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{
				{
					AttributeName: "PublicKeyPolicyName", AttributeType: "PolicyName",
					Cardinality: "ONE_OR_MORE",
					Description: "The policy name of a PublicKeyPolicy.",
				},
			},
		},
		{
			PolicyTypeName: policyTypeSSLNeg,
			Description: "Policy that configures front-end connections using the protocols " +
				"and ciphers available in the OpenSSL library.",
			PolicyAttributeTypeDescriptions: []PolicyAttributeTypeDescription{
				{
					AttributeName: attrProtocolTLS12, AttributeType: attrTypeBoolean,
					Cardinality: cardinalityZeroOrOne, DefaultValue: boolStrTrue,
				},
				{
					AttributeName: attrProtocolTLS11, AttributeType: attrTypeBoolean,
					Cardinality: cardinalityZeroOrOne, DefaultValue: boolStrFalse,
				},
				{
					AttributeName: attrProtocolTLS10, AttributeType: attrTypeBoolean,
					Cardinality: cardinalityZeroOrOne, DefaultValue: boolStrFalse,
				},
				{
					AttributeName: attrServerCipherOrder, AttributeType: attrTypeBoolean,
					Cardinality: cardinalityZeroOrOne, DefaultValue: boolStrFalse,
				},
				{
					AttributeName: attrRefSecurityPolicy, AttributeType: attrTypeString,
					Cardinality: cardinalityZeroOrOne,
					Description: "The reference security policy name. " +
						"Mutually exclusive with explicit protocol/cipher attributes.",
				},
			},
		},
	}
}

// DescribeLoadBalancerPolicyTypes returns the specified policy type descriptions.
// If policyTypeNames is non-empty, an error is returned for any unknown type name.
func (b *InMemoryBackend) DescribeLoadBalancerPolicyTypes(
	_ context.Context, policyTypeNames []string,
) ([]PolicyTypeDescription, error) {
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
