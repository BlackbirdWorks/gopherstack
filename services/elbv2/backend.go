// Package elbv2 provides an in-memory implementation of the AWS Elastic Load
// Balancing v2 (Application Load Balancer / Network Load Balancer) service.
package elbv2

import (
	"fmt"
	"maps"
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
	// ErrDuplicateRulePriority is returned when two rules have the same priority.
	ErrDuplicateRulePriority = awserr.New("DuplicatePriority", awserr.ErrInvalidParameter)
	// ErrOperationNotPermitted is returned when the operation is not allowed (e.g. deleting default rule).
	ErrOperationNotPermitted = awserr.New("OperationNotPermitted", awserr.ErrInvalidParameter)
	// ErrDuplicateListener is returned when a listener on the same port already exists.
	ErrDuplicateListener = awserr.New("DuplicateListener", awserr.ErrAlreadyExists)
	// ErrTargetGroupInUse is returned when attempting to delete a target group that is still referenced.
	ErrTargetGroupInUse = awserr.New("TargetGroupAssociationLimit", awserr.ErrInvalidParameter)
	// ErrInvalidConfigurationRequest is returned when a configuration is invalid for the LB type.
	ErrInvalidConfigurationRequest = awserr.New("InvalidConfigurationRequest", awserr.ErrInvalidParameter)
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
	Attributes            map[string]string `json:"attributes,omitempty"`
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
	Tags                       *tags.Tags        `json:"tags,omitempty"`
	TargetGroupAttributes      map[string]string `json:"targetGroupAttributes,omitempty"`
	TargetGroupArn             string            `json:"targetGroupArn"`
	TargetGroupName            string            `json:"targetGroupName"`
	Protocol                   string            `json:"protocol"`
	ProtocolVersion            string            `json:"protocolVersion,omitempty"`
	VpcID                      string            `json:"vpcId"`
	TargetType                 string            `json:"targetType"`
	HealthCheckProtocol        string            `json:"healthCheckProtocol"`
	HealthCheckPort            string            `json:"healthCheckPort"`
	HealthCheckPath            string            `json:"healthCheckPath"`
	Matcher                    Matcher           `json:"matcher"`
	Targets                    []Target          `json:"targets"`
	LoadBalancerArns           []string          `json:"loadBalancerArns,omitempty"`
	Port                       int32             `json:"port"`
	HealthCheckIntervalSeconds int32             `json:"healthCheckIntervalSeconds"`
	HealthCheckTimeoutSeconds  int32             `json:"healthCheckTimeoutSeconds"`
	HealthyThresholdCount      int32             `json:"healthyThresholdCount"`
	UnhealthyThresholdCount    int32             `json:"unhealthyThresholdCount"`
	HealthCheckEnabled         bool              `json:"healthCheckEnabled"`
	CrossZoneLoadBalancing     bool              `json:"crossZoneLoadBalancing"`
}

// Target represents a registered target in a target group.
type Target struct {
	ID   string `json:"id"`
	Port int32  `json:"port"`
}

// TargetHealthDescription describes the health state of a registered target.
type TargetHealthDescription struct {
	HealthState  string `json:"healthState"`
	HealthReason string `json:"healthReason,omitempty"`
	Target       Target `json:"target"`
}

// Action represents a listener or rule action.
type Action struct {
	RedirectConfig            *RedirectConfig            `json:"redirectConfig,omitempty"`
	FixedResponseConfig       *FixedResponseConfig       `json:"fixedResponseConfig,omitempty"`
	ForwardConfig             *ForwardConfig             `json:"forwardConfig,omitempty"`
	AuthenticateCognitoConfig *AuthenticateCognitoConfig `json:"authenticateCognitoConfig,omitempty"`
	AuthenticateOidcConfig    *AuthenticateOidcConfig    `json:"authenticateOidcConfig,omitempty"`
	Type                      string                     `json:"type"`
	TargetGroupArn            string                     `json:"targetGroupArn"`
	Order                     int32                      `json:"order,omitempty"`
}

// RedirectConfig holds configuration for redirect actions.
type RedirectConfig struct {
	Protocol   string `json:"protocol,omitempty"`
	Port       string `json:"port,omitempty"`
	Host       string `json:"host,omitempty"`
	Path       string `json:"path,omitempty"`
	Query      string `json:"query,omitempty"`
	StatusCode string `json:"statusCode"`
}

// FixedResponseConfig holds configuration for fixed-response actions.
type FixedResponseConfig struct {
	MessageBody string `json:"messageBody,omitempty"`
	StatusCode  string `json:"statusCode"`
	ContentType string `json:"contentType,omitempty"`
}

// TargetGroupTuple is a target group reference used in ForwardConfig.
type TargetGroupTuple struct {
	TargetGroupArn string `json:"targetGroupArn"`
	Weight         int32  `json:"weight,omitempty"`
}

// ForwardConfig holds configuration for forward actions with multiple target groups.
type ForwardConfig struct {
	TargetGroups []TargetGroupTuple `json:"targetGroups,omitempty"`
}

// Condition represents an ELBv2 rule condition (e.g. host-header, path-pattern, http-header).
type Condition struct {
	// Field is the condition type: host-header, path-pattern, http-header,
	// http-request-method, query-string, source-ip.
	Field string `json:"field"`
	// Values holds the condition values (used for host-header, path-pattern,
	// http-request-method, source-ip).
	Values []string `json:"values,omitempty"`
	// HTTPHeaderName is only set for http-header conditions.
	HTTPHeaderName string `json:"httpHeaderName,omitempty"`
	// QueryStringPairs holds key/value pairs for query-string conditions.
	QueryStringPairs []QueryStringPair `json:"queryStringPairs,omitempty"`
}

// QueryStringPair is a key/value pair used in query-string rule conditions.
type QueryStringPair struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value"`
}

// AuthenticateCognitoConfig holds configuration for authenticate-cognito actions.
type AuthenticateCognitoConfig struct {
	AuthenticationRequestExtraParams map[string]string `json:"authenticationRequestExtraParams,omitempty"`
	UserPoolArn                      string            `json:"userPoolArn"`
	UserPoolClientID                 string            `json:"userPoolClientId"`
	UserPoolDomain                   string            `json:"userPoolDomain"`
	SessionCookieName                string            `json:"sessionCookieName,omitempty"`
	Scope                            string            `json:"scope,omitempty"`
	OnUnauthenticatedRequest         string            `json:"onUnauthenticatedRequest,omitempty"`
	SessionTimeout                   int64             `json:"sessionTimeout,omitempty"`
}

// AuthenticateOidcConfig holds configuration for authenticate-oidc actions.
type AuthenticateOidcConfig struct {
	AuthenticationRequestExtraParams map[string]string `json:"authenticationRequestExtraParams,omitempty"`
	Issuer                           string            `json:"issuer"`
	AuthorizationEndpoint            string            `json:"authorizationEndpoint"`
	TokenEndpoint                    string            `json:"tokenEndpoint"`
	UserInfoEndpoint                 string            `json:"userInfoEndpoint"`
	ClientID                         string            `json:"clientId"`
	ClientSecret                     string            `json:"clientSecret,omitempty"`
	SessionCookieName                string            `json:"sessionCookieName,omitempty"`
	Scope                            string            `json:"scope,omitempty"`
	OnUnauthenticatedRequest         string            `json:"onUnauthenticatedRequest,omitempty"`
	SessionTimeout                   int64             `json:"sessionTimeout,omitempty"`
}

// MutualAuthentication holds mTLS configuration for a listener.
type MutualAuthentication struct {
	TrustStoreArn                     string `json:"trustStoreArn,omitempty"`
	Mode                              string `json:"mode"`
	IgnoreClientCertificateExpiration bool   `json:"ignoreClientCertificateExpiration,omitempty"`
}

// Matcher holds health-check matcher codes for a target group.
type Matcher struct {
	HTTPCode string `json:"httpCode,omitempty"`
	GrpcCode string `json:"grpcCode,omitempty"`
}

// Certificate represents a listener certificate.
type Certificate struct {
	CertificateArn string `json:"certificateArn"`
	IsDefault      bool   `json:"isDefault"`
}

// Listener represents an ELBv2 listener.
type Listener struct {
	Tags                 *tags.Tags            `json:"tags,omitempty"`
	Attributes           map[string]string     `json:"attributes,omitempty"`
	MutualAuthentication *MutualAuthentication `json:"mutualAuthentication,omitempty"`
	ListenerArn          string                `json:"listenerArn"`
	LoadBalancerArn      string                `json:"loadBalancerArn"`
	Protocol             string                `json:"protocol"`
	SSLPolicy            string                `json:"sslPolicy,omitempty"`
	AlpnPolicy           string                `json:"alpnPolicy,omitempty"`
	DefaultActions       []Action              `json:"defaultActions"`
	Certificates         []Certificate         `json:"certificates,omitempty"`
	Port                 int32                 `json:"port"`
}

// Rule represents an ELBv2 listener rule.
type Rule struct {
	Tags        *tags.Tags  `json:"tags,omitempty"`
	RuleArn     string      `json:"ruleArn"`
	ListenerArn string      `json:"listenerArn"`
	Priority    string      `json:"priority"`
	Actions     []Action    `json:"actions"`
	Conditions  []Condition `json:"conditions,omitempty"`
	IsDefault   bool        `json:"isDefault"`
}

// TrustStoreRevocation represents a single revocation entry stored in a trust store.
type TrustStoreRevocation struct {
	RevocationID           string `json:"revocationId"`
	RevocationType         string `json:"revocationType"`
	NumberOfRevokedEntries int64  `json:"numberOfRevokedEntries"`
}

// TrustStore represents an ELBv2 trust store.
type TrustStore struct {
	Tags                *tags.Tags             `json:"tags,omitempty"`
	TrustStoreArn       string                 `json:"trustStoreArn"`
	Name                string                 `json:"name"`
	Status              string                 `json:"status"`
	Revocations         []TrustStoreRevocation `json:"revocations,omitempty"`
	TotalRevokedEntries int64                  `json:"totalRevokedEntries"`
}

// StorageBackend is the interface for ELBv2 storage operations.
type StorageBackend interface {
	CreateLoadBalancer(input CreateLoadBalancerInput) (*LoadBalancer, error)
	DescribeLoadBalancers(arns []string, names []string) ([]LoadBalancer, error)
	DeleteLoadBalancer(lbArn string) error
	ModifyLoadBalancerAttributes(lbArn string, attrs map[string]string) (*LoadBalancer, error)
	SetSecurityGroups(lbArn string, sgs []string) (*LoadBalancer, error)
	SetSubnets(lbArn string, azs []string) (*LoadBalancer, error)
	SetIPAddressType(lbArn string, ipType string) (*LoadBalancer, error)
	CreateTargetGroup(input CreateTargetGroupInput) (*TargetGroup, error)
	DescribeTargetGroups(arns []string, names []string, lbArn string) ([]TargetGroup, error)
	DeleteTargetGroup(tgArn string) error
	ModifyTargetGroup(input ModifyTargetGroupInput) (*TargetGroup, error)
	ModifyTargetGroupAttributes(tgArn string, attrs map[string]string) (*TargetGroup, error)
	DescribeTargetGroupAttributes(tgArn string) (map[string]string, error)
	RegisterTargets(tgArn string, targets []Target) error
	DeregisterTargets(tgArn string, targets []Target) error
	DescribeTargetHealth(tgArn string) ([]TargetHealthDescription, error)
	CreateListener(input CreateListenerInput) (*Listener, error)
	DescribeListeners(lbArn string, listenerArns []string) ([]Listener, error)
	DeleteListener(listenerArn string) error
	ModifyListener(input ModifyListenerInput) (*Listener, error)
	ModifyListenerAttributes(listenerArn string, attrs map[string]string) (*Listener, error)
	DescribeListenerAttributes(listenerArn string) (map[string]string, error)
	CreateRule(input CreateRuleInput) (*Rule, error)
	DescribeRules(listenerArn string, ruleArns []string) ([]Rule, error)
	DeleteRule(ruleArn string) error
	ModifyRule(ruleArn string, actions []Action, conditions []Condition) (*Rule, error)
	AddTags(resourceArns []string, kvs []tags.KV) error
	RemoveTags(resourceArns []string, keys []string) error
	DescribeTags(resourceArns []string) (map[string][]tags.KV, error)
	// TrustStore operations.
	CreateTrustStore(name string, kvs []tags.KV) (*TrustStore, error)
	DescribeTrustStores(arns []string, names []string) ([]TrustStore, error)
	DeleteTrustStore(trustStoreArn string) error
	ModifyTrustStore(trustStoreArn, name string) (*TrustStore, error)
	AddTrustStoreRevocations(trustStoreArn string, revocations []TrustStoreRevocation) error
	RemoveTrustStoreRevocations(trustStoreArn string, revocationIDs []string) error
	DescribeTrustStoreRevocations(trustStoreArn string) ([]TrustStoreRevocation, error)
	DescribeTrustStoreAssociations(trustStoreArn string) ([]string, error)
	// Rule priority operations.
	SetRulePriorities(priorities []RulePriority) ([]Rule, error)
	// Listener certificate operations.
	AddListenerCertificates(listenerArn string, certs []Certificate) error
	DescribeListenerCertificates(listenerArn string) ([]Certificate, error)
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
	Name                       string
	Protocol                   string
	ProtocolVersion            string
	VpcID                      string
	TargetType                 string
	HealthCheckProtocol        string
	HealthCheckPort            string
	HealthCheckPath            string
	Matcher                    Matcher
	Tags                       []tags.KV
	Port                       int32
	HealthCheckIntervalSeconds int32
	HealthCheckTimeoutSeconds  int32
	HealthyThresholdCount      int32
	UnhealthyThresholdCount    int32
	HealthCheckEnabled         bool
}

// ModifyTargetGroupInput holds the parameters for modifying a target group.
// HealthCheckEnabled is a pointer so that an absent parameter does not overwrite the stored value.
type ModifyTargetGroupInput struct {
	HealthCheckEnabled         *bool
	Matcher                    Matcher
	TargetGroupArn             string
	HealthCheckProtocol        string
	HealthCheckPort            string
	HealthCheckPath            string
	HealthCheckIntervalSeconds int32
	HealthCheckTimeoutSeconds  int32
	HealthyThresholdCount      int32
	UnhealthyThresholdCount    int32
}

// CreateListenerInput holds the parameters for creating a listener.
type CreateListenerInput struct {
	MutualAuthentication *MutualAuthentication
	LoadBalancerArn      string
	Protocol             string
	SSLPolicy            string
	AlpnPolicy           string
	DefaultActions       []Action
	Tags                 []tags.KV
	Certificates         []Certificate
	Port                 int32
}

// ModifyListenerInput holds the parameters for modifying a listener.
type ModifyListenerInput struct {
	MutualAuthentication *MutualAuthentication
	ListenerArn          string
	Protocol             string
	SSLPolicy            string
	AlpnPolicy           string
	DefaultActions       []Action
	Certificates         []Certificate
	Port                 int32
}

// CreateRuleInput holds the parameters for creating a listener rule.
type CreateRuleInput struct {
	ListenerArn string
	Priority    string
	Actions     []Action
	Conditions  []Condition
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
	ruleCounter   int // monotonically increasing counter for rule ARN generation
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

// validatePort returns ErrInvalidParameter if port is not in the valid range 1-65535.
func validatePort(port int32) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("%w: port must be between 1 and 65535", ErrInvalidParameter)
	}

	return nil
}

// validateResourceName returns ErrInvalidParameter if name violates the ELBv2 naming rules:
// non-empty, alphanumeric characters, hyphens, and underscores;
// cannot start or end with a hyphen.
func validateResourceName(name, kind string) error {
	if len(name) == 0 {
		return fmt.Errorf("%w: %s name must not be empty", ErrInvalidParameter, kind)
	}

	if name[0] == '-' || name[len(name)-1] == '-' {
		return fmt.Errorf("%w: %s name cannot start or end with a hyphen", ErrInvalidParameter, kind)
	}

	for _, c := range name {
		lowerAlpha := c >= 'a' && c <= 'z'
		upperAlpha := c >= 'A' && c <= 'Z'
		digit := c >= '0' && c <= '9'
		hyphen := c == '-'
		underscore := c == '_'

		if !lowerAlpha && !upperAlpha && !digit && !hyphen && !underscore {
			return fmt.Errorf(
				"%w: %s name may only contain alphanumeric characters, hyphens, and underscores",
				ErrInvalidParameter, kind,
			)
		}
	}

	return nil
}

// isValidTargetType returns true if the target type is a recognized ELBv2 value.
func isValidTargetType(tt string) bool {
	switch tt {
	case "instance", "ip", targetTypeLambda, "alb":
		return true
	}

	return false
}

// isValidTGProtocol returns true if the protocol is accepted for non-lambda target groups.
func isValidTGProtocol(proto string) bool {
	switch proto {
	case protoHTTP, protoHTTPS, "TCP", protoTLS, "UDP", "TCP_UDP", "GENEVE":
		return true
	}

	return false
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
func lbDNSName(name, lbType, region string) string {
	const fixedID = "00000001"
	switch lbType {
	case lbTypeNetwork:
		return fmt.Sprintf("%s-%s.elb.%s.amazonaws.com", name, fixedID, region)
	default:
		return fmt.Sprintf("%s-%s.%s.elb.amazonaws.com", name, fixedID, region)
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

	if err := validateResourceName(input.Name, "load balancer"); err != nil {
		return nil, err
	}

	for _, lb := range b.loadBalancers {
		if lb.LoadBalancerName == input.Name {
			return nil, ErrLoadBalancerAlreadyExists
		}
	}

	lbArn := b.lbARN(input.Name)

	lbType := input.Type
	switch lbType {
	case "", lbTypeApplication:
		lbType = lbTypeApplication
	case "network", "gateway":
		// valid as-is
	default:
		return nil, fmt.Errorf(
			"%w: invalid Type %q; must be application, network, or gateway",
			ErrInvalidParameter, lbType,
		)
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

	defaultAttrs := map[string]string{
		"access_logs.s3.enabled":                          attrValueFalse,
		"deletion_protection.enabled":                     attrValueFalse,
		"idle_timeout.timeout_seconds":                    "60",
		"routing.http2.enabled":                           attrValueTrue,
		"routing.http.desync_mitigation_mode":             "defensive",
		"routing.http.drop_invalid_header_fields.enabled": attrValueFalse,
		"routing.http.preserve_host_header.enabled":       attrValueFalse,
		"routing.http.xff_client_port.enabled":            attrValueFalse,
		"routing.http.xff_header_processing.mode":         "append",
		"load_balancing.cross_zone.enabled":               attrValueTrue,
		"waf.fail_open.enabled":                           attrValueFalse,
	}

	lb := &LoadBalancer{
		LoadBalancerArn:       lbArn,
		LoadBalancerName:      input.Name,
		DNSName:               lbDNSName(input.Name, lbType, b.region),
		CanonicalHostedZoneID: canonicalHostedZoneIDForLB(lbType, b.region),
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
		Attributes: defaultAttrs,
		Tags:       t,
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

	// Cascade: delete all listeners and their rules.
	for listenerArn, l := range b.listeners {
		if l.LoadBalancerArn != lbArn {
			continue
		}

		for ruleArn, r := range b.rules {
			if r.ListenerArn == listenerArn {
				r.Tags.Close()
				delete(b.rules, ruleArn)
			}
		}

		l.Tags.Close()
		delete(b.listeners, listenerArn)
	}

	b.loadBalancers[lbArn].Tags.Close()
	delete(b.loadBalancers, lbArn)

	return nil
}

// ModifyLoadBalancerAttributes updates attributes on a load balancer.
func (b *InMemoryBackend) ModifyLoadBalancerAttributes(lbArn string, attrs map[string]string) (*LoadBalancer, error) {
	b.mu.Lock("ModifyLoadBalancerAttributes")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers[lbArn]
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if lb.Attributes == nil {
		lb.Attributes = make(map[string]string)
	}

	maps.Copy(lb.Attributes, attrs)

	cp := *lb

	return &cp, nil
}

// SetSecurityGroups updates the security groups associated with a load balancer.
func (b *InMemoryBackend) SetSecurityGroups(lbArn string, sgs []string) (*LoadBalancer, error) {
	b.mu.Lock("SetSecurityGroups")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers[lbArn]
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	lb.SecurityGroups = sgs
	cp := *lb

	return &cp, nil
}

// SetSubnets updates the availability zones / subnets associated with a load balancer.
func (b *InMemoryBackend) SetSubnets(lbArn string, azs []string) (*LoadBalancer, error) {
	b.mu.Lock("SetSubnets")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers[lbArn]
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	lb.AvailabilityZones = azs
	cp := *lb

	return &cp, nil
}

// SetIPAddressType updates the IP address type of a load balancer.
func (b *InMemoryBackend) SetIPAddressType(lbArn string, ipType string) (*LoadBalancer, error) {
	b.mu.Lock("SetIPAddressType")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers[lbArn]
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	switch ipType {
	case "ipv4", "dualstack", "dualstack-without-public-ipv4":
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: invalid IpAddressType %q; must be ipv4, dualstack, or dualstack-without-public-ipv4",
			ErrInvalidParameter, ipType,
		)
	}

	lb.IPAddressType = ipType
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

	if err := validateResourceName(input.Name, "target group"); err != nil {
		return nil, err
	}

	for _, tg := range b.targetGroups {
		if tg.TargetGroupName == input.Name {
			return nil, ErrTargetGroupAlreadyExists
		}
	}

	tgArn := b.tgARN(input.Name)

	targetType := input.TargetType
	if targetType == "" {
		targetType = "instance"
	}

	if !isValidTargetType(targetType) {
		return nil, fmt.Errorf(
			"%w: invalid TargetType %q; must be instance, ip, lambda, or alb",
			ErrInvalidParameter, targetType,
		)
	}

	proto := input.Protocol
	if targetType == targetTypeLambda {
		// Lambda target groups have no protocol or port.
		proto = ""
	} else {
		if proto == "" {
			proto = protoHTTP
		}

		if !isValidTGProtocol(proto) {
			return nil, fmt.Errorf(
				"%w: invalid Protocol %q for target group",
				ErrInvalidParameter, proto,
			)
		}
	}

	t := tags.New("elbv2.tg." + input.Name + ".tags")
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	// Apply health-check defaults.
	input = applyTGHealthCheckDefaults(proto, input)
	matcher := defaultTGMatcher(input.HealthCheckProtocol, input.Matcher)

	tg := &TargetGroup{
		TargetGroupArn:             tgArn,
		TargetGroupName:            input.Name,
		Protocol:                   proto,
		ProtocolVersion:            input.ProtocolVersion,
		Port:                       input.Port,
		VpcID:                      input.VpcID,
		TargetType:                 targetType,
		HealthCheckProtocol:        input.HealthCheckProtocol,
		HealthCheckPort:            input.HealthCheckPort,
		HealthCheckPath:            input.HealthCheckPath,
		Matcher:                    matcher,
		HealthCheckEnabled:         input.HealthCheckEnabled,
		HealthCheckIntervalSeconds: input.HealthCheckIntervalSeconds,
		HealthCheckTimeoutSeconds:  input.HealthCheckTimeoutSeconds,
		HealthyThresholdCount:      input.HealthyThresholdCount,
		UnhealthyThresholdCount:    input.UnhealthyThresholdCount,
		CrossZoneLoadBalancing:     true,
		Targets:                    []Target{},
		TargetGroupAttributes: map[string]string{
			"deregistration_delay.timeout_seconds": "300",
			"stickiness.enabled":                   attrValueFalse,
			"stickiness.type":                      "lb_cookie",
			"load_balancing.algorithm.type":        "round_robin",
			"slow_start.duration_seconds":          "0",
		},
		Tags: t,
	}

	b.targetGroups[tgArn] = tg

	cp := *tg

	return &cp, nil
}

func applyTGHealthCheckDefaults(proto string, input CreateTargetGroupInput) CreateTargetGroupInput {
	if input.HealthCheckProtocol == "" {
		input.HealthCheckProtocol = proto
	}

	if input.HealthCheckPort == "" {
		input.HealthCheckPort = "traffic-port"
	}

	if input.HealthCheckPath == "" && (proto == protoHTTP || proto == protoHTTPS) {
		input.HealthCheckPath = "/"
	}

	if input.HealthCheckIntervalSeconds == 0 {
		input.HealthCheckIntervalSeconds = 30
	}

	if input.HealthCheckTimeoutSeconds == 0 {
		input.HealthCheckTimeoutSeconds = 5
	}

	if input.HealthyThresholdCount == 0 {
		input.HealthyThresholdCount = 3
	}

	if input.UnhealthyThresholdCount == 0 {
		input.UnhealthyThresholdCount = 3
	}

	return input
}

func defaultTGMatcher(hcProtocol string, matcher Matcher) Matcher {
	if matcher.HTTPCode == "" && matcher.GrpcCode == "" && (hcProtocol == protoHTTP || hcProtocol == protoHTTPS) {
		matcher.HTTPCode = "200"
	}

	return matcher
}

func collectTGArns(actions []Action, arns map[string]bool) {
	for _, a := range actions {
		if a.TargetGroupArn != "" {
			arns[a.TargetGroupArn] = true
		}

		if a.ForwardConfig != nil {
			for _, tgt := range a.ForwardConfig.TargetGroups {
				if tgt.TargetGroupArn != "" {
					arns[tgt.TargetGroupArn] = true
				}
			}
		}
	}
}

func collectLBArnsForTG(lbArn string, actions []Action, result map[string]map[string]bool) {
	for _, a := range actions {
		if a.TargetGroupArn != "" {
			if result[a.TargetGroupArn] == nil {
				result[a.TargetGroupArn] = make(map[string]bool)
			}

			result[a.TargetGroupArn][lbArn] = true
		}

		if a.ForwardConfig != nil {
			for _, tgt := range a.ForwardConfig.TargetGroups {
				if tgt.TargetGroupArn != "" {
					if result[tgt.TargetGroupArn] == nil {
						result[tgt.TargetGroupArn] = make(map[string]bool)
					}

					result[tgt.TargetGroupArn][lbArn] = true
				}
			}
		}
	}
}

func actionsReferenceTG(actions []Action, tgArn string) bool {
	for _, a := range actions {
		if a.TargetGroupArn == tgArn {
			return true
		}

		if a.ForwardConfig != nil {
			for _, tgt := range a.ForwardConfig.TargetGroups {
				if tgt.TargetGroupArn == tgArn {
					return true
				}
			}
		}
	}

	return false
}

// tgArnsForLB returns the set of target group ARNs referenced by listeners and rules on the given LB.
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) tgArnsForLB(lbArn string) map[string]bool {
	arns := make(map[string]bool)

	for _, l := range b.listeners {
		if l.LoadBalancerArn != lbArn {
			continue
		}

		collectTGArns(l.DefaultActions, arns)

		for _, r := range b.rules {
			if r.ListenerArn == l.ListenerArn {
				collectTGArns(r.Actions, arns)
			}
		}
	}

	return arns
}

// tgToLBArnsLocked returns a map from target group ARN to the set of LB ARNs that reference it.
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) tgToLBArnsLocked() map[string]map[string]bool {
	result := make(map[string]map[string]bool)

	for _, l := range b.listeners {
		collectLBArnsForTG(l.LoadBalancerArn, l.DefaultActions, result)

		for _, r := range b.rules {
			if r.ListenerArn == l.ListenerArn {
				collectLBArnsForTG(l.LoadBalancerArn, r.Actions, result)
			}
		}
	}

	return result
}

// DescribeTargetGroups returns target groups filtered by ARNs, names, or load balancer ARN.
// The returned TargetGroup values contain a Tags pointer that is backend-owned; callers must treat it as read-only.
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

	var lbTGArns map[string]bool
	if lbArn != "" {
		lbTGArns = b.tgArnsForLB(lbArn)
	}

	// Build TG -> LB ARNs mapping to populate LoadBalancerArns field.
	tgLBMap := b.tgToLBArnsLocked()

	result := make([]TargetGroup, 0)

	for _, tg := range b.targetGroups {
		if len(arns) > 0 && !arnSet[tg.TargetGroupArn] {
			continue
		}

		if len(names) > 0 && !nameSet[tg.TargetGroupName] {
			continue
		}

		if lbTGArns != nil && !lbTGArns[tg.TargetGroupArn] {
			continue
		}

		cp := *tg

		// Populate LoadBalancerArns from the computed map.
		lbArns := make([]string, 0, len(tgLBMap[tg.TargetGroupArn]))
		for lb := range tgLBMap[tg.TargetGroupArn] {
			lbArns = append(lbArns, lb)
		}

		sort.Strings(lbArns)
		cp.LoadBalancerArns = lbArns
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TargetGroupName < result[j].TargetGroupName
	})

	return result, nil
}

// isTGInUseLocked returns true if the target group ARN is referenced by any listener or rule.
// Caller must hold b.mu (read or write).
func (b *InMemoryBackend) isTGInUseLocked(tgArn string) bool {
	for _, l := range b.listeners {
		if actionsReferenceTG(l.DefaultActions, tgArn) {
			return true
		}
	}

	for _, r := range b.rules {
		if actionsReferenceTG(r.Actions, tgArn) {
			return true
		}
	}

	return false
}

// DeleteTargetGroup deletes a target group by ARN.
func (b *InMemoryBackend) DeleteTargetGroup(tgArn string) error {
	b.mu.Lock("DeleteTargetGroup")
	defer b.mu.Unlock()

	if _, ok := b.targetGroups[tgArn]; !ok {
		return ErrTargetGroupNotFound
	}

	if b.isTGInUseLocked(tgArn) {
		return fmt.Errorf("%w: target group %s is still in use by a listener or rule", ErrTargetGroupInUse, tgArn)
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
		existing[t.ID+":"+strconv.Itoa(int(t.Port))] = true
	}

	for _, t := range targets {
		key := t.ID + ":" + strconv.Itoa(int(t.Port))
		if !existing[key] {
			tg.Targets = append(tg.Targets, t)
			existing[key] = true
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
		remove[t.ID+":"+strconv.Itoa(int(t.Port))] = true
	}

	remaining := make([]Target, 0, len(tg.Targets))

	for _, t := range tg.Targets {
		if !remove[t.ID+":"+strconv.Itoa(int(t.Port))] {
			remaining = append(remaining, t)
		}
	}

	tg.Targets = remaining

	return nil
}

// DescribeTargetHealth returns health descriptions for targets registered with the target group.
func (b *InMemoryBackend) DescribeTargetHealth(tgArn string) ([]TargetHealthDescription, error) {
	b.mu.RLock("DescribeTargetHealth")
	defer b.mu.RUnlock()

	tg, ok := b.targetGroups[tgArn]
	if !ok {
		return nil, ErrTargetGroupNotFound
	}

	result := make([]TargetHealthDescription, len(tg.Targets))
	for i, t := range tg.Targets {
		result[i] = TargetHealthDescription{
			Target:      t,
			HealthState: "healthy",
		}
	}

	return result, nil
}

const (
	protoHTTP         = "HTTP"
	protoHTTPS        = "HTTPS"
	protoTLS          = "TLS"
	lbTypeApplication = "application"
	lbTypeNetwork     = "network"
	targetTypeLambda  = "lambda"
	priorityDefault   = "default"
)

func isALBProtocol(proto string) bool {
	switch proto {
	case protoHTTP, protoHTTPS:
		return true
	}

	return false
}

func isNLBProtocol(proto string) bool {
	switch proto {
	case "TCP", "UDP", protoTLS, "TCP_UDP":
		return true
	}

	return false
}

func isGWLBProtocol(proto string) bool { return proto == "GENEVE" }

func validateListenerProtocol(lbType, proto string) error {
	switch lbType {
	case lbTypeApplication:
		if !isALBProtocol(proto) {
			return fmt.Errorf(
				"%w: protocol %s is not supported for Application Load Balancers; use HTTP or HTTPS",
				ErrInvalidConfigurationRequest, proto,
			)
		}
	case "network":
		if !isNLBProtocol(proto) {
			return fmt.Errorf(
				"%w: protocol %s is not supported for Network Load Balancers; use TCP, UDP, TLS, or TCP_UDP",
				ErrInvalidConfigurationRequest, proto,
			)
		}
	case "gateway":
		if !isGWLBProtocol(proto) {
			return fmt.Errorf(
				"%w: protocol %s is not supported for Gateway Load Balancers; use GENEVE",
				ErrInvalidConfigurationRequest, proto,
			)
		}
	}

	return nil
}

func requireCertsForProtocol(proto string, certs []Certificate) error {
	if (proto == protoHTTPS || proto == protoTLS) && len(certs) == 0 {
		return fmt.Errorf(
			"%w: %s listener requires at least one certificate",
			ErrInvalidParameter, proto,
		)
	}

	return nil
}

func checkDuplicateListenerPort(listeners map[string]*Listener, lbArn string, port int32) error {
	for _, existing := range listeners {
		if existing.LoadBalancerArn == lbArn && existing.Port == port {
			return fmt.Errorf(
				"%w: a listener on port %d already exists on this load balancer",
				ErrDuplicateListener, port,
			)
		}
	}

	return nil
}

// CreateListener creates a new listener on a load balancer.
func (b *InMemoryBackend) CreateListener(input CreateListenerInput) (*Listener, error) {
	b.mu.Lock("CreateListener")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers[input.LoadBalancerArn]
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	// Validate protocol against LB type.
	proto := input.Protocol
	if err := validateListenerProtocol(lb.Type, proto); err != nil {
		return nil, err
	}

	if err := requireCertsForProtocol(proto, input.Certificates); err != nil {
		return nil, err
	}

	if err := checkDuplicateListenerPort(b.listeners, input.LoadBalancerArn, input.Port); err != nil {
		return nil, err
	}

	listenerArn := b.listenerARN(lb.LoadBalancerName, input.Port)

	t := tags.New(fmt.Sprintf("elbv2.listener.%s.%d.tags", lb.LoadBalancerName, input.Port))
	for _, kv := range input.Tags {
		t.Set(kv.Key, kv.Value)
	}

	// Initialize default listener attributes based on protocol.
	listenerAttrs := map[string]string{}
	if proto == protoHTTP || proto == protoHTTPS {
		listenerAttrs["routing.http2.enabled"] = attrValueTrue
		listenerAttrs["idle_timeout.timeout_seconds"] = "60"
		listenerAttrs["routing.http.desync_mitigation_mode"] = "defensive"
	}

	listener := &Listener{
		ListenerArn:          listenerArn,
		LoadBalancerArn:      input.LoadBalancerArn,
		Protocol:             proto,
		Port:                 input.Port,
		DefaultActions:       input.DefaultActions,
		Certificates:         input.Certificates,
		SSLPolicy:            input.SSLPolicy,
		AlpnPolicy:           input.AlpnPolicy,
		MutualAuthentication: input.MutualAuthentication,
		Attributes:           listenerAttrs,
		Tags:                 t,
	}

	b.listeners[listenerArn] = listener

	// Auto-create default rule (AWS behaviour: every listener has a default rule).
	defaultRuleArn := b.ruleARN(listenerArn, priorityDefault)
	defaultTags := tags.New("elbv2.rule." + defaultRuleArn + ".tags")
	defaultActions := make([]Action, len(input.DefaultActions))
	copy(defaultActions, input.DefaultActions)
	b.rules[defaultRuleArn] = &Rule{
		RuleArn:     defaultRuleArn,
		ListenerArn: listenerArn,
		Priority:    priorityDefault,
		IsDefault:   true,
		Actions:     defaultActions,
		Tags:        defaultTags,
	}

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
		return result[i].Port < result[j].Port
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

	// Cascade: delete all rules belonging to this listener.
	for ruleArn, r := range b.rules {
		if r.ListenerArn == listenerArn {
			r.Tags.Close()
			delete(b.rules, ruleArn)
		}
	}

	b.listeners[listenerArn].Tags.Close()
	delete(b.listeners, listenerArn)

	return nil
}

// syncDefaultRuleActions updates the default rule's actions to match the listener's new default actions.
// Caller must hold b.mu (write).
func (b *InMemoryBackend) syncDefaultRuleActions(listenerArn string, actions []Action) {
	for _, r := range b.rules {
		if r.ListenerArn == listenerArn && r.IsDefault {
			actsCopy := make([]Action, len(actions))
			copy(actsCopy, actions)
			r.Actions = actsCopy

			break
		}
	}
}

// ModifyListener updates the properties of an existing listener.
func (b *InMemoryBackend) ModifyListener(input ModifyListenerInput) (*Listener, error) {
	b.mu.Lock("ModifyListener")
	defer b.mu.Unlock()

	l, ok := b.listeners[input.ListenerArn]
	if !ok {
		return nil, ErrListenerNotFound
	}

	if err := b.applyListenerProtocol(l, input); err != nil {
		return nil, err
	}

	if input.Port != 0 && input.Port != l.Port {
		if err := checkDuplicateListenerPort(b.listeners, l.LoadBalancerArn, input.Port); err != nil {
			return nil, err
		}

		l.Port = input.Port
	}

	if len(input.DefaultActions) > 0 {
		l.DefaultActions = input.DefaultActions
		b.syncDefaultRuleActions(input.ListenerArn, input.DefaultActions)
	}

	if len(input.Certificates) > 0 {
		l.Certificates = input.Certificates
	}

	if input.SSLPolicy != "" {
		l.SSLPolicy = input.SSLPolicy
	}

	if input.AlpnPolicy != "" {
		l.AlpnPolicy = input.AlpnPolicy
	}

	if input.MutualAuthentication != nil {
		l.MutualAuthentication = input.MutualAuthentication
	}

	cp := *l

	return &cp, nil
}

// applyListenerProtocol validates and applies a protocol change to a listener.
// Caller must hold b.mu (write).
func (b *InMemoryBackend) applyListenerProtocol(l *Listener, input ModifyListenerInput) error {
	if input.Protocol == "" {
		return nil
	}

	if lb, ok := b.loadBalancers[l.LoadBalancerArn]; ok {
		if err := validateListenerProtocol(lb.Type, input.Protocol); err != nil {
			return err
		}
	}

	candidateCerts := l.Certificates
	if len(input.Certificates) > 0 {
		candidateCerts = input.Certificates
	}

	if err := requireCertsForProtocol(input.Protocol, candidateCerts); err != nil {
		return err
	}

	l.Protocol = input.Protocol

	return nil
}

// CreateRule creates a new rule on a listener.
func (b *InMemoryBackend) CreateRule(input CreateRuleInput) (*Rule, error) {
	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	if _, ok := b.listeners[input.ListenerArn]; !ok {
		return nil, ErrListenerNotFound
	}

	// Validate and check for duplicate priority.
	if input.Priority != "" && input.Priority != priorityDefault {
		p, parseErr := strconv.ParseInt(input.Priority, 10, 32)
		if parseErr != nil || p < 1 || p > 50000 {
			return nil, fmt.Errorf("%w: priority must be an integer between 1 and 50000", ErrInvalidParameter)
		}

		for _, r := range b.rules {
			if r.ListenerArn == input.ListenerArn && r.Priority == input.Priority {
				return nil, fmt.Errorf("%w: priority %s already in use", ErrDuplicateRulePriority, input.Priority)
			}
		}
	}

	b.ruleCounter++
	ruleArn := b.ruleARN(input.ListenerArn, strconv.Itoa(b.ruleCounter))

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
		Conditions:  input.Conditions,
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

	// Sort numerically by priority; "default" sorts last (highest priority number).
	sort.Slice(result, func(i, j int) bool {
		pi, pj := result[i].Priority, result[j].Priority
		if pi == pj {
			return false
		}

		if pi == priorityDefault {
			return false
		}

		if pj == priorityDefault {
			return true
		}

		ni, errI := strconv.Atoi(pi)
		nj, errJ := strconv.Atoi(pj)
		if errI != nil || errJ != nil {
			return pi < pj
		}

		return ni < nj
	})

	return result, nil
}

// DeleteRule deletes a rule by ARN.
func (b *InMemoryBackend) DeleteRule(ruleArn string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	rule, ok := b.rules[ruleArn]
	if !ok {
		return ErrRuleNotFound
	}

	if rule.IsDefault {
		return fmt.Errorf("%w: cannot delete the default rule of a listener", ErrOperationNotPermitted)
	}

	rule.Tags.Close()
	delete(b.rules, ruleArn)

	return nil
}

// ModifyRule updates the actions and/or conditions of an existing rule.
func (b *InMemoryBackend) ModifyRule(ruleArn string, actions []Action, conditions []Condition) (*Rule, error) {
	b.mu.Lock("ModifyRule")
	defer b.mu.Unlock()

	rule, ok := b.rules[ruleArn]
	if !ok {
		return nil, ErrRuleNotFound
	}

	if len(actions) > 0 {
		rule.Actions = actions
	}

	if len(conditions) > 0 {
		rule.Conditions = conditions
	}

	cp := *rule

	return &cp, nil
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

	if ts, ok := b.trustStores[resArn]; ok {
		return ts.Tags
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
		Revocations:   []TrustStoreRevocation{},
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
func (b *InMemoryBackend) AddTrustStoreRevocations(trustStoreArn string, revocations []TrustStoreRevocation) error {
	b.mu.Lock("AddTrustStoreRevocations")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[trustStoreArn]
	if !ok {
		return ErrTrustStoreNotFound
	}

	ts.Revocations = append(ts.Revocations, revocations...)

	return nil
}

// DescribeTrustStoreAssociations returns listener ARNs whose trust store is set to this ARN.
func (b *InMemoryBackend) DescribeTrustStoreAssociations(trustStoreArn string) ([]string, error) {
	b.mu.RLock("DescribeTrustStoreAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.trustStores[trustStoreArn]; !ok {
		return nil, ErrTrustStoreNotFound
	}

	var result []string

	for _, l := range b.listeners {
		if l.MutualAuthentication != nil && l.MutualAuthentication.TrustStoreArn == trustStoreArn {
			result = append(result, l.ListenerArn)
		}
	}

	if result == nil {
		result = []string{}
	}

	return result, nil
}

// AddListenerCertificates adds certificates to a listener.
func (b *InMemoryBackend) AddListenerCertificates(listenerArn string, certs []Certificate) error {
	b.mu.Lock("AddListenerCertificates")
	defer b.mu.Unlock()

	listener, ok := b.listeners[listenerArn]
	if !ok {
		return ErrListenerNotFound
	}

	existing := make(map[string]bool, len(listener.Certificates))
	for _, c := range listener.Certificates {
		existing[c.CertificateArn] = true
	}

	for _, c := range certs {
		if !existing[c.CertificateArn] {
			listener.Certificates = append(listener.Certificates, c)
			existing[c.CertificateArn] = true
		}
	}

	return nil
}

// DescribeListenerCertificates returns certificates on a listener.
func (b *InMemoryBackend) DescribeListenerCertificates(listenerArn string) ([]Certificate, error) {
	b.mu.RLock("DescribeListenerCertificates")
	defer b.mu.RUnlock()

	listener, ok := b.listeners[listenerArn]
	if !ok {
		return nil, ErrListenerNotFound
	}

	result := make([]Certificate, len(listener.Certificates))
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

	remaining := make([]Certificate, 0, len(listener.Certificates))
	for _, c := range listener.Certificates {
		if !remove[c.CertificateArn] {
			remaining = append(remaining, c)
		}
	}

	if len(remaining) == 0 && (listener.Protocol == protoHTTPS || listener.Protocol == protoTLS) {
		return fmt.Errorf(
			"%w: Cannot remove the last certificate from an HTTPS/TLS listener",
			ErrInvalidParameter,
		)
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

// RemoveTrustStoreRevocations removes revocation entries from a trust store by RevocationID.
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

	remaining := make([]TrustStoreRevocation, 0, len(ts.Revocations))
	for _, r := range ts.Revocations {
		if !remove[r.RevocationID] {
			remaining = append(remaining, r)
		}
	}

	ts.Revocations = remaining

	return nil
}

// DescribeTrustStoreRevocations returns revocation entries for a trust store.
func (b *InMemoryBackend) DescribeTrustStoreRevocations(trustStoreArn string) ([]TrustStoreRevocation, error) {
	b.mu.RLock("DescribeTrustStoreRevocations")
	defer b.mu.RUnlock()

	ts, ok := b.trustStores[trustStoreArn]
	if !ok {
		return nil, ErrTrustStoreNotFound
	}

	result := make([]TrustStoreRevocation, len(ts.Revocations))
	copy(result, ts.Revocations)

	return result, nil
}

// SetRulePriorities updates the priorities of one or more rules.
func (b *InMemoryBackend) SetRulePriorities(priorities []RulePriority) ([]Rule, error) {
	b.mu.Lock("SetRulePriorities")
	defer b.mu.Unlock()

	// Check for duplicates within the request.
	seen := make(map[string]bool, len(priorities))
	for _, p := range priorities {
		if seen[p.Priority] {
			return nil, fmt.Errorf("%w: priority %s specified more than once", ErrDuplicateRulePriority, p.Priority)
		}

		seen[p.Priority] = true
	}

	// Validate all rules exist and none is a default rule (AWS does not allow reordering defaults).
	for _, p := range priorities {
		r, ok := b.rules[p.RuleArn]
		if !ok {
			return nil, ErrRuleNotFound
		}

		if r.IsDefault {
			return nil, fmt.Errorf("%w: cannot set priority on the default rule", ErrOperationNotPermitted)
		}
	}

	result := make([]Rule, 0, len(priorities))

	for _, p := range priorities {
		r := b.rules[p.RuleArn]
		r.Priority = p.Priority
		result = append(result, *r)
	}

	return result, nil
}

// ModifyTargetGroup updates health-check settings on a target group.
func (b *InMemoryBackend) ModifyTargetGroup(input ModifyTargetGroupInput) (*TargetGroup, error) {
	b.mu.Lock("ModifyTargetGroup")
	defer b.mu.Unlock()

	tg, ok := b.targetGroups[input.TargetGroupArn]
	if !ok {
		return nil, ErrTargetGroupNotFound
	}

	if input.HealthCheckProtocol != "" {
		tg.HealthCheckProtocol = input.HealthCheckProtocol
	}

	if input.HealthCheckPort != "" {
		tg.HealthCheckPort = input.HealthCheckPort
	}

	if input.HealthCheckPath != "" {
		tg.HealthCheckPath = input.HealthCheckPath
	}

	if input.Matcher.HTTPCode != "" || input.Matcher.GrpcCode != "" {
		tg.Matcher = input.Matcher
	}

	if input.HealthCheckEnabled != nil {
		tg.HealthCheckEnabled = *input.HealthCheckEnabled
	}

	if input.HealthCheckIntervalSeconds != 0 {
		tg.HealthCheckIntervalSeconds = input.HealthCheckIntervalSeconds
	}

	if input.HealthCheckTimeoutSeconds != 0 {
		tg.HealthCheckTimeoutSeconds = input.HealthCheckTimeoutSeconds
	}

	if input.HealthyThresholdCount != 0 {
		tg.HealthyThresholdCount = input.HealthyThresholdCount
	}

	if input.UnhealthyThresholdCount != 0 {
		tg.UnhealthyThresholdCount = input.UnhealthyThresholdCount
	}

	cp := *tg

	return &cp, nil
}

// ModifyTargetGroupAttributes updates attributes on a target group.
func (b *InMemoryBackend) ModifyTargetGroupAttributes(tgArn string, attrs map[string]string) (*TargetGroup, error) {
	b.mu.Lock("ModifyTargetGroupAttributes")
	defer b.mu.Unlock()

	tg, ok := b.targetGroups[tgArn]
	if !ok {
		return nil, ErrTargetGroupNotFound
	}

	if tg.TargetGroupAttributes == nil {
		tg.TargetGroupAttributes = make(map[string]string)
	}

	maps.Copy(tg.TargetGroupAttributes, attrs)

	cp := *tg

	return &cp, nil
}

// DescribeTargetGroupAttributes returns attributes for a target group.
func (b *InMemoryBackend) DescribeTargetGroupAttributes(tgArn string) (map[string]string, error) {
	b.mu.RLock("DescribeTargetGroupAttributes")
	defer b.mu.RUnlock()

	tg, ok := b.targetGroups[tgArn]
	if !ok {
		return nil, ErrTargetGroupNotFound
	}

	result := make(map[string]string, len(tg.TargetGroupAttributes))
	maps.Copy(result, tg.TargetGroupAttributes)

	return result, nil
}

// ModifyListenerAttributes updates attributes on a listener.
func (b *InMemoryBackend) ModifyListenerAttributes(listenerArn string, attrs map[string]string) (*Listener, error) {
	b.mu.Lock("ModifyListenerAttributes")
	defer b.mu.Unlock()

	l, ok := b.listeners[listenerArn]
	if !ok {
		return nil, ErrListenerNotFound
	}

	if l.Attributes == nil {
		l.Attributes = make(map[string]string)
	}

	maps.Copy(l.Attributes, attrs)

	cp := *l

	return &cp, nil
}

// DescribeListenerAttributes returns attributes for a listener.
func (b *InMemoryBackend) DescribeListenerAttributes(listenerArn string) (map[string]string, error) {
	b.mu.RLock("DescribeListenerAttributes")
	defer b.mu.RUnlock()

	l, ok := b.listeners[listenerArn]
	if !ok {
		return nil, ErrListenerNotFound
	}

	result := make(map[string]string, len(l.Attributes))
	maps.Copy(result, l.Attributes)

	return result, nil
}
