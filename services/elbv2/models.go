package elbv2

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// LoadBalancerState represents the state of a load balancer.
type LoadBalancerState struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

// AvailabilityZone holds the zone name and subnet ID for an LB availability zone mapping.
type AvailabilityZone struct {
	ZoneName string `json:"zoneName"`
	SubnetID string `json:"subnetId,omitempty"`
}

// SubnetMapping holds subnet configuration for CreateLoadBalancer and SetSubnets.
type SubnetMapping struct {
	SubnetID           string
	AllocationID       string
	PrivateIPv4Address string
	IPv6Address        string
}

// CapacityReservation holds the capacity reservation state for a load balancer,
// as set by ModifyCapacityReservation and read by DescribeCapacityReservation.
type CapacityReservation struct {
	LastModifiedTime          time.Time `json:"lastModifiedTime"`
	MinimumCapacityUnits      int32     `json:"minimumCapacityUnits"`
	DecreaseRequestsRemaining int32     `json:"decreaseRequestsRemaining"`
}

// LoadBalancer represents an ELBv2 load balancer.
type LoadBalancer struct {
	CreatedTime           time.Time            `json:"createdTime"`
	State                 LoadBalancerState    `json:"state"`
	Tags                  *tags.Tags           `json:"tags,omitempty"`
	Attributes            map[string]string    `json:"attributes,omitempty"`
	CapacityReservation   *CapacityReservation `json:"capacityReservation,omitempty"`
	LoadBalancerArn       string               `json:"loadBalancerArn"`
	LoadBalancerName      string               `json:"loadBalancerName"`
	DNSName               string               `json:"dnsName"`
	CanonicalHostedZoneID string               `json:"canonicalHostedZoneId"`
	VpcID                 string               `json:"vpcId"`
	Scheme                string               `json:"scheme"`
	Type                  string               `json:"type"`
	IPAddressType         string               `json:"ipAddressType"`
	IPv4IPAMPoolID        string               `json:"ipv4IpamPoolId,omitempty"`
	AvailabilityZones     []AvailabilityZone   `json:"availabilityZones"`
	SecurityGroups        []string             `json:"securityGroups"`
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
	ID           string `json:"id"`
	HealthState  string `json:"healthState,omitempty"`
	HealthReason string `json:"healthReason,omitempty"`
	Port         int32  `json:"port"`
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
	// RegexValues holds regular expressions to match instead of exact/wildcard
	// Values. Valid only for host-header, path-pattern, and http-header conditions
	// (types.RuleCondition.RegexValues / HostHeaderConditionConfig.RegexValues /
	// PathPatternConditionConfig.RegexValues / HttpHeaderConditionConfig.RegexValues).
	RegexValues []string `json:"regexValues,omitempty"`
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
	// AlpnPolicy is a list on the wire (AlpnPolicy.member.N / <AlpnPolicy><member>…),
	// not a bare string — verified against aws-sdk-go-v2 types.Listener.AlpnPolicy ([]string).
	AlpnPolicy     []string      `json:"alpnPolicy,omitempty"`
	DefaultActions []Action      `json:"defaultActions"`
	Certificates   []Certificate `json:"certificates,omitempty"`
	Port           int32         `json:"port"`
}

// Rule represents an ELBv2 listener rule.
type Rule struct {
	Tags        *tags.Tags      `json:"tags,omitempty"`
	RuleArn     string          `json:"ruleArn"`
	ListenerArn string          `json:"listenerArn"`
	Priority    string          `json:"priority"`
	Actions     []Action        `json:"actions"`
	Conditions  []Condition     `json:"conditions,omitempty"`
	Transforms  []RuleTransform `json:"transforms,omitempty"`
	IsDefault   bool            `json:"isDefault"`
}

// RewriteConfig holds a regex/replace pair applied by a host-header-rewrite or
// url-rewrite transform. Both Regex and Replace are required on the real wire
// (types.RewriteConfig).
type RewriteConfig struct {
	Regex   string `json:"regex"`
	Replace string `json:"replace"`
}

// HostHeaderRewriteConfig holds the rewrite rules for a host-header-rewrite transform.
type HostHeaderRewriteConfig struct {
	Rewrites []RewriteConfig `json:"rewrites,omitempty"`
}

// URLRewriteConfig holds the rewrite rules for a url-rewrite transform.
type URLRewriteConfig struct {
	Rewrites []RewriteConfig `json:"rewrites,omitempty"`
}

// RuleTransform represents a single request transform applied before forwarding to
// targets. Type is one of "host-header-rewrite"/"url-rewrite" (types.TransformTypeEnum);
// exactly one of HostHeaderRewriteConfig/URLRewriteConfig is set, matching Type.
type RuleTransform struct {
	HostHeaderRewriteConfig *HostHeaderRewriteConfig `json:"hostHeaderRewriteConfig,omitempty"`
	URLRewriteConfig        *URLRewriteConfig        `json:"urlRewriteConfig,omitempty"`
	Type                    string                   `json:"type"`
}

// TrustStoreRevocation represents a single revocation entry stored in a trust store.
// RevocationID is int64 -- verified against aws-sdk-go-v2 types.TrustStoreRevocation /
// types.DescribeTrustStoreRevocation (RevocationId *int64). AWS assigns this ID itself
// when it parses the uploaded revocation file; it is never client-supplied.
type TrustStoreRevocation struct {
	RevocationType         string `json:"revocationType"`
	RevocationID           int64  `json:"revocationId"`
	NumberOfRevokedEntries int64  `json:"numberOfRevokedEntries"`
}

// RevocationContentInput represents a single revocation-file reference submitted to
// AddTrustStoreRevocations. Mirrors aws-sdk-go-v2 types.RevocationContent exactly --
// S3Bucket/S3Key/S3ObjectVersion/RevocationType is the only real wire shape; there is
// no plain/inline revocation-content field on the real API.
type RevocationContentInput struct {
	S3Bucket        string `json:"s3Bucket,omitempty"`
	S3Key           string `json:"s3Key,omitempty"`
	S3ObjectVersion string `json:"s3ObjectVersion,omitempty"`
	RevocationType  string `json:"revocationType,omitempty"`
}

// TrustStore represents an ELBv2 trust store. CaCertificatesBundleS3* fields
// are stored inertly -- this emulator has no real S3 to fetch the bundle
// from (see GetTrustStoreCaCertificatesBundle's always-empty Location), so
// they are recorded but never used to compute NumberOfCaCertificates or content.
type TrustStore struct {
	Tags                                *tags.Tags             `json:"tags,omitempty"`
	TrustStoreArn                       string                 `json:"trustStoreArn"`
	Name                                string                 `json:"name"`
	Status                              string                 `json:"status"`
	CaCertificatesBundleS3Bucket        string                 `json:"caCertificatesBundleS3Bucket,omitempty"`
	CaCertificatesBundleS3Key           string                 `json:"caCertificatesBundleS3Key,omitempty"`
	CaCertificatesBundleS3ObjectVersion string                 `json:"caCertificatesBundleS3ObjectVersion,omitempty"`
	Revocations                         []TrustStoreRevocation `json:"revocations,omitempty"`
	TotalRevokedEntries                 int64                  `json:"totalRevokedEntries"`
}

// CreateLoadBalancerInput holds the parameters for creating a load balancer.
type CreateLoadBalancerInput struct {
	Name           string
	Scheme         string
	Type           string
	IPAddressType  string
	Subnets        []string        // plain subnet IDs (Subnets.member.N)
	SubnetMappings []SubnetMapping // rich subnet mappings (SubnetMappings.member.N)
	SecurityGroups []string
	Tags           []tags.KV
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
	AlpnPolicy           []string
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
	AlpnPolicy           []string
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
	Transforms  []RuleTransform
	Tags        []tags.KV
}

// RulePriority holds an ARN-to-priority mapping used by SetRulePriorities.
type RulePriority struct {
	RuleArn  string
	Priority string
}
