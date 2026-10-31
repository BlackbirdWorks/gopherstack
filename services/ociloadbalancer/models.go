package ociloadbalancer

import "time"

// Load balancer lifecycle states.
const (
	stateCreating = "CREATING"
	stateActive   = "ACTIVE"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
)

// Tags are the shared tag sets of a load balancer.
type Tags struct {
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}

	if in.SecurityAttributes != nil {
		t.SecurityAttributes = in.SecurityAttributes
	}
}

// ShapeDetails are the flexible-shape bandwidth bounds in Mbps.
type ShapeDetails struct {
	Minimum int `json:"minimumBandwidthInMbps"`
	Maximum int `json:"maximumBandwidthInMbps"`
}

// ReservedIP references a reserved public IP.
type ReservedIP struct {
	ID string `json:"id,omitempty"`
}

// IPAddress is an address assigned to a load balancer.
type IPAddress struct {
	ReservedIP *ReservedIP `json:"reservedIp,omitempty"`
	IPAddress  string      `json:"ipAddress"`
	IsPublic   bool        `json:"isPublic"`
}

// SSLConfig is a listener or backend set TLS configuration.
type SSLConfig struct {
	VerifyDepth                    *int     `json:"verifyDepth,omitempty"`
	VerifyPeerCertificate          *bool    `json:"verifyPeerCertificate,omitempty"`
	HasSessionResumption           *bool    `json:"hasSessionResumption,omitempty"`
	CertificateName                string   `json:"certificateName,omitempty"`
	CipherSuiteName                string   `json:"cipherSuiteName,omitempty"`
	ServerOrderPreference          string   `json:"serverOrderPreference,omitempty"`
	TrustedCertificateAuthorityIDs []string `json:"trustedCertificateAuthorityIds,omitempty"`
	CertificateIDs                 []string `json:"certificateIds,omitempty"`
	Protocols                      []string `json:"protocols,omitempty"`
}

// ConnectionConfig tunes listener connections.
type ConnectionConfig struct {
	BackendTCPProxyProtocolVersion *int     `json:"backendTcpProxyProtocolVersion,omitempty"`
	BackendTCPProxyProtocolOptions []string `json:"backendTcpProxyProtocolOptions,omitempty"`
	IdleTimeout                    int64    `json:"idleTimeout"`
}

// Listener accepts client connections.
type Listener struct {
	SSL        *SSLConfig        `json:"sslConfiguration,omitempty"`
	Connection *ConnectionConfig `json:"connectionConfiguration,omitempty"`
	Name       string            `json:"name"`
	DefaultSet string            `json:"defaultBackendSetName"`
	Protocol   string            `json:"protocol"`
	PathRoutes string            `json:"pathRouteSetName,omitempty"`
	Policy     string            `json:"routingPolicyName,omitempty"`
	Address    string            `json:"gopherstackAddress,omitempty"`
	Hostnames  []string          `json:"hostnameNames,omitempty"`
	RuleSets   []string          `json:"ruleSetNames,omitempty"`
	Port       int               `json:"port"`
}

// Backend is a backend server.
type Backend struct {
	MaxConnections *int   `json:"maxConnections,omitempty"`
	Name           string `json:"name"`
	IPAddress      string `json:"ipAddress"`
	Port           int    `json:"port"`
	Weight         int    `json:"weight"`
	Drain          bool   `json:"drain"`
	Backup         bool   `json:"backup"`
	Offline        bool   `json:"offline"`
}

// HealthChecker is a backend set health check configuration.
type HealthChecker struct {
	Protocol          string `json:"protocol"`
	URLPath           string `json:"urlPath,omitempty"`
	ResponseBodyRegex string `json:"responseBodyRegex"`
	Port              int    `json:"port"`
	ReturnCode        int    `json:"returnCode"`
	Retries           int    `json:"retries"`
	TimeoutInMillis   int    `json:"timeoutInMillis"`
	IntervalInMillis  int    `json:"intervalInMillis"`
	IsForcePlainText  bool   `json:"isForcePlainText"`
}

// SessionPersistence is application-cookie session persistence.
type SessionPersistence struct {
	DisableFallback *bool  `json:"disableFallback,omitempty"`
	CookieName      string `json:"cookieName"`
}

// LBCookiePersistence is load-balancer-inserted cookie session persistence.
type LBCookiePersistence struct {
	DisableFallback *bool  `json:"disableFallback,omitempty"`
	MaxAgeInSeconds *int   `json:"maxAgeInSeconds,omitempty"`
	IsSecure        *bool  `json:"isSecure,omitempty"`
	IsHTTPOnly      *bool  `json:"isHttpOnly,omitempty"`
	CookieName      string `json:"cookieName,omitempty"`
	Domain          string `json:"domain,omitempty"`
	Path            string `json:"path,omitempty"`
}

// BackendSet groups backends under one balancing policy and health check.
type BackendSet struct {
	SSL                   *SSLConfig           `json:"sslConfiguration,omitempty"`
	SessionPersistence    *SessionPersistence  `json:"sessionPersistenceConfiguration,omitempty"`
	LBCookiePersistence   *LBCookiePersistence `json:"lbCookieSessionPersistenceConfiguration,omitempty"`
	BackendMaxConnections *int                 `json:"backendMaxConnections,omitempty"`
	Name                  string               `json:"name"`
	Policy                string               `json:"policy"`
	Backends              []Backend            `json:"backends"`
	HealthChecker         HealthChecker        `json:"healthChecker"`
}

// Hostname is a virtual hostname.
type Hostname struct {
	Name     string `json:"name"`
	Hostname string `json:"hostname"`
}

// MatchType is a path match type wrapper.
type MatchType struct {
	MatchType string `json:"matchType"`
}

// PathRoute routes a path to a backend set.
type PathRoute struct {
	Path           string    `json:"path"`
	BackendSetName string    `json:"backendSetName"`
	PathMatchType  MatchType `json:"pathMatchType"`
}

// PathRouteSet is an ordered list of path routes.
type PathRouteSet struct {
	Name       string      `json:"name"`
	PathRoutes []PathRoute `json:"pathRoutes"`
}

// RoutingAction is a routing rule action.
type RoutingAction struct {
	Name           string `json:"name"`
	BackendSetName string `json:"backendSetName"`
}

// RoutingRule is a conditional routing rule.
type RoutingRule struct {
	Name      string          `json:"name"`
	Condition string          `json:"condition"`
	Actions   []RoutingAction `json:"actions"`
}

// RoutingPolicy is an ordered list of routing rules.
type RoutingPolicy struct {
	Name                     string        `json:"name"`
	ConditionLanguageVersion string        `json:"conditionLanguageVersion"`
	Rules                    []RoutingRule `json:"rules"`
}

// RuleCondition is a rule match condition.
type RuleCondition struct {
	AttributeName  string `json:"attributeName"`
	AttributeValue string `json:"attributeValue"`
	Operator       string `json:"operator,omitempty"`
}

// RedirectURI is the target of a REDIRECT rule.
type RedirectURI struct {
	Protocol *string `json:"protocol,omitempty"`
	Host     *string `json:"host,omitempty"`
	Port     *int    `json:"port,omitempty"`
	Path     *string `json:"path,omitempty"`
	Query    *string `json:"query,omitempty"`
}

// IPMaxConnections caps connections for source addresses.
type IPMaxConnections struct {
	IPAddresses    []string `json:"ipAddresses"`
	MaxConnections int      `json:"maxConnections"`
}

// Rule is one rule set item; the fields in use depend on Action.
type Rule struct {
	ResponseCode          *int               `json:"responseCode,omitempty"`
	StatusCode            *int               `json:"statusCode,omitempty"`
	RedirectURI           *RedirectURI       `json:"redirectUri,omitempty"`
	DefaultMaxConnections *int               `json:"defaultMaxConnections,omitempty"`
	HTTPLargeHeaderSizeKB *int               `json:"httpLargeHeaderSizeInKB,omitempty"`
	InvalidCharsAllowed   *bool              `json:"areInvalidCharactersAllowed,omitempty"`
	Prefix                *string            `json:"prefix,omitempty"`
	Suffix                *string            `json:"suffix,omitempty"`
	Description           *string            `json:"description,omitempty"`
	Action                string             `json:"action"`
	Header                string             `json:"header,omitempty"`
	Value                 *string            `json:"value,omitempty"`
	Conditions            []RuleCondition    `json:"conditions,omitempty"`
	AllowedMethods        []string           `json:"allowedMethods,omitempty"`
	IPMaxConnections      []IPMaxConnections `json:"ipMaxConnections,omitempty"`
}

// RuleSet is an ordered list of rules.
type RuleSet struct {
	Name  string `json:"name"`
	Items []Rule `json:"items"`
}

// SSLCipherSuite is a custom cipher suite.
type SSLCipherSuite struct {
	Name    string   `json:"name"`
	Ciphers []string `json:"ciphers"`
}

// Certificate is a certificate bundle; its private key is held in memory only and never returned.
type Certificate struct {
	Name   string `json:"certificateName"`
	Public string `json:"publicCertificate"`
	CA     string `json:"caCertificate"`
}

// LoadBalancer is a load balancer with all of its sub-resources.
type LoadBalancer struct {
	Tags
	TimeCreated     time.Time                  `json:"timeCreated"`
	BusyUntil       time.Time                  `json:"busyUntil,omitzero"`
	ShapeDetails    *ShapeDetails              `json:"shapeDetails,omitempty"`
	TransitionAt    *time.Time                 `json:"transitionAt,omitempty"`
	Listeners       map[string]*Listener       `json:"listeners,omitempty"`
	Hostnames       map[string]*Hostname       `json:"hostnames,omitempty"`
	SSLCipherSuites map[string]*SSLCipherSuite `json:"sslCipherSuites,omitempty"`
	Certificates    map[string]*Certificate    `json:"certificates,omitempty"`
	BackendSets     map[string]*BackendSet     `json:"backendSets,omitempty"`
	PathRouteSets   map[string]*PathRouteSet   `json:"pathRouteSets,omitempty"`
	RuleSets        map[string]*RuleSet        `json:"ruleSets,omitempty"`
	RoutingPolicies map[string]*RoutingPolicy  `json:"routingPolicies,omitempty"`
	ID              string                     `json:"id"`
	CompartmentID   string                     `json:"compartmentId"`
	DisplayName     string                     `json:"displayName"`
	State           string                     `json:"lifecycleState"`
	ShapeName       string                     `json:"shapeName"`
	IPMode          string                     `json:"ipMode,omitempty"`
	RequestIDHeader string                     `json:"requestIdHeader,omitempty"`
	Etag            string                     `json:"etag,omitempty"`
	SubnetIDs       []string                   `json:"subnetIds"`
	NSGs            []string                   `json:"networkSecurityGroupIds"`
	IPAddresses     []IPAddress                `json:"ipAddresses"`
	IsPrivate       bool                       `json:"isPrivate"`
	DeleteProtected bool                       `json:"isDeleteProtectionEnabled"`
	RequestIDOn     bool                       `json:"isRequestIdEnabled"`
}

// WorkRequest records an asynchronous load balancer operation.
type WorkRequest struct {
	Accepted       time.Time `json:"accepted"`
	FinishAt       time.Time `json:"finishAt"`
	ID             string    `json:"id"`
	LoadBalancerID string    `json:"loadBalancerId"`
	CompartmentID  string    `json:"compartmentId"`
	Type           string    `json:"type"`
	Seq            int64     `json:"seq"`
}
