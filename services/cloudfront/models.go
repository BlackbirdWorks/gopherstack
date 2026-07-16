package cloudfront

import (
	"time"
)

// Distribution represents a CloudFront distribution.
type Distribution struct {
	Tags             map[string]string `json:"tags,omitempty"`
	CallerReference  string            `json:"callerReference"`
	ARN              string            `json:"arn"`
	DomainName       string            `json:"domainName"`
	Status           string            `json:"status"`
	ETag             string            `json:"eTag"`
	ID               string            `json:"id"`
	Comment          string            `json:"comment,omitempty"`
	LastModifiedTime string            `json:"lastModifiedTime,omitempty"`
	PriceClass       string            `json:"priceClass,omitempty"`
	HTTPVersion      string            `json:"httpVersion,omitempty"`
	RawConfig        []byte            `json:"rawConfig,omitempty"`
	IsIPV6Enabled    bool              `json:"isIPV6Enabled"`
	Enabled          bool              `json:"enabled"`
}

// OriginAccessIdentity represents a CloudFront Origin Access Identity.
type OriginAccessIdentity struct {
	ID                string `json:"id"`
	ARN               string `json:"arn"`
	S3CanonicalUserID string `json:"s3CanonicalUserId"`
	ETag              string `json:"eTag"`
	CallerReference   string `json:"callerReference"`
	Comment           string `json:"comment,omitempty"`
}

// Invalidation represents a CloudFront cache invalidation.
//
// distID and tenantID are unexported identity-only fields used purely to key the
// composite invalidations/tenantInvalidations store.Table (see store_setup.go):
// exactly one is populated on any given Invalidation, depending on whether it
// belongs to a distribution (b.invalidations) or a distribution tenant
// (b.tenantInvalidations). Neither is part of the real AWS wire shape (hence
// json:"-"), and keeping them unexported preserves the exported API surface of
// this pre-existing exported type exactly.
type Invalidation struct {
	CreateTime time.Time `json:"createTime"`
	ID         string    `json:"id"`
	Status     string    `json:"status"`
	CallerRef  string    `json:"callerRef,omitempty"`
	distID     string    `json:"-"`
	tenantID   string    `json:"-"`
	Paths      []string  `json:"paths,omitempty"`
}

// AnycastIPList represents a CloudFront Anycast IP list.
type AnycastIPList struct {
	Tags       map[string]string `json:"tags,omitempty"`
	ID         string            `json:"id"`
	ARN        string            `json:"arn"`
	Name       string            `json:"name"`
	Status     string            `json:"status"`
	ETag       string            `json:"eTag"`
	AnycastIPs []string          `json:"anycastIps,omitempty"`
	IPCount    int32             `json:"ipCount"`
}

// CachePolicyHeadersConfig specifies which headers the policy forwards and caches.
type CachePolicyHeadersConfig struct {
	HeaderBehavior string   `json:"headerBehavior"` // none, whitelist
	Headers        []string `json:"headers,omitempty"`
}

// CachePolicyCookiesConfig specifies which cookies the policy forwards and caches.
type CachePolicyCookiesConfig struct {
	CookieBehavior string   `json:"cookieBehavior"` // none, whitelist, allExcept, all
	Cookies        []string `json:"cookies,omitempty"`
}

// CachePolicyQueryStringsConfig specifies which query strings the policy includes in the cache key.
type CachePolicyQueryStringsConfig struct {
	QueryStringBehavior string   `json:"queryStringBehavior"` // none, whitelist, allExcept, all
	QueryStrings        []string `json:"queryStrings,omitempty"`
}

// CachePolicyParams models ParametersInCacheKeyAndForwardedToOrigin.
type CachePolicyParams struct {
	HeadersConfig              CachePolicyHeadersConfig      `json:"headersConfig"`
	CookiesConfig              CachePolicyCookiesConfig      `json:"cookiesConfig"`
	QueryStringsConfig         CachePolicyQueryStringsConfig `json:"queryStringsConfig"`
	EnableAcceptEncodingGzip   bool                          `json:"enableAcceptEncodingGzip"`
	EnableAcceptEncodingBrotli bool                          `json:"enableAcceptEncodingBrotli"`
}

// CachePolicy represents a CloudFront cache policy.
type CachePolicy struct {
	Params     *CachePolicyParams `json:"params,omitempty"`
	ID         string             `json:"id"`
	ETag       string             `json:"etag"`
	Name       string             `json:"name"`
	Comment    string             `json:"comment,omitempty"`
	DefaultTTL int64              `json:"defaultTtl"`
	MaxTTL     int64              `json:"maxTtl"`
	MinTTL     int64              `json:"minTtl"`
}

// ConnectionFunction represents a CloudFront connection function: a small piece of code that
// runs on the connection path for a connection group. Like CloudFront Functions, a connection
// function starts life in the DEVELOPMENT stage and is promoted to LIVE via PublishConnectionFunction.
type ConnectionFunction struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
	Comment          string            `json:"comment,omitempty"`
	Runtime          string            `json:"runtime"`
	Stage            string            `json:"stage"`
	Status           string            `json:"status"`
	ETag             string            `json:"eTag"`
	CreatedTime      string            `json:"createdTime,omitempty"`
	LastModifiedTime string            `json:"lastModifiedTime,omitempty"`
	FunctionCode     []byte            `json:"functionCode,omitempty"`
}

// ConnectionGroup represents a CloudFront connection group: routing configuration (an Anycast
// IP list, IPv6 support, and a generated routing endpoint domain name) that distribution
// tenants are associated with.
type ConnectionGroup struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
	Comment          string            `json:"comment,omitempty"`
	AnycastIPListID  string            `json:"anycastIpListId,omitempty"`
	RoutingEndpoint  string            `json:"routingEndpoint"`
	Status           string            `json:"status"`
	ETag             string            `json:"eTag"`
	CreatedTime      string            `json:"createdTime,omitempty"`
	LastModifiedTime string            `json:"lastModifiedTime,omitempty"`
	IsDefault        bool              `json:"isDefault"`
	IPv6Enabled      bool              `json:"ipv6Enabled"`
	Enabled          bool              `json:"enabled"`
}

// ContinuousDeploymentSessionStickinessConfig configures how long a viewer's session sticks to
// the staging distribution once routed there under a SingleWeight traffic config.
type ContinuousDeploymentSessionStickinessConfig struct {
	IdleTTL    int32 `json:"idleTtl"`
	MaximumTTL int32 `json:"maximumTtl"`
}

// ContinuousDeploymentSingleWeightConfig configures weight-based traffic splitting between the
// primary and staging distributions of a continuous deployment policy.
type ContinuousDeploymentSingleWeightConfig struct {
	SessionStickinessConfig *ContinuousDeploymentSessionStickinessConfig `json:"sessionStickinessConfig,omitempty"`
	Weight                  float64                                      `json:"weight"`
}

// ContinuousDeploymentSingleHeaderConfig configures header-based routing to the staging
// distribution of a continuous deployment policy.
type ContinuousDeploymentSingleHeaderConfig struct {
	Header string `json:"header"`
	Value  string `json:"value"`
}

// ContinuousDeploymentTrafficConfig models the TrafficConfig element of a continuous deployment
// policy: exactly one of SingleWeightConfig or SingleHeaderConfig applies, selected by Type.
type ContinuousDeploymentTrafficConfig struct {
	SingleWeightConfig *ContinuousDeploymentSingleWeightConfig `json:"singleWeightConfig,omitempty"`
	SingleHeaderConfig *ContinuousDeploymentSingleHeaderConfig `json:"singleHeaderConfig,omitempty"`
	Type               string                                  `json:"type,omitempty"`
}

// ContinuousDeploymentPolicy represents a CloudFront continuous deployment policy.
type ContinuousDeploymentPolicy struct {
	TrafficConfig               ContinuousDeploymentTrafficConfig `json:"trafficConfig"`
	StagingDistributionDNS      string                            `json:"stagingDistributionDns,omitempty"`
	ID                          string                            `json:"id"`
	ARN                         string                            `json:"arn"`
	ETag                        string                            `json:"eTag"`
	LastModifiedTime            string                            `json:"lastModifiedTime,omitempty"`
	StagingDistributionDNSNames []string                          `json:"stagingDistributionDnsNames,omitempty"`
	Enabled                     bool                              `json:"enabled"`
}

// FunctionAssociation represents the association of a CloudFront function with an event type.
type FunctionAssociation struct {
	FunctionARN string `json:"functionArn"`
	EventType   string `json:"eventType"` // viewer-request, viewer-response, etc.
}

// OriginAccessControl represents a CloudFront Origin Access Control (OAC).
type OriginAccessControl struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Description     string `json:"description,omitempty"`
	OriginType      string `json:"originType"`
	SigningBehavior string `json:"signingBehavior"`
	SigningProtocol string `json:"signingProtocol"`
	ETag            string `json:"eTag"`
}

// RHPCorsConfig holds the CORS settings for a ResponseHeadersPolicy.
type RHPCorsConfig struct {
	AccessControlAllowOrigins     []string `json:"accessControlAllowOrigins,omitempty"`
	AccessControlAllowHeaders     []string `json:"accessControlAllowHeaders,omitempty"`
	AccessControlAllowMethods     []string `json:"accessControlAllowMethods,omitempty"`
	AccessControlExposeHeaders    []string `json:"accessControlExposeHeaders,omitempty"`
	AccessControlMaxAgeSec        int64    `json:"accessControlMaxAgeSec,omitempty"`
	AccessControlAllowCredentials bool     `json:"accessControlAllowCredentials"`
	OriginOverride                bool     `json:"originOverride"`
}

// RHPSecurityHeaders holds the security header settings for a ResponseHeadersPolicy.
type RHPSecurityHeaders struct {
	FrameOptionsValue              string `json:"frameOptionsValue,omitempty"`
	ReferrerPolicy                 string `json:"referrerPolicy,omitempty"`
	ContentSecurityPolicy          string `json:"contentSecurityPolicy,omitempty"`
	XSSProtection                  string `json:"xssProtection,omitempty"`
	StrictTransportSecuritySeconds int64  `json:"strictTransportSecuritySeconds,omitempty"`
	ContentTypeOptionsOverride     bool   `json:"contentTypeOptionsOverride"`
	IncludeSubdomains              bool   `json:"includeSubdomains"`
	Preload                        bool   `json:"preload"`
}

// RHPCustomHeader is a single custom header key/value pair for a ResponseHeadersPolicy.
type RHPCustomHeader struct {
	Header   string `json:"header"`
	Value    string `json:"value"`
	Override bool   `json:"override"`
}

// ResponseHeadersPolicyConfig carries optional full-config inputs for CreateResponseHeadersPolicy.
type ResponseHeadersPolicyConfig struct {
	CorsConfig      *RHPCorsConfig
	SecurityHeaders *RHPSecurityHeaders
	CustomHeaders   []RHPCustomHeader
	RemoveHeaders   []string
}

// ResponseHeadersPolicy represents a CloudFront Response Headers Policy.
type ResponseHeadersPolicy struct {
	CorsConfig      *RHPCorsConfig      `json:"corsConfig,omitempty"`
	SecurityHeaders *RHPSecurityHeaders `json:"securityHeaders,omitempty"`
	ID              string              `json:"id"`
	Name            string              `json:"name"`
	Comment         string              `json:"comment,omitempty"`
	ETag            string              `json:"eTag"`
	CustomHeaders   []RHPCustomHeader   `json:"customHeaders,omitempty"`
	RemoveHeaders   []string            `json:"removeHeaders,omitempty"`
}

// Function represents a CloudFront Function.
type Function struct {
	Name             string `json:"name"`
	Comment          string `json:"comment,omitempty"`
	Runtime          string `json:"runtime"`
	FunctionCode     string `json:"functionCode"`
	Status           string `json:"status"` // DEVELOPMENT or LIVE
	ETag             string `json:"eTag"`
	ARN              string `json:"arn"`
	CreatedTime      string `json:"createdTime"`
	LastModifiedTime string `json:"lastModifiedTime"`
}

// ORPHeadersConfig controls which request headers are forwarded to the origin.
type ORPHeadersConfig struct {
	HeaderBehavior string   `json:"headerBehavior"` // none, whitelist, allViewer, allViewerAndWhitelistCloudFront
	Headers        []string `json:"headers,omitempty"`
}

// ORPCookiesConfig controls which cookies are forwarded to the origin.
type ORPCookiesConfig struct {
	CookieBehavior string   `json:"cookieBehavior"` // none, whitelist, all, allExcept
	Cookies        []string `json:"cookies,omitempty"`
}

// ORPQueryStringsConfig controls which query strings are forwarded to the origin.
type ORPQueryStringsConfig struct {
	QueryStringBehavior string   `json:"queryStringBehavior"` // none, whitelist, all, allExcept
	QueryStrings        []string `json:"queryStrings,omitempty"`
}

// OriginRequestPolicy represents a CloudFront Origin Request Policy.
type OriginRequestPolicy struct {
	HeadersConfig      *ORPHeadersConfig      `json:"headersConfig,omitempty"`
	CookiesConfig      *ORPCookiesConfig      `json:"cookiesConfig,omitempty"`
	QueryStringsConfig *ORPQueryStringsConfig `json:"queryStringsConfig,omitempty"`
	ID                 string                 `json:"id"`
	Name               string                 `json:"name"`
	Comment            string                 `json:"comment,omitempty"`
	ETag               string                 `json:"eTag"`
}

// FLEQueryArgProfile associates a query argument with a field-level-encryption
// profile. It mirrors the AWS QueryArgProfile shape.
type FLEQueryArgProfile struct {
	QueryArg  string `json:"queryArg"`
	ProfileID string `json:"profileId"`
}

// FieldLevelEncryption represents a CloudFront Field Level Encryption config.
type FieldLevelEncryption struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"eTag"`
	// QueryArgProfiles are the query-arg → profile associations. Each referenced
	// ProfileID must correspond to an existing FLE profile (referential integrity).
	QueryArgProfiles []FLEQueryArgProfile `json:"queryArgProfiles,omitempty"`
	// ForwardWhenQueryArgProfileIsUnknown mirrors the AWS QueryArgProfileConfig flag.
	ForwardWhenQueryArgProfileIsUnknown bool `json:"forwardWhenQueryArgProfileIsUnknown,omitempty"`
}

// EncryptionEntity is one entity in an FLE profile that maps a public key to a
// set of field patterns. It mirrors the AWS EncryptionEntity shape.
type EncryptionEntity struct {
	PublicKeyID   string   `json:"publicKeyId"`
	ProviderID    string   `json:"providerId"`
	FieldPatterns []string `json:"fieldPatterns,omitempty"`
}

// FieldLevelEncryptionProfile represents a CloudFront Field Level Encryption Profile.
type FieldLevelEncryptionProfile struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"eTag"`
	// EncryptionEntities reference public keys. Each PublicKeyID must correspond to
	// an existing public key (referential integrity enforced on create/update).
	EncryptionEntities []EncryptionEntity `json:"encryptionEntities,omitempty"`
}

// PublicKey represents a CloudFront Public Key.
type PublicKey struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Comment         string `json:"comment,omitempty"`
	EncodedKey      string `json:"encodedKey"`
	CallerReference string `json:"callerReference"`
	ETag            string `json:"eTag"`
}

// KeyGroup represents a CloudFront Key Group.
type KeyGroup struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Comment string   `json:"comment,omitempty"`
	ETag    string   `json:"eTag"`
	Items   []string `json:"items"`
}

// RealtimeLogConfig represents a CloudFront Realtime Log Config.
type RealtimeLogConfig struct {
	ARN          string   `json:"arn"`
	Name         string   `json:"name"`
	Fields       []string `json:"fields"`
	SamplingRate int64    `json:"samplingRate"`
}

// KeyValueStore represents a CloudFront Key Value Store.
type KeyValueStore struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"eTag"`
	// Status reflects the provisioning state (AWS: PROVISIONING → READY). The
	// emulator provisions synchronously and reports READY immediately.
	Status string `json:"status"`
	// LastModifiedTime is an RFC3339 timestamp (CloudFront is a REST-XML API, so
	// timestamps are serialized as ISO-8601 strings, not epoch numbers).
	LastModifiedTime string `json:"lastModifiedTime"`
}

// VpcOrigin represents a CloudFront VPC Origin.
type VpcOrigin struct {
	ID   string `json:"id"`
	ARN  string `json:"arn"`
	Name string `json:"name"`
	ETag string `json:"eTag"`
}

// OriginRequestPolicyConfig carries optional full-config inputs for CreateOriginRequestPolicy.
type OriginRequestPolicyConfig struct {
	HeadersConfig      *ORPHeadersConfig
	CookiesConfig      *ORPCookiesConfig
	QueryStringsConfig *ORPQueryStringsConfig
}

// KVSItem is a single key/value item in a Key Value Store.
type KVSItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DistributionTenant represents a CloudFront distribution tenant.
type DistributionTenant struct {
	Customizations    map[string]any    `json:"Customizations,omitempty"`
	Parameters        map[string]string `json:"Parameters,omitempty"`
	Tags              map[string]string `json:"Tags,omitempty"`
	Name              string            `json:"Name,omitempty"`
	ARN               string            `json:"Arn"`
	DistributionID    string            `json:"DistributionId"`
	ID                string            `json:"Id"`
	Domain            string            `json:"Domain"`
	ConnectionGroupID string            `json:"ConnectionGroupId,omitempty"`
	Status            string            `json:"Status"`
	CreationTime      string            `json:"CreationTime,omitempty"`
	LastModifiedTime  string            `json:"LastModifiedTime,omitempty"`
	ETag              string            `json:"-"`
	Domains           []string          `json:"Domains,omitempty"`
	Enabled           bool              `json:"Enabled"`
}

// DomainConflict describes an existing resource that already claims a domain.
type DomainConflict struct {
	Domain       string
	ResourceType string // "DISTRIBUTION" | "DISTRIBUTION_TENANT"
	ResourceID   string
	AccountID    string
}

// DNSConfiguration reports the DNS verification status for a single domain.
type DNSConfiguration struct {
	Domain string
	Status string // "PASSED" | "FAILED"
	Reason string
}

// DomainAssociationResult is returned by UpdateDomainAssociation.
type DomainAssociationResult struct {
	Domain               string
	DistributionID       string
	DistributionTenantID string
}

// DistributionTenantUpdate carries the mutable fields accepted by UpdateDistributionTenant.
// A zero-value field is left unchanged; Domains and Enabled require an explicit non-empty /
// non-nil value to take effect.
type DistributionTenantUpdate struct {
	Customizations    map[string]any
	Enabled           *bool
	ConnectionGroupID string
	Domains           []string
}

// TrustStoreCertificateBundle models the CA certificate bundle backing a trust store, either
// as a reference to an object in S3 or as an inline PEM-encoded certificate bundle.
type TrustStoreCertificateBundle struct {
	S3Bucket                string `json:"s3Bucket,omitempty"`
	S3Key                   string `json:"s3Key,omitempty"`
	InlineCertificateBundle string `json:"inlineCertificateBundle,omitempty"`
}

// TrustStore represents a CloudFront trust store: a named collection of CA certificates used
// for mutual TLS (mTLS) authentication between viewers and CloudFront.
type TrustStore struct {
	Tags                                   map[string]string           `json:"tags,omitempty"`
	ID                                     string                      `json:"id"`
	ARN                                    string                      `json:"arn"`
	Name                                   string                      `json:"name"`
	Comment                                string                      `json:"comment,omitempty"`
	Status                                 string                      `json:"status"`
	ETag                                   string                      `json:"etag"`
	LastModifiedTime                       string                      `json:"lastModifiedTime,omitempty"`
	CertificateAuthorityCertificatesBundle TrustStoreCertificateBundle `json:"certificateAuthorityCertificatesBundle"`
}

// StreamingDistributionS3Origin models the S3Origin element of a StreamingDistributionConfig.
type StreamingDistributionS3Origin struct {
	DomainName           string `json:"domainName,omitempty"`
	OriginAccessIdentity string `json:"originAccessIdentity,omitempty"`
}

// StreamingDistributionTrustedSigners models the TrustedSigners element of a
// StreamingDistributionConfig.
type StreamingDistributionTrustedSigners struct {
	Items   []string `json:"items,omitempty"`
	Enabled bool     `json:"enabled"`
}

// StreamingDistributionConfig models the mutable configuration of a streaming distribution.
type StreamingDistributionConfig struct {
	CallerReference string                              `json:"callerReference"`
	Comment         string                              `json:"comment,omitempty"`
	PriceClass      string                              `json:"priceClass,omitempty"`
	S3Origin        StreamingDistributionS3Origin       `json:"s3Origin"`
	Aliases         []string                            `json:"aliases,omitempty"`
	TrustedSigners  StreamingDistributionTrustedSigners `json:"trustedSigners"`
	Enabled         bool                                `json:"enabled"`
}

// StreamingDistribution represents a CloudFront RTMP streaming distribution.
type StreamingDistribution struct {
	Tags             map[string]string           `json:"tags,omitempty"`
	ARN              string                      `json:"arn"`
	DomainName       string                      `json:"domainName"`
	Status           string                      `json:"status"`
	ETag             string                      `json:"etag"`
	ID               string                      `json:"id"`
	LastModifiedTime string                      `json:"lastModifiedTime,omitempty"`
	RawConfig        []byte                      `json:"rawConfig,omitempty"`
	Config           StreamingDistributionConfig `json:"config"`
}

// MonitoringSubscription represents real-time metrics settings for a distribution.
type MonitoringSubscription struct {
	RealtimeMetricsSubscriptionStatus string `json:"realtimeMetricsSubscriptionStatus"`
}

// resourcePolicyEntry stores a CloudFront resource policy.
type resourcePolicyEntry struct {
	Policy string `json:"policy"`
}

// ConnectionFunctionTestResult is the result of a TestConnectionFunction invocation: a
// deterministic, input-derived execution result (not a hardcoded constant).
type ConnectionFunctionTestResult struct {
	ComputeUtilization string
	FunctionOutput     string
	ExecutionLogs      []string
}

// ValidationTokenDetail describes the DNS validation record CloudFront expects to see published
// for one domain of a distribution tenant's CloudFront-managed ACM certificate.
type ValidationTokenDetail struct {
	Domain       string `json:"domain"`
	RedirectFrom string `json:"redirectFrom,omitempty"`
	RedirectTo   string `json:"redirectTo,omitempty"`
}

// ManagedCertificateDetails represents the state of the CloudFront-managed ACM certificate
// issued for a distribution tenant's domains.
type ManagedCertificateDetails struct {
	CertificateARN         string                  `json:"certificateArn"`
	CertificateStatus      string                  `json:"certificateStatus"` // SUCCESS | PENDING_VALIDATION
	ValidationTokenHost    string                  `json:"validationTokenHost"`
	ValidationTokenDetails []ValidationTokenDetail `json:"validationTokenDetails,omitempty"`
}
