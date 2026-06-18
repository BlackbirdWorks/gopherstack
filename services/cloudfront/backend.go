package cloudfront

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusDeployed   = "Deployed"
	statusInProgress = "InProgress"

	// maxInvalidationPaths is the AWS limit on paths per invalidation batch.
	maxInvalidationPaths = 3000
	// maxCachePolicyTTL is the AWS upper bound for CachePolicy MaxTTL (1 year).
	maxCachePolicyTTL = 31536000
	// minSamplingRate and maxSamplingRate bound RealtimeLogConfig SamplingRate.
	minSamplingRate = 1
	maxSamplingRate = 100
	// minPublicKeyBits is the minimum RSA key size accepted by CloudFront.
	minPublicKeyBits = 2048
)

// oaiS3CanonicalUserID returns the AWS-style 64-char hex S3 canonical user ID for an OAI.
// AWS derives this deterministically per OAI; we hash the OAI ID for a stable value.
func oaiS3CanonicalUserID(id string) string {
	sum := sha256.Sum256([]byte("oai-canonical:" + id))

	return hex.EncodeToString(sum[:])
}

// validateRuntime returns ErrValidation when the runtime is not a known CloudFront Function runtime.
func validateRuntime(runtime string) error {
	switch runtime {
	case "cloudfront-js-1.0", "cloudfront-js-2.0":
		return nil
	}

	return fmt.Errorf(
		"%w: Runtime must be one of cloudfront-js-1.0 or cloudfront-js-2.0, got %q",
		ErrValidation, runtime,
	)
}

// validateInvalidationPaths checks that every path starts with '/', there are no more than
// maxInvalidationPaths, and wildcards are only used as trailing '/*' on a segment.
func validateInvalidationPaths(paths []string) error {
	if len(paths) > maxInvalidationPaths {
		return fmt.Errorf(
			"%w: too many invalidation paths: %d (max %d)",
			ErrValidation, len(paths), maxInvalidationPaths,
		)
	}

	for _, p := range paths {
		if !strings.HasPrefix(p, "/") {
			return fmt.Errorf("%w: invalidation path must start with '/': %q", ErrValidation, p)
		}
		// Wildcard must be the final segment and the entire segment (e.g. /foo/* is OK, /foo*bar is not).
		if strings.Contains(p, "*") {
			if !strings.HasSuffix(p, "/*") && p != "/*" {
				return fmt.Errorf(
					"%w: wildcard in invalidation path must be trailing '/*': %q",
					ErrValidation, p,
				)
			}
		}
	}

	return nil
}

// validateSamplingRate returns ErrValidation when rate is outside [1, 100].
func validateSamplingRate(rate int64) error {
	if rate < minSamplingRate || rate > maxSamplingRate {
		return fmt.Errorf(
			"%w: SamplingRate must be between %d and %d, got %d",
			ErrValidation, minSamplingRate, maxSamplingRate, rate,
		)
	}

	return nil
}

// validatePEMPublicKey parses encodedKey as a PEM-encoded public key and verifies
// that RSA keys are at least minPublicKeyBits bits.
func validatePEMPublicKey(encodedKey string) error {
	block, _ := pem.Decode([]byte(encodedKey))
	if block == nil {
		return fmt.Errorf("%w: EncodedKey must be a valid PEM-encoded public key", ErrValidation)
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("%w: EncodedKey PEM parse failed: %w", ErrValidation, err)
	}

	const bitsPerByte = 8
	// Only check bit-length for RSA keys; EC keys are accepted unconditionally.
	if rsaPub, ok := pub.(interface{ Size() int }); ok {
		bits := rsaPub.Size() * bitsPerByte
		if bits < minPublicKeyBits {
			return fmt.Errorf(
				"%w: RSA key must be at least %d bits, got %d",
				ErrValidation, minPublicKeyBits, bits,
			)
		}
	}

	return nil
}

var (
	// ErrNotFound is returned when a requested distribution does not exist.
	ErrNotFound = awserr.New("NoSuchDistribution", awserr.ErrNotFound)
	// ErrOAINotFound is returned when a requested OAI does not exist.
	ErrOAINotFound = awserr.New("NoSuchCloudFrontOriginAccessIdentity", awserr.ErrNotFound)
	// ErrCachePolicyNotFound is returned when a requested cache policy does not exist.
	ErrCachePolicyNotFound = awserr.New("NoSuchCachePolicy", awserr.ErrNotFound)
	// ErrAnycastIPListNotFound is returned when a requested anycast IP list does not exist.
	ErrAnycastIPListNotFound = awserr.New("NoSuchAnycastIPList", awserr.ErrNotFound)
	// ErrConnectionFunctionNotFound is returned when a connection function does not exist.
	ErrConnectionFunctionNotFound = awserr.New("NoSuchConnectionFunction", awserr.ErrNotFound)
	// ErrConnectionGroupNotFound is returned when a connection group does not exist.
	ErrConnectionGroupNotFound = awserr.New("NoSuchConnectionGroup", awserr.ErrNotFound)
	// ErrContinuousDeploymentPolicyNotFound is returned when a continuous deployment policy does not exist.
	ErrContinuousDeploymentPolicyNotFound = awserr.New(
		"NoSuchContinuousDeploymentPolicy",
		awserr.ErrNotFound,
	)
	// ErrInvalidationNotFound is returned when a requested invalidation does not exist.
	ErrInvalidationNotFound = awserr.New("NoSuchInvalidation", awserr.ErrNotFound)
	// ErrOACNotFound is returned when a requested origin access control does not exist.
	ErrOACNotFound = awserr.New("NoSuchOriginAccessControl", awserr.ErrNotFound)
	// ErrResponseHeadersPolicyNotFound is returned when a requested response headers policy does not exist.
	ErrResponseHeadersPolicyNotFound = awserr.New("NoSuchResponseHeadersPolicy", awserr.ErrNotFound)
	// ErrFunctionNotFound is returned when a requested CloudFront function does not exist.
	ErrFunctionNotFound = awserr.New("NoSuchFunctionExists", awserr.ErrNotFound)
	// ErrOriginRequestPolicyNotFound is returned when a requested origin request policy does not exist.
	ErrOriginRequestPolicyNotFound = awserr.New("NoSuchOriginRequestPolicy", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("InvalidArgument", awserr.ErrInvalidParameter)
	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("DistributionAlreadyExists", awserr.ErrAlreadyExists)
	// ErrFLENotFound is returned when a requested field level encryption config does not exist.
	ErrFLENotFound = awserr.New("NoSuchFieldLevelEncryptionConfig", awserr.ErrNotFound)
	// ErrFLEProfileNotFound is returned when a requested field level encryption profile does not exist.
	ErrFLEProfileNotFound = awserr.New("NoSuchFieldLevelEncryptionProfile", awserr.ErrNotFound)
	// ErrPublicKeyNotFound is returned when a requested public key does not exist.
	ErrPublicKeyNotFound = awserr.New("NoSuchPublicKey", awserr.ErrNotFound)
	// ErrKeyGroupNotFound is returned when a requested key group does not exist.
	ErrKeyGroupNotFound = awserr.New("NoSuchResource", awserr.ErrNotFound)
	// ErrRealtimeLogConfigNotFound is returned when a requested realtime log config does not exist.
	ErrRealtimeLogConfigNotFound = awserr.New("NoSuchRealtimeLogConfig", awserr.ErrNotFound)
	// ErrKeyValueStoreNotFound is returned when a requested key value store does not exist.
	ErrKeyValueStoreNotFound = awserr.New("EntityNotFound", awserr.ErrNotFound)
	// ErrVpcOriginNotFound is returned when a requested VPC origin does not exist.
	ErrVpcOriginNotFound = awserr.New("NoSuchVpcOrigin", awserr.ErrNotFound)
)

// ErrPreconditionFailed is returned when an If-Match ETag check fails in a data-plane operation.
var ErrPreconditionFailed = errors.New("PreconditionFailed")

const (
	// idChars are the uppercase alphanumeric characters used for CloudFront IDs.
	idChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// idLen is the length of generated CloudFront IDs.
	idLen = 14
)

// generateID generates a random uppercase alphanumeric ID of length 14.
func generateID() string {
	b := make([]byte, idLen)
	for i := range b {
		b[i] = idChars[rand.IntN(len(idChars))] //nolint:gosec // mock service, not security-sensitive
	}

	return string(b)
}

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
type Invalidation struct {
	CreateTime time.Time `json:"createTime"`
	ID         string    `json:"id"`
	Status     string    `json:"status"`
	CallerRef  string    `json:"callerRef,omitempty"`
	Paths      []string  `json:"paths,omitempty"`
}

// AnycastIPList represents a CloudFront Anycast IP list.
type AnycastIPList struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Status  string `json:"status"`
	IPCount int32  `json:"ipCount"`
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

// ConnectionFunction represents a CloudFront connection function.
type ConnectionFunction struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
}

// ConnectionGroup represents a CloudFront connection group.
type ConnectionGroup struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
}

// ContinuousDeploymentPolicy represents a CloudFront continuous deployment policy.
type ContinuousDeploymentPolicy struct {
	StagingDistributionDNS string `json:"stagingDistributionDns,omitempty"`
	ID                     string `json:"id"`
	ETag                   string `json:"eTag"`
	Enabled                bool   `json:"enabled"`
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
	Name         string `json:"name"`
	Comment      string `json:"comment,omitempty"`
	Runtime      string `json:"runtime"`
	FunctionCode string `json:"functionCode"`
	Status       string `json:"status"` // DEVELOPMENT or LIVE
	ETag         string `json:"eTag"`
	ARN          string `json:"arn"`
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

// FieldLevelEncryption represents a CloudFront Field Level Encryption config.
type FieldLevelEncryption struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"eTag"`
}

// FieldLevelEncryptionProfile represents a CloudFront Field Level Encryption Profile.
type FieldLevelEncryptionProfile struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"eTag"`
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
}

// VpcOrigin represents a CloudFront VPC Origin.
type VpcOrigin struct {
	ID   string `json:"id"`
	ARN  string `json:"arn"`
	Name string `json:"name"`
	ETag string `json:"eTag"`
}

// InMemoryBackend stores CloudFront resources in memory.
type InMemoryBackend struct {
	distributions                     map[string]*Distribution
	distributionARNs                  map[string]string          // ARN → distribution ID (O(1) tag lookups)
	distributionCallerRefs            map[string]string          // CallerReference → distribution ID (idempotency)
	distributionAliases               map[string][]string        // distribution ID → aliases
	distributionWebACLs               map[string]string          // distribution ID → web ACL ID
	distributionTenantWebACLs         map[string]string          // tenant ID → web ACL ID
	invalidations                     map[string][]*Invalidation // distribution ID → []Invalidation
	oais                              map[string]*OriginAccessIdentity
	oaiCallerRefs                     map[string]string // CallerReference → OAI ID (idempotency)
	anycastIPLists                    map[string]*AnycastIPList
	cachePolicies                     map[string]*CachePolicy
	cachePolicyByName                 map[string]string              // name → policy ID (uniqueness)
	connectionFunctions               map[string]*ConnectionFunction // key: UUID id
	connectionFunctionByName          map[string]string              // name → UUID id
	connectionGroups                  map[string]*ConnectionGroup
	continuousDeploymentPolicies      map[string]*ContinuousDeploymentPolicy
	originAccessControls              map[string]*OriginAccessControl
	originAccessControlByName         map[string]string // name → OAC ID (uniqueness)
	responseHeadersPolicies           map[string]*ResponseHeadersPolicy
	responseHeadersPolicyByName       map[string]string    // name → policy ID (uniqueness)
	functions                         map[string]*Function // name → function
	originRequestPolicies             map[string]*OriginRequestPolicy
	originRequestPolicyByName         map[string]string // name → policy ID (uniqueness)
	fieldLevelEncryptions             map[string]*FieldLevelEncryption
	fieldLevelEncryptionByName        map[string]string // name → ID
	fieldLevelEncryptionProfiles      map[string]*FieldLevelEncryptionProfile
	fieldLevelEncryptionProfileByName map[string]string // name → ID
	publicKeys                        map[string]*PublicKey
	publicKeyByName                   map[string]string // name → ID
	keyGroups                         map[string]*KeyGroup
	keyGroupByName                    map[string]string             // name → ID
	realtimeLogConfigs                map[string]*RealtimeLogConfig // ARN → config
	realtimeLogConfigByName           map[string]string             // name → ARN
	keyValueStores                    map[string]*KeyValueStore
	keyValueStoreByName               map[string]string // name → ID
	vpcOrigins                        map[string]*VpcOrigin
	distributionFunctionAssociations  map[string][]FunctionAssociation // distribution ID → associations
	// Batch 1 additions.
	trustStores                         map[string]*TrustStore
	streamingDistributions              map[string]*StreamingDistribution
	monitoringSubscriptions             map[string]*MonitoringSubscription // distribution ID → subscription
	resourcePolicies                    map[string]*resourcePolicyEntry    // resource ARN → policy
	distributionCachePolicies           map[string]string                  // distribution ID → cache policy ID
	distributionOriginRequestPolicies   map[string]string                  // distribution ID → ORP ID
	distributionResponseHeadersPolicies map[string]string                  // distribution ID → RHP ID
	distributionRealtimeLogConfigs      map[string]string                  // distribution ID → RLC ARN
	// Batch 2 additions.
	distributionTenants         map[string]*DistributionTenant // key: tenant ID
	distributionTenantsByDomain map[string]string              // key: domain → tenant ID
	tenantInvalidations         map[string][]*Invalidation     // key: tenantID
	managedCertificates         map[string]*ManagedCertificate // key: tenant ID
	// Audit batch additions.
	keyValueStoreData map[string]map[string]string // KVS ID → key → value
	keyValueDataETags map[string]string            // KVS ID → current data-plane ETag
	mu                *lockmetrics.RWMutex
	// lifecycle: tracks when InProgress invalidations become Completed.
	invalidationReadyAt       map[string]map[string]time.Time // distributionID → invID → readyAt
	tenantInvalidationReadyAt map[string]map[string]time.Time // tenantID → invID → readyAt
	stopCh                    chan struct{}
	accountID                 string
	region                    string
}

// NewInMemoryBackend creates a new in-memory CloudFront backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		distributions:                       make(map[string]*Distribution),
		distributionARNs:                    make(map[string]string),
		distributionCallerRefs:              make(map[string]string),
		distributionAliases:                 make(map[string][]string),
		distributionWebACLs:                 make(map[string]string),
		distributionTenantWebACLs:           make(map[string]string),
		invalidations:                       make(map[string][]*Invalidation),
		oais:                                make(map[string]*OriginAccessIdentity),
		oaiCallerRefs:                       make(map[string]string),
		anycastIPLists:                      make(map[string]*AnycastIPList),
		cachePolicies:                       make(map[string]*CachePolicy),
		cachePolicyByName:                   make(map[string]string),
		connectionFunctions:                 make(map[string]*ConnectionFunction),
		connectionFunctionByName:            make(map[string]string),
		connectionGroups:                    make(map[string]*ConnectionGroup),
		continuousDeploymentPolicies:        make(map[string]*ContinuousDeploymentPolicy),
		originAccessControls:                make(map[string]*OriginAccessControl),
		originAccessControlByName:           make(map[string]string),
		responseHeadersPolicies:             make(map[string]*ResponseHeadersPolicy),
		responseHeadersPolicyByName:         make(map[string]string),
		functions:                           make(map[string]*Function),
		originRequestPolicies:               make(map[string]*OriginRequestPolicy),
		originRequestPolicyByName:           make(map[string]string),
		fieldLevelEncryptions:               make(map[string]*FieldLevelEncryption),
		fieldLevelEncryptionByName:          make(map[string]string),
		fieldLevelEncryptionProfiles:        make(map[string]*FieldLevelEncryptionProfile),
		fieldLevelEncryptionProfileByName:   make(map[string]string),
		publicKeys:                          make(map[string]*PublicKey),
		publicKeyByName:                     make(map[string]string),
		keyGroups:                           make(map[string]*KeyGroup),
		keyGroupByName:                      make(map[string]string),
		realtimeLogConfigs:                  make(map[string]*RealtimeLogConfig),
		realtimeLogConfigByName:             make(map[string]string),
		keyValueStores:                      make(map[string]*KeyValueStore),
		keyValueStoreByName:                 make(map[string]string),
		vpcOrigins:                          make(map[string]*VpcOrigin),
		distributionFunctionAssociations:    make(map[string][]FunctionAssociation),
		trustStores:                         make(map[string]*TrustStore),
		streamingDistributions:              make(map[string]*StreamingDistribution),
		monitoringSubscriptions:             make(map[string]*MonitoringSubscription),
		resourcePolicies:                    make(map[string]*resourcePolicyEntry),
		distributionCachePolicies:           make(map[string]string),
		distributionOriginRequestPolicies:   make(map[string]string),
		distributionResponseHeadersPolicies: make(map[string]string),
		distributionRealtimeLogConfigs:      make(map[string]string),
		distributionTenants:                 make(map[string]*DistributionTenant),
		distributionTenantsByDomain:         make(map[string]string),
		tenantInvalidations:                 make(map[string][]*Invalidation),
		managedCertificates:                 make(map[string]*ManagedCertificate),
		keyValueStoreData:                   make(map[string]map[string]string),
		keyValueDataETags:                   make(map[string]string),
		invalidationReadyAt:                 make(map[string]map[string]time.Time),
		tenantInvalidationReadyAt:           make(map[string]map[string]time.Time),
		stopCh:                              make(chan struct{}),
		mu:                                  lockmetrics.New("cloudfront"),
		accountID:                           accountID,
		region:                              region,
	}

	go b.runInvalidationReconciler()

	return b
}

// Close stops the background reconciler goroutine.
func (b *InMemoryBackend) Close() {
	select {
	case <-b.stopCh:
	default:
		close(b.stopCh)
	}
}

// runInvalidationReconciler transitions InProgress invalidations to Completed.
func (b *InMemoryBackend) runInvalidationReconciler() {
	const tick = 20 * time.Millisecond

	timer := time.NewTicker(tick)
	defer timer.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-timer.C:
			b.mu.Lock("invalidationReconciler")
			b.reconcileInvalidationsLocked()
			b.mu.Unlock()
		}
	}
}

// reconcileInvalidationsLocked completes ready invalidations. Must hold b.mu.
func (b *InMemoryBackend) reconcileInvalidationsLocked() {
	now := time.Now()

	for distID, invMap := range b.invalidationReadyAt {
		reconcileInvMap(invMap, b.invalidations[distID], now)
	}

	for tenantID, invMap := range b.tenantInvalidationReadyAt {
		reconcileInvMap(invMap, b.tenantInvalidations[tenantID], now)
	}
}

// reconcileInvMap marks ready InProgress invalidations as Completed and removes them from readyAt.
func reconcileInvMap(invMap map[string]time.Time, invs []*Invalidation, now time.Time) {
	for invID, readyAt := range invMap {
		if !now.After(readyAt) {
			continue
		}

		for _, inv := range invs {
			if inv.ID == invID && inv.Status == statusInProgress {
				inv.Status = "Completed"
			}
		}

		delete(invMap, invID)
	}
}

// Reset clears all stored state, returning the backend to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetDistributions()
	b.resetPoliciesAndKeys()
}

// resetDistributions clears distribution-related maps.
func (b *InMemoryBackend) resetDistributions() {
	b.distributions = make(map[string]*Distribution)
	b.distributionARNs = make(map[string]string)
	b.distributionCallerRefs = make(map[string]string)
	b.distributionAliases = make(map[string][]string)
	b.distributionWebACLs = make(map[string]string)
	b.distributionTenantWebACLs = make(map[string]string)
	b.invalidations = make(map[string][]*Invalidation)
	b.oais = make(map[string]*OriginAccessIdentity)
	b.oaiCallerRefs = make(map[string]string)
	b.anycastIPLists = make(map[string]*AnycastIPList)
	b.cachePolicies = make(map[string]*CachePolicy)
	b.cachePolicyByName = make(map[string]string)
	b.connectionFunctions = make(map[string]*ConnectionFunction)
	b.connectionFunctionByName = make(map[string]string)
	b.connectionGroups = make(map[string]*ConnectionGroup)
	b.continuousDeploymentPolicies = make(map[string]*ContinuousDeploymentPolicy)
	b.originAccessControls = make(map[string]*OriginAccessControl)
	b.originAccessControlByName = make(map[string]string)
	b.responseHeadersPolicies = make(map[string]*ResponseHeadersPolicy)
	b.responseHeadersPolicyByName = make(map[string]string)
	b.functions = make(map[string]*Function)
	b.originRequestPolicies = make(map[string]*OriginRequestPolicy)
	b.originRequestPolicyByName = make(map[string]string)
	b.distributionFunctionAssociations = make(map[string][]FunctionAssociation)
	b.distributionCachePolicies = make(map[string]string)
	b.distributionOriginRequestPolicies = make(map[string]string)
	b.distributionResponseHeadersPolicies = make(map[string]string)
	b.distributionRealtimeLogConfigs = make(map[string]string)
	b.distributionTenants = make(map[string]*DistributionTenant)
	b.distributionTenantsByDomain = make(map[string]string)
	b.tenantInvalidations = make(map[string][]*Invalidation)
	b.managedCertificates = make(map[string]*ManagedCertificate)
}

// resetPoliciesAndKeys clears encryption, key, and store maps.
func (b *InMemoryBackend) resetPoliciesAndKeys() {
	b.fieldLevelEncryptions = make(map[string]*FieldLevelEncryption)
	b.fieldLevelEncryptionByName = make(map[string]string)
	b.fieldLevelEncryptionProfiles = make(map[string]*FieldLevelEncryptionProfile)
	b.fieldLevelEncryptionProfileByName = make(map[string]string)
	b.publicKeys = make(map[string]*PublicKey)
	b.publicKeyByName = make(map[string]string)
	b.keyGroups = make(map[string]*KeyGroup)
	b.keyGroupByName = make(map[string]string)
	b.realtimeLogConfigs = make(map[string]*RealtimeLogConfig)
	b.realtimeLogConfigByName = make(map[string]string)
	b.keyValueStores = make(map[string]*KeyValueStore)
	b.keyValueStoreByName = make(map[string]string)
	b.vpcOrigins = make(map[string]*VpcOrigin)
	b.trustStores = make(map[string]*TrustStore)
	b.streamingDistributions = make(map[string]*StreamingDistribution)
	b.monitoringSubscriptions = make(map[string]*MonitoringSubscription)
	b.resourcePolicies = make(map[string]*resourcePolicyEntry)
	b.keyValueStoreData = make(map[string]map[string]string)
	b.keyValueDataETags = make(map[string]string)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// distributionARN builds an ARN for a CloudFront distribution.
// CloudFront ARNs have no region component.
func (b *InMemoryBackend) distributionARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", b.accountID, id)
}

// oaiARN builds an ARN for an Origin Access Identity.
func (b *InMemoryBackend) oaiARN(id string) string {
	return fmt.Sprintf(
		"arn:aws:cloudfront::%s:origin-access-identity/cloudfront/%s",
		b.accountID,
		id,
	)
}

// anycastIPListARN builds an ARN for an Anycast IP list.
func (b *InMemoryBackend) anycastIPListARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:anycast-ip-list/%s", b.accountID, id)
}

// connectionGroupARN builds an ARN for a connection group.
func (b *InMemoryBackend) connectionGroupARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:connection-group/%s", b.accountID, id)
}

// functionARN builds an ARN for a CloudFront Function.
func (b *InMemoryBackend) functionARN(name string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:function/%s", b.accountID, name)
}

// CreateDistribution creates a new CloudFront distribution.
// If a distribution with the same CallerReference already exists, it is returned
// without creating a duplicate (idempotent).
func (b *InMemoryBackend) CreateDistribution(
	callerRef, comment string,
	enabled bool,
	rawConfig []byte,
) (*Distribution, error) {
	b.mu.Lock("CreateDistribution")
	defer b.mu.Unlock()

	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	// Idempotency: return existing distribution for the same CallerReference.
	if existingID, ok := b.distributionCallerRefs[callerRef]; ok {
		return b.copyDistribution(b.distributions[existingID]), nil
	}

	id := generateID()
	d := &Distribution{
		ID:              id,
		ARN:             b.distributionARN(id),
		DomainName:      strings.ToLower(id) + ".cloudfront.net",
		Status:          statusDeployed,
		ETag:            uuid.NewString(),
		CallerReference: callerRef,
		Comment:         comment,
		Enabled:         enabled,
		RawConfig:       rawConfig,
		Tags:            make(map[string]string),
	}
	b.distributions[id] = d
	b.distributionARNs[d.ARN] = id
	b.distributionCallerRefs[callerRef] = id
	cp := b.copyDistribution(d)

	return cp, nil
}

// GetDistribution returns a distribution by ID.
func (b *InMemoryBackend) GetDistribution(id string) (*Distribution, error) {
	b.mu.RLock("GetDistribution")
	defer b.mu.RUnlock()

	d, ok := b.distributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	return b.copyDistribution(d), nil
}

// UpdateDistribution updates an existing distribution's config.
func (b *InMemoryBackend) UpdateDistribution(
	id, comment string,
	enabled bool,
	rawConfig []byte,
) (*Distribution, error) {
	b.mu.Lock("UpdateDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	d.Comment = comment
	d.Enabled = enabled
	d.RawConfig = rawConfig
	d.ETag = uuid.NewString()
	cp := b.copyDistribution(d)

	return cp, nil
}

// DeleteDistribution deletes a distribution by ID and cleans up related state.
func (b *InMemoryBackend) DeleteDistribution(id string) error {
	b.mu.Lock("DeleteDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions[id]
	if !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	delete(b.distributionARNs, b.distributionARN(id))
	delete(b.distributionCallerRefs, d.CallerReference)
	delete(b.distributions, id)
	delete(b.invalidations, id)
	delete(b.distributionAliases, id)
	delete(b.distributionWebACLs, id)

	return nil
}

// ListDistributions returns all distributions sorted by ID.
func (b *InMemoryBackend) ListDistributions() []*Distribution {
	b.mu.RLock("ListDistributions")
	defer b.mu.RUnlock()

	list := make([]*Distribution, 0, len(b.distributions))
	for _, d := range b.distributions {
		list = append(list, b.copyDistribution(d))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// CreateOAI creates a new Origin Access Identity.
// If an OAI with the same CallerReference already exists, it is returned without
// creating a duplicate (idempotent).
func (b *InMemoryBackend) CreateOAI(callerRef, comment string) (*OriginAccessIdentity, error) {
	b.mu.Lock("CreateCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	// Idempotency: return existing OAI for the same CallerReference.
	if existingID, ok := b.oaiCallerRefs[callerRef]; ok {
		cp := *b.oais[existingID]

		return &cp, nil
	}

	id := generateID()
	oai := &OriginAccessIdentity{
		ID:                id,
		ARN:               b.oaiARN(id),
		S3CanonicalUserID: oaiS3CanonicalUserID(id),
		ETag:              uuid.NewString(),
		CallerReference:   callerRef,
		Comment:           comment,
	}
	b.oais[id] = oai
	b.oaiCallerRefs[callerRef] = id
	cp := *oai

	return &cp, nil
}

// GetOAI returns an OAI by ID.
func (b *InMemoryBackend) GetOAI(id string) (*OriginAccessIdentity, error) {
	b.mu.RLock("GetCloudFrontOriginAccessIdentity")
	defer b.mu.RUnlock()

	oai, ok := b.oais[id]
	if !ok {
		return nil, fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}
	cp := *oai

	return &cp, nil
}

// DeleteOAI deletes an OAI by ID.
func (b *InMemoryBackend) DeleteOAI(id string) error {
	b.mu.Lock("DeleteCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	oai, ok := b.oais[id]
	if !ok {
		return fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}

	delete(b.oaiCallerRefs, oai.CallerReference)
	delete(b.oais, id)

	return nil
}

// UpdateOAI updates an existing Origin Access Identity's comment and rotates its ETag.
func (b *InMemoryBackend) UpdateOAI(id, comment string) (*OriginAccessIdentity, error) {
	b.mu.Lock("UpdateCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	oai, ok := b.oais[id]
	if !ok {
		return nil, fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}

	oai.Comment = comment
	oai.ETag = uuid.NewString()

	cp := *oai

	return &cp, nil
}

// ListOAIs returns all OAIs sorted by ID.
func (b *InMemoryBackend) ListOAIs() []*OriginAccessIdentity {
	b.mu.RLock("ListCloudFrontOriginAccessIdentities")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessIdentity, 0, len(b.oais))
	for _, oai := range b.oais {
		cp := *oai
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// TagResource adds or updates tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if err := validateCFTags(kv); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	id, ok := b.distributionARNs[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	d := b.distributions[id]
	if d.Tags == nil {
		d.Tags = make(map[string]string, len(kv))
	}

	netNew := 0
	for k := range kv {
		if _, exists := d.Tags[k]; !exists {
			netNew++
		}
	}
	if len(d.Tags)+netNew > maxTagCount {
		return fmt.Errorf("%w: resource cannot have more than %d tags", ErrInvalidTagging, maxTagCount)
	}

	maps.Copy(d.Tags, kv)

	return nil
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	id, ok := b.distributionARNs[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	d := b.distributions[id]
	for _, k := range keys {
		delete(d.Tags, k)
	}

	return nil
}

// ListTags returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	id, ok := b.distributionARNs[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	d := b.distributions[id]
	cp := make(map[string]string, len(d.Tags))
	maps.Copy(cp, d.Tags)

	return cp, nil
}

// CreateInvalidation creates a new cache invalidation for the given distribution.
func (b *InMemoryBackend) CreateInvalidation(
	distributionID, callerRef string,
	paths []string,
) (*Invalidation, error) {
	if err := validateInvalidationPaths(paths); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateInvalidation")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	const invalidationDelay = 100 * time.Millisecond

	now := time.Now().UTC()
	inv := &Invalidation{
		ID:         generateID(),
		Status:     statusInProgress,
		CreateTime: now,
		Paths:      append([]string(nil), paths...),
		CallerRef:  callerRef,
	}
	b.invalidations[distributionID] = append(b.invalidations[distributionID], inv)

	if b.invalidationReadyAt[distributionID] == nil {
		b.invalidationReadyAt[distributionID] = make(map[string]time.Time)
	}

	b.invalidationReadyAt[distributionID][inv.ID] = now.Add(invalidationDelay)

	cp := *inv
	cp.Paths = append([]string(nil), inv.Paths...)

	return &cp, nil
}

// ListInvalidations returns all invalidations for a distribution, sorted by ID.
func (b *InMemoryBackend) ListInvalidations(distributionID string) ([]*Invalidation, error) {
	b.mu.RLock("ListInvalidations")
	defer b.mu.RUnlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	src := b.invalidations[distributionID]
	out := make([]*Invalidation, 0, len(src))

	for _, inv := range src {
		cp := *inv
		cp.Paths = append([]string(nil), inv.Paths...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// GetInvalidation returns a specific invalidation by distribution ID and invalidation ID.
func (b *InMemoryBackend) GetInvalidation(
	distributionID, invalidationID string,
) (*Invalidation, error) {
	b.mu.RLock("GetInvalidation")
	defer b.mu.RUnlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	for _, inv := range b.invalidations[distributionID] {
		if inv.ID == invalidationID {
			cp := *inv
			cp.Paths = append([]string(nil), inv.Paths...)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: invalidation %s not found", ErrInvalidationNotFound, invalidationID)
}

// ListAliases returns the aliases for a distribution by ID.
func (b *InMemoryBackend) ListAliases(distributionID string) []string {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	aliases := b.distributionAliases[distributionID]
	if len(aliases) == 0 {
		return nil
	}

	cp := make([]string, len(aliases))
	copy(cp, aliases)

	return cp
}

// AssociateAlias associates a CNAME alias with the specified distribution.
func (b *InMemoryBackend) AssociateAlias(distributionID, alias string) error {
	b.mu.Lock("AssociateAlias")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	if alias == "" {
		return fmt.Errorf("%w: alias must not be empty", ErrValidation)
	}

	existing := b.distributionAliases[distributionID]
	if slices.Contains(existing, alias) {
		return nil // already associated, idempotent
	}

	b.distributionAliases[distributionID] = append(existing, alias)

	return nil
}

// AssociateDistributionWebACL associates a WAF web ACL with the specified distribution.
func (b *InMemoryBackend) AssociateDistributionWebACL(distributionID, webACLID string) error {
	b.mu.Lock("AssociateDistributionWebACL")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	b.distributionWebACLs[distributionID] = webACLID

	return nil
}

// AssociateDistributionTenantWebACL associates a WAF web ACL with a distribution tenant.
func (b *InMemoryBackend) AssociateDistributionTenantWebACL(tenantID, webACLID string) error {
	b.mu.Lock("AssociateDistributionTenantWebACL")
	defer b.mu.Unlock()

	if tenantID == "" {
		return fmt.Errorf("%w: tenantId must not be empty", ErrValidation)
	}

	b.distributionTenantWebACLs[tenantID] = webACLID

	return nil
}

// CopyDistribution creates a copy of an existing distribution.
func (b *InMemoryBackend) CopyDistribution(primaryDistID, callerRef string) (*Distribution, error) {
	b.mu.Lock("CopyDistribution")
	defer b.mu.Unlock()

	src, ok := b.distributions[primaryDistID]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, primaryDistID)
	}

	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	id := generateID()
	rawCopy := make([]byte, len(src.RawConfig))
	copy(rawCopy, src.RawConfig)

	d := &Distribution{
		ID:              id,
		ARN:             b.distributionARN(id),
		DomainName:      strings.ToLower(id) + ".cloudfront.net",
		Status:          statusDeployed,
		ETag:            uuid.NewString(),
		CallerReference: callerRef,
		Comment:         src.Comment,
		Enabled:         src.Enabled,
		RawConfig:       rawCopy,
		Tags:            make(map[string]string),
	}

	b.distributions[id] = d
	b.distributionARNs[d.ARN] = id

	return b.copyDistribution(d), nil
}

// CreateAnycastIPList creates a new Anycast IP list.
func (b *InMemoryBackend) CreateAnycastIPList(name string, ipCount int32) (*AnycastIPList, error) {
	b.mu.Lock("CreateAnycastIpList")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if ipCount <= 0 {
		return nil, fmt.Errorf("%w: IpCount must be greater than 0", ErrValidation)
	}

	id := generateID()
	list := &AnycastIPList{
		ID:      id,
		ARN:     b.anycastIPListARN(id),
		Name:    name,
		Status:  statusDeployed,
		IPCount: ipCount,
	}
	b.anycastIPLists[id] = list
	cp := *list

	return &cp, nil
}

// CreateCachePolicy creates a new cache policy.
// Names must be unique. TTLs must satisfy: 0 ≤ MinTTL ≤ DefaultTTL ≤ MaxTTL.
func (b *InMemoryBackend) CreateCachePolicy(
	name, comment string,
	defaultTTL, maxTTL, minTTL int64,
	params ...*CachePolicyParams,
) (*CachePolicy, error) {
	b.mu.Lock("CreateCachePolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if minTTL < 0 {
		return nil, fmt.Errorf("%w: MinTTL must be >= 0", ErrValidation)
	}

	if defaultTTL < minTTL {
		return nil, fmt.Errorf("%w: DefaultTTL must be >= MinTTL", ErrValidation)
	}

	if maxTTL > maxCachePolicyTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be <= %d, got %d", ErrValidation, maxCachePolicyTTL, maxTTL)
	}

	if maxTTL < defaultTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be >= DefaultTTL", ErrValidation)
	}

	if _, exists := b.cachePolicyByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: cache policy with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	var p *CachePolicyParams
	if len(params) > 0 {
		p = params[0]
	}

	id := generateID()
	policy := &CachePolicy{
		ID:         id,
		ETag:       uuid.NewString(),
		Name:       name,
		Comment:    comment,
		DefaultTTL: defaultTTL,
		MaxTTL:     maxTTL,
		MinTTL:     minTTL,
		Params:     p,
	}
	b.cachePolicies[id] = policy
	b.cachePolicyByName[name] = id
	cp := *policy

	return &cp, nil
}

// CreateConnectionFunction creates a new connection function.
func (b *InMemoryBackend) CreateConnectionFunction(
	name, comment string,
) (*ConnectionFunction, error) {
	b.mu.Lock("CreateConnectionFunction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	id := generateID()
	arn := fmt.Sprintf("arn:aws:cloudfront::%s:connection-function/%s", b.accountID, id)
	fn := &ConnectionFunction{
		ID:      id,
		ARN:     arn,
		Name:    name,
		Comment: comment,
	}
	b.connectionFunctions[id] = fn
	b.connectionFunctionByName[name] = id
	cp := *fn

	return &cp, nil
}

// CreateConnectionGroup creates a new connection group.
func (b *InMemoryBackend) CreateConnectionGroup(name, comment string) (*ConnectionGroup, error) {
	b.mu.Lock("CreateConnectionGroup")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	id := generateID()
	group := &ConnectionGroup{
		ID:      id,
		ARN:     b.connectionGroupARN(id),
		Name:    name,
		Comment: comment,
	}
	b.connectionGroups[id] = group
	cp := *group

	return &cp, nil
}

// CreateContinuousDeploymentPolicy creates a new continuous deployment policy.
func (b *InMemoryBackend) CreateContinuousDeploymentPolicy(
	enabled bool,
	stagingDNS string,
) (*ContinuousDeploymentPolicy, error) {
	b.mu.Lock("CreateContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	id := generateID()
	policy := &ContinuousDeploymentPolicy{
		ID:                     id,
		ETag:                   uuid.NewString(),
		Enabled:                enabled,
		StagingDistributionDNS: stagingDNS,
	}
	b.continuousDeploymentPolicies[id] = policy
	cp := *policy

	return &cp, nil
}

// GetContinuousDeploymentPolicy returns a continuous deployment policy by ID.
func (b *InMemoryBackend) GetContinuousDeploymentPolicy(id string) (*ContinuousDeploymentPolicy, error) {
	b.mu.RLock("GetContinuousDeploymentPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.continuousDeploymentPolicies[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: continuous deployment policy %s not found",
			ErrContinuousDeploymentPolicyNotFound,
			id,
		)
	}

	cp := *policy

	return &cp, nil
}

// ListContinuousDeploymentPolicies returns all continuous deployment policies sorted by ID.
func (b *InMemoryBackend) ListContinuousDeploymentPolicies() []*ContinuousDeploymentPolicy {
	b.mu.RLock("ListContinuousDeploymentPolicies")
	defer b.mu.RUnlock()

	list := make([]*ContinuousDeploymentPolicy, 0, len(b.continuousDeploymentPolicies))
	for _, policy := range b.continuousDeploymentPolicies {
		cp := *policy
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateContinuousDeploymentPolicy updates an existing continuous deployment policy.
func (b *InMemoryBackend) UpdateContinuousDeploymentPolicy(
	id string,
	enabled bool,
	stagingDNS string,
) (*ContinuousDeploymentPolicy, error) {
	b.mu.Lock("UpdateContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	policy, ok := b.continuousDeploymentPolicies[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: continuous deployment policy %s not found",
			ErrContinuousDeploymentPolicyNotFound,
			id,
		)
	}

	policy.Enabled = enabled
	policy.StagingDistributionDNS = stagingDNS
	policy.ETag = uuid.NewString()
	cp := *policy

	return &cp, nil
}

// DeleteContinuousDeploymentPolicy deletes a continuous deployment policy by ID.
func (b *InMemoryBackend) DeleteContinuousDeploymentPolicy(id string) error {
	b.mu.Lock("DeleteContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.continuousDeploymentPolicies[id]; !ok {
		return fmt.Errorf("%w: continuous deployment policy %s not found", ErrContinuousDeploymentPolicyNotFound, id)
	}

	delete(b.continuousDeploymentPolicies, id)

	return nil
}

// SetDistributionFunctionAssociations replaces function associations for a distribution.
func (b *InMemoryBackend) SetDistributionFunctionAssociations(
	distributionID string,
	associations []FunctionAssociation,
) error {
	b.mu.Lock("SetDistributionFunctionAssociations")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	cp := make([]FunctionAssociation, len(associations))
	copy(cp, associations)
	b.distributionFunctionAssociations[distributionID] = cp

	return nil
}

// GetDistributionFunctionAssociations returns function associations for a distribution.
func (b *InMemoryBackend) GetDistributionFunctionAssociations(distributionID string) ([]FunctionAssociation, error) {
	b.mu.RLock("GetDistributionFunctionAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.distributions[distributionID]; !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, distributionID)
	}

	src := b.distributionFunctionAssociations[distributionID]
	cp := make([]FunctionAssociation, len(src))
	copy(cp, src)

	return cp, nil
}

func (b *InMemoryBackend) copyDistribution(d *Distribution) *Distribution {
	cp := *d
	rawCopy := make([]byte, len(d.RawConfig))
	copy(rawCopy, d.RawConfig)
	cp.RawConfig = rawCopy

	tagsCopy := make(map[string]string, len(d.Tags))
	maps.Copy(tagsCopy, d.Tags)
	cp.Tags = tagsCopy

	return &cp
}

// --- Cache Policy CRUD ---

// GetCachePolicy returns a cache policy by ID.
func (b *InMemoryBackend) GetCachePolicy(id string) (*CachePolicy, error) {
	b.mu.RLock("GetCachePolicy")
	defer b.mu.RUnlock()

	p, ok := b.cachePolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	cp := *p

	return &cp, nil
}

// ListCachePolicies returns all cache policies sorted by ID.
func (b *InMemoryBackend) ListCachePolicies() []*CachePolicy {
	b.mu.RLock("ListCachePolicies")
	defer b.mu.RUnlock()

	list := make([]*CachePolicy, 0, len(b.cachePolicies))
	for _, p := range b.cachePolicies {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateCachePolicy updates an existing cache policy.
func (b *InMemoryBackend) UpdateCachePolicy(
	id, name, comment string,
	defaultTTL, maxTTL, minTTL int64,
	params ...*CachePolicyParams,
) (*CachePolicy, error) {
	b.mu.Lock("UpdateCachePolicy")
	defer b.mu.Unlock()

	p, ok := b.cachePolicies[id]
	if !ok {
		return nil, fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if minTTL < 0 {
		return nil, fmt.Errorf("%w: MinTTL must be >= 0", ErrValidation)
	}

	if defaultTTL < minTTL {
		return nil, fmt.Errorf("%w: DefaultTTL must be >= MinTTL", ErrValidation)
	}

	if maxTTL > maxCachePolicyTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be <= %d, got %d", ErrValidation, maxCachePolicyTTL, maxTTL)
	}

	if maxTTL < defaultTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be >= DefaultTTL", ErrValidation)
	}

	// If name changed, ensure uniqueness and update index.
	if name != p.Name {
		if _, exists := b.cachePolicyByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: cache policy with name %q already exists",
				ErrAlreadyExists,
				name,
			)
		}

		delete(b.cachePolicyByName, p.Name)
		b.cachePolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.DefaultTTL = defaultTTL
	p.MaxTTL = maxTTL
	p.MinTTL = minTTL
	p.ETag = uuid.NewString()
	if len(params) > 0 {
		p.Params = params[0]
	}

	cp := *p

	return &cp, nil
}

// DeleteCachePolicy deletes a cache policy by ID.
func (b *InMemoryBackend) DeleteCachePolicy(id string) error {
	b.mu.Lock("DeleteCachePolicy")
	defer b.mu.Unlock()

	p, ok := b.cachePolicies[id]
	if !ok {
		return fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	delete(b.cachePolicyByName, p.Name)
	delete(b.cachePolicies, id)

	return nil
}

// --- Origin Access Control CRUD ---

// CreateOriginAccessControl creates a new Origin Access Control.
func (b *InMemoryBackend) CreateOriginAccessControl(
	name, description, originType, signingBehavior, signingProtocol string,
) (*OriginAccessControl, error) {
	b.mu.Lock("CreateOriginAccessControl")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.originAccessControlByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: origin access control with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := generateID()
	oac := &OriginAccessControl{
		ID:              id,
		Name:            name,
		Description:     description,
		OriginType:      originType,
		SigningBehavior: signingBehavior,
		SigningProtocol: signingProtocol,
		ETag:            uuid.NewString(),
	}
	b.originAccessControls[id] = oac
	b.originAccessControlByName[name] = id
	cp := *oac

	return &cp, nil
}

// GetOriginAccessControl returns an OAC by ID.
func (b *InMemoryBackend) GetOriginAccessControl(id string) (*OriginAccessControl, error) {
	b.mu.RLock("GetOriginAccessControl")
	defer b.mu.RUnlock()

	oac, ok := b.originAccessControls[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	cp := *oac

	return &cp, nil
}

// ListOriginAccessControls returns all OACs sorted by ID.
func (b *InMemoryBackend) ListOriginAccessControls() []*OriginAccessControl {
	b.mu.RLock("ListOriginAccessControls")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessControl, 0, len(b.originAccessControls))
	for _, oac := range b.originAccessControls {
		cp := *oac
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateOriginAccessControl updates an existing OAC.
func (b *InMemoryBackend) UpdateOriginAccessControl(
	id, name, description, originType, signingBehavior, signingProtocol string,
) (*OriginAccessControl, error) {
	b.mu.Lock("UpdateOriginAccessControl")
	defer b.mu.Unlock()

	oac, ok := b.originAccessControls[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != oac.Name {
		if _, exists := b.originAccessControlByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: origin access control with name %q already exists",
				ErrAlreadyExists,
				name,
			)
		}

		delete(b.originAccessControlByName, oac.Name)
		b.originAccessControlByName[name] = id
	}

	oac.Name = name
	oac.Description = description
	oac.OriginType = originType
	oac.SigningBehavior = signingBehavior
	oac.SigningProtocol = signingProtocol
	oac.ETag = uuid.NewString()
	cp := *oac

	return &cp, nil
}

// DeleteOriginAccessControl deletes an OAC by ID.
func (b *InMemoryBackend) DeleteOriginAccessControl(id string) error {
	b.mu.Lock("DeleteOriginAccessControl")
	defer b.mu.Unlock()

	oac, ok := b.originAccessControls[id]
	if !ok {
		return fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	delete(b.originAccessControlByName, oac.Name)
	delete(b.originAccessControls, id)

	return nil
}

// --- Response Headers Policy CRUD ---

// CreateResponseHeadersPolicy creates a new Response Headers Policy.
func (b *InMemoryBackend) CreateResponseHeadersPolicy(
	name, comment string,
	opts ...*ResponseHeadersPolicyConfig,
) (*ResponseHeadersPolicy, error) {
	b.mu.Lock("CreateResponseHeadersPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.responseHeadersPolicyByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: response headers policy with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := generateID()
	p := &ResponseHeadersPolicy{
		ID:      id,
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}

	if len(opts) > 0 && opts[0] != nil {
		cfg := opts[0]
		p.CorsConfig = cfg.CorsConfig
		p.SecurityHeaders = cfg.SecurityHeaders
		p.CustomHeaders = cfg.CustomHeaders
		p.RemoveHeaders = cfg.RemoveHeaders
	}

	b.responseHeadersPolicies[id] = p
	b.responseHeadersPolicyByName[name] = id
	cp := *p

	return &cp, nil
}

// GetResponseHeadersPolicy returns a Response Headers Policy by ID.
func (b *InMemoryBackend) GetResponseHeadersPolicy(id string) (*ResponseHeadersPolicy, error) {
	b.mu.RLock("GetResponseHeadersPolicy")
	defer b.mu.RUnlock()

	p, ok := b.responseHeadersPolicies[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: response headers policy %s not found",
			ErrResponseHeadersPolicyNotFound,
			id,
		)
	}

	cp := *p

	return &cp, nil
}

// ListResponseHeadersPolicies returns all Response Headers Policies sorted by ID.
func (b *InMemoryBackend) ListResponseHeadersPolicies() []*ResponseHeadersPolicy {
	b.mu.RLock("ListResponseHeadersPolicies")
	defer b.mu.RUnlock()

	list := make([]*ResponseHeadersPolicy, 0, len(b.responseHeadersPolicies))
	for _, p := range b.responseHeadersPolicies {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateResponseHeadersPolicy updates an existing Response Headers Policy.
func (b *InMemoryBackend) UpdateResponseHeadersPolicy(
	id, name, comment string,
	opts ...*ResponseHeadersPolicyConfig,
) (*ResponseHeadersPolicy, error) {
	b.mu.Lock("UpdateResponseHeadersPolicy")
	defer b.mu.Unlock()

	p, ok := b.responseHeadersPolicies[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: response headers policy %s not found",
			ErrResponseHeadersPolicyNotFound,
			id,
		)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != p.Name {
		if _, exists := b.responseHeadersPolicyByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: response headers policy with name %q already exists",
				ErrAlreadyExists,
				name,
			)
		}

		delete(b.responseHeadersPolicyByName, p.Name)
		b.responseHeadersPolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.ETag = uuid.NewString()
	if len(opts) > 0 && opts[0] != nil {
		cfg := opts[0]
		p.CorsConfig = cfg.CorsConfig
		p.SecurityHeaders = cfg.SecurityHeaders
		p.CustomHeaders = cfg.CustomHeaders
		p.RemoveHeaders = cfg.RemoveHeaders
	}

	cp := *p

	return &cp, nil
}

// DeleteResponseHeadersPolicy deletes a Response Headers Policy by ID.
func (b *InMemoryBackend) DeleteResponseHeadersPolicy(id string) error {
	b.mu.Lock("DeleteResponseHeadersPolicy")
	defer b.mu.Unlock()

	p, ok := b.responseHeadersPolicies[id]
	if !ok {
		return fmt.Errorf(
			"%w: response headers policy %s not found",
			ErrResponseHeadersPolicyNotFound,
			id,
		)
	}

	delete(b.responseHeadersPolicyByName, p.Name)
	delete(b.responseHeadersPolicies, id)

	return nil
}

// --- CloudFront Function CRUD ---

// CreateFunction creates a new CloudFront Function.
func (b *InMemoryBackend) CreateFunction(
	name, comment, runtime, functionCode string,
) (*Function, error) {
	if err := validateRuntime(runtime); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.functions[name]; exists {
		return nil, fmt.Errorf("%w: function with name %q already exists", ErrAlreadyExists, name)
	}

	fn := &Function{
		Name:         name,
		Comment:      comment,
		Runtime:      runtime,
		FunctionCode: functionCode,
		Status:       "DEVELOPMENT",
		ETag:         uuid.NewString(),
		ARN:          b.functionARN(name),
	}
	b.functions[name] = fn
	cp := *fn

	return &cp, nil
}

// GetFunction returns a CloudFront Function by name.
func (b *InMemoryBackend) GetFunction(name string) (*Function, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	cp := *fn

	return &cp, nil
}

// ListFunctions returns all CloudFront Functions sorted by name.
func (b *InMemoryBackend) ListFunctions() []*Function {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	list := make([]*Function, 0, len(b.functions))
	for _, fn := range b.functions {
		cp := *fn
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// PublishFunction publishes (promotes to LIVE) a CloudFront Function.
func (b *InMemoryBackend) PublishFunction(name string) (*Function, error) {
	b.mu.Lock("PublishFunction")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	fn.Status = "LIVE"
	fn.ETag = uuid.NewString()
	cp := *fn

	return &cp, nil
}

// UpdateFunction updates an existing CloudFront Function.
func (b *InMemoryBackend) UpdateFunction(
	name, comment, runtime, functionCode string,
) (*Function, error) {
	if err := validateRuntime(runtime); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateFunction")
	defer b.mu.Unlock()

	fn, ok := b.functions[name]
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	fn.Comment = comment
	fn.Runtime = runtime
	fn.FunctionCode = functionCode
	fn.Status = "DEVELOPMENT"
	fn.ETag = uuid.NewString()
	cp := *fn

	return &cp, nil
}

// DeleteFunction deletes a CloudFront Function by name.
func (b *InMemoryBackend) DeleteFunction(name string) error {
	b.mu.Lock("DeleteFunction")
	defer b.mu.Unlock()

	if _, ok := b.functions[name]; !ok {
		return fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	delete(b.functions, name)

	return nil
}

// --- Origin Request Policy CRUD ---

// OriginRequestPolicyConfig carries optional full-config inputs for CreateOriginRequestPolicy.
type OriginRequestPolicyConfig struct {
	HeadersConfig      *ORPHeadersConfig
	CookiesConfig      *ORPCookiesConfig
	QueryStringsConfig *ORPQueryStringsConfig
}

// CreateOriginRequestPolicy creates a new Origin Request Policy.
func (b *InMemoryBackend) CreateOriginRequestPolicy(
	name, comment string,
	opts ...*OriginRequestPolicyConfig,
) (*OriginRequestPolicy, error) {
	b.mu.Lock("CreateOriginRequestPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.originRequestPolicyByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: origin request policy with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := generateID()
	p := &OriginRequestPolicy{
		ID:      id,
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}

	if len(opts) > 0 && opts[0] != nil {
		cfg := opts[0]
		p.HeadersConfig = cfg.HeadersConfig
		p.CookiesConfig = cfg.CookiesConfig
		p.QueryStringsConfig = cfg.QueryStringsConfig
	}

	b.originRequestPolicies[id] = p
	b.originRequestPolicyByName[name] = id
	cp := *p

	return &cp, nil
}

// GetOriginRequestPolicy returns an Origin Request Policy by ID.
func (b *InMemoryBackend) GetOriginRequestPolicy(id string) (*OriginRequestPolicy, error) {
	b.mu.RLock("GetOriginRequestPolicy")
	defer b.mu.RUnlock()

	p, ok := b.originRequestPolicies[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: origin request policy %s not found",
			ErrOriginRequestPolicyNotFound,
			id,
		)
	}

	cp := *p

	return &cp, nil
}

// ListOriginRequestPolicies returns all Origin Request Policies sorted by ID.
func (b *InMemoryBackend) ListOriginRequestPolicies() []*OriginRequestPolicy {
	b.mu.RLock("ListOriginRequestPolicies")
	defer b.mu.RUnlock()

	list := make([]*OriginRequestPolicy, 0, len(b.originRequestPolicies))
	for _, p := range b.originRequestPolicies {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateOriginRequestPolicy updates an existing Origin Request Policy.
func (b *InMemoryBackend) UpdateOriginRequestPolicy(
	id, name, comment string,
	opts ...*OriginRequestPolicyConfig,
) (*OriginRequestPolicy, error) {
	b.mu.Lock("UpdateOriginRequestPolicy")
	defer b.mu.Unlock()

	p, ok := b.originRequestPolicies[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: origin request policy %s not found",
			ErrOriginRequestPolicyNotFound,
			id,
		)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != p.Name {
		if _, exists := b.originRequestPolicyByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: origin request policy with name %q already exists",
				ErrAlreadyExists,
				name,
			)
		}

		delete(b.originRequestPolicyByName, p.Name)
		b.originRequestPolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.ETag = uuid.NewString()
	if len(opts) > 0 && opts[0] != nil {
		cfg := opts[0]
		p.HeadersConfig = cfg.HeadersConfig
		p.CookiesConfig = cfg.CookiesConfig
		p.QueryStringsConfig = cfg.QueryStringsConfig
	}

	cp := *p

	return &cp, nil
}

// DeleteOriginRequestPolicy deletes an Origin Request Policy by ID.
func (b *InMemoryBackend) DeleteOriginRequestPolicy(id string) error {
	b.mu.Lock("DeleteOriginRequestPolicy")
	defer b.mu.Unlock()

	p, ok := b.originRequestPolicies[id]
	if !ok {
		return fmt.Errorf(
			"%w: origin request policy %s not found",
			ErrOriginRequestPolicyNotFound,
			id,
		)
	}

	delete(b.originRequestPolicyByName, p.Name)
	delete(b.originRequestPolicies, id)

	return nil
}

// --- Field Level Encryption CRUD ---

// CreateFieldLevelEncryption creates a new Field Level Encryption config.
func (b *InMemoryBackend) CreateFieldLevelEncryption(
	name, comment string,
) (*FieldLevelEncryption, error) {
	b.mu.Lock("CreateFieldLevelEncryption")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.fieldLevelEncryptionByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: field level encryption with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := generateID()
	fle := &FieldLevelEncryption{
		ID:      id,
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}
	b.fieldLevelEncryptions[id] = fle
	b.fieldLevelEncryptionByName[name] = id
	cp := *fle

	return &cp, nil
}

// GetFieldLevelEncryption returns a Field Level Encryption config by ID.
func (b *InMemoryBackend) GetFieldLevelEncryption(id string) (*FieldLevelEncryption, error) {
	b.mu.RLock("GetFieldLevelEncryption")
	defer b.mu.RUnlock()

	fle, ok := b.fieldLevelEncryptions[id]
	if !ok {
		return nil, fmt.Errorf("%w: field level encryption %s not found", ErrFLENotFound, id)
	}

	cp := *fle

	return &cp, nil
}

// ListFieldLevelEncryptions returns all Field Level Encryption configs sorted by ID.
func (b *InMemoryBackend) ListFieldLevelEncryptions() []*FieldLevelEncryption {
	b.mu.RLock("ListFieldLevelEncryptions")
	defer b.mu.RUnlock()

	list := make([]*FieldLevelEncryption, 0, len(b.fieldLevelEncryptions))
	for _, fle := range b.fieldLevelEncryptions {
		cp := *fle
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateFieldLevelEncryption updates an existing Field Level Encryption config.
func (b *InMemoryBackend) UpdateFieldLevelEncryption(
	id, name, comment string,
) (*FieldLevelEncryption, error) {
	b.mu.Lock("UpdateFieldLevelEncryption")
	defer b.mu.Unlock()

	fle, ok := b.fieldLevelEncryptions[id]
	if !ok {
		return nil, fmt.Errorf("%w: field level encryption %s not found", ErrFLENotFound, id)
	}

	if name != fle.Name {
		if _, exists := b.fieldLevelEncryptionByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: field level encryption with name %q already exists",
				ErrAlreadyExists,
				name,
			)
		}

		delete(b.fieldLevelEncryptionByName, fle.Name)
		b.fieldLevelEncryptionByName[name] = id
	}

	fle.Name = name
	fle.Comment = comment
	fle.ETag = uuid.NewString()
	cp := *fle

	return &cp, nil
}

// DeleteFieldLevelEncryption deletes a Field Level Encryption config by ID.
func (b *InMemoryBackend) DeleteFieldLevelEncryption(id string) error {
	b.mu.Lock("DeleteFieldLevelEncryption")
	defer b.mu.Unlock()

	fle, ok := b.fieldLevelEncryptions[id]
	if !ok {
		return fmt.Errorf("%w: field level encryption %s not found", ErrFLENotFound, id)
	}

	delete(b.fieldLevelEncryptionByName, fle.Name)
	delete(b.fieldLevelEncryptions, id)

	return nil
}

// --- Field Level Encryption Profile CRUD ---

// CreateFieldLevelEncryptionProfile creates a new Field Level Encryption Profile.
func (b *InMemoryBackend) CreateFieldLevelEncryptionProfile(
	name, comment string,
) (*FieldLevelEncryptionProfile, error) {
	b.mu.Lock("CreateFieldLevelEncryptionProfile")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.fieldLevelEncryptionProfileByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: field level encryption profile with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := generateID()
	p := &FieldLevelEncryptionProfile{
		ID:      id,
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}
	b.fieldLevelEncryptionProfiles[id] = p
	b.fieldLevelEncryptionProfileByName[name] = id
	cp := *p

	return &cp, nil
}

// GetFieldLevelEncryptionProfile returns a Field Level Encryption Profile by ID.
func (b *InMemoryBackend) GetFieldLevelEncryptionProfile(
	id string,
) (*FieldLevelEncryptionProfile, error) {
	b.mu.RLock("GetFieldLevelEncryptionProfile")
	defer b.mu.RUnlock()

	p, ok := b.fieldLevelEncryptionProfiles[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: field level encryption profile %s not found",
			ErrFLEProfileNotFound,
			id,
		)
	}

	cp := *p

	return &cp, nil
}

// ListFieldLevelEncryptionProfiles returns all FLE profiles sorted by ID.
func (b *InMemoryBackend) ListFieldLevelEncryptionProfiles() []*FieldLevelEncryptionProfile {
	b.mu.RLock("ListFieldLevelEncryptionProfiles")
	defer b.mu.RUnlock()

	list := make([]*FieldLevelEncryptionProfile, 0, len(b.fieldLevelEncryptionProfiles))
	for _, p := range b.fieldLevelEncryptionProfiles {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateFieldLevelEncryptionProfile updates an existing FLE profile.
func (b *InMemoryBackend) UpdateFieldLevelEncryptionProfile(
	id, name, comment string,
) (*FieldLevelEncryptionProfile, error) {
	b.mu.Lock("UpdateFieldLevelEncryptionProfile")
	defer b.mu.Unlock()

	p, ok := b.fieldLevelEncryptionProfiles[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: field level encryption profile %s not found",
			ErrFLEProfileNotFound,
			id,
		)
	}

	if name != p.Name {
		if _, exists := b.fieldLevelEncryptionProfileByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: field level encryption profile with name %q already exists",
				ErrAlreadyExists,
				name,
			)
		}

		delete(b.fieldLevelEncryptionProfileByName, p.Name)
		b.fieldLevelEncryptionProfileByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.ETag = uuid.NewString()
	cp := *p

	return &cp, nil
}

// DeleteFieldLevelEncryptionProfile deletes an FLE profile by ID.
func (b *InMemoryBackend) DeleteFieldLevelEncryptionProfile(id string) error {
	b.mu.Lock("DeleteFieldLevelEncryptionProfile")
	defer b.mu.Unlock()

	p, ok := b.fieldLevelEncryptionProfiles[id]
	if !ok {
		return fmt.Errorf(
			"%w: field level encryption profile %s not found",
			ErrFLEProfileNotFound,
			id,
		)
	}

	delete(b.fieldLevelEncryptionProfileByName, p.Name)
	delete(b.fieldLevelEncryptionProfiles, id)

	return nil
}

// --- Public Key CRUD ---

// CreatePublicKey creates a new CloudFront Public Key.
func (b *InMemoryBackend) CreatePublicKey(
	callerRef, name, comment, encodedKey string,
) (*PublicKey, error) {
	if encodedKey != "" {
		if err := validatePEMPublicKey(encodedKey); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("CreatePublicKey")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.publicKeyByName[name]; exists {
		return nil, fmt.Errorf("%w: public key with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	pk := &PublicKey{
		ID:              id,
		Name:            name,
		Comment:         comment,
		EncodedKey:      encodedKey,
		CallerReference: callerRef,
		ETag:            uuid.NewString(),
	}
	b.publicKeys[id] = pk
	b.publicKeyByName[name] = id
	cp := *pk

	return &cp, nil
}

// GetPublicKey returns a CloudFront Public Key by ID.
func (b *InMemoryBackend) GetPublicKey(id string) (*PublicKey, error) {
	b.mu.RLock("GetPublicKey")
	defer b.mu.RUnlock()

	pk, ok := b.publicKeys[id]
	if !ok {
		return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, id)
	}

	cp := *pk

	return &cp, nil
}

// ListPublicKeys returns all public keys sorted by ID.
func (b *InMemoryBackend) ListPublicKeys() []*PublicKey {
	b.mu.RLock("ListPublicKeys")
	defer b.mu.RUnlock()

	list := make([]*PublicKey, 0, len(b.publicKeys))
	for _, pk := range b.publicKeys {
		cp := *pk
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdatePublicKey updates an existing Public Key comment.
func (b *InMemoryBackend) UpdatePublicKey(id, comment string) (*PublicKey, error) {
	b.mu.Lock("UpdatePublicKey")
	defer b.mu.Unlock()

	pk, ok := b.publicKeys[id]
	if !ok {
		return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, id)
	}

	pk.Comment = comment
	pk.ETag = uuid.NewString()
	cp := *pk

	return &cp, nil
}

// DeletePublicKey deletes a Public Key by ID.
func (b *InMemoryBackend) DeletePublicKey(id string) error {
	b.mu.Lock("DeletePublicKey")
	defer b.mu.Unlock()

	pk, ok := b.publicKeys[id]
	if !ok {
		return fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, id)
	}

	delete(b.publicKeyByName, pk.Name)
	delete(b.publicKeys, id)

	return nil
}

// --- Key Group CRUD ---

// CreateKeyGroup creates a new CloudFront Key Group.
func (b *InMemoryBackend) CreateKeyGroup(name, comment string, items []string) (*KeyGroup, error) {
	b.mu.Lock("CreateKeyGroup")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.keyGroupByName[name]; exists {
		return nil, fmt.Errorf("%w: key group with name %q already exists", ErrAlreadyExists, name)
	}

	for _, itemID := range items {
		if _, ok := b.publicKeys[itemID]; !ok {
			return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, itemID)
		}
	}

	id := generateID()
	kg := &KeyGroup{
		ID:      id,
		Name:    name,
		Comment: comment,
		Items:   append([]string(nil), items...),
		ETag:    uuid.NewString(),
	}
	b.keyGroups[id] = kg
	b.keyGroupByName[name] = id

	return b.copyKeyGroup(kg), nil
}

// GetKeyGroup returns a CloudFront Key Group by ID.
func (b *InMemoryBackend) GetKeyGroup(id string) (*KeyGroup, error) {
	b.mu.RLock("GetKeyGroup")
	defer b.mu.RUnlock()

	kg, ok := b.keyGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: key group %s not found", ErrKeyGroupNotFound, id)
	}

	return b.copyKeyGroup(kg), nil
}

// ListKeyGroups returns all key groups sorted by ID.
func (b *InMemoryBackend) ListKeyGroups() []*KeyGroup {
	b.mu.RLock("ListKeyGroups")
	defer b.mu.RUnlock()

	list := make([]*KeyGroup, 0, len(b.keyGroups))
	for _, kg := range b.keyGroups {
		list = append(list, b.copyKeyGroup(kg))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateKeyGroup updates an existing Key Group.
func (b *InMemoryBackend) UpdateKeyGroup(
	id, name, comment string,
	items []string,
) (*KeyGroup, error) {
	b.mu.Lock("UpdateKeyGroup")
	defer b.mu.Unlock()

	kg, ok := b.keyGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: key group %s not found", ErrKeyGroupNotFound, id)
	}

	if name != kg.Name {
		if _, exists := b.keyGroupByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: key group with name %q already exists",
				ErrAlreadyExists,
				name,
			)
		}

		delete(b.keyGroupByName, kg.Name)
		b.keyGroupByName[name] = id
	}

	for _, itemID := range items {
		if _, exists := b.publicKeys[itemID]; !exists {
			return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, itemID)
		}
	}

	kg.Name = name
	kg.Comment = comment
	kg.Items = append([]string(nil), items...)
	kg.ETag = uuid.NewString()

	return b.copyKeyGroup(kg), nil
}

// DeleteKeyGroup deletes a Key Group by ID.
func (b *InMemoryBackend) DeleteKeyGroup(id string) error {
	b.mu.Lock("DeleteKeyGroup")
	defer b.mu.Unlock()

	kg, ok := b.keyGroups[id]
	if !ok {
		return fmt.Errorf("%w: key group %s not found", ErrKeyGroupNotFound, id)
	}

	delete(b.keyGroupByName, kg.Name)
	delete(b.keyGroups, id)

	return nil
}

func (b *InMemoryBackend) copyKeyGroup(kg *KeyGroup) *KeyGroup {
	cp := *kg
	cp.Items = append([]string(nil), kg.Items...)

	return &cp
}

// --- Realtime Log Config CRUD ---

// realtimeLogConfigARN builds an ARN for a Realtime Log Config.
func (b *InMemoryBackend) realtimeLogConfigARN(name string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:realtime-log-config/%s", b.accountID, name)
}

// CreateRealtimeLogConfig creates a new Realtime Log Config.
func (b *InMemoryBackend) CreateRealtimeLogConfig(
	name string,
	samplingRate int64,
	fields []string,
) (*RealtimeLogConfig, error) {
	if err := validateSamplingRate(samplingRate); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRealtimeLogConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.realtimeLogConfigByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: realtime log config with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	arn := b.realtimeLogConfigARN(name)
	cfg := &RealtimeLogConfig{
		ARN:          arn,
		Name:         name,
		SamplingRate: samplingRate,
		Fields:       append([]string(nil), fields...),
	}
	b.realtimeLogConfigs[arn] = cfg
	b.realtimeLogConfigByName[name] = arn

	return b.copyRealtimeLogConfig(cfg), nil
}

// GetRealtimeLogConfig returns a Realtime Log Config by ARN.
func (b *InMemoryBackend) GetRealtimeLogConfig(arn string) (*RealtimeLogConfig, error) {
	b.mu.RLock("GetRealtimeLogConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.realtimeLogConfigs[arn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: realtime log config %s not found",
			ErrRealtimeLogConfigNotFound,
			arn,
		)
	}

	return b.copyRealtimeLogConfig(cfg), nil
}

// GetRealtimeLogConfigByName returns a Realtime Log Config by name.
func (b *InMemoryBackend) GetRealtimeLogConfigByName(name string) (*RealtimeLogConfig, error) {
	b.mu.RLock("GetRealtimeLogConfigByName")
	defer b.mu.RUnlock()

	arn, ok := b.realtimeLogConfigByName[name]
	if !ok {
		return nil, fmt.Errorf(
			"%w: realtime log config %s not found",
			ErrRealtimeLogConfigNotFound,
			name,
		)
	}

	return b.copyRealtimeLogConfig(b.realtimeLogConfigs[arn]), nil
}

// ListRealtimeLogConfigs returns all Realtime Log Configs sorted by name.
func (b *InMemoryBackend) ListRealtimeLogConfigs() []*RealtimeLogConfig {
	b.mu.RLock("ListRealtimeLogConfigs")
	defer b.mu.RUnlock()

	list := make([]*RealtimeLogConfig, 0, len(b.realtimeLogConfigs))
	for _, cfg := range b.realtimeLogConfigs {
		list = append(list, b.copyRealtimeLogConfig(cfg))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRealtimeLogConfig updates an existing Realtime Log Config.
func (b *InMemoryBackend) UpdateRealtimeLogConfig(
	arn string,
	samplingRate int64,
	fields []string,
) (*RealtimeLogConfig, error) {
	if err := validateSamplingRate(samplingRate); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateRealtimeLogConfig")
	defer b.mu.Unlock()

	cfg, ok := b.realtimeLogConfigs[arn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: realtime log config %s not found",
			ErrRealtimeLogConfigNotFound,
			arn,
		)
	}

	cfg.SamplingRate = samplingRate
	cfg.Fields = append([]string(nil), fields...)

	return b.copyRealtimeLogConfig(cfg), nil
}

// DeleteRealtimeLogConfig deletes a Realtime Log Config by ARN.
func (b *InMemoryBackend) DeleteRealtimeLogConfig(arn string) error {
	b.mu.Lock("DeleteRealtimeLogConfig")
	defer b.mu.Unlock()

	cfg, ok := b.realtimeLogConfigs[arn]
	if !ok {
		return fmt.Errorf("%w: realtime log config %s not found", ErrRealtimeLogConfigNotFound, arn)
	}

	delete(b.realtimeLogConfigByName, cfg.Name)
	delete(b.realtimeLogConfigs, arn)

	return nil
}

func (b *InMemoryBackend) copyRealtimeLogConfig(cfg *RealtimeLogConfig) *RealtimeLogConfig {
	cp := *cfg
	cp.Fields = append([]string(nil), cfg.Fields...)

	return &cp
}

// --- Key Value Store CRUD ---

// keyValueStoreARN builds an ARN for a Key Value Store.
func (b *InMemoryBackend) keyValueStoreARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:key-value-store/%s", b.accountID, id)
}

// CreateKeyValueStore creates a new CloudFront Key Value Store.
func (b *InMemoryBackend) CreateKeyValueStore(name, comment string) (*KeyValueStore, error) {
	b.mu.Lock("CreateKeyValueStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.keyValueStoreByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: key value store with name %q already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := uuid.NewString()
	kvs := &KeyValueStore{
		ID:      id,
		ARN:     b.keyValueStoreARN(id),
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}
	b.keyValueStores[id] = kvs
	b.keyValueStoreByName[name] = id
	cp := *kvs

	return &cp, nil
}

// GetKeyValueStore returns a Key Value Store by ID or ARN.
func (b *InMemoryBackend) GetKeyValueStore(idOrARN string) (*KeyValueStore, error) {
	b.mu.RLock("GetKeyValueStore")
	defer b.mu.RUnlock()

	if kvs, ok := b.keyValueStores[idOrARN]; ok {
		cp := *kvs

		return &cp, nil
	}

	for _, kvs := range b.keyValueStores {
		if kvs.ARN == idOrARN {
			cp := *kvs

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, idOrARN)
}

// ListKeyValueStores returns all Key Value Stores sorted by name.
func (b *InMemoryBackend) ListKeyValueStores() []*KeyValueStore {
	b.mu.RLock("ListKeyValueStores")
	defer b.mu.RUnlock()

	list := make([]*KeyValueStore, 0, len(b.keyValueStores))
	for _, kvs := range b.keyValueStores {
		cp := *kvs
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// DeleteKeyValueStore deletes a Key Value Store by ID.
func (b *InMemoryBackend) DeleteKeyValueStore(id string) error {
	b.mu.Lock("DeleteKeyValueStore")
	defer b.mu.Unlock()

	kvs, ok := b.keyValueStores[id]
	if !ok {
		return fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, id)
	}

	delete(b.keyValueStoreByName, kvs.Name)
	delete(b.keyValueStores, id)

	return nil
}

// --- KVS Data Plane ---

// kvsDataETag returns or creates the ETag for a KVS data store.
func (b *InMemoryBackend) kvsDataETag(id string) string {
	if etag, ok := b.keyValueDataETags[id]; ok {
		return etag
	}

	etag := uuid.NewString()
	b.keyValueDataETags[id] = etag

	return etag
}

// GetKVSValue returns the value for a key in a Key Value Store.
func (b *InMemoryBackend) GetKVSValue(kvsID, key string) (string, string, error) {
	b.mu.RLock("GetKVSValue")
	defer b.mu.RUnlock()

	if _, ok := b.keyValueStores[kvsID]; !ok {
		return "", "", fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, kvsID)
	}

	data := b.keyValueStoreData[kvsID]
	val, ok := data[key]
	if !ok {
		return "", "", fmt.Errorf("%w: key %q not found in kvs %s", ErrNotFound, key, kvsID)
	}

	return val, b.keyValueDataETags[kvsID], nil
}

// PutKVSValue creates or updates a key/value pair in a Key Value Store.
func (b *InMemoryBackend) PutKVSValue(kvsID, key, value, ifMatch string) (string, error) {
	b.mu.Lock("PutKVSValue")
	defer b.mu.Unlock()

	if _, ok := b.keyValueStores[kvsID]; !ok {
		return "", fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, kvsID)
	}

	currentETag := b.kvsDataETag(kvsID)
	if ifMatch != "" && ifMatch != currentETag {
		return "", fmt.Errorf("%w: If-Match ETag mismatch", ErrPreconditionFailed)
	}

	if b.keyValueStoreData[kvsID] == nil {
		b.keyValueStoreData[kvsID] = make(map[string]string)
	}
	b.keyValueStoreData[kvsID][key] = value
	newETag := uuid.NewString()
	b.keyValueDataETags[kvsID] = newETag

	return newETag, nil
}

// DeleteKVSValue deletes a key from a Key Value Store.
func (b *InMemoryBackend) DeleteKVSValue(kvsID, key, ifMatch string) (string, error) {
	b.mu.Lock("DeleteKVSValue")
	defer b.mu.Unlock()

	if _, ok := b.keyValueStores[kvsID]; !ok {
		return "", fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, kvsID)
	}

	currentETag := b.kvsDataETag(kvsID)
	if ifMatch != "" && ifMatch != currentETag {
		return "", fmt.Errorf("%w: If-Match ETag mismatch", ErrPreconditionFailed)
	}

	if b.keyValueStoreData[kvsID] != nil {
		delete(b.keyValueStoreData[kvsID], key)
	}
	newETag := uuid.NewString()
	b.keyValueDataETags[kvsID] = newETag

	return newETag, nil
}

// KVSItem is a single key/value item in a Key Value Store.
type KVSItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ListKVSValues returns all key/value pairs in a Key Value Store.
func (b *InMemoryBackend) ListKVSValues(kvsID string) ([]*KVSItem, string, error) {
	b.mu.RLock("ListKVSValues")
	defer b.mu.RUnlock()

	if _, ok := b.keyValueStores[kvsID]; !ok {
		return nil, "", fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, kvsID)
	}

	data := b.keyValueStoreData[kvsID]
	items := make([]*KVSItem, 0, len(data))
	for k, v := range data {
		items = append(items, &KVSItem{Key: k, Value: v})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })

	return items, b.kvsDataETag(kvsID), nil
}

// UpdateKVSValues performs a batch put/delete on a Key Value Store.
func (b *InMemoryBackend) UpdateKVSValues(kvsID, ifMatch string, puts []*KVSItem, deletes []string) (string, error) {
	b.mu.Lock("UpdateKVSValues")
	defer b.mu.Unlock()

	if _, ok := b.keyValueStores[kvsID]; !ok {
		return "", fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, kvsID)
	}

	currentETag := b.kvsDataETag(kvsID)
	if ifMatch != "" && ifMatch != currentETag {
		return "", fmt.Errorf("%w: If-Match ETag mismatch", ErrPreconditionFailed)
	}

	if b.keyValueStoreData[kvsID] == nil {
		b.keyValueStoreData[kvsID] = make(map[string]string)
	}
	for _, item := range puts {
		b.keyValueStoreData[kvsID][item.Key] = item.Value
	}
	for _, key := range deletes {
		delete(b.keyValueStoreData[kvsID], key)
	}
	newETag := uuid.NewString()
	b.keyValueDataETags[kvsID] = newETag

	return newETag, nil
}

// --- VPC Origin CRUD ---

// vpcOriginARN builds an ARN for a VPC Origin.
func (b *InMemoryBackend) vpcOriginARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:vpc-origin/%s", b.accountID, id)
}

// CreateVpcOrigin creates a new CloudFront VPC Origin.
func (b *InMemoryBackend) CreateVpcOrigin(name string) (*VpcOrigin, error) {
	b.mu.Lock("CreateVpcOrigin")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	id := generateID()
	origin := &VpcOrigin{
		ID:   id,
		ARN:  b.vpcOriginARN(id),
		Name: name,
		ETag: uuid.NewString(),
	}
	b.vpcOrigins[id] = origin
	cp := *origin

	return &cp, nil
}

// GetVpcOrigin returns a VPC Origin by ID.
func (b *InMemoryBackend) GetVpcOrigin(id string) (*VpcOrigin, error) {
	b.mu.RLock("GetVpcOrigin")
	defer b.mu.RUnlock()

	origin, ok := b.vpcOrigins[id]
	if !ok {
		return nil, fmt.Errorf("%w: vpc origin %s not found", ErrVpcOriginNotFound, id)
	}

	cp := *origin

	return &cp, nil
}

// ListVpcOrigins returns all VPC Origins sorted by ID.
func (b *InMemoryBackend) ListVpcOrigins() []*VpcOrigin {
	b.mu.RLock("ListVpcOrigins")
	defer b.mu.RUnlock()

	list := make([]*VpcOrigin, 0, len(b.vpcOrigins))
	for _, origin := range b.vpcOrigins {
		cp := *origin
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateVpcOrigin updates a VPC Origin.
func (b *InMemoryBackend) UpdateVpcOrigin(id, name string) (*VpcOrigin, error) {
	b.mu.Lock("UpdateVpcOrigin")
	defer b.mu.Unlock()

	origin, ok := b.vpcOrigins[id]
	if !ok {
		return nil, fmt.Errorf("%w: vpc origin %s not found", ErrVpcOriginNotFound, id)
	}

	origin.Name = name
	origin.ETag = uuid.NewString()
	cp := *origin

	return &cp, nil
}

// DeleteVpcOrigin deletes a VPC Origin by ID.
func (b *InMemoryBackend) DeleteVpcOrigin(id string) error {
	b.mu.Lock("DeleteVpcOrigin")
	defer b.mu.Unlock()

	if _, ok := b.vpcOrigins[id]; !ok {
		return fmt.Errorf("%w: vpc origin %s not found", ErrVpcOriginNotFound, id)
	}

	delete(b.vpcOrigins, id)

	return nil
}
