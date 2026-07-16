package cloudfront

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a requested distribution does not exist.
	ErrNotFound = awserr.New("NoSuchDistribution", awserr.ErrNotFound)
	// ErrDistributionNotDisabled is returned when attempting to delete an enabled distribution.
	ErrDistributionNotDisabled = awserr.New("DistributionNotDisabled", awserr.ErrConflict)
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
	// ErrConnectionGroupAlreadyExists is returned when a connection group name is already in use.
	ErrConnectionGroupAlreadyExists = awserr.New("EntityAlreadyExists", awserr.ErrAlreadyExists)
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
	// ErrAlreadyExists is the generic fallback for a resource whose identifier already
	// exists but which has no dedicated AlreadyExists error type in the CloudFront API
	// (e.g. Anycast IP lists, key value stores). AWS itself falls back to this same
	// generic code for such resources.
	ErrAlreadyExists = awserr.New("EntityAlreadyExists", awserr.ErrAlreadyExists)
	// ErrCachePolicyAlreadyExists is returned when a cache policy name is already in use.
	ErrCachePolicyAlreadyExists = awserr.New("CachePolicyAlreadyExists", awserr.ErrAlreadyExists)
	// ErrOriginRequestPolicyAlreadyExists is returned when an origin request policy name
	// is already in use.
	ErrOriginRequestPolicyAlreadyExists = awserr.New("OriginRequestPolicyAlreadyExists", awserr.ErrAlreadyExists)
	// ErrResponseHeadersPolicyAlreadyExists is returned when a response headers policy
	// name is already in use.
	ErrResponseHeadersPolicyAlreadyExists = awserr.New(
		"ResponseHeadersPolicyAlreadyExists",
		awserr.ErrAlreadyExists,
	)
	// ErrOriginAccessControlAlreadyExists is returned when an origin access control name
	// is already in use.
	ErrOriginAccessControlAlreadyExists = awserr.New("OriginAccessControlAlreadyExists", awserr.ErrAlreadyExists)
	// ErrFunctionAlreadyExists is returned when a CloudFront function name is already in use.
	ErrFunctionAlreadyExists = awserr.New("FunctionAlreadyExists", awserr.ErrAlreadyExists)
	// ErrFLEAlreadyExists is returned when a field-level-encryption config's CallerReference
	// collides with an existing config of a different shape.
	ErrFLEAlreadyExists = awserr.New("FieldLevelEncryptionConfigAlreadyExists", awserr.ErrAlreadyExists)
	// ErrFLEProfileAlreadyExists is returned when a field-level-encryption profile name is
	// already in use.
	ErrFLEProfileAlreadyExists = awserr.New("FieldLevelEncryptionProfileAlreadyExists", awserr.ErrAlreadyExists)
	// ErrPublicKeyAlreadyExists is returned when a public key name is already in use.
	ErrPublicKeyAlreadyExists = awserr.New("PublicKeyAlreadyExists", awserr.ErrAlreadyExists)
	// ErrKeyGroupAlreadyExists is returned when a key group name is already in use.
	ErrKeyGroupAlreadyExists = awserr.New("KeyGroupAlreadyExists", awserr.ErrAlreadyExists)
	// ErrRealtimeLogConfigAlreadyExists is returned when a realtime log config name is
	// already in use.
	ErrRealtimeLogConfigAlreadyExists = awserr.New("RealtimeLogConfigAlreadyExists", awserr.ErrAlreadyExists)
	// ErrCachePolicyInUse is returned when attempting to delete a cache policy that is
	// still referenced by a distribution's default or ordered cache behavior.
	ErrCachePolicyInUse = awserr.New("CachePolicyInUse", awserr.ErrConflict)
	// ErrOriginRequestPolicyInUse is returned when attempting to delete an origin
	// request policy that is still referenced by a distribution's cache behavior.
	ErrOriginRequestPolicyInUse = awserr.New("OriginRequestPolicyInUse", awserr.ErrConflict)
	// ErrResponseHeadersPolicyInUse is returned when attempting to delete a response
	// headers policy that is still referenced by a distribution's cache behavior.
	ErrResponseHeadersPolicyInUse = awserr.New("ResponseHeadersPolicyInUse", awserr.ErrConflict)
	// ErrFunctionInUse is returned when attempting to delete a CloudFront function that
	// is still associated with a distribution's cache behavior.
	ErrFunctionInUse = awserr.New("FunctionInUse", awserr.ErrConflict)
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
	// ErrResourcePolicyNotFound is returned when no resource policy has been put for a resource ARN.
	ErrResourcePolicyNotFound = awserr.New("NoSuchResourcePolicy", awserr.ErrNotFound)
	// ErrMonitoringSubscriptionNotFound is returned when no monitoring subscription exists for a
	// distribution.
	ErrMonitoringSubscriptionNotFound = awserr.New("NoSuchMonitoringSubscription", awserr.ErrNotFound)
	// ErrPublicKeyInUse is returned when a public key is still referenced by a key
	// group or a field-level-encryption profile and therefore cannot be deleted.
	ErrPublicKeyInUse = awserr.New("PublicKeyInUse", awserr.ErrConflict)
	// ErrFLEProfileInUse is returned when a field-level-encryption profile is still
	// referenced by a field-level-encryption config and therefore cannot be deleted.
	ErrFLEProfileInUse = awserr.New("FieldLevelEncryptionProfileInUse", awserr.ErrConflict)
	// ErrInconsistentQuantities is returned when a config payload declares a Quantity
	// for a list that does not match the number of Items actually provided. AWS
	// validates this pervasively across DistributionConfig and policy configs.
	ErrInconsistentQuantities = awserr.New("InconsistentQuantities", awserr.ErrInvalidParameter)
)

// ErrPreconditionFailed is returned when an If-Match ETag check fails in a data-plane operation.
var ErrPreconditionFailed = errors.New("PreconditionFailed")

// ErrDistributionTenantNotFound is returned when a distribution tenant does not exist.
var ErrDistributionTenantNotFound = awserr.New("NoSuchDistributionTenant", awserr.ErrNotFound)

// ErrInvalidTagging is returned when tag key/value constraints are violated.
var ErrInvalidTagging = awserr.New("InvalidTagging", awserr.ErrInvalidParameter)

// ErrDomainConflict is returned when a domain is already associated with another
// distribution tenant or distribution.
var ErrDomainConflict = awserr.New("DomainConflictException", awserr.ErrConflict)

// ErrTrustStoreNotFound is returned when a trust store does not exist.
var ErrTrustStoreNotFound = awserr.New("NoSuchTrustStore", awserr.ErrNotFound)

// ErrStreamingDistributionNotFound is returned when a streaming distribution does not exist.
var ErrStreamingDistributionNotFound = awserr.New("NoSuchStreamingDistribution", awserr.ErrNotFound)

// ErrStreamingDistributionNotDisabled is returned when deleting a streaming distribution
// that is still enabled.
var ErrStreamingDistributionNotDisabled = awserr.New("StreamingDistributionNotDisabled", awserr.ErrConflict)

// ---------------------------------------------------------------------------
// TrustStore
// ---------------------------------------------------------------------------
