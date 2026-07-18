package sts

import "errors"

var (
	// ErrMFACodeRequired is returned when SerialNumber is supplied without a TokenCode.
	ErrMFACodeRequired = errors.New("TokenCode is required when SerialNumber is provided")

	// ErrTooManyTags is returned when the number of session tags exceeds MaxTagCount.
	ErrTooManyTags = errors.New("too many session tags: maximum is 50")

	// ErrTooManyAudiences is returned when the audience list exceeds MaxAudienceCount.
	ErrTooManyAudiences = errors.New("too many audience entries: maximum is 10")

	// ErrMissingRoleArn is returned when AssumeRole is called without a RoleArn.
	ErrMissingRoleArn = errors.New("RoleArn is required")

	// ErrMissingSessionName is returned when AssumeRole is called without a RoleSessionName.
	ErrMissingSessionName = errors.New("RoleSessionName is required")

	// ErrInvalidDuration is returned when DurationSeconds is out of the allowed range.
	ErrInvalidDuration = errors.New("DurationSeconds is out of the allowed range")

	// ErrAccessDenied is returned when ExternalId validation fails.
	ErrAccessDenied = errors.New("AccessDenied")

	// ErrMissingFederationTokenName is returned when GetFederationToken is called without a Name.
	ErrMissingFederationTokenName = errors.New("Name is required for GetFederationToken")

	// ErrMissingWebIdentityToken is returned when AssumeRoleWithWebIdentity is called without a WebIdentityToken.
	ErrMissingWebIdentityToken = errors.New(
		"WebIdentityToken is required for AssumeRoleWithWebIdentity",
	)

	// ErrMissingSAMLAssertion is returned when AssumeRoleWithSAML is called without a SAMLAssertion.
	ErrMissingSAMLAssertion = errors.New("SAMLAssertion is required for AssumeRoleWithSAML")

	// ErrMissingPrincipalArn is returned when AssumeRoleWithSAML is called without a PrincipalArn.
	ErrMissingPrincipalArn = errors.New("PrincipalArn is required for AssumeRoleWithSAML")

	// ErrMissingTargetPrincipal is returned when AssumeRoot is called without a TargetPrincipal.
	ErrMissingTargetPrincipal = errors.New("TargetPrincipal is required for AssumeRoot")

	// ErrMissingTaskPolicyArn is returned when AssumeRoot is called without a TaskPolicyArn.
	ErrMissingTaskPolicyArn = errors.New("TaskPolicyArn is required for AssumeRoot")

	// ErrMissingTradeInToken is returned when GetDelegatedAccessToken is called without a TradeInToken.
	ErrMissingTradeInToken = errors.New("TradeInToken is required for GetDelegatedAccessToken")

	// ErrMissingAudience is returned when GetWebIdentityToken is called without an Audience.
	ErrMissingAudience = errors.New("audience list is required for GetWebIdentityToken")

	// ErrMissingSigningAlgorithm is returned when GetWebIdentityToken is called without a SigningAlgorithm.
	ErrMissingSigningAlgorithm = errors.New("SigningAlgorithm is required for GetWebIdentityToken")

	// ErrSessionNotFound is returned when a session lookup by access key ID yields no result.
	ErrSessionNotFound = errors.New("session not found")

	// ErrInvalidSessionName is returned when the session name does not meet AWS length requirements.
	ErrInvalidSessionName = errors.New("session name must be 2-64 characters")

	// ErrInvalidFederationName is returned when the federation token name does not meet AWS length requirements.
	ErrInvalidFederationName = errors.New("federation token name must be 2-32 characters")

	// ErrMissingEncodedMessage is returned when DecodeAuthorizationMessage is called without an EncodedMessage.
	ErrMissingEncodedMessage = errors.New(
		"EncodedMessage is required for DecodeAuthorizationMessage",
	)

	// ErrEmptyAccessKeyID is returned when GetAccessKeyInfo is called with an empty AccessKeyId.
	ErrEmptyAccessKeyID = errors.New("AccessKeyId must not be empty")

	// ErrUnknownAccessKeyID is returned when GetAccessKeyInfo cannot find the given key ID.
	ErrUnknownAccessKeyID = errors.New("unknown access key ID")

	// ErrValidation is returned when a parameter value fails semantic validation.
	ErrValidation = errors.New("invalid parameter value")

	// ErrInvalidRoleArn is returned when the RoleArn is not a valid ARN.
	ErrInvalidRoleArn = errors.New("RoleArn is not a valid ARN")

	// ErrSessionExpired is returned when a session credential is presented after its expiry.
	ErrSessionExpired = errors.New("session token has expired")

	// ErrMalformedPolicyDocument is returned when an inline policy is not valid JSON.
	ErrMalformedPolicyDocument = errors.New("malformed policy document")

	// ErrPackedPolicyTooLarge is returned when the combined session policy exceeds the 2048-byte budget.
	ErrPackedPolicyTooLarge = errors.New("packed policy too large")

	// ErrExpiredToken is returned when a web-identity JWT has expired.
	ErrExpiredToken = errors.New("token has expired")

	// ErrExpiredTradeInToken is returned when GetDelegatedAccessToken's TradeInToken
	// has passed its "exp" claim (maps to the real AWS ExpiredTradeInTokenException).
	ErrExpiredTradeInToken = errors.New("trade-in token has expired")

	// ErrInvalidIdentityToken is returned when a web-identity JWT is structurally invalid or its claims are wrong.
	ErrInvalidIdentityToken = errors.New("invalid identity token")

	// ErrIDPRejectedClaim is returned when the identity provider rejects the claim.
	ErrIDPRejectedClaim = errors.New("IDP rejected claim")

	// ErrInvalidAuthorizationMessage is returned when DecodeAuthorizationMessage receives a non-STS-issued blob.
	ErrInvalidAuthorizationMessage = errors.New("invalid authorization message")

	// ErrInvalidSAMLAssertion is returned when AssumeRoleWithSAML receives a SAMLAssertion
	// that is not valid base64 or does not decode to XML.
	ErrInvalidSAMLAssertion = errors.New("SAMLAssertion must be a base64-encoded XML document")

	// ErrTooManyPolicyArns is returned when more than MaxPolicyArnsCount policy ARNs are supplied.
	ErrTooManyPolicyArns = errors.New("too many policy ARNs: maximum is 10")

	// ErrInvalidSourceIdentity is returned when SourceIdentity fails regex or length validation.
	ErrInvalidSourceIdentity = errors.New("invalid SourceIdentity value")

	// ErrInvalidMFASerialNumber is returned when SerialNumber does not match the expected ARN shape.
	ErrInvalidMFASerialNumber = errors.New("invalid MFA serial number format")

	// ErrInvalidMFATokenCode is returned when TokenCode is not exactly 6 digits.
	ErrInvalidMFATokenCode = errors.New("TokenCode must be exactly 6 digits")

	// ErrInvalidTagKey is returned when a session tag key fails length or charset validation.
	ErrInvalidTagKey = errors.New("invalid session tag key")

	// ErrInvalidTagValue is returned when a session tag value exceeds the allowed length.
	ErrInvalidTagValue = errors.New("invalid session tag value")

	// ErrInvalidPolicyArn is returned when a policy ARN fails shape validation.
	ErrInvalidPolicyArn = errors.New("invalid policy ARN")

	// ErrInvalidProvidedContext is returned when a ProvidedContext entry exceeds length limits.
	ErrInvalidProvidedContext = errors.New("invalid provided context")

	// ErrInvalidTargetPrincipal is returned when AssumeRoot TargetPrincipal is not a 12-digit account ID.
	ErrInvalidTargetPrincipal = errors.New("TargetPrincipal must be a 12-digit AWS account ID")

	// ErrTokenCodeWithoutSerial is returned when a TokenCode is supplied without a SerialNumber.
	ErrTokenCodeWithoutSerial = errors.New("SerialNumber is required when TokenCode is provided")

	// ErrInvalidPrincipalArn is returned when AssumeRoleWithSAML PrincipalArn is not a valid SAML provider ARN.
	ErrInvalidPrincipalArn = errors.New("PrincipalArn is not a valid SAML provider ARN")

	// ErrMissingAction is returned when the Action field is absent from the request.
	ErrMissingAction = errors.New("action is required")

	// ErrInvalidAction is returned when the Action is not a supported STS operation.
	ErrInvalidAction = errors.New("invalid action")

	// ErrNilAppContext is returned when Init is called with a nil AppContext.
	ErrNilAppContext = errors.New("sts: nil app context")
)
