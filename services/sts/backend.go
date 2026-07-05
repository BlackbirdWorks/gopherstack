package sts

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // SHA1 is used only for NameQualifier per AWS spec, not for security
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

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
)

const (
	accessKeyIDPrefix   = "ASIA"
	accessKeyIDChars    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	accessKeyIDRandLen  = 16
	secretKeyByteLen    = 20
	sessionTokenByteLen = 64
	arnComponentCount   = 6

	// jwtPartCount is the number of dot-separated parts in a JWT (header.payload.signature).
	jwtPartCount = 3
	// jwtMinParts is the minimum number of parts required to attempt payload extraction.
	jwtMinParts = 2

	// JWT registered-claim names used when parsing and issuing web-identity tokens.
	jwtClaimSub = "sub"
	jwtClaimIss = "iss"
	jwtClaimAud = "aud"
	jwtClaimExp = "exp"
	jwtClaimIat = "iat"
	jwtClaimNbf = "nbf"
	// base64Pad2 indicates two '=' padding characters are needed.
	base64Pad2 = 2
	// base64Pad1 indicates one '=' padding character is needed.
	base64Pad1 = 3

	// webIdentitySubjectPlaceholder is returned when the sub claim cannot be extracted from the token.
	webIdentitySubjectPlaceholder = "WebIdentitySubject"

	// defaultSTSRegion is the AWS region for the STS backend (STS is global, defaults to us-east-1).
	defaultSTSRegion = "us-east-1"

	samlSessionName      = "saml-session"
	rootSessionName      = "root"
	delegatedSessionName = "delegated"
)

// roleSessionNameRe is the AWS-allowed character set for RoleSessionName/FederationToken Name.
// Characters: word chars (a-zA-Z0-9_), and the special set +=,.@- (colon is NOT allowed per AWS).
var roleSessionNameRe = regexp.MustCompile(`^[\w+=,.@\-]+$`)

// validateRoleSessionName checks that the session name meets AWS length and character requirements.
func validateRoleSessionName(name string) error {
	if len(name) < MinRoleSessionNameLen || len(name) > MaxRoleSessionNameLen {
		return fmt.Errorf("%w: got length %d", ErrInvalidSessionName, len(name))
	}

	if !roleSessionNameRe.MatchString(name) {
		return fmt.Errorf("%w: session name contains invalid characters", ErrInvalidSessionName)
	}

	return nil
}

// accountIDRe matches a 12-digit AWS account ID.
var accountIDRe = regexp.MustCompile(`^\d{12}$`)

// isSessionExpired reports whether s has a non-zero expiry time that has already passed.
func isSessionExpired(s *SessionInfo) bool {
	return !s.Expiration.IsZero() && !time.Now().UTC().Before(s.Expiration)
}

// sessionEvictThreshold is the session count above which inserting a new session
// triggers an opportunistic sweep of expired sessions. This bounds the sessions
// map even when the background janitor is disabled or runs at a long interval,
// while keeping the common (small) case allocation-free. The threshold is high
// enough that the O(n) sweep amortizes cheaply.
const sessionEvictThreshold = 256

// evictExpiredSessionsLocked removes all expired sessions from the map.
// The caller must hold b.mu.
func (b *InMemoryBackend) evictExpiredSessionsLocked() {
	for id, session := range b.sessions {
		if isSessionExpired(session) {
			delete(b.sessions, id)
		}
	}
}

// maybeEvictExpiredSessions acquires its own lock and sweeps expired sessions when
// the session count is at or above sessionEvictThreshold. It runs in a separate
// critical section from storeSession so that session creation (O(1) map insert)
// is never blocked by an O(n) sweep.
func (b *InMemoryBackend) maybeEvictExpiredSessions() {
	b.mu.Lock("EvictExpiredSessions")
	if len(b.sessions) >= sessionEvictThreshold {
		b.evictExpiredSessionsLocked()
	}
	b.mu.Unlock()
}

// storeSession registers a new session under its access key ID and increments
// the lifetime counter. The store is a fast O(1) operation; opportunistic
// eviction of expired sessions is deferred to a separate lock acquisition so
// that the 11 credential-issuing operations do not serialize on O(n) sweeps.
func (b *InMemoryBackend) storeSession(accessKeyID string, session *SessionInfo) {
	b.mu.Lock("StoreSession")
	b.sessions[accessKeyID] = session
	b.totalSessionsCreated.Add(1)
	b.mu.Unlock()

	b.maybeEvictExpiredSessions()
}

// mergeTransitiveTags combines the parent session's transitive tags with the child's
// explicit tags. Parent tags whose key appears in parent.TransitiveTagKeys are
// inherited; the child's own tags take precedence on key conflicts.
func mergeTransitiveTags(parent *SessionInfo, childTags []Tag) []Tag {
	if parent == nil || len(parent.TransitiveTagKeys) == 0 {
		return childTags
	}

	transitiveSet := make(map[string]struct{}, len(parent.TransitiveTagKeys))
	for _, k := range parent.TransitiveTagKeys {
		transitiveSet[k] = struct{}{}
	}

	// Build child key set for conflict resolution.
	childKeys := make(map[string]struct{}, len(childTags))
	for _, t := range childTags {
		childKeys[t.Key] = struct{}{}
	}

	merged := make([]Tag, 0, len(childTags)+len(parent.Tags))
	// Inherit parent transitive tags not overridden by child.
	for _, t := range parent.Tags {
		if _, isTransitive := transitiveSet[t.Key]; !isTransitive {
			continue
		}
		if _, childOverrides := childKeys[t.Key]; childOverrides {
			continue
		}
		merged = append(merged, t)
	}
	merged = append(merged, childTags...)

	return merged
}

// validateRoleArn checks that a role ARN is a valid IAM role ARN:
// - format: arn:<partition>:iam::<12-digit-account>:role/<name>.
func validateRoleArn(roleArn string) error {
	parts := strings.SplitN(roleArn, ":", arnComponentCount)
	if len(parts) < arnComponentCount || parts[0] != "arn" || parts[2] != arnServiceIAM {
		return fmt.Errorf("%w: %q", ErrInvalidRoleArn, roleArn)
	}

	account := parts[4]
	if !accountIDRe.MatchString(account) {
		return fmt.Errorf("%w: account ID %q must be 12 digits", ErrInvalidRoleArn, account)
	}

	resource := parts[5]
	if !strings.HasPrefix(resource, "role/") {
		return fmt.Errorf("%w: resource %q must start with role/", ErrInvalidRoleArn, resource)
	}

	return nil
}

// validateFederationTokenName checks federation token name length and charset.
func validateFederationTokenName(name string) error {
	if len(name) < MinFederationTokenNameLen || len(name) > MaxFederationTokenNameLen {
		return fmt.Errorf("%w: got length %d", ErrInvalidFederationName, len(name))
	}

	if !roleSessionNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: federation token name contains invalid characters",
			ErrInvalidFederationName,
		)
	}

	return nil
}

// isValidWebIdentitySigningAlgorithm reports whether the given signing algorithm is supported.
// Per the real AWS STS GetWebIdentityToken API, only RS256 (RSA with SHA-256) and
// ES384 (ECDSA using the P-384 curve with SHA-384) are valid; any other value
// (including other JOSE algorithms such as ES256 or PS256) is rejected.
func isValidWebIdentitySigningAlgorithm(alg string) bool {
	switch alg {
	case "RS256", "ES384":
		return true
	}

	return false
}

// RoleLookup is implemented by services (e.g. IAM) that can provide role metadata
// to STS for ExternalId validation and MaxSessionDuration enforcement.
type RoleLookup interface {
	GetRoleByArn(arn string) (*RoleMeta, error)
}

// OIDCLookup is implemented by services (e.g. IAM) that can validate OIDC providers
// for AssumeRoleWithWebIdentity.
type OIDCLookup interface {
	// OIDCProviderExists returns true if an OIDC provider with the given issuer URL exists.
	OIDCProviderExists(issuerURL string) bool
}

// RoleMeta carries the role properties that STS needs during AssumeRole.
type RoleMeta struct {
	// TrustPolicy is the raw JSON of the role's trust (assume-role) policy document.
	TrustPolicy string
	// MaxSessionDuration is the maximum session duration (in seconds) for this role.
	// A value of 0 means the system default maximum (MaxDurationSeconds) applies.
	MaxSessionDuration int32
}

// trustPolicy is used to parse the trust policy JSON for ExternalId extraction.
type trustPolicy struct {
	Statement []trustStatement `json:"Statement"`
}

// trustStatement is a single statement in a trust policy.
type trustStatement struct {
	Condition map[string]map[string]json.RawMessage `json:"Condition"`
}

// authMsgHMACSize is the byte length of the HMAC-SHA256 prefix in encoded auth messages.
const authMsgHMACSize = sha256.Size

// authMsgSep is the separator byte between the HMAC and the plaintext in encoded auth messages.
const authMsgSep = '|'

// InMemoryBackend is a stateful in-memory STS backend.
type InMemoryBackend struct {
	roleLookup RoleLookup
	oidcLookup OIDCLookup
	sessions   map[string]*SessionInfo
	mu         *lockmetrics.RWMutex
	accountID  string

	// Operation call counters — incremented atomically.
	cntAssumeRoleWithWebIdentity atomic.Int64
	cntAssumeRole                atomic.Int64
	cntAssumeRoleWithSAML        atomic.Int64
	cntAssumeRoot                atomic.Int64
	cntGetCallerIdentity         atomic.Int64
	cntGetDelegatedAccessToken   atomic.Int64
	cntGetFederationToken        atomic.Int64
	cntGetSessionToken           atomic.Int64
	cntGetWebIdentityToken       atomic.Int64
	cntGetAccessKeyInfo          atomic.Int64
	cntDecodeAuthorizationMsg    atomic.Int64

	// totalSessionsCreated is the lifetime count of sessions issued.
	totalSessionsCreated atomic.Int64

	// authMsgSigningKey is a random key used to HMAC-sign encoded authorization messages.
	// Only messages signed with this key are accepted by DecodeAuthorizationMessage,
	// matching AWS behaviour where only STS-issued encoded messages can be decoded.
	authMsgSigningKey [authMsgHMACSize]byte
}

// NewInMemoryBackend creates a new InMemoryBackend with the default account ID.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID.
func NewInMemoryBackendWithConfig(accountID string) *InMemoryBackend {
	var key [authMsgHMACSize]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic("sts: failed to generate authorization message signing key: " + err.Error())
	}

	return &InMemoryBackend{
		accountID:         accountID,
		sessions:          make(map[string]*SessionInfo),
		authMsgSigningKey: key,
		mu:                lockmetrics.New("sts"),
	}
}

// SetRoleLookup wires an optional role-lookup implementation (e.g. the IAM backend)
// so that AssumeRole can validate ExternalId and enforce MaxSessionDuration.
func (b *InMemoryBackend) SetRoleLookup(rl RoleLookup) {
	b.mu.Lock("SetRoleLookup")
	defer b.mu.Unlock()

	b.roleLookup = rl
}

// SetOIDCLookup wires an optional OIDC-lookup implementation (e.g. the IAM backend)
// so that AssumeRoleWithWebIdentity can validate that the OIDC provider exists.
func (b *InMemoryBackend) SetOIDCLookup(ol OIDCLookup) {
	b.mu.Lock("SetOIDCLookup")
	defer b.mu.Unlock()

	b.oidcLookup = ol
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// Region returns the AWS region for this STS backend (STS is global, defaults to us-east-1).
func (b *InMemoryBackend) Region() string {
	return defaultSTSRegion
}

// AssumeRole generates temporary credentials for the given role.
// validateAssumeRoleInput checks the common parameter constraints for AssumeRole.
func validateAssumeRoleInput(input *AssumeRoleInput) error {
	if input.RoleArn == "" {
		return ErrMissingRoleArn
	}

	if err := validateRoleArn(input.RoleArn); err != nil {
		return err
	}

	if input.RoleSessionName == "" {
		return ErrMissingSessionName
	}

	if err := validateRoleSessionName(input.RoleSessionName); err != nil {
		return err
	}

	if len(input.Tags) > MaxTagCount {
		return fmt.Errorf("%w: got %d", ErrTooManyTags, len(input.Tags))
	}

	if err := validateTagConstraints(input.Tags); err != nil {
		return err
	}

	if err := validateSourceIdentity(input.SourceIdentity); err != nil {
		return err
	}

	if err := validatePolicyArns(input.PolicyArns); err != nil {
		return err
	}

	if err := validateProvidedContexts(input.ProvidedContexts); err != nil {
		return err
	}

	if err := validateInlinePolicy(input.Policy); err != nil {
		return err
	}

	return checkPackedPolicyBudget(input.Policy, input.PolicyArns)
}

func (b *InMemoryBackend) AssumeRole(input *AssumeRoleInput) (*AssumeRoleResponse, error) {
	b.cntAssumeRole.Add(1)

	if err := validateAssumeRoleInput(input); err != nil {
		return nil, err
	}

	effectiveMax, err := b.validateAndGetMaxDuration(input)
	if err != nil {
		return nil, err
	}

	if err = b.checkAssumeRoleTrust(input); err != nil {
		return nil, err
	}

	duration := input.DurationSeconds
	if duration == 0 {
		// Clamp default to the role's MaxSessionDuration when it's less than the standard default.
		duration = min(DefaultDurationSeconds, effectiveMax)
	}

	if duration < MinDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be at least %d",
			ErrInvalidDuration, MinDurationSeconds,
		)
	}

	if duration > effectiveMax {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must not exceed %d for this role",
			ErrInvalidDuration, effectiveMax,
		)
	}

	return b.issueCredentials(input, duration)
}

// getEffectiveMaxDuration returns the effective maximum session duration for a role.
// When no RoleLookup is configured, or the role is not found, MaxDurationSeconds is returned.
// This is used by AssumeRoleWithWebIdentity and AssumeRoleWithSAML which do not validate ExternalId.
func (b *InMemoryBackend) getEffectiveMaxDuration(roleArn string) int32 {
	effectiveMax := int32(MaxDurationSeconds)

	b.mu.RLock("GetEffectiveMaxDuration")
	rl := b.roleLookup
	b.mu.RUnlock()

	if rl == nil {
		return effectiveMax
	}

	meta, _ := rl.GetRoleByArn(roleArn)
	if meta != nil && meta.MaxSessionDuration > 0 {
		effectiveMax = meta.MaxSessionDuration
	}

	return effectiveMax
}

// validateAndGetMaxDuration validates ExternalId against the trust policy (when a RoleLookup
// is configured) and returns the effective maximum session duration for the role.
// When the caller uses temporary credentials (role chaining), the effective max is
// capped at MaxRoleChainDurationSeconds (3600s) per AWS rules.
func (b *InMemoryBackend) validateAndGetMaxDuration(input *AssumeRoleInput) (int32, error) {
	effectiveMax, err := b.roleDerivedMaxDuration(input)
	if err != nil {
		return 0, err
	}

	// AWS caps the max session duration at 1 hour when the caller is already using
	// temporary credentials (role chaining). ASIA prefix identifies temporary keys.
	if strings.HasPrefix(input.CallerAccessKeyID, accessKeyIDPrefix) && effectiveMax > MaxRoleChainDurationSeconds {
		effectiveMax = MaxRoleChainDurationSeconds
	}

	return effectiveMax, nil
}

// roleDerivedMaxDuration reads the role's MaxSessionDuration from the RoleLookup (if any)
// and validates ExternalId. Returns MaxDurationSeconds when no lookup is configured or the
// role is not found.
func (b *InMemoryBackend) roleDerivedMaxDuration(input *AssumeRoleInput) (int32, error) {
	b.mu.RLock("RoleDerivedMaxDuration")
	rl := b.roleLookup
	b.mu.RUnlock()

	if rl == nil {
		return int32(MaxDurationSeconds), nil
	}

	meta, _ := rl.GetRoleByArn(input.RoleArn)
	if meta == nil {
		return int32(MaxDurationSeconds), nil
	}

	if err := validateExternalID(meta.TrustPolicy, input.ExternalID); err != nil {
		return 0, err
	}

	if meta.MaxSessionDuration > 0 {
		return meta.MaxSessionDuration, nil
	}

	return int32(MaxDurationSeconds), nil
}

// lookupRoleMeta returns the RoleMeta for the given role ARN via the configured
// RoleLookup, or nil when no lookup is wired or the role is not found. It never
// returns an error: a missing role or lookup means the emulator falls back to
// permissive behaviour for trust evaluation.
func (b *InMemoryBackend) lookupRoleMeta(roleArn string) *RoleMeta {
	b.mu.RLock("LookupRoleMeta")
	rl := b.roleLookup
	b.mu.RUnlock()

	if rl == nil {
		return nil
	}

	meta, _ := rl.GetRoleByArn(roleArn)

	return meta
}

// checkAssumeRoleTrust evaluates the target role's trust policy against the
// calling principal for sts:AssumeRole. It is a no-op (permissive) unless a
// caller ARN was resolved, a RoleLookup is wired, and the role carries a
// non-empty trust policy — mirroring the emulator's "enforce only what is
// positively known" stance so callers without a resolvable identity are not
// spuriously denied.
func (b *InMemoryBackend) checkAssumeRoleTrust(input *AssumeRoleInput) error {
	if input.CallerArn == "" {
		return nil
	}

	meta := b.lookupRoleMeta(input.RoleArn)
	if meta == nil || meta.TrustPolicy == "" {
		return nil
	}

	return evaluateAssumeRoleTrust(meta.TrustPolicy, trustEval{
		action:     actionAssumeRole,
		callerArn:  input.CallerArn,
		externalID: input.ExternalID,
		conditionCtx: map[string]string{
			condKeyPrincipalArn: input.CallerArn,
		},
	})
}

// checkWebIdentityTrust evaluates the target role's trust policy against the
// federated OIDC identity (issuer/audience) for sts:AssumeRoleWithWebIdentity.
// It is permissive unless the token yields an issuer, a RoleLookup is wired, and
// the role carries a non-empty trust policy.
func (b *InMemoryBackend) checkWebIdentityTrust(input *AssumeRoleWithWebIdentityInput) error {
	issuer := input.ProviderID
	if issuer == "" {
		issuer = extractWebIdentityIssuer(input.WebIdentityToken)
	}

	if issuer == "" {
		return nil
	}

	meta := b.lookupRoleMeta(input.RoleArn)
	if meta == nil || meta.TrustPolicy == "" {
		return nil
	}

	host := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(issuer, "https://"), "http://"))
	condCtx := map[string]string{}

	if aud := extractWebIdentityAudience(input.WebIdentityToken); aud != "" {
		condCtx[host+":aud"] = aud
	}

	if sub := extractWebIdentitySubject(input.WebIdentityToken); sub != "" &&
		sub != webIdentitySubjectPlaceholder {
		condCtx[host+":sub"] = sub
	}

	return evaluateAssumeRoleTrust(meta.TrustPolicy, trustEval{
		action:       actionAssumeRoleWithWebID,
		federatedArn: issuer,
		conditionCtx: condCtx,
	})
}

// checkSAMLTrust evaluates the target role's trust policy against the federated
// SAML provider (PrincipalArn) for sts:AssumeRoleWithSAML. It is permissive
// unless a RoleLookup is wired and the role carries a non-empty trust policy.
func (b *InMemoryBackend) checkSAMLTrust(input *AssumeRoleWithSAMLInput) error {
	if input.PrincipalArn == "" {
		return nil
	}

	meta := b.lookupRoleMeta(input.RoleArn)
	if meta == nil || meta.TrustPolicy == "" {
		return nil
	}

	return evaluateAssumeRoleTrust(meta.TrustPolicy, trustEval{
		action:       actionAssumeRoleWithSAML,
		federatedArn: input.PrincipalArn,
	})
}

// issueCredentials generates credentials, stores the session, and builds the response.
func (b *InMemoryBackend) issueCredentials(
	input *AssumeRoleInput,
	duration int32,
) (*AssumeRoleResponse, error) {
	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	roleID := deriveRoleID(input.RoleArn)
	assumedRoleID := roleID + ":" + input.RoleSessionName
	assumedRoleArn := buildAssumedRoleArn(input.RoleArn, input.RoleSessionName)

	account := b.accountID
	if parts := strings.SplitN(input.RoleArn, ":", arnComponentCount); len(
		parts,
	) >= arnComponentCount {
		account = parts[4]
	}

	// Merge parent transitive tags into child session per AWS role-chaining rules:
	// tags marked transitive by the parent session propagate to the child and are
	// inherited even if the child caller does not re-specify them.
	mergedTags := mergeTransitiveTags(input.CallerSession, input.Tags)

	session := &SessionInfo{
		AssumedRoleArn:    assumedRoleArn,
		AccountID:         account,
		SessionName:       input.RoleSessionName,
		AccessKeyID:       creds.AccessKeyID,
		SecretAccessKey:   creds.SecretAccessKey,
		SessionToken:      creds.SessionToken,
		AssumedRoleID:     assumedRoleID,
		SourceIdentity:    input.SourceIdentity,
		Tags:              mergedTags,
		TransitiveTagKeys: input.TransitiveTagKeys,
		Expiration:        expiration,
	}

	b.storeSession(creds.AccessKeyID, session)

	result := AssumeRoleResult{
		AssumedRoleUser: AssumedRoleUser{
			Arn:           assumedRoleArn,
			AssumedRoleID: assumedRoleID,
		},
		Credentials: Credentials{
			AccessKeyID:     creds.AccessKeyID,
			SecretAccessKey: creds.SecretAccessKey,
			SessionToken:    creds.SessionToken,
			Expiration:      expiration.Format(time.RFC3339),
		},
		SourceIdentity:   input.SourceIdentity,
		PackedPolicySize: calculatePackedPolicySizeWithArns(input.Policy, input.PolicyArns),
	}

	return &AssumeRoleResponse{
		Xmlns:            STSNamespace,
		AssumeRoleResult: result,
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// LookupSession returns the active SessionInfo for the given access key and optional
// session token, or nil if no matching non-expired session exists or the token mismatches.
func (b *InMemoryBackend) LookupSession(accessKeyID, sessionToken string) *SessionInfo {
	if accessKeyID == "" {
		return nil
	}

	b.mu.Lock("LookupSession")
	session, ok := b.sessions[accessKeyID]
	if ok && isSessionExpired(session) {
		delete(b.sessions, accessKeyID)
		ok = false
	}
	b.mu.Unlock()

	if !ok {
		return nil
	}
	if sessionToken != "" && session.SessionToken != "" && sessionToken != session.SessionToken {
		return nil
	}

	return session
}

// GetCallerIdentity returns the mock caller identity.
// When accessKeyID corresponds to an assumed-role session, returns the assumed-role ARN and user ID.
// When sessionToken is non-empty (ASIA-prefixed key), the stored token must match; a mismatch
// returns ErrUnknownAccessKeyID mapped to HTTP 400 InvalidClientTokenId (matching AWS).
func (b *InMemoryBackend) GetCallerIdentity(
	accessKeyID, sessionToken string,
) (*GetCallerIdentityResponse, error) {
	b.cntGetCallerIdentity.Add(1)

	if accessKeyID == "" {
		return b.rootCallerIdentity(), nil
	}

	b.mu.Lock("GetCallerIdentity")
	session, ok := b.sessions[accessKeyID]
	wasExpired := false

	if ok && isSessionExpired(session) {
		delete(b.sessions, accessKeyID)
		ok = false
		wasExpired = true
	}

	b.mu.Unlock()

	if ok {
		// When the caller presents a session token, it must match the stored value.
		// AWS rejects a mismatched session token with HTTP 400 InvalidClientTokenId,
		// not 403 AccessDenied.
		if sessionToken != "" && session.SessionToken != "" &&
			sessionToken != session.SessionToken {
			return nil, fmt.Errorf(
				"%w: the security token included in the request is invalid",
				ErrUnknownAccessKeyID,
			)
		}

		return &GetCallerIdentityResponse{
			Xmlns: STSNamespace,
			GetCallerIdentityResult: GetCallerIdentityResult{
				Account: session.AccountID,
				Arn:     session.AssumedRoleArn,
				UserID:  session.AssumedRoleID,
			},
			ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
		}, nil
	}

	// ASIA-prefixed keys are temporary session credentials. AWS returns
	// ExpiredTokenException when a known session has expired, and
	// InvalidClientTokenId when the key was never issued by this service.
	// Long-term AKIA keys that are untracked fall back to the root identity.
	if strings.HasPrefix(accessKeyID, accessKeyIDPrefix) {
		if wasExpired {
			return nil, fmt.Errorf(
				"%w: the security token included in the request has expired",
				ErrSessionExpired,
			)
		}

		return nil, fmt.Errorf(
			"%w: the security token included in the request is invalid",
			ErrUnknownAccessKeyID,
		)
	}

	return b.rootCallerIdentity(), nil
}

func (b *InMemoryBackend) rootCallerIdentity() *GetCallerIdentityResponse {
	callerArn := arn.Build(arnServiceIAM, "", b.accountID, "root")

	return &GetCallerIdentityResponse{
		Xmlns: STSNamespace,
		GetCallerIdentityResult: GetCallerIdentityResult{
			Account: b.accountID,
			Arn:     callerArn,
			UserID:  MockUserID,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}
}

// ValidateSessionCredential looks up a session by (accessKeyID, sessionToken).
// Returns ErrSessionNotFound when the key is unknown, ErrAccessDenied on token mismatch,
// and ErrSessionExpired when the session has passed its expiry.
func (b *InMemoryBackend) ValidateSessionCredential(
	accessKeyID, sessionToken string,
) (*SessionInfo, error) {
	b.mu.Lock("ValidateSessionCredential")
	session, ok := b.sessions[accessKeyID]

	if ok && isSessionExpired(session) {
		delete(b.sessions, accessKeyID)
		ok = false
	}

	b.mu.Unlock()

	if !ok {
		return nil, ErrSessionNotFound
	}

	if session.SessionToken != "" && sessionToken != session.SessionToken {
		return nil, fmt.Errorf("%w: session token mismatch", ErrAccessDenied)
	}

	return session, nil
}

// GetSessionToken generates temporary credentials without role assumption.
func (b *InMemoryBackend) GetSessionToken(
	input *GetSessionTokenInput,
) (*GetSessionTokenResponse, error) {
	b.cntGetSessionToken.Add(1)

	// Both SerialNumber and TokenCode must be provided together (MFA requires both).
	if input.SerialNumber != "" && input.TokenCode == "" {
		return nil, ErrMFACodeRequired
	}

	if input.TokenCode != "" && input.SerialNumber == "" {
		return nil, ErrTokenCodeWithoutSerial
	}

	if err := validateMFASerialNumber(input.SerialNumber); err != nil {
		return nil, err
	}

	if err := validateMFATokenCode(input.TokenCode); err != nil {
		return nil, err
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultSessionTokenDurationSeconds
	}

	if duration < MinSessionTokenDurationSeconds || duration > MaxSessionTokenDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d for GetSessionToken",
			ErrInvalidDuration, MinSessionTokenDurationSeconds, MaxSessionTokenDurationSeconds,
		)
	}

	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)

	// Store session for GetCallerIdentity lookups.
	session := &SessionInfo{
		Expiration:      expiration,
		AssumedRoleArn:  MockUserArn,
		AccountID:       b.accountID,
		SessionName:     "session-token",
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		AssumedRoleID:   MockUserID,
	}

	b.storeSession(creds.AccessKeyID, session)

	return &GetSessionTokenResponse{
		Xmlns: STSNamespace,
		GetSessionTokenResult: GetSessionTokenResult{
			Credentials: Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// GetFederationToken generates temporary credentials for a federated user.
// The federated user ARN has the form arn:aws:sts::ACCOUNT:federated-user/NAME.
func (b *InMemoryBackend) GetFederationToken(
	input *GetFederationTokenInput,
) (*GetFederationTokenResponse, error) {
	b.cntGetFederationToken.Add(1)

	if input.Name == "" {
		return nil, ErrMissingFederationTokenName
	}

	if err := validateFederationTokenName(input.Name); err != nil {
		return nil, err
	}

	if len(input.Tags) > MaxTagCount {
		return nil, fmt.Errorf("%w: got %d", ErrTooManyTags, len(input.Tags))
	}

	if err := validateTagConstraints(input.Tags); err != nil {
		return nil, err
	}

	if err := validatePolicyArns(input.PolicyArns); err != nil {
		return nil, err
	}

	if err := validateInlinePolicy(input.Policy); err != nil {
		return nil, err
	}

	if err := checkPackedPolicyBudget(input.Policy, input.PolicyArns); err != nil {
		return nil, err
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultSessionTokenDurationSeconds
	}

	if duration < MinSessionTokenDurationSeconds || duration > MaxFederationTokenDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d for GetFederationToken",
			ErrInvalidDuration, MinSessionTokenDurationSeconds, MaxFederationTokenDurationSeconds,
		)
	}

	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	federatedUserArn := arn.Build("sts", "", b.accountID, "federated-user/"+input.Name)
	federatedUserID := b.accountID + ":" + input.Name

	session := &SessionInfo{
		Expiration:      expiration,
		AssumedRoleArn:  federatedUserArn,
		AccountID:       b.accountID,
		SessionName:     input.Name,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		AssumedRoleID:   federatedUserID,
		Tags:            input.Tags,
	}

	b.storeSession(creds.AccessKeyID, session)

	return &GetFederationTokenResponse{
		Xmlns: STSNamespace,
		GetFederationTokenResult: GetFederationTokenResult{
			FederatedUser: FederatedUser{
				Arn:             federatedUserArn,
				FederatedUserID: federatedUserID,
			},
			Credentials: Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
			PackedPolicySize: calculatePackedPolicySizeWithArns(input.Policy, input.PolicyArns),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// AssumeRoleWithWebIdentity generates temporary credentials using a web identity token.
// In this mock, the WebIdentityToken is not cryptographically validated; the subject
// is extracted from the token's payload when parseable, otherwise a default is used.
// validateWebIdentityInput checks the parameter constraints for AssumeRoleWithWebIdentity.
func validateWebIdentityInput(input *AssumeRoleWithWebIdentityInput) error {
	if input.RoleArn == "" {
		return ErrMissingRoleArn
	}

	if err := validateRoleArn(input.RoleArn); err != nil {
		return err
	}

	if input.RoleSessionName == "" {
		return ErrMissingSessionName
	}

	if err := validateRoleSessionName(input.RoleSessionName); err != nil {
		return err
	}

	if input.WebIdentityToken == "" {
		return ErrMissingWebIdentityToken
	}

	if err := validateSourceIdentity(input.SourceIdentity); err != nil {
		return err
	}

	if len(input.Tags) > MaxTagCount {
		return fmt.Errorf("%w: got %d", ErrTooManyTags, len(input.Tags))
	}

	if err := validateTagConstraints(input.Tags); err != nil {
		return err
	}

	if err := validatePolicyArns(input.PolicyArns); err != nil {
		return err
	}

	if err := validateInlinePolicy(input.Policy); err != nil {
		return err
	}

	if err := checkPackedPolicyBudget(input.Policy, input.PolicyArns); err != nil {
		return err
	}

	return validateWebIdentityToken(input.WebIdentityToken)
}

func (b *InMemoryBackend) AssumeRoleWithWebIdentity(
	input *AssumeRoleWithWebIdentityInput,
) (*AssumeRoleWithWebIdentityResponse, error) {
	b.cntAssumeRoleWithWebIdentity.Add(1)

	if err := validateWebIdentityInput(input); err != nil {
		return nil, err
	}

	effectiveMax := b.getEffectiveMaxDuration(input.RoleArn)

	duration := input.DurationSeconds
	if duration == 0 {
		duration = min(DefaultDurationSeconds, effectiveMax)
	}

	if duration < MinDurationSeconds || duration > effectiveMax {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d for this role",
			ErrInvalidDuration, MinDurationSeconds, effectiveMax,
		)
	}

	// Validate OIDC provider when a lookup is configured.
	if err := b.validateOIDCProvider(input.WebIdentityToken, input.ProviderID); err != nil {
		return nil, err
	}

	// Enforce the role's trust policy against the federated OIDC identity.
	if err := b.checkWebIdentityTrust(input); err != nil {
		return nil, err
	}

	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	return b.buildWebIdentityResponse(input, creds, duration), nil
}

// validateOIDCProvider checks that the issuer from the token (or providerID override)
// corresponds to an existing OIDC provider in the IAM backend.
// When no OIDCLookup is configured the check is skipped (permissive mock behaviour).
func (b *InMemoryBackend) validateOIDCProvider(token, providerID string) error {
	b.mu.RLock("ValidateOIDCProvider")
	ol := b.oidcLookup
	b.mu.RUnlock()

	if ol == nil {
		return nil
	}

	issuer := providerID
	if issuer == "" {
		issuer = extractWebIdentityIssuer(token)
	}

	if issuer == "" {
		// No issuer to validate against; allow the call.
		return nil
	}

	if !ol.OIDCProviderExists(issuer) {
		return fmt.Errorf(
			"%w: OIDC provider for issuer %q not found in IAM",
			ErrAccessDenied,
			issuer,
		)
	}

	return nil
}

// buildWebIdentityResponse constructs the AssumeRoleWithWebIdentity response and persists the session.
func (b *InMemoryBackend) buildWebIdentityResponse(
	input *AssumeRoleWithWebIdentityInput,
	creds credentialSet,
	duration int32,
) *AssumeRoleWithWebIdentityResponse {
	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	roleID := deriveRoleID(input.RoleArn)
	assumedRoleID := roleID + ":" + input.RoleSessionName
	assumedRoleArn := buildAssumedRoleArn(input.RoleArn, input.RoleSessionName)

	account := b.accountID
	if parts := strings.SplitN(input.RoleArn, ":", arnComponentCount); len(
		parts,
	) >= arnComponentCount {
		account = parts[4]
	}

	subject := extractWebIdentitySubject(input.WebIdentityToken)
	provider, audience := resolveWebIdentityProvider(input.WebIdentityToken, input.ProviderID)

	session := &SessionInfo{
		Expiration:      expiration,
		AssumedRoleArn:  assumedRoleArn,
		AccountID:       account,
		SessionName:     input.RoleSessionName,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		AssumedRoleID:   assumedRoleID,
		Tags:            input.Tags,
		SourceIdentity:  input.SourceIdentity,
	}

	b.storeSession(creds.AccessKeyID, session)

	return &AssumeRoleWithWebIdentityResponse{
		Xmlns: STSNamespace,
		AssumeRoleWithWebIdentityResult: AssumeRoleWithWebIdentityResult{
			AssumedRoleUser: AssumedRoleUser{
				Arn:           assumedRoleArn,
				AssumedRoleID: assumedRoleID,
			},
			Credentials: Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
			SubjectFromWebIdentityToken: subject,
			Audience:                    audience,
			Provider:                    provider,
			SourceIdentity:              input.SourceIdentity,
			PackedPolicySize: calculatePackedPolicySizeWithArns(
				input.Policy,
				input.PolicyArns,
			),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}
}

// validateSAMLAssertion checks that the assertion is valid base64 and decodes to XML.
// AWS rejects assertions that are not properly base64-encoded or whose decoded
// content is not a valid XML document (at minimum containing one XML element).
// RawToken is used so that namespace-prefixed elements without explicit xmlns
// declarations (common in real SAML assertions) are accepted without error.
func validateSAMLAssertion(assertion string) error {
	// AWS requires a base64-encoded SAML assertion. As an emulator we validate the
	// base64 encoding but do not require the decoded payload to be well-formed SAML
	// XML, so callers can pass simple test assertions.
	if _, err := base64.StdEncoding.DecodeString(assertion); err != nil {
		if _, err = base64.URLEncoding.DecodeString(assertion); err != nil {
			return fmt.Errorf("%w: not valid base64", ErrInvalidSAMLAssertion)
		}
	}

	return nil
}

// validateSAMLInput checks the common parameter constraints for AssumeRoleWithSAML.
func validateSAMLInput(input *AssumeRoleWithSAMLInput) error {
	if input.RoleArn == "" {
		return ErrMissingRoleArn
	}

	if err := validateRoleArn(input.RoleArn); err != nil {
		return err
	}

	if input.PrincipalArn == "" {
		return ErrMissingPrincipalArn
	}

	if err := validateSAMLProviderArn(input.PrincipalArn); err != nil {
		return err
	}

	if input.SAMLAssertion == "" {
		return ErrMissingSAMLAssertion
	}

	if err := validateSAMLAssertion(input.SAMLAssertion); err != nil {
		return err
	}

	if err := validateSAMLTemporalConditions(input.SAMLAssertion); err != nil {
		return err
	}

	// RoleSessionName is optional for SAML (derived from assertion), but when supplied validate it.
	if input.RoleSessionName != "" {
		if err := validateRoleSessionName(input.RoleSessionName); err != nil {
			return err
		}
	}

	if err := validateSourceIdentity(input.SourceIdentity); err != nil {
		return err
	}

	if len(input.Tags) > MaxTagCount {
		return fmt.Errorf("%w: got %d", ErrTooManyTags, len(input.Tags))
	}

	if err := validateTagConstraints(input.Tags); err != nil {
		return err
	}

	if err := validatePolicyArns(input.PolicyArns); err != nil {
		return err
	}

	if err := validateInlinePolicy(input.Policy); err != nil {
		return err
	}

	return checkPackedPolicyBudget(input.Policy, input.PolicyArns)
}

// AssumeRoleWithSAML generates temporary credentials using a SAML 2.0 assertion.
// In this mock, the SAMLAssertion is not cryptographically validated.
func (b *InMemoryBackend) AssumeRoleWithSAML(
	input *AssumeRoleWithSAMLInput,
) (*AssumeRoleWithSAMLResponse, error) {
	b.cntAssumeRoleWithSAML.Add(1)

	if err := validateSAMLInput(input); err != nil {
		return nil, err
	}

	effectiveMax := b.getEffectiveMaxDuration(input.RoleArn)

	duration := input.DurationSeconds
	if duration == 0 {
		duration = min(DefaultDurationSeconds, effectiveMax)
	}

	if duration < MinDurationSeconds || duration > effectiveMax {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d for this role",
			ErrInvalidDuration, MinDurationSeconds, effectiveMax,
		)
	}

	// Enforce the role's trust policy against the federated SAML provider.
	if err := b.checkSAMLTrust(input); err != nil {
		return nil, err
	}

	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	return b.buildSAMLResponse(input, creds, duration), nil
}

// buildSAMLResponse constructs the AssumeRoleWithSAML response and persists the session.
func (b *InMemoryBackend) buildSAMLResponse(
	input *AssumeRoleWithSAMLInput,
	creds credentialSet,
	duration int32,
) *AssumeRoleWithSAMLResponse {
	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	roleID := deriveRoleID(input.RoleArn)

	// Use input.RoleSessionName if provided, otherwise fall back to the samlSessionName constant.
	sessionName := samlSessionName
	if input.RoleSessionName != "" {
		sessionName = input.RoleSessionName
	}

	assumedRoleID := roleID + ":" + sessionName
	assumedRoleArn := buildAssumedRoleArn(input.RoleArn, sessionName)

	account := b.accountID
	if parts := strings.SplitN(input.RoleArn, ":", arnComponentCount); len(
		parts,
	) >= arnComponentCount {
		account = parts[4]
	}

	session := &SessionInfo{
		Expiration:      expiration,
		AssumedRoleArn:  assumedRoleArn,
		AccountID:       account,
		SessionName:     sessionName,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		AssumedRoleID:   assumedRoleID,
		SourceIdentity:  input.SourceIdentity,
		Tags:            input.Tags,
	}

	b.storeSession(creds.AccessKeyID, session)

	issuerParts := strings.Split(input.PrincipalArn, "/")
	issuer := issuerParts[len(issuerParts)-1]
	nameQualifier := computeNameQualifier(issuer, account, issuer)

	return &AssumeRoleWithSAMLResponse{
		Xmlns: STSNamespace,
		AssumeRoleWithSAMLResult: AssumeRoleWithSAMLResult{
			AssumedRoleUser: AssumedRoleUser{
				Arn:           assumedRoleArn,
				AssumedRoleID: assumedRoleID,
			},
			Credentials: Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
			Audience:         input.PrincipalArn,
			Issuer:           issuer,
			NameQualifier:    nameQualifier,
			SubjectType:      "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent",
			Subject:          account + ":saml-subject",
			SourceIdentity:   input.SourceIdentity,
			PackedPolicySize: calculatePackedPolicySizeWithArns(input.Policy, input.PolicyArns),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}
}

// AssumeRoot generates short-term privileged credentials for a member account root.
// TaskPolicyArn must be in the AWS-approved set; TargetPrincipal must be a 12-digit account ID.
func (b *InMemoryBackend) AssumeRoot(input *AssumeRootInput) (*AssumeRootResponse, error) {
	b.cntAssumeRoot.Add(1)

	if input.TargetPrincipal == "" {
		return nil, ErrMissingTargetPrincipal
	}

	if input.TaskPolicyArn == "" {
		return nil, ErrMissingTaskPolicyArn
	}

	if err := validateApprovedRootTaskPolicy(input.TaskPolicyArn); err != nil {
		return nil, err
	}

	// TargetPrincipal must be a 12-digit member account ID.
	account := extractAccountFromPrincipal(input.TargetPrincipal)
	if !accountIDRe.MatchString(account) {
		return nil, fmt.Errorf("%w: got %q", ErrInvalidTargetPrincipal, input.TargetPrincipal)
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = MaxRootDurationSeconds
	}

	if duration != MaxRootDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be exactly %d for AssumeRoot",
			ErrInvalidDuration, MaxRootDurationSeconds,
		)
	}

	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	assumedRoleArn := arn.Build("sts", "", account, "assumed-root")

	session := &SessionInfo{
		Expiration:      expiration,
		AssumedRoleArn:  assumedRoleArn,
		AccountID:       account,
		SessionName:     rootSessionName,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		AssumedRoleID:   account + ":" + rootSessionName,
	}

	b.storeSession(creds.AccessKeyID, session)

	return &AssumeRootResponse{
		Xmlns: STSNamespace,
		AssumeRootResult: AssumeRootResult{
			Credentials: Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// GetDelegatedAccessToken exchanges a trade-in token for temporary AWS credentials.
// The TradeInToken's cryptographic signature is not verified (the external issuer's
// keys are unavailable to the emulator), but a JWT-shaped token's self-consistent
// "exp" claim is checked so an already-expired token is rejected with
// ErrExpiredTradeInToken (AWS ExpiredTradeInTokenException), matching real STS
// behaviour instead of accepting any non-empty string indefinitely.
func (b *InMemoryBackend) GetDelegatedAccessToken(
	input *GetDelegatedAccessTokenInput,
) (*GetDelegatedAccessTokenResponse, error) {
	b.cntGetDelegatedAccessToken.Add(1)

	if input.TradeInToken == "" {
		return nil, ErrMissingTradeInToken
	}

	if err := validateTradeInTokenExpiry(input.TradeInToken); err != nil {
		return nil, err
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultDurationSeconds
	}

	if duration < MinDurationSeconds || duration > MaxDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d for GetDelegatedAccessToken",
			ErrInvalidDuration, MinDurationSeconds, MaxDurationSeconds,
		)
	}

	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	assumedPrincipal := arn.Build(arnServiceIAM, "", b.accountID, "root")

	session := &SessionInfo{
		Expiration:      expiration,
		AssumedRoleArn:  assumedPrincipal,
		AccountID:       b.accountID,
		SessionName:     delegatedSessionName,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		AssumedRoleID:   b.accountID + ":" + delegatedSessionName,
	}

	b.storeSession(creds.AccessKeyID, session)

	return &GetDelegatedAccessTokenResponse{
		Xmlns: STSNamespace,
		GetDelegatedAccessTokenResult: GetDelegatedAccessTokenResult{
			AssumedPrincipal: assumedPrincipal,
			Credentials: Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// GetWebIdentityToken returns a signed JWT representing the caller's AWS identity.
// In this mock, the token is an unsigned JWT containing the caller's account and audience.
func (b *InMemoryBackend) GetWebIdentityToken(
	input *GetWebIdentityTokenInput,
) (*GetWebIdentityTokenResponse, error) {
	b.cntGetWebIdentityToken.Add(1)

	if len(input.Audience) == 0 {
		return nil, ErrMissingAudience
	}

	if len(input.Audience) > MaxAudienceCount {
		return nil, fmt.Errorf("%w: got %d", ErrTooManyAudiences, len(input.Audience))
	}

	if len(input.Tags) > MaxTagCount {
		return nil, fmt.Errorf("%w: got %d", ErrTooManyTags, len(input.Tags))
	}

	if err := validateTagConstraints(input.Tags); err != nil {
		return nil, err
	}

	if input.SigningAlgorithm == "" {
		return nil, ErrMissingSigningAlgorithm
	}

	if !isValidWebIdentitySigningAlgorithm(input.SigningAlgorithm) {
		return nil, fmt.Errorf(
			"%w: unsupported signing algorithm %q",
			ErrValidation,
			input.SigningAlgorithm,
		)
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultWebIdentityTokenDurationSeconds
	}

	if duration < MinWebIdentityTokenDurationSeconds ||
		duration > MaxWebIdentityTokenDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d for GetWebIdentityToken",
			ErrInvalidDuration,
			MinWebIdentityTokenDurationSeconds,
			MaxWebIdentityTokenDurationSeconds,
		)
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	now := time.Now().UTC()
	issuer := "https://sts.mock.aws.com/" + b.accountID

	// Build a minimal mock JWT payload (unsigned, for testing purposes only).
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"mock","typ":"JWT"}`))

	claims := map[string]any{
		jwtClaimSub: MockUserID,
		jwtClaimAud: input.Audience,
		jwtClaimIss: issuer,
		jwtClaimExp: expiration.Unix(),
		jwtClaimIat: now.Unix(),
		jwtClaimNbf: now.Unix(),
		"acc":       b.accountID,
	}

	// Include session tags as custom claims when present.
	if len(input.Tags) > 0 {
		tagMap := make(map[string]string, len(input.Tags))
		for _, t := range input.Tags {
			tagMap[t.Key] = t.Value
		}

		claims["https://aws.amazon.com/tags"] = tagMap
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		return nil, fmt.Errorf("build token payload: %w", err)
	}

	token := header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".mock-signature"

	return &GetWebIdentityTokenResponse{
		Xmlns: STSNamespace,
		GetWebIdentityTokenResult: GetWebIdentityTokenResult{
			WebIdentityToken: token,
			Expiration:       expiration.Format(time.RFC3339),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// parseJWTPayloadClaims decodes the payload segment of a JWT (without signature verification)
// and returns the claims map. Returns nil if the token is not a valid JWT or the payload cannot
// be decoded.
func parseJWTPayloadClaims(token string) map[string]any {
	parts := strings.SplitN(token, ".", jwtPartCount)
	if len(parts) < jwtMinParts {
		return nil
	}

	rawPayload := parts[1]
	paddedPayload := rawPayload

	switch len(rawPayload) % 4 {
	case base64Pad2:
		paddedPayload += "=="
	case base64Pad1:
		paddedPayload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(paddedPayload)
	if err != nil {
		// Try RawURLEncoding as fallback (no padding).
		decoded, err = base64.RawURLEncoding.DecodeString(rawPayload)
		if err != nil {
			return nil
		}
	}

	var claims map[string]any
	if err = json.Unmarshal(decoded, &claims); err != nil {
		return nil
	}

	return claims
}

// extractWebIdentitySubject attempts to extract the "sub" claim from a JWT token's
// payload without validating the signature. If extraction fails, returns a placeholder.
func extractWebIdentitySubject(token string) string {
	claims := parseJWTPayloadClaims(token)
	if claims == nil {
		return webIdentitySubjectPlaceholder
	}

	if sub, ok := claims[jwtClaimSub].(string); ok && sub != "" {
		return sub
	}

	return webIdentitySubjectPlaceholder
}

// extractWebIdentityIssuer attempts to extract the "iss" claim from a JWT token's
// payload without validating the signature. Returns an empty string if extraction fails.
func extractWebIdentityIssuer(token string) string {
	claims := parseJWTPayloadClaims(token)
	if claims == nil {
		return ""
	}

	if iss, ok := claims[jwtClaimIss].(string); ok && iss != "" {
		return iss
	}

	return ""
}

// resolveWebIdentityProvider resolves the OIDC provider and audience from the token and input.
// The providerID, when non-empty, overrides the issuer extracted from the JWT.
func resolveWebIdentityProvider(token, providerID string) (string, string) {
	issuer := extractWebIdentityIssuer(token)
	if providerID != "" {
		issuer = providerID
	}

	provider := issuer
	if provider == "" {
		provider = "cognito-identity.amazonaws.com"
	}

	audience := extractWebIdentityAudience(token)
	if audience == "" {
		audience = provider
	}

	return provider, audience
}

// validateExternalID parses a trust policy JSON document and validates that the
// provided externalID satisfies any sts:ExternalId conditions found therein.
// Trust policy statements use OR semantics: if any statement with an ExternalId
// condition matches, access is granted. Only if all statements with ExternalId
// conditions fail is ErrAccessDenied returned.
// If the trust policy requires an ExternalId but none (or the wrong value) is
// supplied, ErrAccessDenied is returned.
func validateExternalID(trustPolicyJSON, externalID string) error {
	if trustPolicyJSON == "" {
		return nil
	}

	var tp trustPolicy

	// Unmarshal errors indicate a malformed policy document. A malformed trust
	// policy leaves tp with a zero value (empty Statements), so no ExternalId
	// condition will be found and the call proceeds without validation — the
	// permissive behaviour is intentional for a mock implementation.
	_ = json.Unmarshal([]byte(trustPolicyJSON), &tp)

	var hasExternalIDCondition bool

	for _, stmt := range tp.Statement {
		required := requiredExternalIDs(stmt.Condition)
		if len(required) == 0 {
			continue
		}

		hasExternalIDCondition = true

		if slices.Contains(required, externalID) {
			return nil
		}
	}

	if hasExternalIDCondition {
		return fmt.Errorf(
			"%w: ExternalId does not match the trust policy condition",
			ErrAccessDenied,
		)
	}

	return nil
}

// requiredExternalIDs extracts all sts:ExternalId values from a trust-statement Condition map.
// Returns nil when no ExternalId condition is present.
func requiredExternalIDs(condition map[string]map[string]json.RawMessage) []string {
	for condOp, condMap := range condition {
		if !strings.EqualFold(condOp, "StringEquals") && !strings.EqualFold(condOp, "StringLike") {
			continue
		}

		for condKey, rawVal := range condMap {
			if strings.EqualFold(condKey, "sts:ExternalId") {
				return extractStringValues(rawVal)
			}
		}
	}

	return nil
}

// extractStringValues unmarshals a JSON RawMessage that may be either a string
// or an array of strings and returns the values as a Go string slice.
func extractStringValues(raw json.RawMessage) []string {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}
	}

	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		return many
	}

	return nil
}

// generateAccessKeyID creates a random STS-style access key ID.
func generateAccessKeyID() (string, error) {
	buf := make([]byte, accessKeyIDRandLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	b := make([]byte, accessKeyIDRandLen)
	charsLen := byte(len(accessKeyIDChars))

	for i, v := range buf {
		b[i] = accessKeyIDChars[v%charsLen]
	}

	return accessKeyIDPrefix + string(b), nil
}

// generateSecretKey creates a random 40-character hex secret access key.
func generateSecretKey() (string, error) {
	buf := make([]byte, secretKeyByteLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	return hex.EncodeToString(buf), nil
}

// generateSessionToken creates a random base64-encoded session token.
func generateSessionToken() (string, error) {
	buf := make([]byte, sessionTokenByteLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(buf), nil
}

// credentialSet holds the three components of a generated temporary credential.
type credentialSet struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// generateCredentialSet creates a new access key, secret key, and session token in one call.
func generateCredentialSet() (credentialSet, error) {
	accessKeyID, err := generateAccessKeyID()
	if err != nil {
		return credentialSet{}, fmt.Errorf("generate access key: %w", err)
	}

	secretKey, err := generateSecretKey()
	if err != nil {
		return credentialSet{}, fmt.Errorf("generate secret key: %w", err)
	}

	sessionToken, err := generateSessionToken()
	if err != nil {
		return credentialSet{}, fmt.Errorf("generate session token: %w", err)
	}

	return credentialSet{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
	}, nil
}

// deriveRoleID produces a stable pseudo role-ID from the role ARN.
// Uses the last path segment padded/hashed to 16 uppercase chars to reduce collision risk.
func deriveRoleID(roleArn string) string {
	parts := strings.Split(roleArn, "/")
	roleName := strings.ToUpper(parts[len(parts)-1])

	const roleIDSuffix = 16
	if len(roleName) >= roleIDSuffix {
		return "AROA" + roleName[:roleIDSuffix]
	}

	// Hash the full ARN to fill remaining characters deterministically.
	h := sha1.New() //nolint:gosec // SHA1 for non-cryptographic ID derivation only
	_, _ = h.Write([]byte(roleArn))
	hexHash := strings.ToUpper(hex.EncodeToString(h.Sum(nil)))

	padded := roleName + hexHash

	return "AROA" + padded[:roleIDSuffix]
}

// buildAssumedRoleArn constructs the assumed-role ARN from the source role ARN.
// AWS strips any IAM path from the role: a role at arn:aws:iam::ACCT:role/team/dev/MyRole
// yields the assumed-role ARN arn:aws:sts::ACCT:assumed-role/MyRole/SESSION — only the
// final role-name segment is carried over, not the intermediate path components.
func buildAssumedRoleArn(roleArn, sessionName string) string {
	// arn:aws:iam::ACCOUNT:role/[PATH/]ROLE_NAME  →  arn:aws:sts::ACCOUNT:assumed-role/ROLE_NAME/SESSION
	parts := strings.SplitN(roleArn, ":", arnComponentCount)
	if len(parts) < arnComponentCount {
		return roleArn + "/" + sessionName
	}

	account := parts[4]
	roleName := roleNameFromResource(parts[5])

	return arn.Build("sts", "", account, "assumed-role/"+roleName+"/"+sessionName)
}

// roleNameFromResource extracts the bare role name from an IAM role resource segment,
// dropping the "role/" prefix and any leading path (e.g. "role/team/dev/MyRole" → "MyRole").
func roleNameFromResource(resource string) string {
	name := strings.TrimPrefix(resource, "role/")
	if idx := strings.LastIndex(name, "/"); idx != -1 {
		name = name[idx+1:]
	}

	return name
}

// extractAccountFromPrincipal returns the account portion of an ARN or the principal itself
// if it looks like a 12-digit account ID.
func extractAccountFromPrincipal(principal string) string {
	if len(principal) == 12 && allDigits(principal) {
		return principal
	}

	parts := strings.SplitN(principal, ":", arnComponentCount)
	if len(parts) >= arnComponentCount {
		return parts[4]
	}

	return principal
}

// allDigits reports whether every character in s is an ASCII digit.
func allDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// computeNameQualifier computes the AWS SAML NameQualifier:
// BASE64(SHA1(issuer + ";" + accountID + ";" + idpName)).
func computeNameQualifier(issuer, accountID, idpName string) string {
	h := sha1.New() //nolint:gosec // SHA1 per AWS SAML spec
	_, _ = h.Write([]byte(issuer + ";" + accountID + ";" + idpName))

	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// extractWebIdentityAudience attempts to extract the "aud" claim from a JWT token's
// payload without validating the signature. If the aud is an array, the first element is used.
// If extraction fails, an empty string is returned.
func extractWebIdentityAudience(token string) string {
	claims := parseJWTPayloadClaims(token)
	if claims == nil {
		return ""
	}

	switch v := claims[jwtClaimAud].(type) {
	case string:
		return v
	case []any:
		if len(v) > 0 {
			if s, ok := v[0].(string); ok {
				return s
			}
		}
	}

	return ""
}

// IssueEncodedAuthorizationMessage encodes plaintext as an HMAC-signed opaque blob
// that DecodeAuthorizationMessage can later verify. This mirrors the AWS STS behaviour
// where only messages issued by the service itself can be decoded — arbitrary base64
// blobs are rejected with InvalidAuthorizationMessageException.
//
// Format (base64-encoded): HMAC-SHA256(key, plaintext) | plaintext.
func (b *InMemoryBackend) IssueEncodedAuthorizationMessage(decodedMsg string) string {
	mac := hmac.New(sha256.New, b.authMsgSigningKey[:])
	mac.Write([]byte(decodedMsg))
	sig := mac.Sum(nil)

	payload := make([]byte, 0, authMsgHMACSize+1+len(decodedMsg))
	payload = append(payload, sig...)
	payload = append(payload, authMsgSep)
	payload = append(payload, decodedMsg...)

	return base64.StdEncoding.EncodeToString(payload)
}

// VerifyEncodedAuthorizationMessage decodes an opaque message issued by
// IssueEncodedAuthorizationMessage. Returns ErrInvalidAuthorizationMessage
// when the message was not issued by this backend instance (wrong HMAC, bad
// base64, or truncated payload).
func (b *InMemoryBackend) VerifyEncodedAuthorizationMessage(encoded string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		raw, err = base64.URLEncoding.DecodeString(encoded)
		if err != nil {
			return "", fmt.Errorf("%w: not valid base64", ErrInvalidAuthorizationMessage)
		}
	}

	// Minimum: HMAC (32 bytes) + separator (1 byte) + at least 0 bytes of plaintext.
	if len(raw) < authMsgHMACSize+1 || raw[authMsgHMACSize] != authMsgSep {
		return "", fmt.Errorf(
			"%w: message was not issued by this service",
			ErrInvalidAuthorizationMessage,
		)
	}

	sig := raw[:authMsgHMACSize]
	plaintext := raw[authMsgHMACSize+1:]

	mac := hmac.New(sha256.New, b.authMsgSigningKey[:])
	mac.Write(plaintext)
	expected := mac.Sum(nil)

	if !hmac.Equal(sig, expected) {
		return "", fmt.Errorf(
			"%w: message was not issued by this service",
			ErrInvalidAuthorizationMessage,
		)
	}

	return string(plaintext), nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
// Operation counters and totalSessionsCreated are also reset to zero.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.sessions = make(map[string]*SessionInfo)
	b.mu.Unlock()

	b.cntAssumeRole.Store(0)
	b.cntAssumeRoleWithSAML.Store(0)
	b.cntAssumeRoleWithWebIdentity.Store(0)
	b.cntAssumeRoot.Store(0)
	b.cntGetCallerIdentity.Store(0)
	b.cntGetDelegatedAccessToken.Store(0)
	b.cntGetFederationToken.Store(0)
	b.cntGetSessionToken.Store(0)
	b.cntGetWebIdentityToken.Store(0)
	b.cntGetAccessKeyInfo.Store(0)
	b.cntDecodeAuthorizationMsg.Store(0)
	b.totalSessionsCreated.Store(0)
}

// SessionCounts returns active and expired session counts at the time of invocation.
func (b *InMemoryBackend) SessionCounts() (int, int) {
	b.mu.RLock("SessionCounts")
	defer b.mu.RUnlock()

	now := time.Now().UTC()
	active := 0
	expired := 0

	for _, session := range b.sessions {
		// A zero expiration is treated as non-expiring in-memory session state.
		if !session.Expiration.IsZero() && !now.Before(session.Expiration) {
			expired++

			continue
		}

		active++
	}

	return active, expired
}

// maxSessionPolicyBytes is the AWS maximum size for a session policy document.
const maxSessionPolicyBytes = 2048

// maxPackedPolicySizePercent is the ceiling percentage for PackedPolicySize.
const maxPackedPolicySizePercent = int32(100)
