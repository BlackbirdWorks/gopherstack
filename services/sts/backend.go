package sts

import (
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // SHA1 is used only for NameQualifier per AWS spec, not for security
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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
	ErrMissingWebIdentityToken = errors.New("WebIdentityToken is required for AssumeRoleWithWebIdentity")

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
	ErrMissingEncodedMessage = errors.New("EncodedMessage is required for DecodeAuthorizationMessage")

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

	// ErrInvalidIdentityToken is returned when a web-identity JWT is structurally invalid or its claims are wrong.
	ErrInvalidIdentityToken = errors.New("invalid identity token")

	// ErrIDPRejectedClaim is returned when the identity provider rejects the claim.
	ErrIDPRejectedClaim = errors.New("IDP rejected claim")

	// ErrInvalidAuthorizationMessage is returned when DecodeAuthorizationMessage receives a non-STS-issued blob.
	ErrInvalidAuthorizationMessage = errors.New("invalid authorization message")

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

// evictExpiredSessionsLocked removes expired sessions from the map. It is a no-op
// below sessionEvictThreshold so steady-state inserts stay O(1). The caller must
// hold b.mu.
func (b *InMemoryBackend) evictExpiredSessionsLocked() {
	if len(b.sessions) < sessionEvictThreshold {
		return
	}

	for id, session := range b.sessions {
		if isSessionExpired(session) {
			delete(b.sessions, id)
		}
	}
}

// storeSession registers a new session under its access key ID, increments the
// lifetime counter, and opportunistically evicts expired sessions so the map
// stays bounded even when the background janitor is not running.
func (b *InMemoryBackend) storeSession(accessKeyID string, session *SessionInfo) {
	b.mu.Lock()
	b.evictExpiredSessionsLocked()
	b.sessions[accessKeyID] = session
	b.totalSessionsCreated.Add(1)
	b.mu.Unlock()
}

// validateRoleArn checks that a role ARN is a valid IAM role ARN:
// - format: arn:<partition>:iam::<12-digit-account>:role/<name>.
func validateRoleArn(roleArn string) error {
	parts := strings.SplitN(roleArn, ":", arnComponentCount)
	if len(parts) < arnComponentCount || parts[0] != "arn" || parts[2] != "iam" {
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
		return fmt.Errorf("%w: federation token name contains invalid characters", ErrInvalidFederationName)
	}

	return nil
}

// isValidWebIdentitySigningAlgorithm reports whether the given signing algorithm is supported.
func isValidWebIdentitySigningAlgorithm(alg string) bool {
	switch alg {
	case "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512":
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

// InMemoryBackend is a stateful in-memory STS backend.
type InMemoryBackend struct {
	roleLookup RoleLookup
	oidcLookup OIDCLookup
	sessions   map[string]*SessionInfo
	accountID  string
	mu         sync.Mutex

	// Operation call counters — incremented atomically.
	cntAssumeRole                atomic.Int64
	cntAssumeRoleWithSAML        atomic.Int64
	cntAssumeRoleWithWebIdentity atomic.Int64
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
}

// NewInMemoryBackend creates a new InMemoryBackend with the default account ID.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID.
func NewInMemoryBackendWithConfig(accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID: accountID,
		sessions:  make(map[string]*SessionInfo),
	}
}

// SetRoleLookup wires an optional role-lookup implementation (e.g. the IAM backend)
// so that AssumeRole can validate ExternalId and enforce MaxSessionDuration.
func (b *InMemoryBackend) SetRoleLookup(rl RoleLookup) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.roleLookup = rl
}

// SetOIDCLookup wires an optional OIDC-lookup implementation (e.g. the IAM backend)
// so that AssumeRoleWithWebIdentity can validate that the OIDC provider exists.
func (b *InMemoryBackend) SetOIDCLookup(ol OIDCLookup) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.oidcLookup = ol
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string {
	b.mu.Lock()
	defer b.mu.Unlock()

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

	b.mu.Lock()
	rl := b.roleLookup
	b.mu.Unlock()

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
func (b *InMemoryBackend) validateAndGetMaxDuration(input *AssumeRoleInput) (int32, error) {
	effectiveMax := int32(MaxDurationSeconds)

	b.mu.Lock()
	rl := b.roleLookup
	b.mu.Unlock()

	if rl == nil {
		return effectiveMax, nil
	}

	meta, _ := rl.GetRoleByArn(input.RoleArn)
	if meta == nil {
		// Role not in lookup; allow the call without validation.
		return effectiveMax, nil
	}

	if err2 := validateExternalID(meta.TrustPolicy, input.ExternalID); err2 != nil {
		return 0, err2
	}

	if meta.MaxSessionDuration > 0 {
		effectiveMax = meta.MaxSessionDuration
	}

	return effectiveMax, nil
}

// issueCredentials generates credentials, stores the session, and builds the response.
func (b *InMemoryBackend) issueCredentials(input *AssumeRoleInput, duration int32) (*AssumeRoleResponse, error) {
	creds, err := generateCredentialSet()
	if err != nil {
		return nil, err
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	roleID := deriveRoleID(input.RoleArn)
	assumedRoleID := roleID + ":" + input.RoleSessionName
	assumedRoleArn := buildAssumedRoleArn(input.RoleArn, input.RoleSessionName)

	account := b.accountID
	if parts := strings.SplitN(input.RoleArn, ":", arnComponentCount); len(parts) >= arnComponentCount {
		account = parts[4]
	}

	session := &SessionInfo{
		AssumedRoleArn:    assumedRoleArn,
		AccountID:         account,
		SessionName:       input.RoleSessionName,
		AccessKeyID:       creds.AccessKeyID,
		SecretAccessKey:   creds.SecretAccessKey,
		SessionToken:      creds.SessionToken,
		AssumedRoleID:     assumedRoleID,
		SourceIdentity:    input.SourceIdentity,
		Tags:              input.Tags,
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

// GetCallerIdentity returns the mock caller identity.
// When accessKeyID corresponds to an assumed-role session, returns the assumed-role ARN and user ID.
// When sessionToken is non-empty (ASIA-prefixed key), the stored token must match; a mismatch
// returns ErrUnknownAccessKeyID mapped to HTTP 400 InvalidClientTokenId (matching AWS).
func (b *InMemoryBackend) GetCallerIdentity(accessKeyID, sessionToken string) (*GetCallerIdentityResponse, error) {
	b.cntGetCallerIdentity.Add(1)

	if accessKeyID != "" {
		b.mu.Lock()
		session, ok := b.sessions[accessKeyID]

		if ok && isSessionExpired(session) {
			delete(b.sessions, accessKeyID)
			ok = false
		}

		b.mu.Unlock()

		if ok {
			if sessionToken != "" && session.SessionToken != "" && sessionToken != session.SessionToken {
				return nil, fmt.Errorf("%w: the security token included in the request is invalid", ErrAccessDenied)
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
	}

	callerArn := arn.Build("iam", "", b.accountID, "root")

	return &GetCallerIdentityResponse{
		Xmlns: STSNamespace,
		GetCallerIdentityResult: GetCallerIdentityResult{
			Account: b.accountID,
			Arn:     callerArn,
			UserID:  MockUserID,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// ValidateSessionCredential looks up a session by (accessKeyID, sessionToken).
// Returns ErrSessionNotFound when the key is unknown, ErrAccessDenied on token mismatch,
// and ErrSessionExpired when the session has passed its expiry.
func (b *InMemoryBackend) ValidateSessionCredential(accessKeyID, sessionToken string) (*SessionInfo, error) {
	b.mu.Lock()
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
func (b *InMemoryBackend) GetSessionToken(input *GetSessionTokenInput) (*GetSessionTokenResponse, error) {
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
func (b *InMemoryBackend) GetFederationToken(input *GetFederationTokenInput) (*GetFederationTokenResponse, error) {
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

	return validateJWTNotExpired(input.WebIdentityToken)
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
	b.mu.Lock()
	ol := b.oidcLookup
	b.mu.Unlock()

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
		return fmt.Errorf("%w: OIDC provider for issuer %q not found in IAM", ErrAccessDenied, issuer)
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
	if parts := strings.SplitN(input.RoleArn, ":", arnComponentCount); len(parts) >= arnComponentCount {
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
			PackedPolicySize:            calculatePackedPolicySizeWithArns(input.Policy, input.PolicyArns),
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}
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
func (b *InMemoryBackend) AssumeRoleWithSAML(input *AssumeRoleWithSAMLInput) (*AssumeRoleWithSAMLResponse, error) {
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
	if parts := strings.SplitN(input.RoleArn, ":", arnComponentCount); len(parts) >= arnComponentCount {
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
// In this mock, the TradeInToken is accepted as-is without validation.
func (b *InMemoryBackend) GetDelegatedAccessToken(
	input *GetDelegatedAccessTokenInput,
) (*GetDelegatedAccessTokenResponse, error) {
	b.cntGetDelegatedAccessToken.Add(1)

	if input.TradeInToken == "" {
		return nil, ErrMissingTradeInToken
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
	assumedPrincipal := arn.Build("iam", "", b.accountID, "root")

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
func (b *InMemoryBackend) GetWebIdentityToken(input *GetWebIdentityTokenInput) (*GetWebIdentityTokenResponse, error) {
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
		return nil, fmt.Errorf("%w: unsupported signing algorithm %q", ErrValidation, input.SigningAlgorithm)
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultWebIdentityTokenDurationSeconds
	}

	if duration < MinWebIdentityTokenDurationSeconds || duration > MaxWebIdentityTokenDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d for GetWebIdentityToken",
			ErrInvalidDuration, MinWebIdentityTokenDurationSeconds, MaxWebIdentityTokenDurationSeconds,
		)
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	now := time.Now().UTC()
	issuer := "https://sts.mock.aws.com/" + b.accountID

	// Build a minimal mock JWT payload (unsigned, for testing purposes only).
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"mock","typ":"JWT"}`))

	claims := map[string]any{
		"sub": MockUserID,
		"aud": input.Audience,
		"iss": issuer,
		"exp": expiration.Unix(),
		"iat": now.Unix(),
		"nbf": now.Unix(),
		"acc": b.accountID,
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

	if sub, ok := claims["sub"].(string); ok && sub != "" {
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

	if iss, ok := claims["iss"].(string); ok && iss != "" {
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
		return fmt.Errorf("%w: ExternalId does not match the trust policy condition", ErrAccessDenied)
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
func buildAssumedRoleArn(roleArn, sessionName string) string {
	// arn:aws:iam::ACCOUNT:role/ROLE_NAME  →  arn:aws:sts::ACCOUNT:assumed-role/ROLE_NAME/SESSION
	parts := strings.SplitN(roleArn, ":", arnComponentCount)
	if len(parts) < arnComponentCount {
		return roleArn + "/" + sessionName
	}

	account := parts[4]
	rolePath := strings.TrimPrefix(parts[5], "role/")

	return arn.Build("sts", "", account, "assumed-role/"+rolePath+"/"+sessionName)
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

	switch v := claims["aud"].(type) {
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

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
// Operation counters and totalSessionsCreated are also reset to zero.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

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
