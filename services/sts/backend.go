package sts

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var (
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
)

// RoleLookup is implemented by services (e.g. IAM) that can provide role metadata
// to STS for ExternalId validation and MaxSessionDuration enforcement.
type RoleLookup interface {
	GetRoleByArn(arn string) (*RoleMeta, error)
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

// StorageBackend defines the STS service backend interface.
type StorageBackend interface {
	AssumeRole(input *AssumeRoleInput) (*AssumeRoleResponse, error)
	AssumeRoleWithWebIdentity(input *AssumeRoleWithWebIdentityInput) (*AssumeRoleWithWebIdentityResponse, error)
	GetCallerIdentity(accessKeyID string) (*GetCallerIdentityResponse, error)
	GetFederationToken(input *GetFederationTokenInput) (*GetFederationTokenResponse, error)
	GetSessionToken(input *GetSessionTokenInput) (*GetSessionTokenResponse, error)
}

// InMemoryBackend is a stateful in-memory STS backend.
type InMemoryBackend struct {
	roleLookup RoleLookup
	sessions   map[string]*SessionInfo
	accountID  string
	mu         sync.Mutex
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

// AssumeRole generates temporary credentials for the given role.
func (b *InMemoryBackend) AssumeRole(input *AssumeRoleInput) (*AssumeRoleResponse, error) {
	if input.RoleArn == "" {
		return nil, ErrMissingRoleArn
	}

	if input.RoleSessionName == "" {
		return nil, ErrMissingSessionName
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultDurationSeconds
	}

	if duration < MinDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be at least %d",
			ErrInvalidDuration, MinDurationSeconds,
		)
	}

	effectiveMax, err := b.validateAndGetMaxDuration(input)
	if err != nil {
		return nil, err
	}

	if duration > effectiveMax {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must not exceed %d for this role",
			ErrInvalidDuration, effectiveMax,
		)
	}

	return b.issueCredentials(input, duration)
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
	accessKeyID, err := generateAccessKeyID()
	if err != nil {
		return nil, fmt.Errorf("generate access key: %w", err)
	}

	secretKey, err := generateSecretKey()
	if err != nil {
		return nil, fmt.Errorf("generate secret key: %w", err)
	}

	sessionToken, err := generateSessionToken()
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
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
		AccessKeyID:       accessKeyID,
		AssumedRoleID:     assumedRoleID,
		SourceIdentity:    input.SourceIdentity,
		Tags:              input.Tags,
		TransitiveTagKeys: input.TransitiveTagKeys,
		Expiration:        expiration,
	}

	b.mu.Lock()
	b.sessions[accessKeyID] = session
	b.mu.Unlock()

	result := AssumeRoleResult{
		AssumedRoleUser: AssumedRoleUser{
			Arn:           assumedRoleArn,
			AssumedRoleID: assumedRoleID,
		},
		Credentials: Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretKey,
			SessionToken:    sessionToken,
			Expiration:      expiration.Format(time.RFC3339),
		},
		SourceIdentity: input.SourceIdentity,
	}

	return &AssumeRoleResponse{
		Xmlns:            STSNamespace,
		AssumeRoleResult: result,
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// GetCallerIdentity returns the mock caller identity.
// When accessKeyID corresponds to an assumed-role session, returns the assumed-role ARN and user ID.
func (b *InMemoryBackend) GetCallerIdentity(accessKeyID string) (*GetCallerIdentityResponse, error) {
	if accessKeyID != "" {
		b.mu.Lock()
		session, ok := b.sessions[accessKeyID]

		if ok && !session.Expiration.IsZero() && !time.Now().UTC().Before(session.Expiration) {
			// Opportunistically evict the expired session.
			delete(b.sessions, accessKeyID)
			ok = false
		}

		b.mu.Unlock()

		if ok {
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

// GetSessionToken generates temporary credentials without role assumption.
func (b *InMemoryBackend) GetSessionToken(input *GetSessionTokenInput) (*GetSessionTokenResponse, error) {
	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultSessionTokenDurationSeconds
	}

	if duration < MinSessionTokenDurationSeconds || duration > MaxDurationSeconds {
		return nil, ErrInvalidDuration
	}

	accessKeyID, err := generateAccessKeyID()
	if err != nil {
		return nil, fmt.Errorf("generate access key: %w", err)
	}

	secretKey, err := generateSecretKey()
	if err != nil {
		return nil, fmt.Errorf("generate secret key: %w", err)
	}

	sessionToken, err := generateSessionToken()
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)

	return &GetSessionTokenResponse{
		Xmlns: STSNamespace,
		GetSessionTokenResult: GetSessionTokenResult{
			Credentials: Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				SessionToken:    sessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// GetFederationToken generates temporary credentials for a federated user.
// The federated user ARN has the form arn:aws:sts::ACCOUNT:federated-user/NAME.
func (b *InMemoryBackend) GetFederationToken(input *GetFederationTokenInput) (*GetFederationTokenResponse, error) {
	if input.Name == "" {
		return nil, ErrMissingFederationTokenName
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

	accessKeyID, err := generateAccessKeyID()
	if err != nil {
		return nil, fmt.Errorf("generate access key: %w", err)
	}

	secretKey, err := generateSecretKey()
	if err != nil {
		return nil, fmt.Errorf("generate secret key: %w", err)
	}

	sessionToken, err := generateSessionToken()
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	federatedUserArn := arn.Build("sts", "", b.accountID, "federated-user/"+input.Name)
	federatedUserID := b.accountID + ":" + input.Name

	session := &SessionInfo{
		Expiration:     expiration,
		AssumedRoleArn: federatedUserArn,
		AccountID:      b.accountID,
		SessionName:    input.Name,
		AccessKeyID:    accessKeyID,
		AssumedRoleID:  federatedUserID,
		Tags:           input.Tags,
	}

	b.mu.Lock()
	b.sessions[accessKeyID] = session
	b.mu.Unlock()

	return &GetFederationTokenResponse{
		Xmlns: STSNamespace,
		GetFederationTokenResult: GetFederationTokenResult{
			FederatedUser: FederatedUser{
				Arn:             federatedUserArn,
				FederatedUserID: federatedUserID,
			},
			Credentials: Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				SessionToken:    sessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// AssumeRoleWithWebIdentity generates temporary credentials using a web identity token.
// In this mock, the WebIdentityToken is not cryptographically validated; the subject
// is extracted from the token's payload when parseable, otherwise a default is used.
func (b *InMemoryBackend) AssumeRoleWithWebIdentity(
	input *AssumeRoleWithWebIdentityInput,
) (*AssumeRoleWithWebIdentityResponse, error) {
	if input.RoleArn == "" {
		return nil, ErrMissingRoleArn
	}

	if input.RoleSessionName == "" {
		return nil, ErrMissingSessionName
	}

	if input.WebIdentityToken == "" {
		return nil, ErrMissingWebIdentityToken
	}

	duration := input.DurationSeconds
	if duration == 0 {
		duration = DefaultDurationSeconds
	}

	if duration < MinDurationSeconds || duration > MaxDurationSeconds {
		return nil, fmt.Errorf(
			"%w: DurationSeconds must be between %d and %d",
			ErrInvalidDuration, MinDurationSeconds, MaxDurationSeconds,
		)
	}

	accessKeyID, err := generateAccessKeyID()
	if err != nil {
		return nil, fmt.Errorf("generate access key: %w", err)
	}

	secretKey, err := generateSecretKey()
	if err != nil {
		return nil, fmt.Errorf("generate secret key: %w", err)
	}

	sessionToken, err := generateSessionToken()
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	expiration := time.Now().UTC().Add(time.Duration(duration) * time.Second)
	roleID := deriveRoleID(input.RoleArn)
	assumedRoleID := roleID + ":" + input.RoleSessionName
	assumedRoleArn := buildAssumedRoleArn(input.RoleArn, input.RoleSessionName)

	account := b.accountID
	if parts := strings.SplitN(input.RoleArn, ":", arnComponentCount); len(parts) >= arnComponentCount {
		account = parts[4]
	}

	subject := extractWebIdentitySubject(input.WebIdentityToken)
	provider := input.ProviderID
	if provider == "" {
		provider = "cognito-identity.amazonaws.com"
	}

	session := &SessionInfo{
		Expiration:     expiration,
		AssumedRoleArn: assumedRoleArn,
		AccountID:      account,
		SessionName:    input.RoleSessionName,
		AccessKeyID:    accessKeyID,
		AssumedRoleID:  assumedRoleID,
	}

	b.mu.Lock()
	b.sessions[accessKeyID] = session
	b.mu.Unlock()

	return &AssumeRoleWithWebIdentityResponse{
		Xmlns: STSNamespace,
		AssumeRoleWithWebIdentityResult: AssumeRoleWithWebIdentityResult{
			AssumedRoleUser: AssumedRoleUser{
				Arn:           assumedRoleArn,
				AssumedRoleID: assumedRoleID,
			},
			Credentials: Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				SessionToken:    sessionToken,
				Expiration:      expiration.Format(time.RFC3339),
			},
			SubjectFromWebIdentityToken: subject,
			Audience:                    subject,
			Provider:                    provider,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// extractWebIdentitySubject attempts to extract the "sub" claim from a JWT token's
// payload without validating the signature. If extraction fails, returns a placeholder.
func extractWebIdentitySubject(token string) string {
	// JWT format: header.payload.signature (base64url-encoded parts)
	parts := strings.SplitN(token, ".", jwtPartCount)
	if len(parts) < jwtMinParts {
		return webIdentitySubjectPlaceholder
	}

	// Add padding if needed for base64url decode.
	rawPayload := parts[1]
	paddedPayload := rawPayload

	switch len(rawPayload) % 4 {
	case base64Pad2:
		paddedPayload += "=="
	case base64Pad1:
		paddedPayload += "="
	}

	decoded, decodeErr := base64.URLEncoding.DecodeString(paddedPayload)
	if decodeErr != nil {
		// Try RawURLEncoding as fallback (no padding).
		var fallbackErr error

		decoded, fallbackErr = base64.RawURLEncoding.DecodeString(rawPayload)
		if fallbackErr != nil {
			return webIdentitySubjectPlaceholder
		}
	}

	var claims map[string]any
	if unmarshalErr := json.Unmarshal(decoded, &claims); unmarshalErr != nil {
		return webIdentitySubjectPlaceholder
	}

	if sub, ok := claims["sub"].(string); ok && sub != "" {
		return sub
	}

	return webIdentitySubjectPlaceholder
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

// deriveRoleID extracts a pseudo role-ID from the ARN (uses last segment).
func deriveRoleID(roleArn string) string {
	parts := strings.Split(roleArn, "/")

	return "AROA" + strings.ToUpper(parts[len(parts)-1])
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

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.sessions = make(map[string]*SessionInfo)
}
