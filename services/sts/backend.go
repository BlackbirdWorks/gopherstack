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
)

const (
	accessKeyIDPrefix   = "ASIA"
	accessKeyIDChars    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	accessKeyIDRandLen  = 16
	secretKeyByteLen    = 20
	sessionTokenByteLen = 64
	arnComponentCount   = 6
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
	GetCallerIdentity(accessKeyID string) (*GetCallerIdentityResponse, error)
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
