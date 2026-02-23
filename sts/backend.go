package sts

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrMissingRoleArn is returned when AssumeRole is called without a RoleArn.
	ErrMissingRoleArn = errors.New("RoleArn is required")

	// ErrMissingSessionName is returned when AssumeRole is called without a RoleSessionName.
	ErrMissingSessionName = errors.New("RoleSessionName is required")

	// ErrInvalidDuration is returned when DurationSeconds is out of range.
	ErrInvalidDuration = errors.New("DurationSeconds must be between 900 and 43200")
)

const (
	accessKeyIDPrefix   = "ASIA"
	accessKeyIDChars    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	accessKeyIDRandLen  = 16
	secretKeyByteLen    = 20
	sessionTokenByteLen = 64
	arnComponentCount   = 6
)

// StorageBackend defines the STS service backend interface.
type StorageBackend interface {
	AssumeRole(input *AssumeRoleInput) (*AssumeRoleResponse, error)
	GetCallerIdentity() (*GetCallerIdentityResponse, error)
}

// InMemoryBackend is a stateless in-memory STS backend.
type InMemoryBackend struct {
	accountID string
}

// NewInMemoryBackend creates a new InMemoryBackend with the default account ID.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID.
func NewInMemoryBackendWithConfig(accountID string) *InMemoryBackend {
	return &InMemoryBackend{accountID: accountID}
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

	if duration < MinDurationSeconds || duration > MaxDurationSeconds {
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
	roleID := deriveRoleID(input.RoleArn)
	assumedRoleID := roleID + ":" + input.RoleSessionName
	assumedRoleArn := buildAssumedRoleArn(input.RoleArn, input.RoleSessionName)

	return &AssumeRoleResponse{
		Xmlns: STSNamespace,
		AssumeRoleResult: AssumeRoleResult{
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
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
}

// GetCallerIdentity returns the mock caller identity using the configured account ID.
func (b *InMemoryBackend) GetCallerIdentity() (*GetCallerIdentityResponse, error) {
	arn := "arn:aws:iam::" + b.accountID + ":root"

	return &GetCallerIdentityResponse{
		Xmlns: STSNamespace,
		GetCallerIdentityResult: GetCallerIdentityResult{
			Account: b.accountID,
			Arn:     arn,
			UserID:  MockUserID,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}, nil
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

	return "arn:aws:sts::" + account + ":assumed-role/" + rolePath + "/" + sessionName
}
