package sts

import (
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // SHA1 is used only for non-cryptographic ID derivation, not for security
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	accessKeyIDPrefix   = "ASIA"
	accessKeyIDChars    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	accessKeyIDRandLen  = 16
	secretKeyByteLen    = 20
	sessionTokenByteLen = 64
	arnComponentCount   = 6
)

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
