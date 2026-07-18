package lakeformation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	randAccessKeyBytes  = 16
	randSecretKeyBytes  = 20
	randSessionKeyBytes = 32
)

// randomHex returns a random hex string of n bytes (2n hex chars).
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// defaultCredentialsDuration is the AWS default lifetime for temporary
// credentials when the caller does not specify DurationSeconds.
const defaultCredentialsDuration = time.Hour

// credentialsDuration returns the requested credential lifetime, falling
// back to defaultCredentialsDuration when durationSeconds is unset or
// non-positive.
func credentialsDuration(durationSeconds *int32) time.Duration {
	if durationSeconds != nil && *durationSeconds > 0 {
		return time.Duration(*durationSeconds) * time.Second
	}

	return defaultCredentialsDuration
}

// AssumeDecoratedRoleWithSAML returns synthetic temporary credentials.
// The actual SAML assertion and role are not validated in the in-memory backend.
func (b *InMemoryBackend) AssumeDecoratedRoleWithSAML(
	_, _, _ string,
	durationSeconds *int32,
) *SAMLCredentials {
	return &SAMLCredentials{
		AccessKeyID:     "ASIA" + randomHex(randAccessKeyBytes),
		SecretAccessKey: randomHex(randSecretKeyBytes),
		SessionToken:    randomHex(randSessionKeyBytes),
		Expiration:      awstime.Epoch(time.Now().Add(credentialsDuration(durationSeconds))),
	}
}

// GetDataLakePrincipal returns a synthetic caller-identity principal.
// In a real deployment, this returns the ARN of the calling IAM entity.
func (b *InMemoryBackend) GetDataLakePrincipal(ctx context.Context) *DataLakePrincipal {
	account := awsmeta.Account(ctx)
	if account == "" {
		account = awsmeta.DefaultAccount
	}

	return &DataLakePrincipal{
		DataLakePrincipalIdentifier: "arn:aws:iam::" + account + ":user/gopherstack-user",
	}
}

// GetTemporaryCredentials returns synthetic temporary AWS credentials.
func (b *InMemoryBackend) GetTemporaryCredentials(durationSeconds *int32) *TemporaryCredentials {
	return &TemporaryCredentials{
		AccessKeyID:     "ASIA" + randomHex(randAccessKeyBytes),
		SecretAccessKey: randomHex(randSecretKeyBytes),
		SessionToken:    randomHex(randSessionKeyBytes),
		Expiration:      awstime.Epoch(time.Now().Add(credentialsDuration(durationSeconds))),
	}
}
