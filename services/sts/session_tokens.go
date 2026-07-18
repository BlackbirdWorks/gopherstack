package sts

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

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

	b.storeSession(session)

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
