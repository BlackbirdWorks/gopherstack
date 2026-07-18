package sts

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// delegatedSessionName is the SessionName recorded for GetDelegatedAccessToken-issued sessions.
const delegatedSessionName = "delegated"

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

	b.storeSession(session)

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
