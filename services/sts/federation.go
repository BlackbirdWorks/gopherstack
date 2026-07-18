package sts

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

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

	b.storeSession(session)

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
