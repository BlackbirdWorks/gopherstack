package sagemaker

import (
	"context"
	"fmt"
)

// ---------------------------------------------------------------------------
// CreatePresignedDomainURL
// ---------------------------------------------------------------------------

// CreatePresignedDomainURL validates that domainID and userProfileName refer
// to existing resources and returns a synthetic (but stably-shaped)
// presigned authorization URL for the domain's user profile.
func (b *InMemoryBackend) CreatePresignedDomainURL(
	ctx context.Context,
	domainID, userProfileName string,
) (string, error) {
	b.mu.RLock("CreatePresignedDomainURL")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	d, ok := b.domainsStoreRO(region).Get(domainID)
	if !ok {
		for _, cand := range b.domainsStoreRO(region).All() {
			if cand.DomainName == domainID {
				d = cand
				ok = true

				break
			}
		}
	}

	if !ok {
		return "", fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainID)
	}

	key := userProfileKeyString(userProfileKey{DomainID: d.DomainID, UserProfileName: userProfileName})
	if _, profileOK := b.userProfilesStoreRO(region).Get(key); !profileOK {
		return "", fmt.Errorf(
			"%w: user profile %q not found in domain %q", ErrUserProfileNotFound, userProfileName, domainID,
		)
	}

	token := generateID()

	return fmt.Sprintf("%s/auth?token=%s", d.URL, token), nil
}

// ---------------------------------------------------------------------------
// StartSession
//
// StartSession establishes a remote IDE connection to a space application.
// There is no separate "session" resource to model — the response is a
// freshly generated session identifier/URL/token tuple, matching AWS's own
// per-call opaque credentials.
// ---------------------------------------------------------------------------

// StartSessionResult holds the synthetic session identifiers returned by StartSession.
type StartSessionResult struct {
	SessionID  string
	StreamURL  string
	TokenValue string
}

// StartSession validates resourceIdentifier is non-empty and returns a fresh
// synthetic session.
func (b *InMemoryBackend) StartSession(_ context.Context, resourceIdentifier string) (*StartSessionResult, error) {
	if resourceIdentifier == "" {
		return nil, fmt.Errorf("%w: ResourceIdentifier is required", ErrValidation)
	}

	sessionID := generateID()

	return &StartSessionResult{
		SessionID:  sessionID,
		StreamURL:  fmt.Sprintf("wss://session.sagemaker.aws/%s", sessionID),
		TokenValue: generateID(),
	}, nil
}
