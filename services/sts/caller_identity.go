package sts

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// GetCallerIdentity returns the mock caller identity.
// When accessKeyID corresponds to an assumed-role session, returns the assumed-role ARN and user ID.
// When sessionToken is non-empty (ASIA-prefixed key), the stored token must match; a mismatch
// returns ErrUnknownAccessKeyID mapped to HTTP 400 InvalidClientTokenId (matching AWS).
func (b *InMemoryBackend) GetCallerIdentity(
	accessKeyID, sessionToken string,
) (*GetCallerIdentityResponse, error) {
	b.cntGetCallerIdentity.Add(1)

	if accessKeyID == "" {
		return b.rootCallerIdentity(), nil
	}

	b.mu.Lock("GetCallerIdentity")
	session, ok := b.sessions.Get(accessKeyID)
	wasExpired := false

	if ok && isSessionExpired(session) {
		b.sessions.Delete(accessKeyID)
		ok = false
		wasExpired = true
	}

	b.mu.Unlock()

	if ok {
		// When the caller presents a session token, it must match the stored value.
		// AWS rejects a mismatched session token with HTTP 400 InvalidClientTokenId,
		// not 403 AccessDenied.
		if sessionToken != "" && session.SessionToken != "" &&
			sessionToken != session.SessionToken {
			return nil, fmt.Errorf(
				"%w: the security token included in the request is invalid",
				ErrUnknownAccessKeyID,
			)
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

	// ASIA-prefixed keys are temporary session credentials. AWS returns
	// ExpiredTokenException when a known session has expired, and
	// InvalidClientTokenId when the key was never issued by this service.
	// Long-term AKIA keys that are untracked fall back to the root identity.
	if strings.HasPrefix(accessKeyID, accessKeyIDPrefix) {
		if wasExpired {
			return nil, fmt.Errorf(
				"%w: the security token included in the request has expired",
				ErrSessionExpired,
			)
		}

		return nil, fmt.Errorf(
			"%w: the security token included in the request is invalid",
			ErrUnknownAccessKeyID,
		)
	}

	return b.rootCallerIdentity(), nil
}

func (b *InMemoryBackend) rootCallerIdentity() *GetCallerIdentityResponse {
	callerArn := arn.Build(arnServiceIAM, "", b.accountID, "root")

	return &GetCallerIdentityResponse{
		Xmlns: STSNamespace,
		GetCallerIdentityResult: GetCallerIdentityResult{
			Account: b.accountID,
			Arn:     callerArn,
			UserID:  MockUserID,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	}
}
