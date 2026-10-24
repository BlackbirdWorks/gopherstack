package cognitoidp

import (
	"fmt"
	"slices"
)

// GetClientToken issues an M2M access token after authenticating the app client
// with its secret. The client must enable ALLOW_CLIENT_TOKEN_AUTH.
func (b *InMemoryBackend) GetClientToken(
	clientID, secret string, scopes []string, metadata map[string]string,
) (string, int32, error) {
	b.mu.RLock("GetClientToken")

	client, ok := b.clients.Get(clientID)
	if !ok {
		b.mu.RUnlock()

		return "", 0, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	cp := *client
	b.mu.RUnlock()

	if !slices.Contains(cp.ExplicitAuthFlows, explicitClientTok) {
		return "", 0, fmt.Errorf("%w: client is not enabled for %s", ErrNotAuthorized, explicitClientTok)
	}

	if !secretMatches(clientSecrets(&cp), secret) {
		return "", 0, fmt.Errorf("%w: invalid client secret", ErrNotAuthorized)
	}

	granted, err := b.clientTokenScopes(&cp, scopes)
	if err != nil {
		return "", 0, err
	}

	return b.issueClientCredentialsToken(clientID, granted, metadata)
}

func (b *InMemoryBackend) clientTokenScopes(client *UserPoolClient, requested []string) ([]string, error) {
	if len(requested) == 0 {
		return b.customScopesFor(client), nil
	}

	for _, s := range requested {
		if !b.scopeKnown(client.UserPoolID, s) {
			return nil, fmt.Errorf("%w: scope %q is not defined", ErrInvalidParameter, s)
		}

		if !slices.Contains(client.AllowedOAuthScopes, s) {
			return nil, fmt.Errorf("%w: scope %q is not allowed for this client", ErrInvalidParameter, s)
		}
	}

	return requested, nil
}
