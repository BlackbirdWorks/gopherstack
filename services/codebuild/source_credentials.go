package codebuild

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ImportSourceCredentials imports source credentials and returns the ARN.
// shouldOverwrite mirrors ImportSourceCredentialsInput.ShouldOverwrite ("Set
// to false to prevent overwriting the repository source credentials. The
// default value is true.") -- when false and a credential for serverType
// already exists, the import is rejected with ResourceAlreadyExistsException
// instead of silently replacing it.
func (b *InMemoryBackend) ImportSourceCredentials(
	authType, serverType, token, username string,
	shouldOverwrite bool,
) (string, error) {
	b.mu.Lock("ImportSourceCredentials")
	defer b.mu.Unlock()

	arnStr := arn.Build("codebuild", b.region, b.accountID, "token/"+serverType)

	if !shouldOverwrite {
		if _, ok := b.sourceCredentials.Get(arnStr); ok {
			return "", fmt.Errorf(
				"%w: source credentials for %s already exist",
				ErrAlreadyExists,
				serverType,
			)
		}
	}

	b.sourceCredentials.Put(&SourceCredentials{
		Arn:        arnStr,
		ServerType: serverType,
		AuthType:   authType,
		Username:   username,
		Token:      token,
	})

	return arnStr, nil
}

// DeleteSourceCredentials removes source credentials by ARN.
func (b *InMemoryBackend) DeleteSourceCredentials(arnStr string) error {
	b.mu.Lock("DeleteSourceCredentials")
	defer b.mu.Unlock()

	if !b.sourceCredentials.Delete(arnStr) {
		return ErrNotFound
	}

	return nil
}

// ListSourceCredentials returns all stored source credentials.
func (b *InMemoryBackend) ListSourceCredentials() []*SourceCredentials {
	b.mu.RLock("ListSourceCredentials")
	defer b.mu.RUnlock()

	items := b.sourceCredentials.All()
	result := make([]*SourceCredentials, 0, len(items))

	for _, sc := range items {
		out := *sc
		result = append(result, &out)
	}

	return result
}

// sourceToken returns the imported token and username for serverType.
func (b *InMemoryBackend) sourceToken(serverType string) (string, string, bool) {
	b.mu.RLock("sourceToken")
	defer b.mu.RUnlock()

	for _, sc := range b.sourceCredentials.All() {
		if sc.ServerType == serverType && sc.Token != "" {
			return sc.Token, sc.Username, true
		}
	}

	return "", "", false
}
