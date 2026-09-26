package ecrpublic

import (
	"context"
	"encoding/base64"
	"time"
)

const (
	authTokenTTL  = 12 * time.Hour
	authTokenUser = "AWS"
	// authTokenPassword is a stable dummy credential, not a real secret; this
	// emulator does not enforce docker-login authentication against it
	// (matches services/ecr).
	//nolint:gosec // dummy emulator credential, not a real secret
	authTokenPassword = "gopherstack-ecr-public-dummy-password"
)

// GetAuthorizationToken returns a base64(user:password) authorization token
// and its expiry. The emulator does not model per-principal credentials --
// every caller gets the same stable token, honestly reflecting that this is
// not a real authentication backend (see PARITY.md).
func (b *InMemoryBackend) GetAuthorizationToken(_ context.Context) (string, int64, error) {
	token := base64.StdEncoding.EncodeToString([]byte(authTokenUser + ":" + authTokenPassword))
	expiresAt := time.Now().Add(authTokenTTL).Unix()

	return token, expiresAt, nil
}
