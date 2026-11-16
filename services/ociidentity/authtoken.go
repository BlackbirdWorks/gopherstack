package ociidentity

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	authTokenIterations = 4096
	authTokenSaltBytes  = 16
	authTokenKeyBytes   = 32
)

func deriveAuthToken(token string, salt []byte) []byte {
	key, err := pbkdf2.Key(sha256.New, token, salt, authTokenIterations, authTokenKeyBytes)
	if err != nil {
		return nil
	}

	return key
}

func hashAuthToken(token string) (string, string) {
	salt := make([]byte, authTokenSaltBytes)
	_, _ = rand.Read(salt)

	return hex.EncodeToString(salt), hex.EncodeToString(deriveAuthToken(token, salt))
}

// VerifyAuthToken reports whether token is a current auth token of the active user with the given name, and
// returns that user's OCID. Identity keeps only a salted PBKDF2 digest of each token.
func (b *InMemoryBackend) VerifyAuthToken(userName, token string) (string, bool) {
	type candidate struct{ salt, digest string }

	var (
		userID string
		cands  []candidate
	)

	b.mu.RLock("VerifyAuthToken")

	for _, u := range b.users {
		if u.LifecycleState != stateActive || !strs.Equal(u.Name, userName) {
			continue
		}

		userID = u.ID

		for _, c := range u.AuthTokens {
			cands = append(cands, candidate{c.Salt, c.Digest})
		}
	}

	b.mu.RUnlock()

	matched := false

	for _, c := range cands {
		salt, serr := hex.DecodeString(c.salt)
		want, derr := hex.DecodeString(c.digest)

		if serr != nil || derr != nil || len(want) == 0 {
			continue
		}

		if subtle.ConstantTimeCompare(deriveAuthToken(token, salt), want) == 1 {
			matched = true
		}
	}

	if !matched {
		return "", false
	}

	return userID, true
}
