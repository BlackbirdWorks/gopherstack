package azuregraph

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"time"
)

const (
	secretLen            = 40
	secretHintLen        = 3
	secretMarker         = "8Q~"
	secretAlphabet       = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-"
	pbkdf2Iterations     = 10000
	saltLen              = 16
	defaultLifetimeYears = 2
)

var errNoRand = errors.New("azuregraph: entropy source failed")

// newSecret generates a client secret in the format Entra uses: 40 characters with "8Q~" at offset 3.
func newSecret() (string, error) {
	buf := make([]byte, secretLen)

	for i := range buf {
		if i >= secretHintLen && i < secretHintLen+len(secretMarker) {
			buf[i] = secretMarker[i-secretHintLen]

			continue
		}

		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(secretAlphabet))))
		if err != nil {
			return "", fmt.Errorf("%w: %w", errNoRand, err)
		}

		buf[i] = secretAlphabet[n.Int64()]
	}

	return string(buf), nil
}

func hashSecret(secret string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("%w: %w", errNoRand, err)
	}

	return hex.EncodeToString(salt) + "$" + deriveSecret(secret, salt), nil
}

func deriveSecret(secret string, salt []byte) string {
	key, _ := pbkdf2.Key(sha256.New, secret, salt, pbkdf2Iterations, sha256.Size)

	return hex.EncodeToString(key)
}

func secretMatches(stored, secret string) bool {
	saltHex, want, ok := strings.Cut(stored, "$")
	if !ok {
		return false
	}

	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(deriveSecret(secret, salt)), []byte(want)) == 1
}

// credentialsOf returns the key IDs of the password credentials of an application or service principal.
func (b *Backend) credentialsOf(id string) []string {
	var out []string

	if a, ok := b.apps.get(id); ok {
		for _, c := range a.PasswordCredentials {
			out = append(out, c.KeyID)
		}
	}

	if s, ok := b.sps.get(id); ok {
		for _, c := range s.PasswordCredentials {
			out = append(out, c.KeyID)
		}
	}

	return out
}

// credHolder gives mutable access to the credential slices of an application or service principal.
func (b *Backend) credHolder(id string) (*[]PasswordCredential, *[]KeyCredential, error) {
	if !isGUID(id) {
		return nil, nil, badRequest("Invalid object identifier '%s'.", id)
	}

	id = lc(id)

	if a, ok := b.apps.get(id); ok {
		next := *a
		b.apps.put(id, &next)

		return &next.PasswordCredentials, &next.KeyCredentials, nil
	}

	if s, ok := b.sps.get(id); ok {
		if isBuiltin(id) {
			return nil, nil, denied()
		}

		next := *s
		b.sps.put(id, &next)

		return &next.PasswordCredentials, &next.KeyCredentials, nil
	}

	return nil, nil, notFound(id)
}

func (b *Backend) credWindow(start, end time.Time) (time.Time, time.Time, error) {
	if start.IsZero() {
		start = b.stamp()
	}

	if end.IsZero() {
		end = start.AddDate(defaultLifetimeYears, 0, 0)
	}

	if !end.After(start) || !end.After(b.now()) {
		return start, end, badRequest("Invalid value specified for property 'endDateTime' of resource 'PasswordCredential'.")
	}

	return start.UTC().Truncate(time.Second), end.UTC().Truncate(time.Second), nil
}

// AddPassword adds a client secret to an application or service principal. The returned credential carries
// SecretText; it is the only time the secret is available.
func (b *Backend) AddPassword(id string, in PasswordCredential) (PasswordCredential, error) {
	secret, err := newSecret()
	if err != nil {
		return PasswordCredential{}, err
	}

	hashed, err := hashSecret(secret)
	if err != nil {
		return PasswordCredential{}, err
	}

	b.mu.Lock("AddPassword")
	defer b.mu.Unlock()

	start, end, err := b.credWindow(in.StartDateTime, in.EndDateTime)
	if err != nil {
		return PasswordCredential{}, err
	}

	pw, _, err := b.credHolder(id)
	if err != nil {
		return PasswordCredential{}, err
	}

	hint := secret[:secretHintLen]
	cred := PasswordCredential{
		KeyID: newID(), DisplayName: in.DisplayName, Hint: &hint, StartDateTime: start, EndDateTime: end,
		CustomKeyIdentifier: in.CustomKeyIdentifier,
	}
	*pw = append(slices.Clone(*pw), cred)
	b.secrets[cred.KeyID] = hashed
	cred.SecretText = &secret

	return cred, nil
}

// RemovePassword deletes a client secret.
func (b *Backend) RemovePassword(id, keyID string) error {
	b.mu.Lock("RemovePassword")
	defer b.mu.Unlock()

	pw, _, err := b.credHolder(id)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(*pw, func(c PasswordCredential) bool { return strings.EqualFold(c.KeyID, keyID) })
	if i < 0 {
		return badRequest("No password credential found with keyId '%s'.", keyID)
	}

	delete(b.secrets, (*pw)[i].KeyID)
	*pw = slices.Delete(slices.Clone(*pw), i, i+1)

	return nil
}

// AddKey registers a key credential. The key material is validated but not retained.
func (b *Backend) AddKey(id string, in KeyCredential) (KeyCredential, error) {
	if in.Key == nil {
		return KeyCredential{}, badRequest("A value is required for property 'key' of resource 'KeyCredential'.")
	}

	if _, err := base64.StdEncoding.DecodeString(*in.Key); err != nil {
		return KeyCredential{}, badRequest("Invalid value specified for property 'key' of resource 'KeyCredential'.")
	}

	if in.Type != "AsymmetricX509Cert" && in.Type != "X509CertAndPassword" {
		return KeyCredential{}, badRequest("Invalid value specified for property 'type' of resource 'KeyCredential'.")
	}

	if in.Usage != "Verify" && in.Usage != "Sign" {
		return KeyCredential{}, badRequest("Invalid value specified for property 'usage' of resource 'KeyCredential'.")
	}

	b.mu.Lock("AddKey")
	defer b.mu.Unlock()

	start, end, err := b.credWindow(in.StartDateTime, in.EndDateTime)
	if err != nil {
		return KeyCredential{}, err
	}

	_, keys, err := b.credHolder(id)
	if err != nil {
		return KeyCredential{}, err
	}

	cred := KeyCredential{
		KeyID: newID(), Type: in.Type, Usage: in.Usage, DisplayName: in.DisplayName,
		CustomKeyIdentifier: in.CustomKeyIdentifier, StartDateTime: start, EndDateTime: end,
	}
	*keys = append(slices.Clone(*keys), cred)

	return cred, nil
}

// RemoveKey deletes a key credential.
func (b *Backend) RemoveKey(id, keyID string) error {
	b.mu.Lock("RemoveKey")
	defer b.mu.Unlock()

	_, keys, err := b.credHolder(id)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(*keys, func(c KeyCredential) bool { return strings.EqualFold(c.KeyID, keyID) })
	if i < 0 {
		return badRequest("No key credential found with keyId '%s'.", keyID)
	}

	*keys = slices.Delete(slices.Clone(*keys), i, i+1)

	return nil
}

// ClientAuthError is an Entra token endpoint failure for a client-credentials request.
type ClientAuthError struct {
	Name        string
	Description string
	Status      int
	Code        int
}

func (e *ClientAuthError) Error() string { return e.Name + ": " + e.Description }

// AuthenticateClient checks a client-credentials request against the registered applications. known is false for
// a client ID no application or service principal uses, leaving the caller free to apply its own policy. On
// success objectID is the service principal's object ID, the token's oid claim.
func (b *Backend) AuthenticateClient(clientID, secret string) (objectID string, known bool, err error) {
	b.mu.RLock("AuthenticateClient")
	defer b.mu.RUnlock()

	if strings.EqualFold(clientID, b.cfg.DevClientID) {
		return devClientSPID, true, nil
	}

	var sp *ServicePrincipal

	for _, s := range b.sps.all() {
		if strings.EqualFold(s.AppID, clientID) {
			sp = s

			break
		}
	}

	var app *Application

	for _, a := range b.apps.all() {
		if strings.EqualFold(a.AppID, clientID) {
			app = a

			break
		}
	}

	if sp == nil && app == nil {
		return "", false, nil
	}

	if sp == nil {
		return "", true, &ClientAuthError{
			Status: http.StatusBadRequest, Code: 7000229, Name: "invalid_client",
			Description: fmt.Sprintf("AADSTS7000229: The client application %s is missing service principal in the tenant %s.",
				clientID, b.cfg.TenantID),
		}
	}

	if !sp.AccountEnabled {
		return "", true, &ClientAuthError{
			Status: http.StatusBadRequest, Code: 7000112, Name: "unauthorized_client",
			Description: fmt.Sprintf("AADSTS7000112: Application '%s' is disabled.", clientID),
		}
	}

	creds := slices.Clone(sp.PasswordCredentials)
	if app != nil {
		creds = append(creds, app.PasswordCredentials...)
	}

	now := b.now()

	for _, c := range creds {
		if now.Before(c.StartDateTime) || !now.Before(c.EndDateTime) {
			continue
		}

		if secretMatches(b.secrets[c.KeyID], secret) {
			return sp.ID, true, nil
		}
	}

	return "", true, &ClientAuthError{
		Status: http.StatusUnauthorized, Code: 7000215, Name: "invalid_client",
		Description: fmt.Sprintf("AADSTS7000215: Invalid client secret provided. Ensure the secret being sent in the "+
			"request is the client secret value, not the client secret ID, for a secret added to app '%s'.", clientID),
	}
}
