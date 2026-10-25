package ociidentity

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
)

const (
	maxAPIKeys        = 3
	maxSecretsPerUser = 2
	minRSABits        = 2048
	authTokenLen      = 20
	uiPasswordLen     = 16
	secretKeyBytes    = 32
	accessKeyBytes    = 20
	smtpPasswordLen   = 44
	smtpDomainTag     = "ab"
	alnum             = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	symbols           = "!#$%&*+-<=>?[]^_{}~"
)

type apiKeyWire struct {
	TimeCreated    time.Time `json:"timeCreated"`
	KeyID          string    `json:"keyId"`
	KeyValue       string    `json:"keyValue"`
	Fingerprint    string    `json:"fingerprint"`
	UserID         string    `json:"userId"`
	LifecycleState string    `json:"lifecycleState"`
}

type credWire struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeExpires    *time.Time `json:"timeExpires"`
	ID             string     `json:"id"`
	UserID         string     `json:"userId"`
	LifecycleState string     `json:"lifecycleState"`
	Description    string     `json:"description,omitempty"`
	DisplayName    string     `json:"displayName,omitempty"`
	Username       string     `json:"username,omitempty"`
	Token          string     `json:"token,omitempty"`
	Key            string     `json:"key,omitempty"`
	Password       string     `json:"password,omitempty"`
}

type uiPasswordWire struct {
	TimeCreated    time.Time `json:"timeCreated"`
	UserID         string    `json:"userId"`
	LifecycleState string    `json:"lifecycleState"`
	Password       string    `json:"password,omitempty"`
}

type credKind struct {
	slot    func(*User) *[]Credential
	secret  func() string
	name    string
	nameKey string
}

func authTokenKind() credKind {
	return credKind{
		name: "auth token", nameKey: "description", slot: func(u *User) *[]Credential { return &u.AuthTokens },
		secret: func() string { return randString(alnum+symbols, authTokenLen) },
	}
}

func secretKeyKind() credKind {
	return credKind{
		name: "customer secret key", nameKey: "displayName", slot: func(u *User) *[]Credential { return &u.SecretKeys },
		secret: func() string { return base64.StdEncoding.EncodeToString(randBytes(secretKeyBytes)) },
	}
}

func smtpKind() credKind {
	return credKind{
		name: "SMTP credential", nameKey: "description", slot: func(u *User) *[]Credential { return &u.SMTP },
		secret: func() string { return randString(alnum, smtpPasswordLen) },
	}
}

func randBytes(n int) []byte {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)

	return buf
}

func randString(charset string, n int) string {
	out := make([]byte, n)
	limit := big.NewInt(int64(len(charset)))

	for i := range out {
		idx, err := rand.Int(rand.Reader, limit)
		if err != nil {
			idx = big.NewInt(0)
		}

		out[i] = charset[idx.Int64()]
	}

	return string(out)
}

func apiKeyID(userID, fingerprint string) string {
	return ociauth.KeyID{Tenancy: TenancyID, User: userID, Fingerprint: fingerprint}.String()
}

func (k APIKey) view(userID string) apiKeyWire {
	return apiKeyWire{
		TimeCreated: k.TimeCreated, KeyID: apiKeyID(userID, k.Fingerprint), KeyValue: k.PEM,
		Fingerprint: k.Fingerprint, UserID: userID, LifecycleState: stateActive,
	}
}

// UploadAPIKey registers an RSA public key for a user and with the shared signature registry.
func (b *InMemoryBackend) UploadAPIKey(userID, pemKey string) (apiKeyWire, string, error) {
	if strings.TrimSpace(pemKey) == "" {
		return apiKeyWire{}, "", errMissing("key is required")
	}

	pub, err := ociauth.ParsePublicKeyPEM(pemKey)
	if err != nil {
		return apiKeyWire{}, "", errParam("key must be a PEM encoded RSA public key")
	}

	if pub.N.BitLen() < minRSABits {
		return apiKeyWire{}, "", errParam("key must be an RSA key of at least 2048 bits")
	}

	fp, err := ociauth.Fingerprint(pub)
	if err != nil {
		return apiKeyWire{}, "", errParam("key must be a PEM encoded RSA public key")
	}

	b.mu.Lock("UploadAPIKey")
	defer b.mu.Unlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return apiKeyWire{}, "", aerr
	}

	if !u.Capabilities.CanUseAPIKeys {
		return apiKeyWire{}, "", errState("The user is not allowed to use API keys")
	}

	if slices.ContainsFunc(u.APIKeys, func(k APIKey) bool { return k.Fingerprint == fp }) {
		return apiKeyWire{}, "", errCode(http.StatusConflict, "Conflict", "An API key with fingerprint "+fp+" already exists for the user")
	}

	if len(u.APIKeys) >= maxAPIKeys {
		return apiKeyWire{}, "", errLimit("A user can have at most 3 API keys")
	}

	if _, err = b.Auth.Register(TenancyID, userID, pemKey); err != nil {
		return apiKeyWire{}, "", errParam("key must be a PEM encoded RSA public key")
	}

	k := APIKey{TimeCreated: b.nowUTC(), Fingerprint: fp, PEM: pemKey}
	u.APIKeys = append(u.APIKeys, k)

	return k.view(userID), newEtag(), nil
}

// ListAPIKeys lists a user's API keys.
func (b *InMemoryBackend) ListAPIKeys(userID string) ([]apiKeyWire, error) {
	b.mu.RLock("ListAPIKeys")
	defer b.mu.RUnlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return nil, aerr
	}

	out := make([]apiKeyWire, 0, len(u.APIKeys))
	for _, k := range u.APIKeys {
		out = append(out, k.view(userID))
	}

	return out, nil
}

// DeleteAPIKey removes a key and stops verifying signatures made with it.
func (b *InMemoryBackend) DeleteAPIKey(userID, fingerprint string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return aerr
	}

	idx := slices.IndexFunc(u.APIKeys, func(k APIKey) bool { return k.Fingerprint == fingerprint })
	if idx < 0 {
		return errNotFound(fingerprint)
	}

	u.APIKeys = slices.Delete(u.APIKeys, idx, idx+1)
	b.Auth.Remove(apiKeyID(userID, fingerprint))

	return nil
}

func (c Credential) view(userID string, kind credKind, secret string) credWire {
	w := credWire{
		TimeCreated: c.TimeCreated, ID: c.ID, UserID: userID, LifecycleState: stateActive, Username: c.Username,
	}

	if kind.nameKey == "displayName" {
		w.DisplayName, w.Key = c.Text, secret
	} else {
		w.Description = c.Text
	}

	switch kind.name {
	case "auth token":
		w.Token = secret
	case "SMTP credential":
		w.Password = secret
	}

	return w
}

func validCredText(kind credKind, text string) *APIError {
	if strings.TrimSpace(text) == "" {
		return errMissing(kind.nameKey + " is required")
	}

	if len(text) > maxDescriptionLen {
		return errParam(kind.nameKey + " must be at most 400 characters")
	}

	return nil
}

// CreateCredential creates a credential and returns it with the secret, which is never retrievable again.
func (b *InMemoryBackend) CreateCredential(userID string, kind credKind, text string) (credWire, string, error) {
	if aerr := validCredText(kind, text); aerr != nil {
		return credWire{}, "", aerr
	}

	b.mu.Lock("CreateCredential")
	defer b.mu.Unlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return credWire{}, "", aerr
	}

	slot := kind.slot(u)
	if len(*slot) >= maxSecretsPerUser {
		return credWire{}, "", errLimit("A user can have at most 2 " + kind.name + "s")
	}

	c := Credential{ID: newOCID("credential"), Text: text, TimeCreated: b.nowUTC()}

	switch kind.name {
	case "customer secret key":
		c.ID = hex.EncodeToString(randBytes(accessKeyBytes))
	case "SMTP credential":
		c.Username = userID + "@" + TenancyID + "." + smtpDomainTag + ".com"
	}

	*slot = append(*slot, c)

	return c.view(userID, kind, kind.secret()), newEtag(), nil
}

// ListCredentials lists a user's credentials of one kind, without secrets.
func (b *InMemoryBackend) ListCredentials(userID string, kind credKind) ([]credWire, error) {
	b.mu.RLock("ListCredentials")
	defer b.mu.RUnlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return nil, aerr
	}

	out := make([]credWire, 0)
	for _, c := range *kind.slot(u) {
		out = append(out, c.view(userID, kind, ""))
	}

	return out, nil
}

// UpdateCredential changes the description or display name.
func (b *InMemoryBackend) UpdateCredential(userID, id string, kind credKind, text string) (credWire, string, error) {
	if aerr := validCredText(kind, text); aerr != nil {
		return credWire{}, "", aerr
	}

	b.mu.Lock("UpdateCredential")
	defer b.mu.Unlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return credWire{}, "", aerr
	}

	slot := kind.slot(u)

	idx := slices.IndexFunc(*slot, func(c Credential) bool { return c.ID == id })
	if idx < 0 {
		return credWire{}, "", errNotFound(id)
	}

	(*slot)[idx].Text = text

	return (*slot)[idx].view(userID, kind, ""), newEtag(), nil
}

// DeleteCredential removes a credential.
func (b *InMemoryBackend) DeleteCredential(userID, id string, kind credKind) error {
	b.mu.Lock("DeleteCredential")
	defer b.mu.Unlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return aerr
	}

	slot := kind.slot(u)

	idx := slices.IndexFunc(*slot, func(c Credential) bool { return c.ID == id })
	if idx < 0 {
		return errNotFound(id)
	}

	*slot = slices.Delete(*slot, idx, idx+1)

	return nil
}

// ResetUIPassword generates a console password; it is returned only here.
func (b *InMemoryBackend) ResetUIPassword(userID string) (uiPasswordWire, string, error) {
	b.mu.Lock("ResetUIPassword")
	defer b.mu.Unlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return uiPasswordWire{}, "", aerr
	}

	if !u.Capabilities.CanUseConsolePassword {
		return uiPasswordWire{}, "", errState("The user is not allowed to use a console password")
	}

	now := b.nowUTC()
	u.UIPasswordCreated = &now

	pw := randString("abcdefghijklmnopqrstuvwxyz", uiPasswordLen/4) + randString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", uiPasswordLen/4) +
		randString("0123456789", uiPasswordLen/4) + randString(symbols, uiPasswordLen/4)

	return uiPasswordWire{TimeCreated: now, UserID: userID, LifecycleState: stateActive, Password: pw}, newEtag(), nil
}

// GetUIPasswordInfo returns the console password metadata (never the password).
func (b *InMemoryBackend) GetUIPasswordInfo(userID string) (uiPasswordWire, error) {
	b.mu.RLock("GetUIPasswordInfo")
	defer b.mu.RUnlock()

	u, aerr := b.userByID(userID)
	if aerr != nil {
		return uiPasswordWire{}, aerr
	}

	if u.UIPasswordCreated == nil {
		return uiPasswordWire{}, errNotFound("UI password of " + userID)
	}

	return uiPasswordWire{TimeCreated: *u.UIPasswordCreated, UserID: userID, LifecycleState: stateActive}, nil
}

func (h *Handler) uploadAPIKey(q *req) error {
	var in struct {
		Key string `json:"key"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UploadAPIKey(q.param("userId"), in.Key)

	return reply(q, v, etag, err)
}

func (h *Handler) listAPIKeys(q *req) error {
	v, err := h.Backend.ListAPIKeys(q.param("userId"))

	return reply(q, v, "", err)
}

func (h *Handler) deleteAPIKey(q *req) error {
	fp, uerr := url.PathUnescape(q.param("fingerprint"))
	if uerr != nil {
		fp = q.param("fingerprint")
	}

	return done(q, h.Backend.DeleteAPIKey(q.param("userId"), fp))
}

type credBody struct {
	Description *string `json:"description"`
	DisplayName *string `json:"displayName"`
}

func (c credBody) text(kind credKind) string {
	p := c.Description
	if kind.nameKey == "displayName" {
		p = c.DisplayName
	}

	if p == nil {
		return ""
	}

	return *p
}

func (h *Handler) createCred(q *req, kind credKind) error {
	var in credBody
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateCredential(q.param("userId"), kind, in.text(kind))

	return reply(q, v, etag, err)
}

func (h *Handler) listCreds(q *req, kind credKind) error {
	v, err := h.Backend.ListCredentials(q.param("userId"), kind)

	return reply(q, v, "", err)
}

func (h *Handler) updateCred(q *req, kind credKind, idParam string) error {
	var in credBody
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateCredential(q.param("userId"), q.param(idParam), kind, in.text(kind))

	return reply(q, v, etag, err)
}

func (h *Handler) deleteCred(q *req, kind credKind, idParam string) error {
	return done(q, h.Backend.DeleteCredential(q.param("userId"), q.param(idParam), kind))
}

func (h *Handler) createAuthToken(q *req) error { return h.createCred(q, authTokenKind()) }
func (h *Handler) listAuthTokens(q *req) error  { return h.listCreds(q, authTokenKind()) }
func (h *Handler) updateAuthToken(q *req) error {
	return h.updateCred(q, authTokenKind(), "authTokenId")
}
func (h *Handler) deleteAuthToken(q *req) error {
	return h.deleteCred(q, authTokenKind(), "authTokenId")
}
func (h *Handler) createSecretKey(q *req) error { return h.createCred(q, secretKeyKind()) }
func (h *Handler) listSecretKeys(q *req) error  { return h.listCreds(q, secretKeyKind()) }
func (h *Handler) updateSecretKey(q *req) error {
	return h.updateCred(q, secretKeyKind(), "customerSecretKeyId")
}
func (h *Handler) deleteSecretKey(q *req) error {
	return h.deleteCred(q, secretKeyKind(), "customerSecretKeyId")
}
func (h *Handler) createSMTP(q *req) error { return h.createCred(q, smtpKind()) }
func (h *Handler) listSMTP(q *req) error   { return h.listCreds(q, smtpKind()) }
func (h *Handler) updateSMTP(q *req) error {
	return h.updateCred(q, smtpKind(), "smtpCredentialId")
}
func (h *Handler) deleteSMTP(q *req) error {
	return h.deleteCred(q, smtpKind(), "smtpCredentialId")
}

func (h *Handler) resetUIPassword(q *req) error {
	v, etag, err := h.Backend.ResetUIPassword(q.param("userId"))

	return reply(q, v, etag, err)
}

func (h *Handler) getUIPassword(q *req) error {
	v, err := h.Backend.GetUIPasswordInfo(q.param("userId"))

	return reply(q, v, "", err)
}
