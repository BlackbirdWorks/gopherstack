package cognitoidp

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	// UserStatusExternalProvider marks a user created through federated sign-in.
	UserStatusExternalProvider = "EXTERNAL_PROVIDER"

	attrIdentities  = "identities"
	attrUsernameMap = "username"
	idpStateTTL     = 10 * time.Minute
	maxIDPStates    = 10000
	idpStateBytes   = 24
	providerTypeOID = "OIDC"
)

// idpState is an ephemeral, never-persisted record of an authorize request handed to an external IdP.
type idpState struct {
	ExpiresAt     time.Time
	PoolID        string
	ClientID      string
	ProviderName  string
	RedirectURI   string
	AppState      string
	CodeChallenge string
	AppNonce      string
	ResponseType  string
	IDPNonce      string
	IDPRedirect   string
	SAMLRequestID string
	Scopes        []string
}

type identityRecord struct {
	UserID       string `json:"userId"`
	ProviderName string `json:"providerName"`
	ProviderType string `json:"providerType"`
	Issuer       string `json:"issuer,omitempty"`
	Primary      string `json:"primary"`
	DateCreated  string `json:"dateCreated"`
}

func (b *InMemoryBackend) storeIDPState(st *idpState) (string, error) {
	raw := make([]byte, idpStateBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating idp state: %w", err)
	}

	key := base64.RawURLEncoding.EncodeToString(raw)

	b.mu.Lock("StoreIDPState")
	defer b.mu.Unlock()

	now := time.Now()
	if len(b.idpStates) >= maxIDPStates {
		for k, e := range b.idpStates {
			if !e.ExpiresAt.After(now) {
				delete(b.idpStates, k)
			}
		}
	}

	if len(b.idpStates) >= maxIDPStates {
		var oldestKey string

		var oldest time.Time

		for k, e := range b.idpStates {
			if oldestKey == "" || e.ExpiresAt.Before(oldest) {
				oldestKey, oldest = k, e.ExpiresAt
			}
		}

		delete(b.idpStates, oldestKey)
	}

	st.ExpiresAt = now.Add(idpStateTTL)
	b.idpStates[key] = st

	return key, nil
}

// consumeIDPState removes the state unconditionally so a callback can never be replayed.
func (b *InMemoryBackend) consumeIDPState(key string) (*idpState, bool) {
	b.mu.Lock("ConsumeIDPState")
	defer b.mu.Unlock()

	st, ok := b.idpStates[key]
	if !ok {
		return nil, false
	}

	delete(b.idpStates, key)

	if !st.ExpiresAt.After(time.Now()) {
		return nil, false
	}

	return st, true
}

// federationProvider resolves a provider by name or, when name is empty, by IdpIdentifier.
func (b *InMemoryBackend) federationProvider(poolID, name, identifier string) (*IdentityProvider, bool) {
	b.mu.RLock("FederationProvider")
	defer b.mu.RUnlock()

	if name != "" {
		idp, ok := b.identityProviders.Get(identityProviderKey(poolID, name))
		if !ok {
			return nil, false
		}

		return cloneIdentityProvider(idp), true
	}

	for _, idp := range b.identityProvidersByPool.Get(poolID) {
		if slices.Contains(idp.IdpIdentifiers, identifier) {
			return cloneIdentityProvider(idp), true
		}
	}

	return nil, false
}

func cloneIdentityProvider(idp *IdentityProvider) *IdentityProvider {
	cp := *idp
	cp.ProviderDetails = maps.Clone(idp.ProviderDetails)
	cp.AttributeMapping = maps.Clone(idp.AttributeMapping)
	cp.IdpIdentifiers = slices.Clone(idp.IdpIdentifiers)

	return &cp
}

// claimString renders a scalar IdP claim as a user attribute value.
func claimString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return ""
	}
}

func mappedAttributes(mapping map[string]string, claims map[string]any) map[string]string {
	out := make(map[string]string, len(mapping))

	for attr, claim := range mapping {
		if attr == attrUsernameMap {
			continue
		}

		if v := claimString(claims[claim]); v != "" {
			out[attr] = v
		}
	}

	return out
}

// federatedSignIn links or creates the Cognito user for an IdP subject, refreshing its mapped attributes.
func (b *InMemoryBackend) federatedSignIn(
	poolID, clientID string, idp *IdentityProvider, claims map[string]any, issuer string,
) (string, error) {
	subjectClaim := claimSub
	if c := idp.AttributeMapping[attrUsernameMap]; c != "" {
		subjectClaim = c
	}

	subject := claimString(claims[subjectClaim])
	if subject == "" {
		return "", fmt.Errorf("%w: IdP claim %q is missing", ErrInvalidParameter, subjectClaim)
	}

	b.mu.Lock("FederatedSignIn")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(poolID)
	if !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	attrs := mappedAttributes(idp.AttributeMapping, claims)
	user := b.linkedUserLocked(poolID, idp.ProviderName, subject)

	if user == nil {
		user, _ = b.users.Get(userKey(poolID, idp.ProviderName+"_"+subject))
	}

	if user == nil {
		return b.createFederatedUserLocked(pool, clientID, idp, subject, issuer, attrs)
	}

	if !user.Enabled {
		return "", fmt.Errorf("%w: user %q account is disabled", ErrNotAuthorized, user.Username)
	}

	if user.Attributes == nil {
		user.Attributes = make(map[string]string)
	}

	maps.Copy(user.Attributes, attrs)
	user.Attributes[attrIdentities] = addIdentity(user.Attributes[attrIdentities], idp, subject, issuer)
	user.UpdatedAt = time.Now()

	return user.Username, nil
}

// reservedFederatedNameLocked rejects native usernames in the "<ProviderName>_" namespace of a
// configured identity provider, closing the pre-hijack of a future federated account.
func (b *InMemoryBackend) reservedFederatedNameLocked(poolID, username string) error {
	folded := strs.Fold(username)

	for _, idp := range b.identityProvidersByPool.Get(poolID) {
		if strings.HasPrefix(folded, strs.Fold(idp.ProviderName)+"_") {
			return fmt.Errorf("%w: username prefix %q_ is reserved for identity provider %q",
				ErrInvalidParameter, idp.ProviderName, idp.ProviderName)
		}
	}

	return nil
}

func (b *InMemoryBackend) linkedUserLocked(poolID, providerName, subject string) *User {
	for _, u := range b.usersByPool.Get(poolID) {
		for _, l := range u.LinkedProviders {
			if l.ProviderName == providerName && l.ProviderAttributeValue == subject {
				return u
			}
		}
	}

	return nil
}

// createFederatedUserLocked runs PreSignUp_ExternalProvider, creates the user and runs PostConfirmation.
func (b *InMemoryBackend) createFederatedUserLocked(
	pool *UserPool, clientID string, idp *IdentityProvider, subject, issuer string, attrs map[string]string,
) (string, error) {
	username := idp.ProviderName + "_" + subject
	attrs[attrIdentities] = addIdentity("", idp, subject, issuer)

	if _, err := b.invokeTriggerUnlocked(pool, triggerKeyPreSignUp, triggerSourcePreSignUpExternal, clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(attrs),
			eventKeyValidationData: map[string]any{},
			eventKeyClientMetadata: map[string]any{},
		},
		preSignUpDefaults(),
	); err != nil {
		return "", err
	}

	if err := b.newUserSlotFreeLocked(pool, username); err != nil {
		return "", err
	}

	now := time.Now()

	b.users.Put(&User{
		Sub:        uuid.New().String(),
		Username:   username,
		UserPoolID: pool.ID,
		Status:     UserStatusExternalProvider,
		Attributes: attrs,
		CreatedAt:  now,
		UpdatedAt:  now,
		Enabled:    true,
	})

	if _, err := b.invokeTriggerUnlocked(pool, triggerKeyPostConfirmation, triggerSourcePostConfirmationSignUp,
		clientID, username,
		map[string]any{eventKeyUserAttributes: stringMapToAny(attrs), eventKeyClientMetadata: map[string]any{}},
		map[string]any{},
	); err != nil {
		return "", err
	}

	return username, nil
}

// addIdentity returns the identities attribute JSON with this provider's record present exactly once.
func addIdentity(existing string, idp *IdentityProvider, subject, issuer string) string {
	return appendIdentity(existing, identityRecord{
		UserID: subject, ProviderName: idp.ProviderName, ProviderType: idp.ProviderType, Issuer: issuer,
		Primary: "", DateCreated: strconv.FormatInt(time.Now().UnixMilli(), 10),
	})
}

func parseIdentities(raw string) []identityRecord {
	records := make([]identityRecord, 0, 1)
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &records)
	}

	return records
}

// appendIdentity adds rec unless the provider/user pair is present; an empty Primary means "first record".
func appendIdentity(existing string, rec identityRecord) string {
	records := parseIdentities(existing)

	for _, r := range records {
		if r.ProviderName == rec.ProviderName && r.UserID == rec.UserID {
			return existing
		}
	}

	if rec.Primary == "" {
		rec.Primary = strconv.FormatBool(len(records) == 0)
	}

	out, err := json.Marshal(append(records, rec))
	if err != nil {
		return existing
	}

	return string(out)
}

// removeIdentity drops the provider/user record, returning "" when none remain.
func removeIdentity(existing, providerName, userID string) string {
	records := slices.DeleteFunc(parseIdentities(existing), func(r identityRecord) bool {
		return r.ProviderName == providerName && r.UserID == userID
	})
	if len(records) == 0 {
		return ""
	}

	out, err := json.Marshal(records)
	if err != nil {
		return existing
	}

	return string(out)
}
