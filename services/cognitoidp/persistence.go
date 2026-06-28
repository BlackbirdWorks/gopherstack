package cognitoidp

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

var (
	errDecodePEM = errors.New("failed to decode RSA private key PEM block")
	errNotRSAKey = errors.New("private key is not *rsa.PrivateKey")
)

// userPoolSnapshot holds the serializable fields of a UserPool.
type userPoolSnapshot struct {
	PasswordPolicy         *PasswordPolicy   `json:"passwordPolicy,omitempty"`
	CreatedAt              string            `json:"createdAt,omitempty"`
	ID                     string            `json:"id,omitempty"`
	Name                   string            `json:"name,omitempty"`
	ARN                    string            `json:"arn,omitempty"`
	IssuerURL              string            `json:"issuerUrl,omitempty"`
	KeyID                  string            `json:"keyId,omitempty"`
	PrivKeyPEM             string            `json:"privKeyPem,omitempty"`
	MfaConfiguration       string            `json:"mfaConfiguration,omitempty"`
	CustomAttributes       []SchemaAttribute `json:"customAttributes,omitempty"`
	AutoVerifiedAttributes []string          `json:"autoVerifiedAttributes,omitempty"`
}

// userSnapshot is a copy of User safe for JSON serialization.
type userSnapshot struct {
	CreatedAt            string            `json:"createdAt,omitempty"`
	UpdatedAt            string            `json:"updatedAt,omitempty"`
	ConfirmCodeExpiresAt string            `json:"confirmCodeExpiresAt,omitempty"`
	Attributes           map[string]string `json:"attributes,omitempty"`
	Sub                  string            `json:"sub,omitempty"`
	Username             string            `json:"username,omitempty"`
	UserPoolID           string            `json:"userPoolId,omitempty"`
	PasswordHash         string            `json:"passwordHash,omitempty"`
	Status               string            `json:"status,omitempty"`
	ConfirmCode          string            `json:"confirmCode,omitempty"`
	LinkedIdentities     []LinkedIdentity  `json:"linkedIdentities,omitempty"`
	Enabled              bool              `json:"enabled,omitempty"`
}

type backendSnapshot struct {
	Pools                   map[string]*userPoolSnapshot                `json:"pools,omitempty"`
	Clients                 map[string]*UserPoolClient                  `json:"clients,omitempty"`
	Users                   map[string]map[string]*userSnapshot         `json:"users,omitempty"`
	RefreshTokens           map[string]*refreshTokenEntry               `json:"refreshTokens,omitempty"`
	Groups                  map[string]map[string]*Group                `json:"groups,omitempty"`
	GroupMembers            map[string]map[string]map[string]struct{}   `json:"groupMembers,omitempty"`
	ResourceServers         map[string]map[string]*ResourceServer       `json:"resourceServers,omitempty"`
	TokenRevokedBefore      map[string]time.Time                        `json:"tokenRevokedBefore,omitempty"`
	Domains                 map[string]*UserPoolDomain                  `json:"domains,omitempty"`
	ResourceTags            map[string]map[string]string                `json:"resourceTags,omitempty"`
	RiskConfigurations      map[string]*RiskConfiguration               `json:"riskConfigurations,omitempty"`
	LogDeliveryConfigs      map[string]*LogDeliveryConfig               `json:"logDeliveryConfigs,omitempty"`
	UICustomizations        map[string]*UICustomization                 `json:"uiCustomizations,omitempty"`
	ManagedLoginBrandings   map[string]map[string]*ManagedLoginBranding `json:"managedLoginBrandings,omitempty"`
	Terms                   map[string]*Terms                           `json:"terms,omitempty"`
	UserImportJobs          map[string]map[string]*UserImportJob        `json:"userImportJobs,omitempty"`
	PoolMfaConfigs          map[string]*UserPoolMfaFullConfig           `json:"poolMfaConfigs,omitempty"`
	TypedRiskConfigurations map[string]*TypedRiskConfiguration          `json:"typedRiskConfigurations,omitempty"`
	IdentityProviders       map[string]map[string]*IdentityProvider     `json:"identityProviders,omitempty"`
	AccountID               string                                      `json:"accountId,omitempty"`
	Region                  string                                      `json:"region,omitempty"`
	Endpoint                string                                      `json:"endpoint,omitempty"`
}

func marshalRSAKey(key *rsa.PrivateKey) (string, error) {
	if key == nil {
		return "", nil
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", fmt.Errorf("marshal RSA private key: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), nil
}

func unmarshalRSAKey(pemStr string) (*rsa.PrivateKey, error) {
	if pemStr == "" {
		return nil, nil //nolint:nilnil // intentional: absent key is valid
	}

	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errDecodePEM
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS8 private key: %w", err)
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", errNotRSAKey, key)
	}

	return rsaKey, nil
}

// Snapshot serialises the backend state to JSON.
//
//nolint:funlen // serialises the full backend state; length is inherent
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	poolSnaps := make(map[string]*userPoolSnapshot, len(b.pools))
	for id, p := range b.pools {
		pem, err := marshalRSAKey(p.issuer.privateKey)
		if err != nil {
			logger.Load(ctx).
				WarnContext(ctx, "cognitoidp: failed to marshal RSA key for pool snapshot", "poolId", id, "error", err)
			pem = ""
		}

		var ppSnap *PasswordPolicy
		if p.PasswordPolicy != nil {
			pp := *p.PasswordPolicy
			ppSnap = &pp
		}

		var avAttrs []string
		if len(p.AutoVerifiedAttributes) > 0 {
			avAttrs = make([]string, len(p.AutoVerifiedAttributes))
			copy(avAttrs, p.AutoVerifiedAttributes)
		}

		poolSnaps[id] = &userPoolSnapshot{
			CreatedAt:              p.CreatedAt.Format("2006-01-02T15:04:05Z"),
			ID:                     p.ID,
			Name:                   p.Name,
			ARN:                    p.ARN,
			IssuerURL:              p.issuer.issuerURL,
			KeyID:                  p.issuer.keyID,
			PrivKeyPEM:             pem,
			CustomAttributes:       p.CustomAttributes,
			MfaConfiguration:       p.MfaConfiguration,
			PasswordPolicy:         ppSnap,
			AutoVerifiedAttributes: avAttrs,
		}
	}

	userSnaps := make(map[string]map[string]*userSnapshot, len(b.users))
	for poolID, poolUsers := range b.users {
		snaps := make(map[string]*userSnapshot, len(poolUsers))

		for username, u := range poolUsers {
			var codeExpiry string
			if !u.ConfirmCodeExpiresAt.IsZero() {
				codeExpiry = u.ConfirmCodeExpiresAt.Format("2006-01-02T15:04:05Z")
			}

			snaps[username] = &userSnapshot{
				CreatedAt:            u.CreatedAt.Format("2006-01-02T15:04:05Z"),
				UpdatedAt:            u.UpdatedAt.Format("2006-01-02T15:04:05Z"),
				ConfirmCodeExpiresAt: codeExpiry,
				Attributes:           u.Attributes,
				Sub:                  u.Sub,
				Username:             u.Username,
				UserPoolID:           u.UserPoolID,
				PasswordHash:         u.PasswordHash,
				Status:               u.Status,
				ConfirmCode:          u.ConfirmCode,
				LinkedIdentities:     u.LinkedIdentities,
				Enabled:              u.Enabled,
			}
		}

		userSnaps[poolID] = snaps
	}

	snap := backendSnapshot{
		Pools:                   poolSnaps,
		Clients:                 b.clients,
		Users:                   userSnaps,
		RefreshTokens:           b.refreshTokens,
		Groups:                  b.groups,
		GroupMembers:            b.groupMembers,
		ResourceServers:         b.resourceServers,
		TokenRevokedBefore:      b.tokenRevokedBefore,
		Domains:                 b.domains,
		ResourceTags:            b.resourceTags,
		RiskConfigurations:      b.riskConfigurations,
		LogDeliveryConfigs:      b.logDeliveryConfigs,
		UICustomizations:        b.uiCustomizations,
		ManagedLoginBrandings:   b.managedLoginBrandings,
		Terms:                   b.terms,
		UserImportJobs:          b.userImportJobs,
		PoolMfaConfigs:          b.poolMfaConfigs,
		TypedRiskConfigurations: b.typedRiskConfigurations,
		IdentityProviders:       b.identityProviders,
		AccountID:               b.accountID,
		Region:                  b.region,
		Endpoint:                b.endpoint,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cognitoidp: failed to marshal backend snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cognitoidp", data, &snap); err != nil {
		return err
	}

	normalizeBackendSnapshot(&snap)

	pools, poolsByName, err := restorePoolsFromSnapshot(snap.Pools)
	if err != nil {
		return err
	}

	users := restoreUsersFromSnapshot(snap.Users)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.pools = pools
	b.poolsByName = poolsByName
	b.users = users
	b.usersBySub = buildUsersBySubIndex(users)
	b.clients = snap.Clients
	b.clientsByPool = buildClientsByPoolIndex(b.clients)
	b.refreshTokens = snap.RefreshTokens
	b.refreshTokensByClient = buildRefreshTokensByClientIndex(b.refreshTokens)
	b.refreshTokensByUser = buildRefreshTokensByUserIndex(b.refreshTokens)
	b.groups = snap.Groups
	b.groupMembers = snap.GroupMembers
	b.resourceServers = snap.ResourceServers
	b.tokenRevokedBefore = snap.TokenRevokedBefore
	b.domains = snap.Domains
	b.resourceTags = snap.ResourceTags
	b.riskConfigurations = snap.RiskConfigurations
	b.logDeliveryConfigs = snap.LogDeliveryConfigs
	b.uiCustomizations = snap.UICustomizations
	b.managedLoginBrandings = snap.ManagedLoginBrandings
	b.terms = snap.Terms
	b.userImportJobs = snap.UserImportJobs
	b.poolMfaConfigs = snap.PoolMfaConfigs
	b.typedRiskConfigurations = snap.TypedRiskConfigurations
	b.identityProviders = snap.IdentityProviders
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.endpoint = snap.Endpoint

	return nil
}

func buildUsersBySubIndex(users map[string]map[string]*User) map[string]string {
	index := make(map[string]string)
	for poolID, poolUsers := range users {
		for _, u := range poolUsers {
			if u.Sub != "" {
				index[poolID+":"+u.Sub] = u.Username
			}
		}
	}

	return index
}

func buildClientsByPoolIndex(clients map[string]*UserPoolClient) map[string]map[string]*UserPoolClient {
	index := make(map[string]map[string]*UserPoolClient)
	for _, client := range clients {
		if index[client.UserPoolID] == nil {
			index[client.UserPoolID] = make(map[string]*UserPoolClient)
		}

		index[client.UserPoolID][client.ClientID] = client
	}

	return index
}

func buildRefreshTokensByClientIndex(
	refreshTokens map[string]*refreshTokenEntry,
) map[string]map[string]struct{} {
	index := make(map[string]map[string]struct{})
	for token, entry := range refreshTokens {
		if index[entry.ClientID] == nil {
			index[entry.ClientID] = make(map[string]struct{})
		}

		index[entry.ClientID][token] = struct{}{}
	}

	return index
}

func buildRefreshTokensByUserIndex(
	refreshTokens map[string]*refreshTokenEntry,
) map[string]map[string]struct{} {
	index := make(map[string]map[string]struct{})
	for token, entry := range refreshTokens {
		userKey := entry.PoolID + ":" + entry.Username
		if index[userKey] == nil {
			index[userKey] = make(map[string]struct{})
		}

		index[userKey][token] = struct{}{}
	}

	return index
}

func normalizeBackendSnapshot(snap *backendSnapshot) {
	if snap.Clients == nil {
		snap.Clients = make(map[string]*UserPoolClient)
	}

	if snap.RefreshTokens == nil {
		snap.RefreshTokens = make(map[string]*refreshTokenEntry)
	}

	defaultExpiry := time.Now().UTC().Add(defaultRefreshTokenTTL)
	for _, entry := range snap.RefreshTokens {
		if entry.ExpiresAt.IsZero() {
			entry.ExpiresAt = defaultExpiry
		}
	}

	if snap.Groups == nil {
		snap.Groups = make(map[string]map[string]*Group)
	}

	if snap.GroupMembers == nil {
		snap.GroupMembers = make(map[string]map[string]map[string]struct{})
	}

	if snap.ResourceServers == nil {
		snap.ResourceServers = make(map[string]map[string]*ResourceServer)
	}

	if snap.TokenRevokedBefore == nil {
		snap.TokenRevokedBefore = make(map[string]time.Time)
	}
}

func restorePoolsFromSnapshot(
	poolSnapshots map[string]*userPoolSnapshot,
) (map[string]*UserPool, map[string]*UserPool, error) {
	pools := make(map[string]*UserPool, len(poolSnapshots))
	poolsByName := make(map[string]*UserPool, len(poolSnapshots))

	for id, ps := range poolSnapshots {
		rsaKey, err := unmarshalRSAKey(ps.PrivKeyPEM)
		if err != nil {
			return nil, nil, fmt.Errorf("restoring user pool %q: %w", id, err)
		}

		createdAt, _ := time.Parse("2006-01-02T15:04:05Z", ps.CreatedAt)

		pool := &UserPool{
			ID:                     ps.ID,
			Name:                   ps.Name,
			ARN:                    ps.ARN,
			CreatedAt:              createdAt,
			CustomAttributes:       ps.CustomAttributes,
			MfaConfiguration:       ps.MfaConfiguration,
			PasswordPolicy:         ps.PasswordPolicy,
			AutoVerifiedAttributes: ps.AutoVerifiedAttributes,
		}

		if rsaKey != nil {
			pool.issuer = newTokenIssuerFromKey(rsaKey, ps.KeyID, ps.IssuerURL)
		}

		pools[id] = pool
		poolsByName[ps.Name] = pool
	}

	return pools, poolsByName, nil
}

func restoreUsersFromSnapshot(poolUsers map[string]map[string]*userSnapshot) map[string]map[string]*User {
	users := make(map[string]map[string]*User, len(poolUsers))
	for poolID, usersByName := range poolUsers {
		restored := make(map[string]*User, len(usersByName))
		for username, us := range usersByName {
			createdAt, _ := time.Parse("2006-01-02T15:04:05Z", us.CreatedAt)
			updatedAt, _ := time.Parse("2006-01-02T15:04:05Z", us.UpdatedAt)
			codeExpiry, _ := time.Parse("2006-01-02T15:04:05Z", us.ConfirmCodeExpiresAt)

			if updatedAt.IsZero() {
				updatedAt = createdAt
			}

			restored[username] = &User{
				CreatedAt:            createdAt,
				UpdatedAt:            updatedAt,
				ConfirmCodeExpiresAt: codeExpiry,
				Attributes:           us.Attributes,
				Sub:                  us.Sub,
				Username:             us.Username,
				UserPoolID:           us.UserPoolID,
				PasswordHash:         us.PasswordHash,
				Status:               us.Status,
				ConfirmCode:          us.ConfirmCode,
				LinkedIdentities:     us.LinkedIdentities,
				Enabled:              us.Enabled,
			}
		}

		users[poolID] = restored
	}

	return users
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
