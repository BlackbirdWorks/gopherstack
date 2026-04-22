package cognitoidp

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var (
	errDecodePEM = errors.New("failed to decode RSA private key PEM block")
	errNotRSAKey = errors.New("private key is not *rsa.PrivateKey")
)

// userPoolSnapshot holds the serializable fields of a UserPool.
type userPoolSnapshot struct {
	CreatedAt        string            `json:"createdAt"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	ARN              string            `json:"arn"`
	IssuerURL        string            `json:"issuerUrl"`
	KeyID            string            `json:"keyId"`
	PrivKeyPEM       string            `json:"privKeyPem"`
	CustomAttributes []SchemaAttribute `json:"customAttributes,omitempty"`
}

// userSnapshot is a copy of User safe for JSON serialization.
type userSnapshot struct {
	CreatedAt    string            `json:"createdAt"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Sub          string            `json:"sub"`
	Username     string            `json:"username"`
	UserPoolID   string            `json:"userPoolId"`
	PasswordHash string            `json:"passwordHash"`
	Status       string            `json:"status"`
	ConfirmCode  string            `json:"confirmCode,omitempty"`
	Enabled      bool              `json:"enabled"`
}

type backendSnapshot struct {
	Pools         map[string]*userPoolSnapshot              `json:"pools"`
	Clients       map[string]*UserPoolClient                `json:"clients"`
	Users         map[string]map[string]*userSnapshot       `json:"users"`
	RefreshTokens map[string]*refreshTokenEntry             `json:"refreshTokens,omitempty"`
	Groups        map[string]map[string]*Group              `json:"groups,omitempty"`
	GroupMembers  map[string]map[string]map[string]struct{} `json:"groupMembers,omitempty"`
	AccountID     string                                    `json:"accountId"`
	Region        string                                    `json:"region"`
	Endpoint      string                                    `json:"endpoint"`
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
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	poolSnaps := make(map[string]*userPoolSnapshot, len(b.pools))
	for id, p := range b.pools {
		pem, err := marshalRSAKey(p.issuer.privateKey)
		if err != nil {
			slog.Default().Warn("cognitoidp: failed to marshal RSA key for pool snapshot", "poolId", id, "error", err)
			pem = ""
		}

		poolSnaps[id] = &userPoolSnapshot{
			CreatedAt:        p.CreatedAt.Format("2006-01-02T15:04:05Z"),
			ID:               p.ID,
			Name:             p.Name,
			ARN:              p.ARN,
			IssuerURL:        p.issuer.issuerURL,
			KeyID:            p.issuer.keyID,
			PrivKeyPEM:       pem,
			CustomAttributes: p.CustomAttributes,
		}
	}

	userSnaps := make(map[string]map[string]*userSnapshot, len(b.users))
	for poolID, poolUsers := range b.users {
		snaps := make(map[string]*userSnapshot, len(poolUsers))

		for username, u := range poolUsers {
			snaps[username] = &userSnapshot{
				CreatedAt:    u.CreatedAt.Format("2006-01-02T15:04:05Z"),
				Attributes:   u.Attributes,
				Sub:          u.Sub,
				Username:     u.Username,
				UserPoolID:   u.UserPoolID,
				PasswordHash: u.PasswordHash,
				Status:       u.Status,
				ConfirmCode:  u.ConfirmCode,
				Enabled:      u.Enabled,
			}
		}

		userSnaps[poolID] = snaps
	}

	snap := backendSnapshot{
		Pools:         poolSnaps,
		Clients:       b.clients,
		Users:         userSnaps,
		RefreshTokens: b.refreshTokens,
		Groups:        b.groups,
		GroupMembers:  b.groupMembers,
		AccountID:     b.accountID,
		Region:        b.region,
		Endpoint:      b.endpoint,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("cognitoidp: failed to marshal backend snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
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
	b.clients = snap.Clients
	b.clientsByPool = buildClientsByPoolIndex(b.clients)
	b.refreshTokens = snap.RefreshTokens
	b.refreshTokensByClient = buildRefreshTokensByClientIndex(b.refreshTokens)
	b.groups = snap.Groups
	b.groupMembers = snap.GroupMembers
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.endpoint = snap.Endpoint

	return nil
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

		pool := &UserPool{
			ID:               ps.ID,
			Name:             ps.Name,
			ARN:              ps.ARN,
			CustomAttributes: ps.CustomAttributes,
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
			restored[username] = &User{
				Attributes:   us.Attributes,
				Sub:          us.Sub,
				Username:     us.Username,
				UserPoolID:   us.UserPoolID,
				PasswordHash: us.PasswordHash,
				Status:       us.Status,
				ConfirmCode:  us.ConfirmCode,
				Enabled:      us.Enabled,
			}
		}

		users[poolID] = restored
	}

	return users
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
