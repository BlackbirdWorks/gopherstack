package cognitoidp

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
)

var (
	errDecodePEM = errors.New("failed to decode RSA private key PEM block")
	errNotRSAKey = errors.New("private key is not *rsa.PrivateKey")
)

// userPoolSnapshot holds the serializable fields of a UserPool.
type userPoolSnapshot struct {
	CreatedAt  string `json:"createdAt"`
	ID         string `json:"id"`
	Name       string `json:"name"`
	ARN        string `json:"arn"`
	IssuerURL  string `json:"issuerUrl"`
	KeyID      string `json:"keyId"`
	PrivKeyPEM string `json:"privKeyPem"`
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
}

type backendSnapshot struct {
	Pools         map[string]*userPoolSnapshot                    `json:"pools"`
	Clients       map[string]*UserPoolClient                      `json:"clients"`
	Users         map[string]map[string]*userSnapshot             `json:"users"`
	RefreshTokens map[string]*refreshTokenEntry                   `json:"refreshTokens,omitempty"`
	Groups        map[string]map[string]*Group                    `json:"groups,omitempty"`
	GroupMembers  map[string]map[string]map[string]struct{}       `json:"groupMembers,omitempty"`
	AccountID     string                                          `json:"accountId"`
	Region        string                                          `json:"region"`
	Endpoint      string                                          `json:"endpoint"`
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
			pem = ""
		}

		poolSnaps[id] = &userPoolSnapshot{
			CreatedAt:  p.CreatedAt.Format("2006-01-02T15:04:05Z"),
			ID:         p.ID,
			Name:       p.Name,
			ARN:        p.ARN,
			IssuerURL:  p.issuer.issuerURL,
			KeyID:      p.issuer.keyID,
			PrivKeyPEM: pem,
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

	data, _ := json.Marshal(snap)

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	// Reconstruct UserPool objects from snapshots (requires RSA key reconstruction).
	pools := make(map[string]*UserPool, len(snap.Pools))
	poolsByName := make(map[string]*UserPool, len(snap.Pools))

	for id, ps := range snap.Pools {
		rsaKey, err := unmarshalRSAKey(ps.PrivKeyPEM)
		if err != nil {
			return fmt.Errorf("restoring user pool %q: %w", id, err)
		}

		pool := &UserPool{
			ID:   ps.ID,
			Name: ps.Name,
			ARN:  ps.ARN,
		}

		if rsaKey != nil {
			pool.issuer = newTokenIssuerFromKey(rsaKey, ps.KeyID, ps.IssuerURL)
		}

		pools[id] = pool
		poolsByName[ps.Name] = pool
	}

	// Reconstruct User objects from snapshots.
	users := make(map[string]map[string]*User, len(snap.Users))
	for poolID, poolUsers := range snap.Users {
		pUsers := make(map[string]*User, len(poolUsers))

		for username, us := range poolUsers {
			pUsers[username] = &User{
				Attributes:   us.Attributes,
				Sub:          us.Sub,
				Username:     us.Username,
				UserPoolID:   us.UserPoolID,
				PasswordHash: us.PasswordHash,
				Status:       us.Status,
				ConfirmCode:  us.ConfirmCode,
			}
		}

		users[poolID] = pUsers
	}

	if snap.Clients == nil {
		snap.Clients = make(map[string]*UserPoolClient)
	}

	if snap.RefreshTokens == nil {
		snap.RefreshTokens = make(map[string]*refreshTokenEntry)
	}

	if snap.Groups == nil {
		snap.Groups = make(map[string]map[string]*Group)
	}

	if snap.GroupMembers == nil {
		snap.GroupMembers = make(map[string]map[string]map[string]struct{})
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.pools = pools
	b.poolsByName = poolsByName
	b.users = users
	b.clients = snap.Clients
	b.refreshTokens = snap.RefreshTokens
	b.groups = snap.Groups
	b.groupMembers = snap.GroupMembers
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.endpoint = snap.Endpoint

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
