package s3control

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	dataAccessDefaultDuration = time.Hour
	dataAccessKeyIDLen        = 16
	dataAccessSecretLen       = 20
	dataAccessTokenLen        = 64
	dataAccessKeyAlphabet     = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var errNoMatchingGrant = errors.New("no access grant matches the requested target and permission")

// DataAccess is the result of a successful GetDataAccess.
type DataAccess struct {
	Expiration        time.Time
	AccessKeyID       string
	SecretAccessKey   string
	SessionToken      string
	GranteeType       string
	GranteeIdentifier string
}

// GetDataAccess finds a grant covering target with at least the requested
// permission and vends temporary credentials for it.
func (b *InMemoryBackend) GetDataAccess(
	accountID, target, permission string, duration time.Duration,
) (*DataAccess, error) {
	b.mu.RLock("GetDataAccess")
	defer b.mu.RUnlock()

	if !b.accessGrantsInstances.Has(accountID) {
		return nil, awserr.New("AccessGrantsInstanceNotExistsError", awserr.ErrNotFound)
	}

	var matches []*AccessGrant

	for _, g := range b.accessGrants.All() {
		if g.AccountID == accountID && grantCovers(g, target, permission) {
			matches = append(matches, g)
		}
	}

	if len(matches) == 0 {
		return nil, errNoMatchingGrant
	}

	slices.SortFunc(matches, func(a, c *AccessGrant) int { return strings.Compare(a.AccessGrantID, c.AccessGrantID) })

	if duration <= 0 {
		duration = dataAccessDefaultDuration
	}

	keyID, secret, token, err := newTemporaryCredentials()
	if err != nil {
		return nil, err
	}

	return &DataAccess{
		AccessKeyID:       keyID,
		SecretAccessKey:   secret,
		SessionToken:      token,
		Expiration:        time.Now().UTC().Add(duration),
		GranteeType:       matches[0].GranteeType,
		GranteeIdentifier: matches[0].GranteeIdentifier,
	}, nil
}

func grantCovers(g *AccessGrant, target, permission string) bool {
	if !permissionCovers(g.Permission, permission) {
		return false
	}

	scope := g.GrantScope
	if prefix, ok := strings.CutSuffix(scope, "*"); ok {
		return strings.HasPrefix(target, prefix)
	}

	rest := strings.TrimPrefix(scope, "s3://")
	if !strings.Contains(rest, "/") || strings.HasSuffix(scope, "/") {
		return strings.HasPrefix(target, scope)
	}

	return target == scope
}

func permissionCovers(granted, requested string) bool {
	if granted == requested {
		return true
	}

	return granted == "READWRITE" && (requested == "READ" || requested == "WRITE")
}

func newTemporaryCredentials() (string, string, string, error) {
	idBuf := make([]byte, dataAccessKeyIDLen)
	secBuf := make([]byte, dataAccessSecretLen)
	tokBuf := make([]byte, dataAccessTokenLen)

	for _, buf := range [][]byte{idBuf, secBuf, tokBuf} {
		if _, err := rand.Read(buf); err != nil {
			return "", "", "", err
		}
	}

	id := make([]byte, dataAccessKeyIDLen)
	for i, v := range idBuf {
		id[i] = dataAccessKeyAlphabet[int(v)%len(dataAccessKeyAlphabet)]
	}

	return "ASIA" + string(id), hex.EncodeToString(secBuf), base64.StdEncoding.EncodeToString(tokBuf), nil
}
