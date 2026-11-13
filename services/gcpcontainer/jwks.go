package gcpcontainer

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"

	"cloud.google.com/go/container/apiv1/containerpb"
)

const jwkRSABits = 2048

func newSigningJWK() (*containerpb.Jwk, error) {
	key, err := rsa.GenerateKey(rand.Reader, jwkRSABits)
	if err != nil {
		return nil, fmt.Errorf("cluster signing key: %w", err)
	}

	enc := base64.RawURLEncoding
	n := key.N.Bytes()
	e := big.NewInt(int64(key.E)).Bytes()
	sum := sha256.Sum256(n)

	return &containerpb.Jwk{
		Kty: "RSA",
		Alg: "RS256",
		Use: "sig",
		Kid: enc.EncodeToString(sum[:]),
		N:   enc.EncodeToString(n),
		E:   enc.EncodeToString(e),
	}, nil
}

// GetJSONWebKeys returns the public key that verifies the cluster's service account tokens.
func (b *InMemoryBackend) GetJSONWebKeys(
	req *containerpb.GetJSONWebKeysRequest,
) (*containerpb.GetJSONWebKeysResponse, error) {
	r, err := resolveCluster(req.GetParent(), "", "", "")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("GetJSONWebKeys")
	defer b.mu.Unlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	if c.JWK == nil {
		if c.JWK, err = newSigningJWK(); err != nil {
			return nil, err
		}
	}

	return &containerpb.GetJSONWebKeysResponse{Keys: []*containerpb.Jwk{clone(c.JWK)}}, nil
}
