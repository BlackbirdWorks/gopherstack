package bedrockagentcorecontrol

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"math/big"
	"time"
)

const (
	jwksTTL         = 10 * time.Minute
	jwksRefetchGap  = 30 * time.Second
	jwksNegativeTTL = time.Minute
	maxJWKSKeys     = 64
	maxJWKSIssuers  = 32
	minRSABits      = 2048
	maxRSAExponent  = 1 << 31
)

type jwksEntry struct {
	fetched time.Time
	keys    map[string]*rsa.PublicKey
	issuer  string
}

func (b *InMemoryBackend) cachedJWKS(discovery string) (jwksEntry, bool, bool) {
	b.mu.RLock("cachedJWKS")
	defer b.mu.RUnlock()

	e, ok := b.jwksCache[discovery]
	until, failed := b.jwksNegative[discovery]

	return e, ok, failed && b.now().Before(until)
}

func (b *InMemoryBackend) rememberJWKSFailure(discovery string) {
	b.mu.Lock("rememberJWKSFailure")
	defer b.mu.Unlock()

	if b.jwksNegative == nil {
		b.jwksNegative = map[string]time.Time{}
	}

	now := b.now()

	for k, until := range b.jwksNegative {
		if !now.Before(until) {
			delete(b.jwksNegative, k)
		}
	}

	if _, ok := b.jwksNegative[discovery]; !ok && len(b.jwksNegative) >= maxJWKSIssuers {
		return
	}

	b.jwksNegative[discovery] = now.Add(jwksNegativeTTL)
}

// jwksKey returns the RSA key kid of the issuer whose discovery document is at discovery. Fetched sets are cached
// for jwksTTL; an unknown kid refetches at most once per jwksRefetchGap.
func (b *InMemoryBackend) jwksKey(ctx context.Context, discovery, kid string) (*rsa.PublicKey, string, error) {
	e, ok, failed := b.cachedJWKS(discovery)
	if failed && (!ok || b.now().Sub(e.fetched) >= jwksTTL) {
		return nil, "", errJWKSUnavailable
	}

	age := b.now().Sub(e.fetched)

	if ok && age < jwksTTL {
		if key := e.pick(kid); key != nil {
			return key, e.issuer, nil
		}

		if age < jwksRefetchGap {
			return nil, e.issuer, errNoSigningKey
		}
	}

	e, err := b.fetchJWKS(ctx, discovery)
	if err != nil {
		b.rememberJWKSFailure(discovery)

		return nil, e.issuer, err
	}

	b.mu.Lock("jwksKey")
	if b.jwksCache == nil {
		b.jwksCache = map[string]jwksEntry{}
	}

	b.storeJWKSLocked(discovery, e)
	b.mu.Unlock()

	if key := e.pick(kid); key != nil {
		return key, e.issuer, nil
	}

	return nil, e.issuer, errNoSigningKey
}

func (b *InMemoryBackend) storeJWKSLocked(discovery string, e jwksEntry) {
	if _, ok := b.jwksCache[discovery]; !ok && len(b.jwksCache) >= maxJWKSIssuers {
		oldest := ""

		for k, v := range b.jwksCache {
			if oldest == "" || v.fetched.Before(b.jwksCache[oldest].fetched) {
				oldest = k
			}
		}

		delete(b.jwksCache, oldest)
	}

	b.jwksCache[discovery] = e
}

func (e jwksEntry) pick(kid string) *rsa.PublicKey {
	if kid != "" {
		return e.keys[kid]
	}

	for _, k := range e.keys {
		return k
	}

	return nil
}

func (b *InMemoryBackend) fetchJWKS(ctx context.Context, discovery string) (jwksEntry, error) {
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}

	if err := b.getJSON(ctx, discovery, &doc); err != nil {
		return jwksEntry{}, fmt.Errorf("fetch discovery document: %w", err)
	}

	var set struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}

	if err := b.getJSON(ctx, doc.JWKSURI, &set); err != nil {
		return jwksEntry{issuer: doc.Issuer}, fmt.Errorf("fetch JWKS: %w", err)
	}

	e := jwksEntry{fetched: b.now(), issuer: doc.Issuer, keys: map[string]*rsa.PublicKey{}}

	for _, k := range set.Keys {
		if k.Kty != "RSA" || len(e.keys) >= maxJWKSKeys {
			continue
		}

		n, nerr := base64.RawURLEncoding.DecodeString(k.N)
		ex, eerr := base64.RawURLEncoding.DecodeString(k.E)

		if nerr != nil || eerr != nil {
			continue
		}

		exp := new(big.Int).SetBytes(ex)
		if !exp.IsInt64() || exp.Int64() <= 0 || exp.Int64() >= maxRSAExponent {
			continue
		}

		mod := new(big.Int).SetBytes(n)
		if mod.BitLen() < minRSABits {
			continue
		}

		e.keys[k.Kid] = &rsa.PublicKey{N: mod, E: int(exp.Int64())}
	}

	return e, nil
}
