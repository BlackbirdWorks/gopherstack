package gcpiam

import (
	"crypto/rsa"
	"crypto/x509"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// VerificationKeys returns the public keys of an enabled service account's usable keys, for services that
// verify tokens signed by it (Identity Platform custom tokens). Unknown or disabled accounts yield none.
func (b *InMemoryBackend) VerificationKeys(email string) []*rsa.PublicKey {
	b.mu.RLock("VerificationKeys")
	defer b.mu.RUnlock()

	var out []*rsa.PublicKey

	for _, rec := range b.accounts {
		if !rec.active() || rec.pb.GetDisabled() || !strs.Equal(rec.pb.GetEmail(), email) {
			continue
		}

		keys := make([]*keyRec, 0, len(rec.keys)+1)
		for _, k := range rec.keys {
			if !k.pb.GetDisabled() {
				keys = append(keys, k)
			}
		}

		if rec.system != nil {
			keys = append(keys, rec.system)
		}

		for _, k := range keys {
			if pub, err := x509.ParsePKIXPublicKey(k.public); err == nil {
				if rk, ok := pub.(*rsa.PublicKey); ok {
					out = append(out, rk)
				}
			}
		}
	}

	return out
}
