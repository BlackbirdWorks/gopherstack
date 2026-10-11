package eks

import (
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/big"
)

const (
	irsaKeyBits     = 2048
	irsaPrimeBytes  = irsaKeyBits / 16
	irsaPublicExp   = 65537
	irsaMillerRound = 20
)

var errIRSAKeyDerivation = errors.New("eks: derive oidc signing key")

// deriveIRSAKey builds a deterministic RSA key from seed so a cluster's signing
// key survives snapshot restore without being persisted.
func deriveIRSAKey(seed string) (*rsa.PrivateKey, error) {
	p := derivePrime(seed, "p", nil)
	q := derivePrime(seed, "q", p)

	one := big.NewInt(1)
	n := new(big.Int).Mul(p, q)
	phi := new(big.Int).Mul(new(big.Int).Sub(p, one), new(big.Int).Sub(q, one))
	e := big.NewInt(irsaPublicExp)

	d := new(big.Int).ModInverse(e, phi)
	if d == nil || n.BitLen() != irsaKeyBits {
		return nil, errIRSAKeyDerivation
	}

	key := &rsa.PrivateKey{D: d, Primes: []*big.Int{p, q}}
	key.N = n
	key.E = irsaPublicExp

	if err := key.Validate(); err != nil {
		return nil, errors.Join(errIRSAKeyDerivation, err)
	}

	key.Precompute()

	return key, nil
}

func derivePrime(seed, label string, avoid *big.Int) *big.Int {
	e := big.NewInt(irsaPublicExp)
	one := big.NewInt(1)
	gcd := new(big.Int)

	for attempt := uint32(0); ; attempt++ {
		buf := make([]byte, 0, irsaPrimeBytes)

		for block := uint32(0); len(buf) < irsaPrimeBytes; block++ {
			h := sha256.New()
			h.Write([]byte("gopherstack-eks-irsa/" + label + "/" + seed))
			_ = binary.Write(h, binary.BigEndian, [2]uint32{attempt, block})
			buf = h.Sum(buf)
		}

		buf = buf[:irsaPrimeBytes]
		buf[0] |= 0xC0
		buf[len(buf)-1] |= 1

		c := new(big.Int).SetBytes(buf)
		if avoid != nil && c.Cmp(avoid) == 0 {
			continue
		}

		if !c.ProbablyPrime(irsaMillerRound) {
			continue
		}

		if gcd.GCD(nil, nil, e, new(big.Int).Sub(c, one)).Cmp(one) != 0 {
			continue
		}

		return c
	}
}

// irsaKeyID is the JWK "kid" for pub.
func irsaKeyID(pub *rsa.PublicKey) string {
	sum := sha256.Sum256(append(pub.N.Bytes(), big.NewInt(int64(pub.E)).Bytes()...))

	return base64.RawURLEncoding.EncodeToString(sum[:])
}
