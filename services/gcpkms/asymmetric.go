package gcpkms

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"fmt"

	"cloud.google.com/go/kms/apiv1/kmspb"
)

// digestOf returns the hash and bytes of a request digest.
func digestOf(d *kmspb.Digest) (crypto.Hash, []byte) {
	switch v := d.GetDigest().(type) {
	case *kmspb.Digest_Sha256:
		return crypto.SHA256, v.Sha256
	case *kmspb.Digest_Sha384:
		return crypto.SHA384, v.Sha384
	case *kmspb.Digest_Sha512:
		return crypto.SHA512, v.Sha512
	default:
		return 0, nil
	}
}

// signingDigest resolves the digest to sign from a request's digest or data field.
func signingDigest(s algSpec, req *kmspb.AsymmetricSignRequest) ([]byte, error) {
	if req.GetDigest() != nil && len(req.GetData()) > 0 {
		return nil, errInvalid("digest and data must not both be supplied.")
	}

	if req.GetDigest() == nil {
		if len(req.GetData()) == 0 {
			return nil, errInvalid("Either digest or data must be supplied.")
		}

		h := s.hash.New()
		h.Write(req.GetData())

		return h.Sum(nil), nil
	}

	h, sum := digestOf(req.GetDigest())
	if h != s.hash {
		return nil, errInvalid("The digest type does not match the key algorithm.")
	}

	if len(sum) != h.Size() {
		return nil, errInvalid("The digest has the wrong length for its type.")
	}

	return sum, nil
}

func signAsymmetric(s algSpec, material []byte, req *kmspb.AsymmetricSignRequest) ([]byte, error) {
	if s.kind == kindEd25519 {
		if len(req.GetData()) == 0 || req.GetDigest() != nil {
			return nil, errInvalid("EC_SIGN_ED25519 signs the data field; digest is not accepted.")
		}

		priv, err := parsePrivate(material)
		if err != nil {
			return nil, err
		}

		key, ok := priv.(ed25519.PrivateKey)
		if !ok {
			return nil, errInvalid("Key material is not an Ed25519 key.")
		}

		return ed25519.Sign(key, req.GetData()), nil
	}

	if s.mode == modeRawPKCS1 {
		return signRawPKCS1(s, material, req)
	}

	digest, err := signingDigest(s, req)
	if err != nil {
		return nil, err
	}

	priv, err := parsePrivate(material)
	if err != nil {
		return nil, err
	}

	return signDigest(s, priv, digest)
}

func signDigest(s algSpec, priv crypto.PrivateKey, digest []byte) ([]byte, error) {
	var sig []byte

	var err error

	switch key := priv.(type) {
	case *rsa.PrivateKey:
		if s.mode == modePSS {
			sig, err = rsa.SignPSS(rand.Reader, key, s.hash, digest, &rsa.PSSOptions{SaltLength: s.hash.Size()})
		} else {
			sig, err = rsa.SignPKCS1v15(rand.Reader, key, s.hash, digest)
		}
	case *ecdsa.PrivateKey:
		sig, err = ecdsa.SignASN1(rand.Reader, key, digest)
	default:
		return nil, errInvalid("Key material does not match the algorithm.")
	}

	if err != nil {
		return nil, fmt.Errorf("gcpkms: sign: %w", err)
	}

	return sig, nil
}

func decryptAsymmetric(s algSpec, material, ciphertext []byte) ([]byte, error) {
	priv, err := parsePrivate(material)
	if err != nil {
		return nil, err
	}

	key, ok := priv.(*rsa.PrivateKey)
	if !ok {
		return nil, errInvalid("Key material is not an RSA key.")
	}

	pt, err := rsa.DecryptOAEP(s.hash.New(), nil, key, ciphertext, nil)
	if err != nil {
		return nil, errInvalid("Decryption failed: the ciphertext is invalid.")
	}

	return pt, nil
}

// signRawPKCS1 signs data as an already-encoded PKCS#1 v1.5 message (no hashing, no DigestInfo).
func signRawPKCS1(s algSpec, material []byte, req *kmspb.AsymmetricSignRequest) ([]byte, error) {
	if req.GetDigest() != nil || len(req.GetData()) == 0 {
		return nil, errInvalid("RSA_SIGN_RAW_PKCS1 signs the data field; digest is not accepted.")
	}

	const (
		pkcs1Overhead = 11
		bitsPerByte   = 8
	)

	if limit := s.size/bitsPerByte - pkcs1Overhead; len(req.GetData()) > limit {
		return nil, errInvalid("data must be at most %d bytes for this key size.", limit)
	}

	priv, err := parsePrivate(material)
	if err != nil {
		return nil, err
	}

	key, ok := priv.(*rsa.PrivateKey)
	if !ok {
		return nil, errInvalid("Key material is not an RSA key.")
	}

	sig, err := rsa.SignPKCS1v15(nil, key, 0, req.GetData())
	if err != nil {
		return nil, fmt.Errorf("gcpkms: sign: %w", err)
	}

	return sig, nil
}
