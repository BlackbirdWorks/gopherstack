package gcpkms

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"hash/crc32"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

func randomBytes(n int) ([]byte, error) {
	out := make([]byte, n)
	if _, err := rand.Read(out); err != nil {
		return nil, fmt.Errorf("gcpkms: random bytes: %w", err)
	}

	return out, nil
}

// generateMaterial creates fresh key material: raw bytes for symmetric kinds, PKCS#8 DER otherwise.
func generateMaterial(s algSpec) ([]byte, error) {
	var priv crypto.PrivateKey

	var err error

	switch s.kind {
	case kindGoogleSym, kindAES, kindHMAC:
		return randomBytes(s.size)
	case kindRSA:
		priv, err = rsa.GenerateKey(rand.Reader, s.size)
	case kindEC:
		priv, err = ecdsa.GenerateKey(s.curve, rand.Reader)
	case kindEd25519:
		_, priv, err = ed25519.GenerateKey(rand.Reader)
	}

	if err != nil {
		return nil, fmt.Errorf("gcpkms: generate key: %w", err)
	}

	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("gcpkms: marshal key: %w", err)
	}

	return der, nil
}

func parsePrivate(material []byte) (crypto.PrivateKey, error) {
	k, err := x509.ParsePKCS8PrivateKey(material)
	if err != nil {
		return nil, errInvalid("Key material is not a valid PKCS#8 private key.")
	}

	return k, nil
}

func signerOf(material []byte) (crypto.Signer, error) {
	k, err := parsePrivate(material)
	if err != nil {
		return nil, err
	}

	signer, ok := k.(crypto.Signer)
	if !ok {
		return nil, errInvalid("Key material cannot sign.")
	}

	return signer, nil
}

// validateImported checks that imported material matches the algorithm's key shape.
func validateImported(s algSpec, material []byte) error {
	if s.kind == kindGoogleSym || s.kind == kindAES || s.kind == kindHMAC {
		if len(material) != s.size {
			return errInvalid("Imported key must be %d bytes for this algorithm, got %d.", s.size, len(material))
		}

		return nil
	}

	k, err := parsePrivate(material)
	if err != nil {
		return err
	}

	switch key := k.(type) {
	case *rsa.PrivateKey:
		if s.kind == kindRSA && key.N.BitLen() == s.size {
			return nil
		}
	case *ecdsa.PrivateKey:
		if s.kind == kindEC && key.Curve == s.curve {
			return nil
		}
	case ed25519.PrivateKey:
		if s.kind == kindEd25519 {
			return nil
		}
	}

	return errInvalid("Imported key does not match the algorithm.")
}

func publicKeyDER(material []byte) ([]byte, error) {
	signer, err := signerOf(material)
	if err != nil {
		return nil, err
	}

	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return nil, fmt.Errorf("gcpkms: marshal public key: %w", err)
	}

	return der, nil
}

func pemOf(der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func crc32c(b []byte) int64 {
	return int64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
}

func crcValue(b []byte) *wrapperspb.Int64Value { return wrapperspb.Int64(crc32c(b)) }

// verifyCRC checks an optional client-supplied checksum and reports whether one was supplied.
func verifyCRC(want *wrapperspb.Int64Value, data []byte, field string) (bool, error) {
	if want == nil {
		return false, nil
	}

	if want.GetValue() != crc32c(data) {
		return false, errInvalid("The checksum in field %s did not match the data in the request.", field)
	}

	return true, nil
}
