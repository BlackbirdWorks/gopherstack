package ocivault

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"strconv"
)

// Key algorithms.
const (
	algAES   = "AES"
	algRSA   = "RSA"
	algECDSA = "ECDSA"
)

const (
	rsaLenToBits = 8
	rsaMinBytes  = 256
)

// KeyShape is the algorithm/length/curve triple of a key.
type KeyShape struct {
	Algorithm string `json:"algorithm"`
	CurveID   string `json:"curveId,omitempty"`
	Length    int    `json:"length"`
}

var errBadMaterial = errors.New("ocivault: unusable key material")

type curveInfo struct {
	curve elliptic.Curve
	id    string
}

//nolint:gochecknoglobals // read-only curve table
var ecdsaCurves = map[int]curveInfo{
	32: {elliptic.P256(), "NIST_P256"},
	48: {elliptic.P384(), "NIST_P384"},
	66: {elliptic.P521(), "NIST_P521"},
}

//nolint:gochecknoglobals // read-only allowed key lengths
var (
	aesLengths = []int{16, 24, 32}
	rsaLengths = []int{256, 384, 512}
)

func validateShape(s KeyShape) *APIError {
	switch s.Algorithm {
	case algAES:
		if !slices.Contains(aesLengths, s.Length) {
			return errParam("AES key length must be 16, 24 or 32 bytes")
		}
	case algRSA:
		if !slices.Contains(rsaLengths, s.Length) {
			return errParam("RSA key length must be 256, 384 or 512 bytes")
		}
	case algECDSA:
		ci, ok := ecdsaCurves[s.Length]
		if !ok {
			return errParam("ECDSA key length must be 32, 48 or 66 bytes")
		}

		if s.CurveID != ci.id {
			return errParam("ECDSA key length " + strconv.Itoa(s.Length) + " requires curveId " + ci.id)
		}
	default:
		return errParam("keyShape.algorithm must be AES, RSA or ECDSA")
	}

	return nil
}

func generateMaterial(s KeyShape) ([]byte, error) {
	switch s.Algorithm {
	case algAES:
		b := make([]byte, s.Length)
		_, err := rand.Read(b)

		return b, err
	case algRSA:
		k, err := rsa.GenerateKey(rand.Reader, s.Length*rsaLenToBits)
		if err != nil {
			return nil, fmt.Errorf("generate rsa key: %w", err)
		}

		return x509.MarshalPKCS8PrivateKey(k)
	default:
		k, err := ecdsa.GenerateKey(ecdsaCurves[s.Length].curve, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate ecdsa key: %w", err)
		}

		return x509.MarshalPKCS8PrivateKey(k)
	}
}

// checkMaterial validates imported material against the declared shape.
func checkMaterial(s KeyShape, mat []byte) *APIError {
	if s.Algorithm == algAES {
		if len(mat) != s.Length {
			return errParam("imported AES key material must be exactly the declared key length")
		}

		return nil
	}

	signer, err := parseSigner(mat)
	if err != nil {
		return errParam("imported key material must be a PKCS#8 DER private key")
	}

	switch k := signer.(type) {
	case *rsa.PrivateKey:
		if s.Algorithm != algRSA || k.PublicKey.Size() != s.Length {
			return errParam("imported key material does not match the declared key shape")
		}
	case *ecdsa.PrivateKey:
		ci, ok := ecdsaCurves[s.Length]
		if s.Algorithm != algECDSA || !ok || k.Curve != ci.curve {
			return errParam("imported key material does not match the declared key shape")
		}
	default:
		return errParam("unsupported imported key type")
	}

	return nil
}

func parseSigner(mat []byte) (crypto.Signer, error) {
	k, err := x509.ParsePKCS8PrivateKey(mat)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errBadMaterial, err)
	}

	s, ok := k.(crypto.Signer)
	if !ok {
		return nil, errBadMaterial
	}

	return s, nil
}

func publicKeyPEM(mat []byte) string {
	signer, err := parseSigner(mat)
	if err != nil {
		return ""
	}

	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return ""
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func parseRSAPublicPEM(s string) (*rsa.PublicKey, error) {
	blk, _ := pem.Decode([]byte(s))
	if blk == nil {
		return nil, errBadMaterial
	}

	pub, err := x509.ParsePKIXPublicKey(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errBadMaterial, err)
	}

	rk, ok := pub.(*rsa.PublicKey)
	if !ok || rk.Size() < rsaMinBytes {
		return nil, errBadMaterial
	}

	return rk, nil
}

func parseRSAPrivate(mat []byte) (*rsa.PrivateKey, *APIError) {
	signer, err := parseSigner(mat)
	if err != nil {
		return nil, errParam("key material is unusable")
	}

	rk, ok := signer.(*rsa.PrivateKey)
	if !ok {
		return nil, errParam("operation requires an RSA key")
	}

	return rk, nil
}
