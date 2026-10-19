package gcpkms

import (
	"crypto"
	"crypto/elliptic"
	_ "crypto/sha256" // registers SHA-224 and SHA-256 for crypto.Hash.New
	_ "crypto/sha512" // registers SHA-384 and SHA-512 for crypto.Hash.New

	"cloud.google.com/go/kms/apiv1/kmspb"
)

type (
	alg     = kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
	purpose = kmspb.CryptoKey_CryptoKeyPurpose
)

type keyKind int

const (
	kindGoogleSym keyKind = iota + 1
	kindAES
	kindHMAC
	kindRSA
	kindEC
	kindEd25519
)

type mode int

const (
	modeGCM mode = iota + 1
	modeCBC
	modeCTR
	modePSS
	modePKCS1
	modeOAEP
)

const (
	bits2048 = 2048
	bits3072 = 3072
	bits4096 = 4096
	aes128   = 16
	aes256   = 32
)

type algSpec struct {
	curve   elliptic.Curve
	size    int
	purpose purpose
	kind    keyKind
	mode    mode
	hash    crypto.Hash
}

func specOf(a alg) (algSpec, bool) {
	for _, f := range []func(alg) (algSpec, bool){symSpec, rawSpec, rsaSignSpec, rsaDecryptSpec, ecSpec, hmacSpec} {
		if s, ok := f(a); ok {
			return s, true
		}
	}

	return algSpec{}, false
}

func symSpec(a alg) (algSpec, bool) {
	if a == kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION {
		return algSpec{purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT, kind: kindGoogleSym, size: aes256, mode: modeGCM}, true
	}

	return algSpec{}, false
}

func aesSpec(size int, m mode) (algSpec, bool) {
	return algSpec{purpose: kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT, kind: kindAES, size: size, mode: m}, true
}

func rawSpec(a alg) (algSpec, bool) {
	switch a {
	case kmspb.CryptoKeyVersion_AES_128_GCM:
		return aesSpec(aes128, modeGCM)
	case kmspb.CryptoKeyVersion_AES_256_GCM:
		return aesSpec(aes256, modeGCM)
	case kmspb.CryptoKeyVersion_AES_128_CBC:
		return aesSpec(aes128, modeCBC)
	case kmspb.CryptoKeyVersion_AES_256_CBC:
		return aesSpec(aes256, modeCBC)
	case kmspb.CryptoKeyVersion_AES_128_CTR:
		return aesSpec(aes128, modeCTR)
	case kmspb.CryptoKeyVersion_AES_256_CTR:
		return aesSpec(aes256, modeCTR)
	default:
		return algSpec{}, false
	}
}

func rsaSpec(p purpose, bits int, h crypto.Hash, m mode) (algSpec, bool) {
	return algSpec{purpose: p, kind: kindRSA, size: bits, hash: h, mode: m}, true
}

func rsaSignSpec(a alg) (algSpec, bool) {
	sign := kmspb.CryptoKey_ASYMMETRIC_SIGN

	switch a {
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256:
		return rsaSpec(sign, bits2048, crypto.SHA256, modePSS)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_3072_SHA256:
		return rsaSpec(sign, bits3072, crypto.SHA256, modePSS)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA256:
		return rsaSpec(sign, bits4096, crypto.SHA256, modePSS)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA512:
		return rsaSpec(sign, bits4096, crypto.SHA512, modePSS)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256:
		return rsaSpec(sign, bits2048, crypto.SHA256, modePKCS1)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_3072_SHA256:
		return rsaSpec(sign, bits3072, crypto.SHA256, modePKCS1)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_4096_SHA256:
		return rsaSpec(sign, bits4096, crypto.SHA256, modePKCS1)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_4096_SHA512:
		return rsaSpec(sign, bits4096, crypto.SHA512, modePKCS1)
	default:
		return algSpec{}, false
	}
}

func rsaDecryptSpec(a alg) (algSpec, bool) {
	dec := kmspb.CryptoKey_ASYMMETRIC_DECRYPT

	switch a {
	case kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256:
		return rsaSpec(dec, bits2048, crypto.SHA256, modeOAEP)
	case kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_3072_SHA256:
		return rsaSpec(dec, bits3072, crypto.SHA256, modeOAEP)
	case kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA256:
		return rsaSpec(dec, bits4096, crypto.SHA256, modeOAEP)
	case kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA512:
		return rsaSpec(dec, bits4096, crypto.SHA512, modeOAEP)
	default:
		return algSpec{}, false
	}
}

func ecSpec(a alg) (algSpec, bool) {
	sign := kmspb.CryptoKey_ASYMMETRIC_SIGN

	switch a {
	case kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256:
		return algSpec{purpose: sign, kind: kindEC, curve: elliptic.P256(), hash: crypto.SHA256}, true
	case kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384:
		return algSpec{purpose: sign, kind: kindEC, curve: elliptic.P384(), hash: crypto.SHA384}, true
	case kmspb.CryptoKeyVersion_EC_SIGN_ED25519:
		return algSpec{purpose: sign, kind: kindEd25519}, true
	default:
		return algSpec{}, false
	}
}

func hmacSpec(a alg) (algSpec, bool) {
	mac := func(size int, h crypto.Hash) (algSpec, bool) {
		return algSpec{purpose: kmspb.CryptoKey_MAC, kind: kindHMAC, size: size, hash: h}, true
	}

	const (
		size224 = 28
		size256 = 32
		size384 = 48
		size512 = 64
	)

	switch a {
	case kmspb.CryptoKeyVersion_HMAC_SHA224:
		return mac(size224, crypto.SHA224)
	case kmspb.CryptoKeyVersion_HMAC_SHA256:
		return mac(size256, crypto.SHA256)
	case kmspb.CryptoKeyVersion_HMAC_SHA384:
		return mac(size384, crypto.SHA384)
	case kmspb.CryptoKeyVersion_HMAC_SHA512:
		return mac(size512, crypto.SHA512)
	default:
		return algSpec{}, false
	}
}

// defaultAlgorithm returns the algorithm used when a purpose's template leaves it unspecified.
func defaultAlgorithm(p purpose) alg {
	if p == kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION
	}

	return kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED
}

func supportedPurpose(p purpose) bool {
	switch p {
	case kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKey_ASYMMETRIC_DECRYPT,
		kmspb.CryptoKey_MAC, kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT:
		return true
	default:
		return false
	}
}

// specFor validates that a is implemented and belongs to purpose p.
func specFor(p purpose, a alg) (algSpec, error) {
	s, ok := specOf(a)
	if !ok {
		return algSpec{}, errInvalid("Algorithm %s is not supported by this emulator.", a)
	}

	if s.purpose != p {
		return algSpec{}, errInvalid("Algorithm %s is not valid for purpose %s.", a, p)
	}

	return s, nil
}
