package acmpca

import (
	"fmt"
	"strings"
)

// keyFamily maps a types.KeyAlgorithm to the signing family it supports,
// or "" for an unknown value.
func keyFamily(key string) string {
	switch key {
	case "RSA_2048", "RSA_3072", "RSA_4096":
		return "rsa"
	case "EC_prime256v1", "EC_secp384r1", "EC_secp521r1":
		return "ec"
	case "ML_DSA_44", "ML_DSA_65", "ML_DSA_87":
		return key
	case "SM2":
		return "sm2"
	}

	return ""
}

// signingFamily maps a types.SigningAlgorithm to its key family, or "" for
// an unknown value.
func signingFamily(sign string) string {
	switch sign {
	case "SM3WITHSM2":
		return "sm2"
	case "ML_DSA_44", "ML_DSA_65", "ML_DSA_87":
		return sign
	}

	hash, scheme, ok := strings.Cut(sign, "WITH")
	if !ok || (hash != "SHA256" && hash != "SHA384" && hash != "SHA512") {
		return ""
	}

	switch scheme {
	case "RSA", "RSA_PSS":
		return "rsa"
	case "ECDSA":
		return "ec"
	}

	return ""
}

// validateCAAlgorithms checks KeyAlgorithm/SigningAlgorithm against the
// types.KeyAlgorithm/SigningAlgorithm enums and that the signing family fits
// the key. Empty values are defaulted later and skipped here.
func validateCAAlgorithms(keyAlg, signAlg string) error {
	if keyAlg != "" && keyFamily(keyAlg) == "" {
		return fmt.Errorf("%w: unsupported KeyAlgorithm %q", ErrInvalidArgs, keyAlg)
	}

	if signAlg != "" && signingFamily(signAlg) == "" {
		return fmt.Errorf("%w: unsupported SigningAlgorithm %q", ErrInvalidArgs, signAlg)
	}

	if keyAlg != "" && signAlg != "" && keyFamily(keyAlg) != signingFamily(signAlg) {
		return fmt.Errorf("%w: SigningAlgorithm %s is not compatible with KeyAlgorithm %s",
			ErrInvalidArgs, signAlg, keyAlg)
	}

	return nil
}
