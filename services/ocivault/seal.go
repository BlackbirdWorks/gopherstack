package ocivault

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // RSA_OAEP_SHA_1 is an OCI Vault encryption algorithm
	"crypto/sha256"
	_ "crypto/sha512" // registers SHA-384/512 for crypto.Hash.New
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"hash"
)

const (
	algAESGCM      = "AES_256_GCM"
	algOAEPSHA1    = "RSA_OAEP_SHA_1"
	algOAEPSHA256  = "RSA_OAEP_SHA_256"
	sealMagic      = "OVK1"
	maxPlaintext   = 4096
	maxSignMessage = 4096
)

type signSpec struct {
	hash  crypto.Hash
	pss   bool
	ecdsa bool
}

//nolint:gochecknoglobals // read-only table of the documented OCI signing algorithms
var signSpecs = map[string]signSpec{
	"SHA_224_RSA_PKCS_PSS":   {hash: crypto.SHA224, pss: true},
	"SHA_256_RSA_PKCS_PSS":   {hash: crypto.SHA256, pss: true},
	"SHA_384_RSA_PKCS_PSS":   {hash: crypto.SHA384, pss: true},
	"SHA_512_RSA_PKCS_PSS":   {hash: crypto.SHA512, pss: true},
	"SHA_224_RSA_PKCS1_V1_5": {hash: crypto.SHA224},
	"SHA_256_RSA_PKCS1_V1_5": {hash: crypto.SHA256},
	"SHA_384_RSA_PKCS1_V1_5": {hash: crypto.SHA384},
	"SHA_512_RSA_PKCS1_V1_5": {hash: crypto.SHA512},
	"ECDSA_SHA_256":          {hash: crypto.SHA256, ecdsa: true},
	"ECDSA_SHA_384":          {hash: crypto.SHA384, ecdsa: true},
	"ECDSA_SHA_512":          {hash: crypto.SHA512, ecdsa: true},
}

func aadBytes(aad map[string]string) []byte {
	if len(aad) == 0 {
		return nil
	}

	b, _ := json.Marshal(aad) //nolint:errchkjson // map[string]string always marshals; keys are sorted

	return b
}

func checksum(b []byte) string {
	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:])
}

// aesSeal returns base64(magic | len(versionID) | versionID | nonce | ciphertext+tag).
func aesSeal(versionID string, material, plaintext []byte, aad map[string]string) (string, *APIError) {
	gcm, err := newGCM(material)
	if err != nil {
		return "", errParam("key material is unusable")
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", errParam("unable to generate nonce")
	}

	buf := append([]byte(sealMagic), byte(len(versionID))) //nolint:gosec // version ids are short OCIDs
	buf = append(buf, versionID...)
	buf = append(buf, nonce...)
	buf = gcm.Seal(buf, nonce, plaintext, aadBytes(aad))

	return base64.StdEncoding.EncodeToString(buf), nil
}

func newGCM(material []byte) (cipher.AEAD, error) {
	blk, err := aes.NewCipher(material)
	if err != nil {
		return nil, err //nolint:wrapcheck // internal helper
	}

	return cipher.NewGCM(blk)
}

// sealVersion extracts the key version id embedded in an aesSeal ciphertext.
func sealVersion(ciphertext string) (string, []byte, bool) {
	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil || len(raw) < len(sealMagic)+1 || string(raw[:len(sealMagic)]) != sealMagic {
		return "", nil, false
	}

	n := int(raw[len(sealMagic)])
	rest := raw[len(sealMagic)+1:]

	if len(rest) < n {
		return "", nil, false
	}

	return string(rest[:n]), rest[n:], true
}

func aesOpen(material, body []byte, aad map[string]string) ([]byte, *APIError) {
	gcm, err := newGCM(material)
	if err != nil || len(body) < gcm.NonceSize() {
		return nil, errParam("The ciphertext could not be decrypted")
	}

	out, err := gcm.Open(nil, body[:gcm.NonceSize()], body[gcm.NonceSize():], aadBytes(aad))
	if err != nil {
		return nil, errParam("The ciphertext could not be decrypted with the given key and associatedData")
	}

	return out, nil
}

func oaepHash(alg string) (hash.Hash, bool) {
	switch alg {
	case algOAEPSHA1:
		return sha1.New(), true //nolint:gosec // see import
	case algOAEPSHA256, "":
		return sha256.New(), true
	}

	return nil, false
}

func rsaSeal(material, plaintext []byte, alg string) (string, *APIError) {
	h, ok := oaepHash(alg)
	if !ok {
		return "", errParam("encryptionAlgorithm must be RSA_OAEP_SHA_1 or RSA_OAEP_SHA_256 for RSA keys")
	}

	priv, aerr := parseRSAPrivate(material)
	if aerr != nil {
		return "", aerr
	}

	out, err := rsa.EncryptOAEP(h, rand.Reader, &priv.PublicKey, plaintext, nil)
	if err != nil {
		return "", errParam("plaintext is too large for the RSA key and algorithm")
	}

	return base64.StdEncoding.EncodeToString(out), nil
}

func rsaOpen(material []byte, ciphertext, alg string) ([]byte, *APIError) {
	h, ok := oaepHash(alg)
	if !ok {
		return nil, errParam("encryptionAlgorithm must be RSA_OAEP_SHA_1 or RSA_OAEP_SHA_256 for RSA keys")
	}

	priv, aerr := parseRSAPrivate(material)
	if aerr != nil {
		return nil, aerr
	}

	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return nil, errParam("ciphertext must be base64")
	}

	out, err := rsa.DecryptOAEP(h, nil, priv, raw, nil)
	if err != nil {
		return nil, errParam("The ciphertext could not be decrypted with the given key version")
	}

	return out, nil
}

func digestFor(spec signSpec, msg []byte, isDigest bool) ([]byte, *APIError) {
	if isDigest {
		if len(msg) != spec.hash.Size() {
			return nil, errParam("message digest length does not match the signing algorithm's hash")
		}

		return msg, nil
	}

	if len(msg) > maxSignMessage {
		return nil, errParam("message exceeds 4096 bytes; sign a message digest instead")
	}

	h := spec.hash.New()
	h.Write(msg)

	return h.Sum(nil), nil
}

func specFor(shape KeyShape, alg string) (signSpec, *APIError) {
	spec, ok := signSpecs[alg]
	if !ok {
		return spec, errParam("unsupported signingAlgorithm " + alg)
	}

	switch {
	case shape.Algorithm == algAES:
		return spec, errParam("AES keys cannot sign")
	case shape.Algorithm == algRSA && spec.ecdsa, shape.Algorithm == algECDSA && !spec.ecdsa:
		return spec, errParam("signingAlgorithm " + alg + " is not valid for " + shape.Algorithm + " keys")
	}

	return spec, nil
}

func signDigest(material []byte, shape KeyShape, alg string, msg []byte, isDigest bool) ([]byte, *APIError) {
	spec, aerr := specFor(shape, alg)
	if aerr != nil {
		return nil, aerr
	}

	digest, aerr := digestFor(spec, msg, isDigest)
	if aerr != nil {
		return nil, aerr
	}

	signer, err := parseSigner(material)
	if err != nil {
		return nil, errParam("key material is unusable")
	}

	var sig []byte

	switch k := signer.(type) {
	case *ecdsa.PrivateKey:
		sig, err = ecdsa.SignASN1(rand.Reader, k, digest)
	case *rsa.PrivateKey:
		if spec.pss {
			sig, err = rsa.SignPSS(
				rand.Reader,
				k,
				spec.hash,
				digest,
				&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash},
			)
		} else {
			sig, err = rsa.SignPKCS1v15(rand.Reader, k, spec.hash, digest)
		}
	}

	if err != nil {
		return nil, errParam("unable to sign the message with this key")
	}

	return sig, nil
}

func verifyDigest(material []byte, shape KeyShape, alg string, msg, sig []byte, isDigest bool) (bool, *APIError) {
	spec, aerr := specFor(shape, alg)
	if aerr != nil {
		return false, aerr
	}

	digest, aerr := digestFor(spec, msg, isDigest)
	if aerr != nil {
		return false, aerr
	}

	signer, err := parseSigner(material)
	if err != nil {
		return false, errParam("key material is unusable")
	}

	switch pub := signer.Public().(type) {
	case *ecdsa.PublicKey:
		return ecdsa.VerifyASN1(pub, digest, sig), nil
	case *rsa.PublicKey:
		if spec.pss {
			err = rsa.VerifyPSS(pub, spec.hash, digest, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
		} else {
			err = rsa.VerifyPKCS1v15(pub, spec.hash, digest, sig)
		}

		return err == nil, nil
	}

	return false, errParam("key material is unusable")
}
