package azurekeyvault

import (
	"bytes"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // RSA-OAEP (SHA-1) is a Key Vault algorithm
	"crypto/sha256"
	_ "crypto/sha512" // registers SHA-384/512 for RSA and ECDSA operations
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/binary"
	"math/big"
	"strings"
)

const (
	ktyRSA = "RSA"
	ktyEC  = "EC"
	ktyOct = "OCT"

	curveP256 = "P-256"

	defaultRSABits = 2048
	defaultOctBits = 256
	gcmNonceSize   = 12
	gcmTagSize     = 16
	kwBlock        = 8
	kwRounds       = 6
	ecSigParts     = 2
	kwPair         = 2 * kwBlock
	kwMinWrapped   = 2 * kwBlock
	kwMinUnwrap    = 3 * kwBlock
	aes128Bytes    = 16
	aes192Bytes    = 24
	aes256Bytes    = 32
	bitsPerByte    = 8
)

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func unb64u(s string) ([]byte, error) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
	if err != nil {
		return nil, errBadParameter("Invalid base64url value.")
	}

	return b, nil
}

// ktyFamily returns RSA, EC or OCT for a Key Vault kty string.
func ktyFamily(kty string) string {
	switch strings.ToUpper(kty) {
	case "RSA", "RSA-HSM":
		return ktyRSA
	case "EC", "EC-HSM":
		return ktyEC
	case "OCT", "OCT-HSM":
		return ktyOct
	}

	return ""
}

func curveFor(crv string) (elliptic.Curve, error) {
	switch crv {
	case curveP256, "":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	}

	return nil, errBadParameter("Unsupported elliptic curve %q.", crv)
}

func curveName(c elliptic.Curve) string { return c.Params().Name }

// generateKeyMaterial creates key material, returning PKCS#8 DER (RSA/EC) or raw bytes (oct)
// along with the resolved size and curve.
func generateKeyMaterial(kty string, size int, crv string) ([]byte, int, string, error) {
	switch ktyFamily(kty) {
	case ktyRSA:
		if size == 0 {
			size = defaultRSABits
		}

		if size != 2048 && size != 3072 && size != 4096 {
			return nil, 0, "", errBadParameter("Unsupported RSA key size %d.", size)
		}

		k, err := rsa.GenerateKey(rand.Reader, size)
		if err != nil {
			return nil, 0, "", err
		}

		der, err := x509.MarshalPKCS8PrivateKey(k)

		return der, size, "", err
	case ktyEC:
		c, err := curveFor(crv)
		if err != nil {
			return nil, 0, "", err
		}

		k, err := ecdsa.GenerateKey(c, rand.Reader)
		if err != nil {
			return nil, 0, "", err
		}

		der, err := x509.MarshalPKCS8PrivateKey(k)

		return der, 0, curveName(c), err
	case ktyOct:
		if size == 0 {
			size = defaultOctBits
		}

		if size != 128 && size != 192 && size != 256 {
			return nil, 0, "", errBadParameter("Unsupported symmetric key size %d.", size)
		}

		k := make([]byte, size/bitsPerByte)
		_, _ = rand.Read(k)

		return k, size, "", nil
	}

	return nil, 0, "", errBadParameter("Unsupported key type %q.", kty)
}

func hashFor(alg string) crypto.Hash {
	switch {
	case strings.HasSuffix(alg, "256"):
		return crypto.SHA256
	case strings.HasSuffix(alg, "384"):
		return crypto.SHA384
	case strings.HasSuffix(alg, "512"):
		return crypto.SHA512
	}

	return 0
}

// rsaEncrypt implements RSA-OAEP, RSA-OAEP-256 and RSA1_5.
func rsaEncrypt(pub *rsa.PublicKey, alg string, msg []byte) ([]byte, error) {
	switch alg {
	case "RSA-OAEP":
		return rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, msg, nil) //nolint:gosec // Key Vault algorithm
	case "RSA-OAEP-256":
		return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, msg, nil)
	case "RSA1_5":
		return rsa.EncryptPKCS1v15(rand.Reader, pub, msg) //nolint:staticcheck // Key Vault algorithm
	}

	return nil, errBadParameter("Algorithm %q is not supported for RSA keys.", alg)
}

func rsaDecrypt(priv *rsa.PrivateKey, alg string, msg []byte) ([]byte, error) {
	var (
		out []byte
		err error
	)

	switch alg {
	case "RSA-OAEP":
		out, err = rsa.DecryptOAEP(sha1.New(), nil, priv, msg, nil) //nolint:gosec // Key Vault algorithm
	case "RSA-OAEP-256":
		out, err = rsa.DecryptOAEP(sha256.New(), nil, priv, msg, nil)
	case "RSA1_5":
		out, err = rsa.DecryptPKCS1v15(nil, priv, msg) //nolint:staticcheck // Key Vault algorithm
	default:
		return nil, errBadParameter("Algorithm %q is not supported for RSA keys.", alg)
	}

	if err != nil {
		return nil, errBadParameter("The ciphertext could not be decrypted with this key.")
	}

	return out, nil
}

// signDigest signs a pre-computed digest with RS*, PS* or ES*.
func signDigest(priv any, alg string, digest []byte) ([]byte, error) {
	h := hashFor(alg)
	if h == 0 {
		return nil, errBadParameter("Unsupported signature algorithm %q.", alg)
	}

	if len(digest) != h.Size() {
		return nil, errBadParameter("The digest length does not match algorithm %q.", alg)
	}

	switch k := priv.(type) {
	case *rsa.PrivateKey:
		switch {
		case strings.HasPrefix(alg, "RS"):
			return rsa.SignPKCS1v15(rand.Reader, k, h, digest)
		case strings.HasPrefix(alg, "PS"):
			return rsa.SignPSS(rand.Reader, k, h, digest, &rsa.PSSOptions{SaltLength: h.Size()})
		}
	case *ecdsa.PrivateKey:
		if strings.HasPrefix(alg, "ES") && esCurve(alg) == curveName(k.Curve) {
			return ecSign(k, digest)
		}
	}

	return nil, errBadParameter("Algorithm %q is not supported for this key.", alg)
}

func esCurve(alg string) string {
	switch alg {
	case "ES256":
		return curveP256
	case "ES384":
		return "P-384"
	case "ES512":
		return "P-521"
	}

	return ""
}

type ecSig struct{ R, S *big.Int }

func ecSign(k *ecdsa.PrivateKey, digest []byte) ([]byte, error) {
	der, err := ecdsa.SignASN1(rand.Reader, k, digest)
	if err != nil {
		return nil, err
	}

	var sig ecSig
	if _, err = asn1.Unmarshal(der, &sig); err != nil {
		return nil, err
	}

	size := (k.Curve.Params().BitSize + bitsPerByte - 1) / bitsPerByte
	out := make([]byte, ecSigParts*size)
	sig.R.FillBytes(out[:size])
	sig.S.FillBytes(out[size:])

	return out, nil
}

// verifyDigest verifies a signature produced by signDigest.
func verifyDigest(pub any, alg string, digest, sig []byte) (bool, error) {
	h := hashFor(alg)
	if h == 0 {
		return false, errBadParameter("Unsupported signature algorithm %q.", alg)
	}

	if len(digest) != h.Size() {
		return false, errBadParameter("The digest length does not match algorithm %q.", alg)
	}

	switch k := pub.(type) {
	case *rsa.PublicKey:
		switch {
		case strings.HasPrefix(alg, "RS"):
			return rsa.VerifyPKCS1v15(k, h, digest, sig) == nil, nil
		case strings.HasPrefix(alg, "PS"):
			return rsa.VerifyPSS(k, h, digest, sig, &rsa.PSSOptions{SaltLength: h.Size()}) == nil, nil
		}
	case *ecdsa.PublicKey:
		if strings.HasPrefix(alg, "ES") && esCurve(alg) == curveName(k.Curve) {
			size := (k.Curve.Params().BitSize + bitsPerByte - 1) / bitsPerByte
			if len(sig) != ecSigParts*size {
				return false, nil
			}

			der, err := asn1.Marshal(ecSig{R: new(big.Int).SetBytes(sig[:size]), S: new(big.Int).SetBytes(sig[size:])})
			if err != nil {
				return false, err
			}

			return ecdsa.VerifyASN1(k, digest, der), nil
		}
	}

	return false, errBadParameter("Algorithm %q is not supported for this key.", alg)
}

// symResult is the output of a symmetric operation.
type symResult struct {
	Value []byte
	IV    []byte
	Tag   []byte
}

func newBlock(key []byte, alg string) (cipher.Block, error) {
	want := map[string]int{"128": aes128Bytes, "192": aes192Bytes, "256": aes256Bytes}
	for suffix, n := range want {
		if strings.HasPrefix(alg, "A"+suffix) && len(key) == n {
			return aes.NewCipher(key)
		}
	}

	return nil, errBadParameter("Algorithm %q does not match the key size.", alg)
}

// symEncrypt implements A*GCM, A*CBCPAD and A*KW.
func symEncrypt(key []byte, alg string, plain, iv, aad []byte) (symResult, error) {
	block, err := newBlock(key, alg)
	if err != nil {
		return symResult{}, err
	}

	switch {
	case strings.HasSuffix(alg, "KW"):
		out, kwErr := aesKeyWrap(block, plain)

		return symResult{Value: out}, kwErr
	case strings.HasSuffix(alg, "GCM"):
		if len(iv) == 0 {
			iv = make([]byte, gcmNonceSize)
			_, _ = rand.Read(iv)
		}

		gcm, gErr := cipher.NewGCMWithNonceSize(block, len(iv))
		if gErr != nil {
			return symResult{}, errBadParameter("Invalid IV for AES-GCM.")
		}

		sealed := gcm.Seal(nil, iv, plain, aad)
		cut := len(sealed) - gcmTagSize

		return symResult{Value: sealed[:cut], IV: iv, Tag: sealed[cut:]}, nil
	case strings.HasSuffix(alg, "CBCPAD"):
		if len(iv) == 0 {
			iv = make([]byte, aes.BlockSize)
			_, _ = rand.Read(iv)
		}

		if len(iv) != aes.BlockSize {
			return symResult{}, errBadParameter("Invalid IV for AES-CBC.")
		}

		pad := aes.BlockSize - len(plain)%aes.BlockSize
		buf := append(append([]byte(nil), plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
		cipher.NewCBCEncrypter(block, iv).CryptBlocks(buf, buf)

		return symResult{Value: buf, IV: iv}, nil
	}

	return symResult{}, errBadParameter("Algorithm %q is not supported for symmetric keys.", alg)
}

func symDecrypt(key []byte, alg string, ct, iv, aad, tag []byte) ([]byte, error) {
	block, err := newBlock(key, alg)
	if err != nil {
		return nil, err
	}

	switch {
	case strings.HasSuffix(alg, "KW"):
		return aesKeyUnwrap(block, ct)
	case strings.HasSuffix(alg, "GCM"):
		gcm, gErr := cipher.NewGCMWithNonceSize(block, len(iv))
		if gErr != nil {
			return nil, errBadParameter("Invalid IV for AES-GCM.")
		}

		out, oErr := gcm.Open(nil, iv, append(append([]byte(nil), ct...), tag...), aad)
		if oErr != nil {
			return nil, errBadParameter("The ciphertext could not be decrypted with this key.")
		}

		return out, nil
	case strings.HasSuffix(alg, "CBCPAD"):
		if len(iv) != aes.BlockSize || len(ct) == 0 || len(ct)%aes.BlockSize != 0 {
			return nil, errBadParameter("Invalid ciphertext or IV for AES-CBC.")
		}

		buf := append([]byte(nil), ct...)
		cipher.NewCBCDecrypter(block, iv).CryptBlocks(buf, buf)

		pad := int(buf[len(buf)-1])
		if pad == 0 || pad > aes.BlockSize || pad > len(buf) {
			return nil, errBadParameter("The ciphertext could not be decrypted with this key.")
		}

		return buf[:len(buf)-pad], nil
	}

	return nil, errBadParameter("Algorithm %q is not supported for symmetric keys.", alg)
}

// kwIV returns the RFC 3394 default initial value.
func kwIV() [kwBlock]byte {
	return [kwBlock]byte{0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6}
}

// aesKeyWrap implements RFC 3394.
func aesKeyWrap(block cipher.Block, plain []byte) ([]byte, error) {
	if len(plain)%kwBlock != 0 || len(plain) < kwMinWrapped {
		return nil, errBadParameter("Key to wrap must be a multiple of 8 bytes and at least 16 bytes.")
	}

	n := len(plain) / kwBlock
	a := kwIV()
	r := make([][]byte, n)

	for i := range r {
		r[i] = append([]byte(nil), plain[i*kwBlock:(i+1)*kwBlock]...)
	}

	buf := make([]byte, kwPair)

	var t uint64

	for range kwRounds {
		for i := range n {
			t++

			copy(buf, a[:])
			copy(buf[kwBlock:], r[i])
			block.Encrypt(buf, buf)
			xorCounter(a[:], buf[:kwBlock], t)
			copy(r[i], buf[kwBlock:])
		}
	}

	out := append([]byte(nil), a[:]...)
	for _, blk := range r {
		out = append(out, blk...)
	}

	return out, nil
}

// xorCounter sets dst = src XOR big-endian(t).
func xorCounter(dst, src []byte, t uint64) {
	var tb [kwBlock]byte

	binary.BigEndian.PutUint64(tb[:], t)

	for k := range kwBlock {
		dst[k] = src[k] ^ tb[k]
	}
}

func aesKeyUnwrap(block cipher.Block, ct []byte) ([]byte, error) {
	if len(ct)%kwBlock != 0 || len(ct) < kwMinUnwrap {
		return nil, errBadParameter("Invalid wrapped key length.")
	}

	n := len(ct)/kwBlock - 1
	a := [kwBlock]byte{}
	copy(a[:], ct[:kwBlock])

	r := make([][]byte, n)
	for i := range r {
		r[i] = append([]byte(nil), ct[(i+1)*kwBlock:][:kwBlock]...)
	}

	buf := make([]byte, kwPair)
	t := uint64(kwRounds) * uint64(n) //nolint:gosec // n is a small positive length

	for range kwRounds {
		for i := n - 1; i >= 0; i-- {
			xorCounter(buf[:kwBlock], a[:], t)
			t--

			copy(buf[kwBlock:], r[i])
			block.Decrypt(buf, buf)
			copy(a[:], buf[:kwBlock])
			copy(r[i], buf[kwBlock:])
		}
	}

	if a != kwIV() {
		return nil, errBadParameter("The wrapped key could not be unwrapped with this key.")
	}

	var out []byte
	for _, blk := range r {
		out = append(out, blk...)
	}

	return out, nil
}
