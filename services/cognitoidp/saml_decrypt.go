package cognitoidp

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rsa"
	_ "crypto/sha1" //nolint:gosec // XML-Enc rsa-oaep-mgf1p defaults to SHA-1; registers crypto.SHA1
	"encoding/base64"
	"strings"
)

const (
	nsXEnc        = "http://www.w3.org/2001/04/xmlenc#"
	nsXEnc11      = "http://www.w3.org/2009/xmlenc11#"
	algRSAOAEP    = nsXEnc + "rsa-oaep-mgf1p"
	algRSAOAEP11  = nsXEnc11 + "rsa-oaep"
	algMGF1Pfx    = nsXEnc11 + "mgf1sha"
	algDigestSHA1 = nsDSig + "sha1"
	cbcIVLen      = aes.BlockSize
	gcmNonceLen   = 12
)

var mgfHashes = map[string]crypto.Hash{ //nolint:gochecknoglobals // static lookup table
	"1": crypto.SHA1, "256": crypto.SHA256, "384": crypto.SHA384, "512": crypto.SHA512,
}

// aesAlgorithms maps the supported XML-Enc block ciphers to their key length and mode.
var aesAlgorithms = map[string]struct { //nolint:gochecknoglobals // static lookup table
	keyLen int
	gcm    bool
}{
	nsXEnc + "aes128-cbc":   {16, false},
	nsXEnc + "aes192-cbc":   {24, false},
	nsXEnc + "aes256-cbc":   {32, false},
	nsXEnc11 + "aes128-gcm": {16, true},
	nsXEnc11 + "aes192-gcm": {24, true},
	nsXEnc11 + "aes256-gcm": {32, true},
}

// decryptAssertion decrypts an EncryptedAssertion with the pool's private key. RSA 1.5 key transport
// is refused (padding-oracle prone); only RSA-OAEP is accepted.
func decryptAssertion(enc *xnode, key *rsa.PrivateKey) (*xnode, error) {
	if key == nil {
		return nil, samlErrorf("encrypted assertions are not supported")
	}

	data := enc.child(nsXEnc, "EncryptedData")
	if data == nil {
		return nil, samlErrorf("EncryptedAssertion has no EncryptedData")
	}

	ek := findEncryptedKey(enc, data)
	if ek == nil {
		return nil, samlErrorf("EncryptedAssertion has no EncryptedKey")
	}

	sym, err := unwrapKey(ek, key)
	if err != nil {
		return nil, err
	}

	plain, err := decryptContent(data, sym)
	if err != nil {
		return nil, err
	}

	a, err := parseXML(plain)
	if err != nil {
		return nil, samlErrorf("decrypted assertion: %v", err)
	}

	if !a.is(nsSAMLAssertion, "Assertion") {
		return nil, samlErrorf("decrypted content is not an assertion")
	}

	return a, nil
}

func findEncryptedKey(enc, data *xnode) *xnode {
	if k := enc.child(nsXEnc, "EncryptedKey"); k != nil {
		return k
	}

	var found *xnode

	data.walk(func(n *xnode) {
		if found == nil && n.is(nsXEnc, "EncryptedKey") {
			found = n
		}
	})

	return found
}

func cipherValue(n *xnode) ([]byte, error) {
	cd := n.child(nsXEnc, "CipherData")
	if cd == nil || cd.child(nsXEnc, "CipherValue") == nil {
		return nil, samlErrorf("encrypted element has no CipherValue")
	}

	out, err := base64.StdEncoding.DecodeString(stripSpace(cd.child(nsXEnc, "CipherValue").textContent()))
	if err != nil || len(out) == 0 {
		return nil, samlErrorf("CipherValue is not base64")
	}

	return out, nil
}

func unwrapKey(ek *xnode, key *rsa.PrivateKey) ([]byte, error) {
	method := ek.child(nsXEnc, "EncryptionMethod")
	if method == nil {
		return nil, samlErrorf("EncryptedKey has no EncryptionMethod")
	}

	h, err := oaepHash(method)
	if err != nil {
		return nil, err
	}

	ct, err := cipherValue(ek)
	if err != nil {
		return nil, err
	}

	label := []byte(nil)
	if p := method.child(nsXEnc, "OAEPparams"); p != nil {
		if label, err = base64.StdEncoding.DecodeString(stripSpace(p.textContent())); err != nil {
			return nil, samlErrorf("OAEPparams is not base64")
		}
	}

	sym, err := rsa.DecryptOAEP(h.New(), nil, key, ct, label)
	if err != nil {
		return nil, samlErrorf("key transport decryption failed")
	}

	return sym, nil
}

// oaepHash returns the OAEP hash; Go uses one hash for both digest and MGF1, so MGF1 must match it.
func oaepHash(method *xnode) (crypto.Hash, error) {
	alg := method.attr("Algorithm")
	if alg != algRSAOAEP && alg != algRSAOAEP11 {
		return 0, samlErrorf("unsupported key transport algorithm %q", alg)
	}

	h := crypto.SHA1

	if dm := method.child(nsDSig, "DigestMethod"); dm != nil && dm.attr("Algorithm") != algDigestSHA1 {
		var ok bool
		if h, ok = hashFor(dm.attr("Algorithm")); !ok {
			return 0, samlErrorf("unsupported OAEP digest %q", dm.attr("Algorithm"))
		}
	}

	if err := checkMGF(method, alg, h); err != nil {
		return 0, err
	}

	return h, nil
}

func checkMGF(method *xnode, alg string, h crypto.Hash) error {
	mgf := crypto.SHA1
	if m := method.child(nsXEnc11, "MGF"); m != nil && alg == algRSAOAEP11 {
		suffix, ok := strings.CutPrefix(m.attr("Algorithm"), algMGF1Pfx)
		if !ok {
			return samlErrorf("unsupported MGF %q", m.attr("Algorithm"))
		}

		mgf = mgfHashes[suffix]
	}

	if mgf != h || mgf == 0 {
		return samlErrorf("OAEP digest and MGF1 hash must match")
	}

	return nil
}

func decryptContent(data *xnode, sym []byte) ([]byte, error) {
	method := data.child(nsXEnc, "EncryptionMethod")
	if method == nil {
		return nil, samlErrorf("EncryptedData has no EncryptionMethod")
	}

	spec, ok := aesAlgorithms[method.attr("Algorithm")]
	if !ok || len(sym) != spec.keyLen {
		return nil, samlErrorf("unsupported data encryption algorithm %q", method.attr("Algorithm"))
	}

	ct, err := cipherValue(data)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(sym)
	if err != nil {
		return nil, samlErrorf("data key: %v", err)
	}

	if spec.gcm {
		return openGCM(block, ct)
	}

	return openCBC(block, ct)
}

func openGCM(block cipher.Block, ct []byte) ([]byte, error) {
	g, err := cipher.NewGCM(block)
	if err != nil || len(ct) < gcmNonceLen+g.Overhead() {
		return nil, samlErrorf("malformed AES-GCM data")
	}

	plain, err := g.Open(nil, ct[:gcmNonceLen], ct[gcmNonceLen:], nil)
	if err != nil {
		return nil, samlErrorf("assertion decryption failed")
	}

	return plain, nil
}

// openCBC decrypts XML-Enc AES-CBC (IV prefix, ISO 10126 padding where only the last byte counts).
func openCBC(block cipher.Block, ct []byte) ([]byte, error) {
	if len(ct) < 2*cbcIVLen || len(ct)%cbcIVLen != 0 {
		return nil, samlErrorf("malformed AES-CBC data")
	}

	plain := make([]byte, len(ct)-cbcIVLen)
	cipher.NewCBCDecrypter(block, ct[:cbcIVLen]).CryptBlocks(plain, ct[cbcIVLen:])

	pad := int(plain[len(plain)-1])
	if pad < 1 || pad > cbcIVLen || pad > len(plain) {
		return nil, samlErrorf("assertion decryption failed")
	}

	return plain[:len(plain)-pad], nil
}
