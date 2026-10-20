package cognitoidp_test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	encModeCBCSHA1   = "cbc-oaep-sha1"
	encModeGCMSHA256 = "gcm-oaep-sha256"
	encModeRSA15     = "cbc-rsa15"
	nsXEncTest       = "http://www.w3.org/2001/04/xmlenc#"
	nsXEnc11Test     = "http://www.w3.org/2009/xmlenc11#"
)

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func mustRand(n int) []byte {
	out := make([]byte, n)
	if _, err := rand.Read(out); err != nil {
		panic(err)
	}

	return out
}

// encryptAssertionXML wraps plain in an xenc EncryptedAssertion for pub, in the named mode.
func encryptAssertionXML(pub *rsa.PublicKey, plain, mode string) string {
	var (
		dataAlg, keyAlg, keyExtra string
		sym                       []byte
		data                      []byte
		wrapped                   []byte
		err                       error
	)

	switch mode {
	case encModeGCMSHA256:
		sym = mustRand(32)
		block, _ := aes.NewCipher(sym)
		gcm, _ := cipher.NewGCM(block)
		nonce := mustRand(gcm.NonceSize())
		data = slices.Concat(nonce, gcm.Seal(nil, nonce, []byte(plain), nil))
		dataAlg = nsXEnc11Test + "aes256-gcm"
		keyAlg = nsXEnc11Test + "rsa-oaep"
		keyExtra = `<ds:DigestMethod ` +
			`Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"></ds:DigestMethod>` +
			`<xenc11:MGF xmlns:xenc11="` + nsXEnc11Test + `" Algorithm="` + nsXEnc11Test + `mgf1sha256"></xenc11:MGF>`
		wrapped, err = rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, sym, nil)
	default:
		sym = mustRand(16)
		block, _ := aes.NewCipher(sym)
		iv := mustRand(aes.BlockSize)
		pad := aes.BlockSize - len(plain)%aes.BlockSize
		buf := append([]byte(plain), make([]byte, pad)...)
		buf[len(buf)-1] = byte(pad)
		out := make([]byte, len(buf))
		cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, buf)
		data = slices.Concat(iv, out)
		dataAlg = nsXEncTest + "aes128-cbc"
		keyAlg = nsXEncTest + "rsa-oaep-mgf1p"
		keyExtra = `<ds:DigestMethod ` +
			`Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"></ds:DigestMethod>`
		wrapped, err = rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, sym, nil)

		if mode == encModeRSA15 {
			keyAlg = nsXEncTest + "rsa-1_5"
			keyExtra = ""
			wrapped = mustRand(pub.Size())
		}
	}

	if err != nil {
		panic(err)
	}

	return `<saml:EncryptedAssertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">` +
		`<xenc:EncryptedData xmlns:xenc="` + nsXEncTest + `" Type="http://www.w3.org/2001/04/xmlenc#Element">` +
		`<xenc:EncryptionMethod Algorithm="` + dataAlg + `"></xenc:EncryptionMethod>` +
		`<ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#"><xenc:EncryptedKey>` +
		`<xenc:EncryptionMethod Algorithm="` + keyAlg + `">` + keyExtra + `</xenc:EncryptionMethod>` +
		`<xenc:CipherData><xenc:CipherValue>` + b64(wrapped) + `</xenc:CipherValue></xenc:CipherData>` +
		`</xenc:EncryptedKey></ds:KeyInfo>` +
		`<xenc:CipherData><xenc:CipherValue>` + b64(data) + `</xenc:CipherValue></xenc:CipherData>` +
		`</xenc:EncryptedData></saml:EncryptedAssertion>`
}

func TestSAMLEncryptedAssertion(t *testing.T) {
	t.Parallel()

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		opts      func(spPub *rsa.PublicKey) samlOpts
		name      string
		errSubstr string
		wantOK    bool
	}{
		{
			name:   "cbc oaep sha1 signed assertion",
			opts:   func(p *rsa.PublicKey) samlOpts { return samlOpts{encryptTo: p, encMode: encModeCBCSHA1} },
			wantOK: true,
		},
		{
			name:   "gcm oaep sha256 signed assertion",
			opts:   func(p *rsa.PublicKey) samlOpts { return samlOpts{encryptTo: p, encMode: encModeGCMSHA256} },
			wantOK: true,
		},
		{
			name: "signed response covers encrypted assertion",
			opts: func(p *rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: p, encMode: encModeCBCSHA1, sign: "response"}
			},
			wantOK: true,
		},
		{
			name: "unsigned encrypted assertion",
			opts: func(p *rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: p, encMode: encModeCBCSHA1, sign: "none"}
			},
			errSubstr: "not signed",
		},
		{
			name: "rsa 1.5 key transport refused",
			opts: func(p *rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: p, encMode: encModeRSA15}
			},
			errSubstr: "unsupported key transport",
		},
		{
			name: "encrypted to another key",
			opts: func(*rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: &otherKey.PublicKey, encMode: encModeCBCSHA1}
			},
			errSubstr: "key transport decryption failed",
		},
		{
			name: "inner assertion signed by wrong key",
			opts: func(p *rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: p, encMode: encModeCBCSHA1, signKey: otherKey}
			},
			errSubstr: "does not verify",
		},
		{
			name: "wrong audience inside encryption",
			opts: func(p *rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: p, encMode: encModeGCMSHA256, audience: "urn:amazon:cognito:sp:other"}
			},
			errSubstr: "audience",
		},
		{
			name: "plain assertion beside encrypted one",
			opts: func(p *rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: p, encMode: encModeCBCSHA1, mutate: func(s string) string {
					return regexp.MustCompile(`</samlp:Status>`).ReplaceAllString(s,
						`</samlp:Status><saml:Assertion xmlns:saml="`+samlNSAssert+`" ID="_evil"></saml:Assertion>`)
				}}
			},
			errSubstr: "exactly one assertion",
		},
		{
			name: "tampered gcm ciphertext",
			opts: func(p *rsa.PublicKey) samlOpts {
				return samlOpts{encryptTo: p, encMode: encModeGCMSHA256, mutate: tamperLastCipherValue}
			},
			errSubstr: "decryption failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMLEnv(t, false)
			spKey, _, kerr := cognitoidp.PoolSAMLKeyForTest(env.backend, env.poolID)
			require.NoError(t, kerr)

			reqID, relay, _ := env.start(t)
			opts := tt.opts(&spKey.PublicKey)
			doc := env.idp.response(reqID, env.acs(), env.poolID, &opts)
			raw, derr := base64.StdEncoding.DecodeString(doc)
			require.NoError(t, derr)

			verr := cognitoidp.ValidateSAMLResponseWithKeyForTest(
				raw, []byte(env.idp.metadata()), env.poolID, env.acs(), reqID, spKey, time.Now())

			if tt.wantOK {
				require.NoError(t, verr)
				assert.NotEmpty(t, locationQuery(t, env.post(t, relay, doc)).Get("code"))

				return
			}

			require.ErrorContains(t, verr, tt.errSubstr)

			q := locationQuery(t, env.post(t, relay, doc))
			assert.Equal(t, "invalid_request", q.Get("error"))
			assert.Empty(t, q.Get("code"))
		})
	}
}

// tamperLastCipherValue flips a byte in the final CipherValue (the EncryptedData payload).
func tamperLastCipherValue(doc string) string {
	re := regexp.MustCompile(`<xenc:CipherValue>([^<]+)</xenc:CipherValue></xenc:CipherData></xenc:EncryptedData>`)
	m := re.FindStringSubmatchIndex(doc)
	raw, _ := base64.StdEncoding.DecodeString(doc[m[2]:m[3]])
	raw[len(raw)-1] ^= 0xff

	return doc[:m[2]] + b64(raw) + doc[m[3]:]
}

func TestSAMLEncryptedAssertionNeedsPoolKey(t *testing.T) {
	t.Parallel()

	env := newSAMLEnv(t, false)
	spKey, _, err := cognitoidp.PoolSAMLKeyForTest(env.backend, env.poolID)
	require.NoError(t, err)

	reqID, _, _ := env.start(t)
	opts := samlOpts{encryptTo: &spKey.PublicKey, encMode: encModeCBCSHA1}
	raw, derr := base64.StdEncoding.DecodeString(env.idp.response(reqID, env.acs(), env.poolID, &opts))
	require.NoError(t, derr)

	err = cognitoidp.ValidateSAMLResponseForTest(
		raw, []byte(env.idp.metadata()), env.poolID, env.acs(), reqID, time.Now())
	require.ErrorContains(t, err, "encrypted assertions are not supported")
}
