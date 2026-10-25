package ocivault_test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The functions below are an independent test-side implementation of the wire formats
// (AES-KWP per RFC 5649, PKCS#11 style RSA-AES key wrap, and the sealed AES ciphertext layout),
// used to cross-check what the service emits and accepts.

const (
	wrapAlgAES    = "RSA_OAEP_AES_SHA256"
	wrapAlgSHA256 = "RSA_OAEP_SHA256"
	kwBlock       = 8
)

//nolint:gochecknoglobals // RFC 5649 alternative initial value prefix
var kwpPrefix = []byte{0xA6, 0x59, 0x59, 0xA6}

var errUnwrap = errors.New("unwrap failed")

func kwpWrap(kek, key []byte) ([]byte, error) {
	blk, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}

	padded := make([]byte, (len(key)+kwBlock-1)/kwBlock*kwBlock)
	copy(padded, key)

	aiv := make([]byte, kwBlock)
	copy(aiv, kwpPrefix)
	binary.BigEndian.PutUint32(aiv[4:], uint32(len(key)))

	if len(padded) == kwBlock {
		out := make([]byte, 2*kwBlock)
		blk.Encrypt(out, slices.Concat(aiv, padded))

		return out, nil
	}

	n := len(padded) / kwBlock
	a := aiv
	r := append([]byte(nil), padded...)
	buf := make([]byte, 2*kwBlock)

	for j := range 6 {
		for i := range n {
			copy(buf, a)
			copy(buf[kwBlock:], r[i*kwBlock:(i+1)*kwBlock])
			blk.Encrypt(buf, buf)

			a = append([]byte(nil), buf[:kwBlock]...)
			binary.BigEndian.PutUint64(a, binary.BigEndian.Uint64(a)^uint64(n*j+i+1))
			copy(r[i*kwBlock:], buf[kwBlock:])
		}
	}

	return append(a, r...), nil
}

func kwpUnwrap(kek, wrapped []byte) ([]byte, error) {
	blk, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}

	if len(wrapped) < 2*kwBlock || len(wrapped)%kwBlock != 0 {
		return nil, errUnwrap
	}

	var a, p []byte

	if len(wrapped) == 2*kwBlock {
		tmp := make([]byte, 2*kwBlock)
		blk.Decrypt(tmp, wrapped)
		a, p = tmp[:kwBlock], tmp[kwBlock:]
	} else {
		n := len(wrapped)/kwBlock - 1
		a = append([]byte(nil), wrapped[:kwBlock]...)
		p = append([]byte(nil), wrapped[kwBlock:]...)
		buf := make([]byte, 2*kwBlock)

		for j := 5; j >= 0; j-- {
			for i := n - 1; i >= 0; i-- {
				binary.BigEndian.PutUint64(buf, binary.BigEndian.Uint64(a)^uint64(n*j+i+1))
				copy(buf[kwBlock:], p[i*kwBlock:(i+1)*kwBlock])
				blk.Decrypt(buf, buf)
				copy(a, buf[:kwBlock])
				copy(p[i*kwBlock:], buf[kwBlock:])
			}
		}
	}

	mli := int(binary.BigEndian.Uint32(a[4:]))
	if string(a[:4]) != string(kwpPrefix) || mli > len(p) || mli <= len(p)-kwBlock {
		return nil, errUnwrap
	}

	return p[:mli], nil
}

func wrapRSAAES(pub *rsa.PublicKey, key []byte) ([]byte, error) {
	tmp := make([]byte, 32)
	if _, err := rand.Read(tmp); err != nil {
		return nil, err
	}

	head, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, tmp, nil)
	if err != nil {
		return nil, err
	}

	tail, err := kwpWrap(tmp, key)
	if err != nil {
		return nil, err
	}

	return append(head, tail...), nil
}

func unwrapRSAAES(priv *rsa.PrivateKey, blob []byte) ([]byte, error) {
	size := priv.PublicKey.Size()

	tmp, err := rsa.DecryptOAEP(sha256.New(), nil, priv, blob[:size], nil)
	if err != nil {
		return nil, err
	}

	return kwpUnwrap(tmp, blob[size:])
}

// openSealed decrypts the service's AES ciphertext: "OVK1" | len(versionID) | versionID | nonce | ct+tag.
func openSealed(t *testing.T, material []byte, ciphertext string) string {
	t.Helper()

	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	require.NoError(t, err)
	require.Equal(t, "OVK1", string(raw[:4]))

	rest := raw[5+int(raw[4]):]

	blk, err := aes.NewCipher(material)
	require.NoError(t, err)

	gcm, err := cipher.NewGCM(blk)
	require.NoError(t, err)

	plain, err := gcm.Open(nil, rest[:gcm.NonceSize()], rest[gcm.NonceSize():], nil)
	require.NoError(t, err)

	return string(plain)
}

func TestKWPRFC5649Vectors(t *testing.T) {
	t.Parallel()

	kek, _ := hex.DecodeString("5840df6e29b02af1ab493b705bf16ea1ae8338f4dcc176a8")

	tests := []struct {
		name string
		key  string
		want string
	}{
		{"7 byte key", "466f7250617369", "afbeb0f07dfbf5419200f2ccb50bb24f"},
		{
			"20 byte key",
			"c37b7e6492584340bed12207808941155068f738",
			"138bdeaa9b8fa7fc61f97742e72248ee5ae6ae5360d1ae6a5f54f373fa543b6a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, _ := hex.DecodeString(tt.key)
			wrapped, err := kwpWrap(kek, key)
			require.NoError(t, err)
			assert.Equal(t, tt.want, hex.EncodeToString(wrapped))

			back, err := kwpUnwrap(kek, wrapped)
			require.NoError(t, err)
			assert.Equal(t, key, back)
		})
	}
}
