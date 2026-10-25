package ocivault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

const (
	kwBlock    = 8
	kwPair     = 2 * kwBlock
	kwRounds   = 6
	aesWrapLen = 32
)

//nolint:gochecknoglobals // RFC 5649 alternative initial value prefix
var kwpPrefix = []byte{0xA6, 0x59, 0x59, 0xA6}

var errUnwrap = errors.New("ocivault: key unwrap failed")

func kwpWrap(kek, key []byte) ([]byte, error) {
	blk, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("kwp cipher: %w", err)
	}

	padded := make([]byte, (len(key)+kwBlock-1)/kwBlock*kwBlock)
	copy(padded, key)

	aiv := make([]byte, kwBlock)
	copy(aiv, kwpPrefix)
	keyLen := uint32(len(key)) //nolint:gosec // key length is bounded by the caller
	binary.BigEndian.PutUint32(aiv[len(kwpPrefix):], keyLen)

	if len(padded) == kwBlock {
		out := make([]byte, kwPair)
		blk.Encrypt(out, slices.Concat(aiv, padded))

		return out, nil
	}

	return kwWrap(blk, aiv, padded), nil
}

func kwWrap(blk cipher.Block, a, p []byte) []byte {
	n := len(p) / kwBlock
	r := make([]byte, len(p))
	copy(r, p)

	a = append([]byte(nil), a...)
	buf := make([]byte, kwPair)

	for j := range kwRounds {
		for i := range n {
			copy(buf, a)
			copy(buf[kwBlock:], r[i*kwBlock:(i+1)*kwBlock])
			blk.Encrypt(buf, buf)

			t := uint64(n*j + i + 1)
			copy(a, buf[:kwBlock])
			binary.BigEndian.PutUint64(a, binary.BigEndian.Uint64(a)^t)
			copy(r[i*kwBlock:], buf[kwBlock:])
		}
	}

	return append(a, r...)
}

func kwpUnwrap(kek, wrapped []byte) ([]byte, error) {
	blk, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("kwp cipher: %w", err)
	}

	if len(wrapped) < kwPair || len(wrapped)%kwBlock != 0 {
		return nil, errUnwrap
	}

	var a, p []byte

	if len(wrapped) == kwPair {
		tmp := make([]byte, kwPair)
		blk.Decrypt(tmp, wrapped)
		a, p = tmp[:kwBlock], tmp[kwBlock:]
	} else {
		a, p = kwUnwrap(blk, wrapped)
	}

	return kwpCheck(a, p)
}

func kwUnwrap(blk cipher.Block, c []byte) ([]byte, []byte) {
	n := len(c)/kwBlock - 1
	a := append([]byte(nil), c[:kwBlock]...)
	r := append([]byte(nil), c[kwBlock:]...)
	buf := make([]byte, kwPair)

	for j := kwRounds - 1; j >= 0; j-- {
		for i := n - 1; i >= 0; i-- {
			t := uint64(n*j + i + 1) //nolint:gosec // small positive counter
			binary.BigEndian.PutUint64(buf, binary.BigEndian.Uint64(a)^t)
			copy(buf[kwBlock:], r[i*kwBlock:(i+1)*kwBlock])
			blk.Decrypt(buf, buf)
			copy(a, buf[:kwBlock])
			copy(r[i*kwBlock:], buf[kwBlock:])
		}
	}

	return a, r
}

func kwpCheck(a, p []byte) ([]byte, error) {
	if string(a[:len(kwpPrefix)]) != string(kwpPrefix) {
		return nil, errUnwrap
	}

	mli := int(binary.BigEndian.Uint32(a[len(kwpPrefix):]))
	if mli > len(p) || mli <= len(p)-kwBlock {
		return nil, errUnwrap
	}

	for _, b := range p[mli:] {
		if b != 0 {
			return nil, errUnwrap
		}
	}

	return append([]byte(nil), p[:mli]...), nil
}

// wrapRSAAES implements RSA_OAEP_AES_SHA256: RSA-OAEP(tempAESKey) || AES-KWP(tempAESKey, key).
func wrapRSAAES(pub *rsa.PublicKey, key []byte) ([]byte, error) {
	tmp := make([]byte, aesWrapLen)
	if _, err := rand.Read(tmp); err != nil {
		return nil, fmt.Errorf("temp key: %w", err)
	}

	head, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, tmp, nil)
	if err != nil {
		return nil, fmt.Errorf("wrap temp key: %w", err)
	}

	tail, err := kwpWrap(tmp, key)
	if err != nil {
		return nil, err
	}

	return append(head, tail...), nil
}

func unwrapRSAAES(priv *rsa.PrivateKey, blob []byte) ([]byte, error) {
	size := priv.PublicKey.Size()
	if len(blob) <= size {
		return nil, errUnwrap
	}

	tmp, err := rsa.DecryptOAEP(sha256.New(), nil, priv, blob[:size], nil)
	if err != nil {
		return nil, errUnwrap
	}

	return kwpUnwrap(tmp, blob[size:])
}

func unwrapRSA(priv *rsa.PrivateKey, blob []byte) ([]byte, error) {
	out, err := rsa.DecryptOAEP(sha256.New(), nil, priv, blob, nil)
	if err != nil {
		return nil, errUnwrap
	}

	return out, nil
}

func rsaWrapDirect(pub *rsa.PublicKey, key []byte) ([]byte, error) {
	out, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, key, nil)
	if err != nil {
		return nil, fmt.Errorf("rsa wrap: %w", err)
	}

	return out, nil
}
