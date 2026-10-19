package gcpkms

import (
	"bytes"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"

	"cloud.google.com/go/kms/apiv1/kmspb"
)

const (
	kwpRounds  = 6
	kwpHalf    = 8
	aesWrapLen = 32
	kwpBlock   = 2 * kwpHalf
)

func kwpPrefix() []byte { return []byte{0xA6, 0x59, 0x59, 0xA6} }

// kwpUnwrap implements RFC 5649 AES key unwrap with padding.
func kwpUnwrap(kek, wrapped []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil || len(wrapped)%kwpHalf != 0 || len(wrapped) < kwpBlock {
		return nil, errInvalid("The wrapped key is malformed.")
	}

	var a [kwpHalf]byte

	var padded []byte

	if len(wrapped) == kwpBlock {
		buf := make([]byte, kwpBlock)
		block.Decrypt(buf, wrapped)
		copy(a[:], buf[:kwpHalf])
		padded = buf[kwpHalf:]
	} else {
		copy(a[:], wrapped[:kwpHalf])
		padded = kwpUnwrapBlocks(block, &a, wrapped[kwpHalf:])
	}

	mli := int(binary.BigEndian.Uint32(a[4:]))
	if !bytes.Equal(a[:4], kwpPrefix()) || mli > len(padded) || mli <= len(padded)-kwpHalf ||
		!bytes.Equal(padded[mli:], make([]byte, len(padded)-mli)) {
		return nil, errInvalid("The wrapped key failed integrity verification.")
	}

	return padded[:mli], nil
}

func kwpUnwrapBlocks(block cipher.Block, a *[kwpHalf]byte, c []byte) []byte {
	n := len(c) / kwpHalf
	r := bytes.Clone(c)
	buf := make([]byte, kwpBlock)

	for j := kwpRounds - 1; j >= 0; j-- {
		for i := n; i >= 1; i-- {
			t := uint64(n*j + i) //nolint:gosec // small positive
			binary.BigEndian.PutUint64(buf, binary.BigEndian.Uint64(a[:])^t)
			copy(buf[kwpHalf:], r[(i-1)*kwpHalf:i*kwpHalf])
			block.Decrypt(buf, buf)
			copy(a[:], buf[:kwpHalf])
			copy(r[(i-1)*kwpHalf:], buf[kwpHalf:])
		}
	}

	return r
}

// unwrapImport recovers the target key material from an import job's wrapped key.
func unwrapImport(method kmspb.ImportJob_ImportMethod, priv *rsa.PrivateKey, wrapped []byte) ([]byte, error) {
	switch method {
	case kmspb.ImportJob_RSA_OAEP_3072_SHA256, kmspb.ImportJob_RSA_OAEP_4096_SHA256:
		return oaepDecrypt(priv, crypto.SHA256, wrapped)
	case kmspb.ImportJob_RSA_OAEP_3072_SHA256_AES_256, kmspb.ImportJob_RSA_OAEP_4096_SHA256_AES_256:
		return unwrapRSAAES(priv, crypto.SHA256, wrapped)
	case kmspb.ImportJob_RSA_OAEP_3072_SHA1_AES_256, kmspb.ImportJob_RSA_OAEP_4096_SHA1_AES_256:
		return unwrapRSAAES(priv, crypto.SHA1, wrapped)
	default:
		return nil, errInvalid("Import method %s is not supported by this emulator.", method)
	}
}

func unwrapRSAAES(priv *rsa.PrivateKey, h crypto.Hash, wrapped []byte) ([]byte, error) {
	size := priv.PublicKey.Size()
	if len(wrapped) <= size {
		return nil, errInvalid("The wrapped key is too short.")
	}

	kek, err := oaepDecrypt(priv, h, wrapped[:size])
	if err != nil {
		return nil, err
	}

	if len(kek) != aesWrapLen {
		return nil, errInvalid("The ephemeral AES key must be 256 bits.")
	}

	return kwpUnwrap(kek, wrapped[size:])
}

func oaepDecrypt(priv *rsa.PrivateKey, h crypto.Hash, ct []byte) ([]byte, error) {
	pt, err := rsa.DecryptOAEP(h.New(), rand.Reader, priv, ct, nil)
	if err != nil {
		return nil, errInvalid("The wrapped key could not be decrypted with the import job's key.")
	}

	return pt, nil
}
