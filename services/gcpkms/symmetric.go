package gcpkms

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
)

const (
	maxPlaintext  = 64 << 10
	maxAAD        = 64 << 10
	ciphertextTag = 0x01
	headerLen     = 9
	gcmNonceLen   = 12
	gcmTagLen     = 16
	minTagLen     = 12
	blockLen      = 16
)

func newGCM(key []byte, tagLen int) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("gcpkms: aes cipher: %w", err)
	}

	g, err := cipher.NewGCMWithTagSize(block, tagLen)
	if err != nil {
		return nil, fmt.Errorf("gcpkms: gcm: %w", err)
	}

	return g, nil
}

// sealGoogle encrypts with AES-256-GCM; the ciphertext embeds the version id so Decrypt can route to it.
func sealGoogle(key []byte, id int64, plaintext, aad []byte) ([]byte, error) {
	g, err := newGCM(key, gcmTagLen)
	if err != nil {
		return nil, err
	}

	out := make([]byte, headerLen, headerLen+gcmNonceLen+len(plaintext)+gcmTagLen)
	out[0] = ciphertextTag
	binary.BigEndian.PutUint64(out[1:], uint64(id)) //nolint:gosec // ids are positive

	nonce, err := randomBytes(gcmNonceLen)
	if err != nil {
		return nil, err
	}

	out = append(out, nonce...)

	return g.Seal(out, nonce, plaintext, aad), nil
}

// ciphertextVersion extracts the version id embedded by sealGoogle.
func ciphertextVersion(ct []byte) (int64, bool) {
	if len(ct) < headerLen+gcmNonceLen+gcmTagLen || ct[0] != ciphertextTag {
		return 0, false
	}

	return int64(binary.BigEndian.Uint64(ct[1:headerLen])), true //nolint:gosec // validated by version lookup
}

func openGoogle(key, ct, aad []byte) ([]byte, error) {
	g, err := newGCM(key, gcmTagLen)
	if err != nil {
		return nil, err
	}

	body := ct[headerLen:]

	pt, err := g.Open(nil, body[:gcmNonceLen], body[gcmNonceLen:], aad)
	if err != nil {
		return nil, errInvalid("Decryption failed: the ciphertext is invalid.")
	}

	return pt, nil
}

type sealed struct {
	ct     []byte
	iv     []byte
	tagLen int32
}

// rawSeal encrypts without any KMS framing; GCM output carries its tag at the end.
func rawSeal(s algSpec, key, plaintext, aad, iv []byte) (sealed, error) {
	if s.mode != modeGCM && len(aad) > 0 {
		return sealed{}, errInvalid("additional_authenticated_data is only valid for AES-GCM.")
	}

	if s.mode == modeGCM {
		return gcmSeal(key, plaintext, aad, iv)
	}

	if len(iv) == 0 {
		var err error
		if iv, err = randomBytes(blockLen); err != nil {
			return sealed{}, err
		}
	}

	ct, err := blockCrypt(s.mode, true, key, iv, plaintext)
	if err != nil {
		return sealed{}, err
	}

	return sealed{ct: ct, iv: iv}, nil
}

func gcmSeal(key, plaintext, aad, iv []byte) (sealed, error) {
	g, err := newGCM(key, gcmTagLen)
	if err != nil {
		return sealed{}, err
	}

	if len(iv) == 0 {
		if iv, err = randomBytes(gcmNonceLen); err != nil {
			return sealed{}, err
		}
	}

	if len(iv) != gcmNonceLen {
		return sealed{}, errInvalid("AES-GCM initialization vector must be %d bytes.", gcmNonceLen)
	}

	return sealed{ct: g.Seal(nil, iv, plaintext, aad), iv: iv, tagLen: gcmTagLen}, nil
}

func rawOpen(s algSpec, key, ct, aad, iv []byte, tagLen int) ([]byte, error) {
	if s.mode != modeGCM {
		if len(aad) > 0 {
			return nil, errInvalid("additional_authenticated_data is only valid for AES-GCM.")
		}

		return blockCrypt(s.mode, false, key, iv, ct)
	}

	if tagLen < minTagLen || tagLen > gcmTagLen {
		return nil, errInvalid("tag_length must be between %d and %d for AES-GCM.", minTagLen, gcmTagLen)
	}

	if len(iv) != gcmNonceLen {
		return nil, errInvalid("AES-GCM initialization vector must be %d bytes.", gcmNonceLen)
	}

	g, err := newGCM(key, tagLen)
	if err != nil {
		return nil, err
	}

	pt, err := g.Open(nil, iv, ct, aad)
	if err != nil {
		return nil, errInvalid("Decryption failed: the ciphertext is invalid.")
	}

	return pt, nil
}

func blockCrypt(m mode, encrypt bool, key, iv, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("gcpkms: aes cipher: %w", err)
	}

	if len(iv) != blockLen {
		return nil, errInvalid("Initialization vector must be %d bytes.", blockLen)
	}

	out := make([]byte, len(data))

	if m == modeCTR {
		cipher.NewCTR(block, iv).XORKeyStream(out, data)

		return out, nil
	}

	if len(data)%blockLen != 0 {
		return nil, errInvalid("AES-CBC input must be a multiple of %d bytes.", blockLen)
	}

	if encrypt {
		cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, data)
	} else {
		cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, data)
	}

	return out, nil
}
