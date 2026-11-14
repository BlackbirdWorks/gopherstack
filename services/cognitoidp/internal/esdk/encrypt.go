// Package esdk implements the AWS Encryption SDK message format for KMS-wrapped data keys.
package esdk

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
)

// AWS Encryption SDK message format v2 with the committing, non-signing suite
// ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY (0x0478), per the ESDK data-format spec
// (message-header, message-body, algorithm-suites). Which suite real Cognito uses is undocumented.
const (
	esdkVersion2       = 0x02
	esdkSuiteCommitKey = 0x0478
	esdkContentFramed  = 0x02
	esdkMessageIDLen   = 32
	esdkDataKeyLen     = 32
	esdkGCMTagLen      = 16
	esdkIVLen          = 12
	esdkFrameLen       = 4096
	esdkFinalFrameMark = 0xFFFFFFFF
	esdkKMSProviderID  = "aws-kms"
	esdkDeriveKeyLabel = "DERIVEKEY"
	esdkCommitKeyLabel = "COMMITKEY"
	esdkFrameAADLabel  = "AWSKMSEncryptionClient Frame"
	esdkFinalAADLabel  = "AWSKMSEncryptionClient Final Frame"
	esdkMaxEncCtxPairs = 0xFFFF
)

// ErrInvalidDataKey is returned when the data key is not an AES-256 key.
var ErrInvalidDataKey = errors.New("esdk: data key must be 32 bytes")

// DataKey is a KMS-generated data key: Plaintext encrypts locally, Ciphertext goes in the header,
// and KeyARN is the provider info naming the KMS key.
type DataKey struct {
	KeyARN     string
	Plaintext  []byte
	Ciphertext []byte
}

func esdkSerializeEncryptionContext(encCtx map[string]string) []byte {
	if len(encCtx) == 0 {
		return nil
	}

	keys := make([]string, 0, len(encCtx))
	for k := range encCtx {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	pairs := min(len(keys), esdkMaxEncCtxPairs)
	out := binary.BigEndian.AppendUint16(nil, uint16(pairs))
	for _, k := range keys {
		out = binary.BigEndian.AppendUint16(out, uint16(len(k))) //nolint:gosec // ESDK caps keys at 16 bits
		out = append(out, k...)
		out = binary.BigEndian.AppendUint16(out, uint16(len(encCtx[k]))) //nolint:gosec // ESDK caps values at 16 bits
		out = append(out, encCtx[k]...)
	}

	return out
}

func esdkAppendField16(dst []byte, field []byte) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(field))) //nolint:gosec // fields are 16-bit length-prefixed

	return append(dst, field...)
}

func esdkGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("esdk cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("esdk gcm: %w", err)
	}

	return aead, nil
}

// Encrypt returns an ESDK message that encrypts plaintext under the data key.
func Encrypt(plaintext []byte, encCtx map[string]string, dk *DataKey) ([]byte, error) {
	if len(dk.Plaintext) != esdkDataKeyLen {
		return nil, ErrInvalidDataKey
	}

	msgID := make([]byte, esdkMessageIDLen)
	if _, err := rand.Read(msgID); err != nil {
		return nil, fmt.Errorf("esdk message id: %w", err)
	}

	suite := binary.BigEndian.AppendUint16(nil, esdkSuiteCommitKey)

	dek, err := hkdf.Key(sha512.New, dk.Plaintext, msgID, string(suite)+esdkDeriveKeyLabel, esdkDataKeyLen)
	if err != nil {
		return nil, fmt.Errorf("esdk derive key: %w", err)
	}

	commit, err := hkdf.Key(sha512.New, dk.Plaintext, msgID, esdkCommitKeyLabel, esdkDataKeyLen)
	if err != nil {
		return nil, fmt.Errorf("esdk commit key: %w", err)
	}

	header := []byte{esdkVersion2}
	header = append(header, suite...)
	header = append(header, msgID...)
	header = esdkAppendField16(header, esdkSerializeEncryptionContext(encCtx))
	header = binary.BigEndian.AppendUint16(header, 1)
	header = esdkAppendField16(header, []byte(esdkKMSProviderID))
	header = esdkAppendField16(header, []byte(dk.KeyARN))
	header = esdkAppendField16(header, dk.Ciphertext)
	header = append(header, esdkContentFramed)
	header = binary.BigEndian.AppendUint32(header, esdkFrameLen)
	header = append(header, commit...)

	aead, err := esdkGCM(dek)
	if err != nil {
		return nil, err
	}

	out := slices.Clone(header)
	out = append(out, aead.Seal(nil, make([]byte, esdkIVLen), nil, header)...)

	return esdkAppendBody(out, aead, msgID, plaintext), nil
}

func esdkAppendBody(out []byte, aead cipher.AEAD, msgID, plaintext []byte) []byte {
	seq := uint32(1)
	rest := plaintext

	for len(rest) > esdkFrameLen {
		out = binary.BigEndian.AppendUint32(out, seq)
		out = esdkSealFrame(out, aead, msgID, esdkFrameAADLabel, seq, rest[:esdkFrameLen], false)
		rest = rest[esdkFrameLen:]
		seq++
	}

	out = binary.BigEndian.AppendUint32(out, esdkFinalFrameMark)
	out = binary.BigEndian.AppendUint32(out, seq)

	return esdkSealFrame(out, aead, msgID, esdkFinalAADLabel, seq, rest, true)
}

func esdkSealFrame(
	out []byte, aead cipher.AEAD, msgID []byte, label string, seq uint32, content []byte, final bool,
) []byte {
	iv := make([]byte, esdkIVLen)
	binary.BigEndian.PutUint32(iv[esdkIVLen-4:], seq)

	aad := slices.Clone(msgID)
	aad = append(aad, label...)
	aad = binary.BigEndian.AppendUint32(aad, seq)
	aad = binary.BigEndian.AppendUint64(aad, uint64(len(content)))

	out = append(out, iv...)
	if final {
		out = binary.BigEndian.AppendUint32(out, uint32(len(content))) //nolint:gosec // content is at most one frame
	}

	return append(out, aead.Seal(nil, iv, content, aad)...)
}
