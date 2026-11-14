package esdk

import (
	"bytes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha512"
	"encoding/binary"
	"errors"
)

const (
	u16Len     = 2
	u32Len     = 4
	suiteIDLen = 2
)

// ErrMalformed is returned for a message that does not parse or authenticate as a v2 committed message.
var ErrMalformed = errors.New("esdk: malformed message")

// Message is a decrypted ESDK message.
type Message struct {
	EncryptionContext map[string]string
	ProviderID        string
	ProviderInfo      string
	EncryptedDataKey  []byte
	Plaintext         []byte
}

type reader struct {
	err  error
	data []byte
}

func (r *reader) take(n int) []byte {
	if r.err != nil || n < 0 || len(r.data) < n {
		r.err = ErrMalformed

		return make([]byte, max(n, 0))
	}

	out := r.data[:n]
	r.data = r.data[n:]

	return out
}

func (r *reader) u16() int { return int(binary.BigEndian.Uint16(r.take(u16Len))) }

func (r *reader) u32() uint32 { return binary.BigEndian.Uint32(r.take(u32Len)) }

// Decrypt parses and authenticates a v2 committed message following the ESDK data-format spec.
// unwrap stands in for the keyring: it returns the plaintext data key for the encrypted data key.
func Decrypt(msg []byte, unwrap func(edk []byte, encCtx map[string]string) ([]byte, error)) (*Message, error) {
	r := &reader{data: msg}
	out := &Message{EncryptionContext: map[string]string{}}

	version := r.take(1)[0]
	suite := r.take(suiteIDLen)
	msgID := r.take(esdkMessageIDLen)
	aad := &reader{data: r.take(r.u16())}

	if len(aad.data) > 0 {
		for range aad.u16() {
			k := string(aad.take(aad.u16()))
			out.EncryptionContext[k] = string(aad.take(aad.u16()))
		}
	}

	if r.u16() != 1 {
		return nil, ErrMalformed
	}

	out.ProviderID = string(r.take(r.u16()))
	out.ProviderInfo = string(r.take(r.u16()))
	out.EncryptedDataKey = r.take(r.u16())
	contentType := r.take(1)[0]
	frameLen := int(r.u32())
	commit := r.take(esdkDataKeyLen)
	headerLen := len(msg) - len(r.data)
	tag := r.take(esdkGCMTagLen)

	if r.err != nil || version != esdkVersion2 || contentType != esdkContentFramed ||
		!bytes.Equal(suite, []byte{0x04, 0x78}) {
		return nil, ErrMalformed
	}

	dataKey, err := unwrap(out.EncryptedDataKey, out.EncryptionContext)
	if err != nil {
		return nil, err
	}

	dek, err := hkdf.Key(sha512.New, dataKey, msgID, string(suite)+esdkDeriveKeyLabel, esdkDataKeyLen)
	if err != nil {
		return nil, ErrMalformed
	}

	wantCommit, err := hkdf.Key(sha512.New, dataKey, msgID, esdkCommitKeyLabel, esdkDataKeyLen)
	if err != nil || !bytes.Equal(commit, wantCommit) {
		return nil, ErrMalformed
	}

	aead, err := esdkGCM(dek)
	if err != nil {
		return nil, err
	}

	if _, err = aead.Open(nil, make([]byte, esdkIVLen), tag, msg[:headerLen]); err != nil {
		return nil, ErrMalformed
	}

	if out.Plaintext, err = openBody(r, aead, msgID, frameLen); err != nil {
		return nil, err
	}

	return out, nil
}

func openBody(r *reader, aead cipher.AEAD, msgID []byte, frameLen int) ([]byte, error) {
	var plaintext []byte

	for {
		seq := r.u32()
		final := seq == esdkFinalFrameMark

		if final {
			seq = r.u32()
		}

		iv := r.take(esdkIVLen)
		n := frameLen

		if final {
			n = int(r.u32())
		}

		sealed := r.take(n + esdkGCMTagLen)
		label := esdkFrameAADLabel

		if final {
			label = esdkFinalAADLabel
		}

		aad := append(append([]byte{}, msgID...), label...)
		aad = binary.BigEndian.AppendUint32(aad, seq)
		aad = binary.BigEndian.AppendUint64(aad, uint64(n)) //nolint:gosec // n is non-negative

		if r.err != nil {
			return nil, ErrMalformed
		}

		pt, err := aead.Open(nil, iv, sealed, aad)
		if err != nil {
			return nil, ErrMalformed
		}

		plaintext = append(plaintext, pt...)

		if final {
			break
		}
	}

	if len(r.data) != 0 {
		return nil, ErrMalformed
	}

	return plaintext, nil
}
