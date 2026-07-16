package sts

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// authMsgHMACSize is the byte length of the HMAC-SHA256 prefix in encoded auth messages.
const authMsgHMACSize = sha256.Size

// authMsgSep is the separator byte between the HMAC and the plaintext in encoded auth messages.
const authMsgSep = '|'

// IssueEncodedAuthorizationMessage encodes plaintext as an HMAC-signed opaque blob
// that DecodeAuthorizationMessage can later verify. This mirrors the AWS STS behaviour
// where only messages issued by the service itself can be decoded — arbitrary base64
// blobs are rejected with InvalidAuthorizationMessageException.
//
// Format (base64-encoded): HMAC-SHA256(key, plaintext) | plaintext.
func (b *InMemoryBackend) IssueEncodedAuthorizationMessage(decodedMsg string) string {
	mac := hmac.New(sha256.New, b.authMsgSigningKey[:])
	mac.Write([]byte(decodedMsg))
	sig := mac.Sum(nil)

	payload := make([]byte, 0, authMsgHMACSize+1+len(decodedMsg))
	payload = append(payload, sig...)
	payload = append(payload, authMsgSep)
	payload = append(payload, decodedMsg...)

	return base64.StdEncoding.EncodeToString(payload)
}

// VerifyEncodedAuthorizationMessage decodes an opaque message issued by
// IssueEncodedAuthorizationMessage. Returns ErrInvalidAuthorizationMessage
// when the message was not issued by this backend instance (wrong HMAC, bad
// base64, or truncated payload).
func (b *InMemoryBackend) VerifyEncodedAuthorizationMessage(encoded string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		raw, err = base64.URLEncoding.DecodeString(encoded)
		if err != nil {
			return "", fmt.Errorf("%w: not valid base64", ErrInvalidAuthorizationMessage)
		}
	}

	// Minimum: HMAC (32 bytes) + separator (1 byte) + at least 0 bytes of plaintext.
	if len(raw) < authMsgHMACSize+1 || raw[authMsgHMACSize] != authMsgSep {
		return "", fmt.Errorf(
			"%w: message was not issued by this service",
			ErrInvalidAuthorizationMessage,
		)
	}

	sig := raw[:authMsgHMACSize]
	plaintext := raw[authMsgHMACSize+1:]

	mac := hmac.New(sha256.New, b.authMsgSigningKey[:])
	mac.Write(plaintext)
	expected := mac.Sum(nil)

	if !hmac.Equal(sig, expected) {
		return "", fmt.Errorf(
			"%w: message was not issued by this service",
			ErrInvalidAuthorizationMessage,
		)
	}

	return string(plaintext), nil
}
