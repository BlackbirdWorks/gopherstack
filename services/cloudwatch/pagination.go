package cloudwatch

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"
)

// tokenSecret is used to HMAC-sign pagination tokens so we can reject spoofed ones.
// In a real system this would be loaded from config; here we use a deterministic value.
var tokenSecret = []byte("gopherstack-cw-token-v1") //nolint:gochecknoglobals // package-level signing key

// signToken signs a raw token string and returns a "sig.payload" format string.
func signToken(raw string) string {
	mac := hmac.New(sha256.New, tokenSecret)
	mac.Write([]byte(raw))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return sig + "." + base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// verifyToken checks the signature on a signed token and returns the original payload.
// Returns ErrInvalidNextToken if the token is malformed or the signature does not match.
func verifyToken(signed string) (string, error) {
	sigB64, payB64, ok := strings.Cut(signed, ".")
	if !ok {
		return "", ErrInvalidNextToken
	}

	payload, err := base64.RawURLEncoding.DecodeString(payB64)
	if err != nil {
		return "", ErrInvalidNextToken
	}

	mac := hmac.New(sha256.New, tokenSecret)
	mac.Write(payload)
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(sigB64), []byte(expected)) {
		return "", ErrInvalidNextToken
	}

	return string(payload), nil
}

// tokenPayloadLen is the byte length of a serialized page token offset.
const tokenPayloadLen = 8

// signPageToken wraps a raw integer-offset pagination token with HMAC signing.
// A value of 0 (start) is returned as empty string so callers can omit it.
func signPageToken(offset int) string {
	if offset == 0 {
		return ""
	}

	buf := make([]byte, tokenPayloadLen)
	binary.BigEndian.PutUint64(buf, uint64(offset)) //nolint:gosec // safe: offset is always non-negative

	return signToken(string(buf))
}

// parseSignedPageToken parses a signed page token and returns the integer offset.
// An empty token means offset 0 (start). Invalid tokens return ErrInvalidNextToken.
func parseSignedPageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	raw, err := verifyToken(token)
	if err != nil {
		return 0, err
	}

	if len(raw) != tokenPayloadLen {
		return 0, ErrInvalidNextToken
	}

	u := binary.BigEndian.Uint64([]byte(raw))
	if u > uint64(^uint(0)>>1) {
		return 0, ErrInvalidNextToken
	}

	return int(u), nil
}
