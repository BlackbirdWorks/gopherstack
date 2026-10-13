package mwaa

import (
	"encoding/base64"
	"strings"
)

const listTokenPrefix = "mwaa:"

func encodeListToken(name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(listTokenPrefix + name))
}

func decodeListToken(token string) (string, bool) {
	if token == "" {
		return "", true
	}

	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || !strings.HasPrefix(string(raw), listTokenPrefix) {
		return "", false
	}

	return strings.TrimPrefix(string(raw), listTokenPrefix), true
}
