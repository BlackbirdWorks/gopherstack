package gremlin

import (
	"encoding/hex"
	"strings"

	"github.com/google/uuid"
)

func formatUUID(b []byte) string {
	u, err := uuid.FromBytes(b)
	if err != nil {
		return uuid.Nil.String()
	}

	return u.String()
}

// parseUUID returns the 16 bytes of id; a malformed id becomes the nil uuid.
func parseUUID(id string) []byte {
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil || len(raw) != 16 {
		return make([]byte, 16) //nolint:mnd // uuid bytes
	}

	return raw
}
