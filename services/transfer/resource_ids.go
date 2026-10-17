package transfer

import (
	"strings"

	"github.com/google/uuid"
)

const resourceIDHexLen = 17

// newResourceID returns prefix plus 17 lowercase hex characters, the shape every Transfer Family resource ID has.
func newResourceID(prefix string) string {
	return newHexID(prefix, resourceIDHexLen)
}

// newHexID returns prefix plus n lowercase hex characters (n <= 32).
func newHexID(prefix string, n int) string {
	return prefix + strings.ReplaceAll(uuid.NewString(), "-", "")[:n]
}
