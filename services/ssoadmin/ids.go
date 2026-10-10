package ssoadmin

import (
	"strings"

	"github.com/google/uuid"
)

// newResourceID returns the 16 lowercase hex characters AWS uses in sso instance, permission set and application ARNs.
func newResourceID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:resourceIDLen]
}
