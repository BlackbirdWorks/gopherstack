package docdb

import (
	"crypto/rand"
	"encoding/base32"
)

const (
	resourceIDLen   = 26
	resourceIDBytes = 17
)

// newResourceID returns a region-unique immutable resource id such as
// "cluster-" followed by 26 uppercase alphanumerics.
func newResourceID(prefix string) string {
	raw := make([]byte, resourceIDBytes)
	_, _ = rand.Read(raw)

	return prefix + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)[:resourceIDLen]
}
