package directconnect

import (
	"crypto/sha256"
	"encoding/hex"
)

const endpointDeviceSuffixLen = 12

// endpointDevice returns the Direct Connect endpoint identity for a resource:
// its stored value, else the location's endpoint ("<location>-<12 hex>").
func endpointDevice(stored, location string) string {
	if stored != "" || location == "" {
		return stored
	}

	sum := sha256.Sum256([]byte(location))

	return location + "-" + hex.EncodeToString(sum[:])[:endpointDeviceSuffixLen]
}
