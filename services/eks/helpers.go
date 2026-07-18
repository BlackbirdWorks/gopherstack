package eks

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"maps"
	"time"
)

// cloneStrings returns a deep copy of a string slice (nil-safe).
func cloneStrings(ss []string) []string {
	if ss == nil {
		return nil
	}

	cp := make([]string, len(ss))
	copy(cp, ss)

	return cp
}

// cloneStringMap returns a deep copy of a string map (nil-safe).
func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}

// cloneTaints returns a deep copy of a NodegroupTaint slice (nil-safe).
func cloneTaints(ts []NodegroupTaint) []NodegroupTaint {
	if ts == nil {
		return nil
	}

	cp := make([]NodegroupTaint, len(ts))
	copy(cp, ts)

	return cp
}

// stableID returns a deterministic 8-character hex identifier derived from the
// input string using FNV-1a (a non-cryptographic hash). The identifier is
// stable across calls but only 32 bits long, so collisions are possible at
// scale; it should be used only for non-critical IDs such as test ARN suffixes
// and endpoint URL components, not for strong uniqueness or cryptographic
// guarantees.
func stableID(input string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(input))

	return fmt.Sprintf("%08x", h.Sum32())
}

// oidcIDBytes is the number of random bytes needed to produce a 16-char hex OIDC ID.
const oidcIDBytes = 8

// randomHex16 returns a random 16-character lowercase hex string for OIDC IDs.
func randomHex16() string {
	buf := make([]byte, oidcIDBytes)
	if _, err := rand.Read(buf); err != nil {
		// Fallback: use time-based value (safe for non-cryptographic use).
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}
