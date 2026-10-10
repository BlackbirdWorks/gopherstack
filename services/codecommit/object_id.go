package codecommit

import (
	"crypto/rand"
	"encoding/hex"
)

// newObjectID returns a 40-character hex id, the shape of a git object id.
func newObjectID() string {
	var raw [20]byte
	_, _ = rand.Read(raw[:])

	return hex.EncodeToString(raw[:])
}
