// Package awsid mints AWS-shaped resource IDs.
package awsid

import (
	"crypto/rand"
	"encoding/hex"
)

// HexLen is the hex length of modern AWS resource IDs (e.g. i-0123456789abcdef0).
const HexLen = 17

const hexPerByte = 2

// New returns prefix followed by 17 lowercase hex characters.
func New(prefix string) string {
	return NewN(prefix, HexLen)
}

// NewN returns prefix followed by n lowercase hex characters.
func NewN(prefix string, n int) string {
	b := make([]byte, (n+hexPerByte-1)/hexPerByte)

	_, _ = rand.Read(b) // crypto/rand.Read never returns an error on supported platforms

	return prefix + hex.EncodeToString(b)[:n]
}
