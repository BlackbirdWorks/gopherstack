package organizations

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const (
	// accountIDLength is the expected length of an AWS account ID.
	accountIDLength = 12

	// orgIDLen is the number of random letters in an org ID.
	orgIDLen = 10
	// rootIDLen is the number of random letters in a root ID.
	rootIDLen = 4
	// rootIDPrefixLen is the length of the "r-" prefix to strip when building OU IDs.
	rootIDPrefixLen = 2
	// ouRandomLen is the number of random chars in an OU ID suffix.
	ouRandomLen = 8
	// policyIDLen is the number of random chars in a policy ID.
	policyIDLen = 8
	// handshakeIDLen is the number of random chars in a handshake ID.
	handshakeIDLen = 8

	// govCloudAccountIDOffset is added to the account counter to generate a GovCloud account ID.
	govCloudAccountIDOffset = 1_000_000_000
)

const (
	idChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	hexChars = "0123456789abcdef"
)

func randomChars(n int) string {
	b := make([]byte, n)
	idLen := big.NewInt(int64(len(idChars)))

	for i := range b {
		idx, err := rand.Int(rand.Reader, idLen)
		if err != nil {
			// Fallback: use a fixed character on error (should never happen).
			b[i] = idChars[0]

			continue
		}

		b[i] = idChars[idx.Int64()]
	}

	return string(b)
}

func randomLetters(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	lettersLen := big.NewInt(int64(len(letters)))

	for i := range b {
		idx, err := rand.Int(rand.Reader, lettersLen)
		if err != nil {
			b[i] = letters[0]

			continue
		}

		b[i] = letters[idx.Int64()]
	}

	return string(b)
}

func newOrgID() string  { return "o-" + randomLetters(orgIDLen) }
func newRootID() string { return "r-" + randomLetters(rootIDLen) }
func newOUID(rootID string) string {
	// Strip "r-" prefix for the ou id component.
	base := rootID
	if len(base) > rootIDPrefixLen {
		base = base[rootIDPrefixLen:]
	}

	return "ou-" + base + "-" + randomChars(ouRandomLen)
}

// randomHex returns n random lowercase hex characters (0-9a-f).
func randomHex(n int) string {
	b := make([]byte, n)
	hexLen := big.NewInt(int64(len(hexChars)))

	for i := range b {
		idx, err := rand.Int(rand.Reader, hexLen)
		if err != nil {
			b[i] = hexChars[0]

			continue
		}

		b[i] = hexChars[idx.Int64()]
	}

	return string(b)
}

func newPolicyID() string    { return "p-" + randomHex(policyIDLen) }
func newHandshakeID() string { return "h-" + randomChars(handshakeIDLen) }
func newGovCloudAccountID(counter int) string {
	return fmt.Sprintf("%012d", counter+govCloudAccountIDOffset)
}

// newAccountID generates a 12-digit account ID from an integer counter.
func newAccountID(counter int) string {
	return fmt.Sprintf("%012d", counter)
}
