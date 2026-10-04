package kafkaconnect

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

const defaultListLimit = 100

// arnMaxParts is the number of ":"-separated ARN segments:
// arn:partition:service:region:account:resource.
const arnMaxParts = 6

// arnServiceFieldIndex is the zero-based index of the service field within
// an ARN split by arnMaxParts.
const arnServiceFieldIndex = 2

// decodeResourceARN percent-decodes a path segment carrying an ARN.
func decodeResourceARN(encoded string) (string, error) {
	return url.PathUnescape(encoded)
}

func newVersion() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

// fakeFileChecksum derives a stable, plausible-looking hex digest for a
// custom plugin's S3 object, since this backend does not read S3 object
// bytes -- see PARITY.md.
func fakeFileChecksum(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])[:32]
}
