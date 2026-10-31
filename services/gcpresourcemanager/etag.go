package gcpresourcemanager

import (
	"crypto/sha256"
	"encoding/base64"
	"slices"
	"strings"
	"time"
)

const etagBytes = 12

// etagOf is a checksum of the fields a client can change, so a stale etag is detectable.
func etagOf(id, parent, name string, updated time.Time, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	var sb strings.Builder

	sb.WriteString(id + "\x00" + parent + "\x00" + name + "\x00" + updated.UTC().Format(time.RFC3339Nano))

	for _, k := range keys {
		sb.WriteString("\x00" + k + "=" + labels[k])
	}

	sum := sha256.Sum256([]byte(sb.String()))

	return base64.RawURLEncoding.EncodeToString(sum[:etagBytes])
}
