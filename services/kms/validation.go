package kms

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var keyIDShapeRe = regexp.MustCompile(
	`^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|mrk-[0-9a-f]{32})$`)

func validateListMarker(action string, body []byte) error {
	if !strings.HasPrefix(action, "List") {
		return nil
	}

	var in struct {
		Marker string `json:"Marker"`
	}

	if json.Unmarshal(body, &in) == nil && in.Marker != "" {
		if n, err := strconv.Atoi(in.Marker); err != nil || n < 0 {
			return fmt.Errorf("%w: invalid marker", ErrInvalidMarker)
		}
	}

	return nil
}

// errorMessage drops the sentinel code prefix from the wire message.
func errorMessage(err error) string {
	msg := err.Error()

	for _, m := range kmsErrorTable() {
		if !errors.Is(err, m.sentinel) {
			continue
		}

		if rest, ok := strings.CutPrefix(msg, m.sentinel.Error()+": "); ok {
			return rest
		}

		break
	}

	return msg
}

// embeddedKeyID reads the key ID prefix of a ciphertext blob, rejecting blobs KMS did not produce.
func embeddedKeyID(blob []byte) (string, error) {
	if len(blob) < keyIDPrefixLen {
		return "", ErrCiphertextTooShort
	}

	keyID := strings.TrimRight(string(blob[:keyIDPrefixLen]), "\x00")
	if !keyIDShapeRe.MatchString(keyID) {
		return "", fmt.Errorf("%w: ciphertext was not produced by KMS", ErrInvalidCiphertext)
	}

	return keyID, nil
}
