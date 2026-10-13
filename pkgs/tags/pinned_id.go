package tags

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
)

// PinnedIDKey is the emulator-only create-time tag that pins a generated
// resource ID. It is consumed on create and never stored or listed.
const PinnedIDKey = "gopherstack:id"

// ErrInvalidPinnedID is returned when a pinned ID does not match the
// resource type's ID format.
var ErrInvalidPinnedID = errors.New("invalid " + PinnedIDKey + " tag")

// IsPinnedIDKey reports whether key is the reserved ID-pinning tag.
func IsPinnedIDKey(key string) bool { return key == PinnedIDKey }

// PinnedIDFromMap returns the pinned ID in m and a copy of m without the
// reserved key. m is returned unchanged when the key is absent.
func PinnedIDFromMap(m map[string]string) (string, map[string]string) {
	id, ok := m[PinnedIDKey]
	if !ok {
		return "", m
	}

	rest := make(map[string]string, len(m)-1)

	for k, v := range m {
		if k != PinnedIDKey {
			rest[k] = v
		}
	}

	return id, rest
}

// PinnedIDFromList returns the pinned ID in list and the list without the
// reserved entry; kv extracts the key and value of one element.
func PinnedIDFromList[T any](list []T, kv func(T) (string, string)) (string, []T) {
	var (
		id    string
		found bool
	)

	rest := make([]T, 0, len(list))

	for _, e := range list {
		k, v := kv(e)
		if k == PinnedIDKey {
			id, found = v, true

			continue
		}

		rest = append(rest, e)
	}

	if !found {
		return "", list
	}

	return id, rest
}

// PinnedIDFromTagSpecs returns the pinned ID from the EC2 query-protocol
// TagSpecification.N entries whose ResourceType equals resourceType.
func PinnedIDFromTagSpecs(vals url.Values, resourceType string) string {
	for i := 1; ; i++ {
		rt := vals.Get(fmt.Sprintf("TagSpecification.%d.ResourceType", i))
		if rt == "" {
			return ""
		}

		if rt != resourceType {
			continue
		}

		for j := 1; ; j++ {
			key := vals.Get(fmt.Sprintf("TagSpecification.%d.Tag.%d.Key", i, j))
			if key == "" {
				break
			}

			if key == PinnedIDKey {
				return vals.Get("TagSpecification." + strconv.Itoa(i) + ".Tag." + strconv.Itoa(j) + ".Value")
			}
		}
	}
}

// ValidatePinnedID checks id against re and wraps [ErrInvalidPinnedID] with
// a message naming the expected format.
func ValidatePinnedID(id string, re *regexp.Regexp, format string) error {
	if re.MatchString(id) {
		return nil
	}

	return fmt.Errorf("%w: %q does not match %s", ErrInvalidPinnedID, id, format)
}
