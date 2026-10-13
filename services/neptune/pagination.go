package neptune

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	markerPrefix   = "offset:"
	minPageRecords = 1
	maxPageRecords = 100
)

func encodeMarker(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(markerPrefix + strconv.Itoa(offset)))
}

func decodeMarker(marker string) (int, bool) {
	if marker == "" {
		return 0, true
	}

	raw, err := base64.RawURLEncoding.DecodeString(marker)
	if err != nil {
		return 0, false
	}

	n, err := strconv.Atoi(strings.TrimPrefix(string(raw), markerPrefix))
	if err != nil || !strings.HasPrefix(string(raw), markerPrefix) || n < 0 {
		return 0, false
	}

	return n, true
}

func validatePagination(vals url.Values) error {
	if raw := vals.Get("MaxRecords"); raw != "" {
		if n, err := strconv.Atoi(raw); err != nil || n < minPageRecords || n > maxPageRecords {
			return fmt.Errorf(
				"%w: MaxRecords must be between %d and %d",
				ErrInvalidParameter,
				minPageRecords,
				maxPageRecords,
			)
		}
	}

	if _, ok := decodeMarker(vals.Get("Marker")); !ok {
		return fmt.Errorf("%w: Marker is not valid", ErrInvalidParameter)
	}

	return nil
}
