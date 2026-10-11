package resourcegroups

import (
	"encoding/base64"
	"fmt"
)

const (
	pageSizeMin       = 1
	pageSizeMax       = 50
	pageTokenMaxBytes = 8192
)

// parsePage validates MaxResults (1-50 when set) and decodes the opaque NextToken
// (base64, up to 8192 chars) into the backend cursor.
func parsePage(nextToken string, maxResults int) (string, error) {
	if maxResults != 0 && (maxResults < pageSizeMin || maxResults > pageSizeMax) {
		return "", fmt.Errorf("%w: MaxResults must be between %d and %d", ErrValidation, pageSizeMin, pageSizeMax)
	}

	if nextToken == "" {
		return "", nil
	}

	raw, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil || len(nextToken) > pageTokenMaxBytes || len(raw) == 0 {
		return "", fmt.Errorf("%w: NextToken is not valid", ErrValidation)
	}

	return string(raw), nil
}

func encodePageToken(cursor string) string {
	if cursor == "" {
		return ""
	}

	return base64.StdEncoding.EncodeToString([]byte(cursor))
}
