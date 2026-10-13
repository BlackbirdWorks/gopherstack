package codestarconnections

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxPageResults    = 100
	maxPageTokenChars = 1024
)

// pageArgs validates MaxResults (0-100) and NextToken (opaque, up to 1024 chars).
func pageArgs(nextToken string, maxResults int) (string, int, error) {
	if maxResults < 0 || maxResults > maxPageResults {
		return "", 0, fmt.Errorf("%w: MaxResults must be between 0 and %d", ErrValidation, maxPageResults)
	}

	if nextToken != "" && (len(nextToken) > maxPageTokenChars || page.ValidateToken(nextToken) != nil) {
		return "", 0, fmt.Errorf("%w: NextToken is not valid", ErrValidation)
	}

	return nextToken, maxResults, nil
}
