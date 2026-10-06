package resiliencehub

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// assessmentPageRequest is a list input keyed by assessmentArn with body-bound paging.
type assessmentPageRequest struct {
	AssessmentArn string `json:"assessmentArn"`
	NextToken     string `json:"nextToken,omitempty"`
	MaxResults    int32  `json:"maxResults,omitempty"`
}

// validatePage rejects a negative maxResults or a malformed nextToken.
func validatePage(nextToken string, maxResults int) error {
	if maxResults < 0 {
		return validationError("maxResults must be positive")
	}

	if page.ValidateToken(nextToken) != nil {
		return validationError("invalid nextToken")
	}

	return nil
}

// pageOf pages an already stably-ordered slice.
func pageOf[T any](items []T, nextToken string, maxResults int32) (page.Page[T], error) {
	if err := validatePage(nextToken, int(maxResults)); err != nil {
		return page.Page[T]{}, err
	}

	return page.New(items, nextToken, int(maxResults), defaultPageLimit), nil
}
