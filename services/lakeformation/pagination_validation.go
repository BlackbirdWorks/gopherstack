package lakeformation

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxListResults = 1000

const (
	opSearchDatabasesByLFTags = "SearchDatabasesByLFTags"
	opSearchTablesByLFTags    = "SearchTablesByLFTags"
)

// validatePagination rejects out-of-range MaxResults and malformed NextToken values up front.
func validatePagination(op string, body []byte) error {
	var in struct {
		MaxResults *int   `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}

	if !json.Valid(body) {
		return nil
	}

	_ = json.Unmarshal(body, &in)

	if in.MaxResults != nil && (*in.MaxResults < 1 || *in.MaxResults > maxListResults) {
		return fmt.Errorf("MaxResults must be between 1 and %d: %w", maxListResults, ErrValidation)
	}

	if in.NextToken == "" {
		return nil
	}

	if op == opSearchDatabasesByLFTags || op == opSearchTablesByLFTags {
		if n, err := strconv.Atoi(in.NextToken); err != nil || n < 0 {
			return fmt.Errorf("NextToken is invalid: %w", ErrValidation)
		}

		return nil
	}

	if err := page.ValidateToken(in.NextToken); err != nil {
		return fmt.Errorf("NextToken is invalid: %w", ErrValidation)
	}

	return nil
}
