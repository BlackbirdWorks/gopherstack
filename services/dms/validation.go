package dms

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxIdentifierLen    = 63
	minAllocatedStorage = 5
	maxAllocatedStorage = 6144
)

var identifierRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9-]*$`)

func validateIdentifier(field, id string) error {
	if len(id) > maxIdentifierLen || !identifierRe.MatchString(id) ||
		strings.HasSuffix(id, "-") || strings.Contains(id, "--") {
		return fmt.Errorf("%w: %s %q must be 1-%d alphanumeric characters or hyphens, begin with a letter, "+
			"not end with a hyphen and not contain two consecutive hyphens", ErrValidation, field, id, maxIdentifierLen)
	}

	return nil
}

func validateReplicationInstanceInput(identifier, class string, allocatedStorage int32) error {
	if err := validateIdentifier("ReplicationInstanceIdentifier", identifier); err != nil {
		return err
	}

	if !strings.HasPrefix(class, "dms.") {
		return fmt.Errorf("%w: ReplicationInstanceClass %q must start with \"dms.\"", ErrValidation, class)
	}

	if allocatedStorage != 0 && (allocatedStorage < minAllocatedStorage || allocatedStorage > maxAllocatedStorage) {
		return fmt.Errorf("%w: AllocatedStorage must be between %d and %d",
			ErrValidation, minAllocatedStorage, maxAllocatedStorage)
	}

	return nil
}

func validateTableMappings(mappings string) error {
	if mappings != "" && !json.Valid([]byte(mappings)) {
		return fmt.Errorf("%w: TableMappings must be valid JSON", ErrValidation)
	}

	return nil
}

// checkMarker rejects a non-empty Marker or NextToken this backend did not issue.
func (h *Handler) checkMarker(body []byte) error {
	var in struct {
		Marker    *string `json:"Marker"`
		NextToken *string `json:"NextToken"`
	}

	if json.Unmarshal(body, &in) != nil {
		return nil //nolint:nilerr // malformed bodies are rejected by the op decoder
	}

	for _, tok := range []*string{in.Marker, in.NextToken} {
		if tok == nil || *tok == "" {
			continue
		}

		if page.ValidateToken(*tok) != nil && page.DecodeHMACToken(*tok, h.Backend.PaginationSecret()) <= 0 {
			return fmt.Errorf("%w: Invalid marker", ErrValidation)
		}
	}

	return nil
}
