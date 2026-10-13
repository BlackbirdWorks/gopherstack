package transfer

import (
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxListResults = 1000

type listPagingInput struct {
	MaxResults *int   `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

// validateListPaging rejects a NextToken this service did not issue and a
// MaxResults outside 1..1000 (transfer ListServers et al.).
func validateListPaging(body []byte) error {
	in, ok := decodeListPaging(body)
	if !ok {
		return nil
	}

	if err := page.ValidateToken(in.NextToken); err != nil {
		return fmt.Errorf("%w: NextToken is not valid", errInvalidNextToken)
	}

	if in.MaxResults != nil && (*in.MaxResults < 1 || *in.MaxResults > maxListResults) {
		return fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, maxListResults)
	}

	return nil
}

func decodeListPaging(body []byte) (listPagingInput, bool) {
	var in listPagingInput

	err := json.Unmarshal(body, &in)

	return in, err == nil
}
