package arczonalshift

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func invalidReason(reason, format string, args ...any) *restroute.Error {
	return restroute.Validation(format, args...).With("reason", reason)
}

func conflictReason(reason, shiftID, format string, args ...any) *restroute.Error {
	e := restroute.Conflict(format, args...).With("reason", reason)
	if shiftID != "" {
		e = e.With("zonalShiftId", shiftID)
	}

	return e
}

func paginate[T any](all []T, token string, maxResults int) (page.Page[T], error) {
	if maxResults < 0 || maxResults > listMax {
		return page.Page[T]{}, invalidReason("Other", "maxResults must be between 1 and %d", listMax)
	}

	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, invalidReason("InvalidToken", "nextToken is not valid")
	}

	return page.New(all, token, maxResults, listDefault), nil
}
